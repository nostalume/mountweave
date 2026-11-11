package pan123

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

type partSource struct {
	*strings.Reader
	replay    string
	opened    []*badClose
	failClose bool
}

func (s *partSource) SourceVersion() string { return "test-version" }

func (s *partSource) OpenRange(_ context.Context, offset, length int64) (io.ReadCloser, error) {
	if offset != 0 || length != int64(len(s.replay)) {
		return nil, errors.New("unexpected replay coordinates")
	}
	b := &badClose{ReadCloser: io.NopCloser(strings.NewReader(s.replay))}
	if s.failClose {
		b.remaining = 1
	}
	s.opened = append(s.opened, b)
	return b, nil
}

func TestMultipartWriteConfirmsTheReplayedInput(t *testing.T) {
	const data = "abcdefghi"
	const session = "private-session"
	const signed = "https://slice.example/part%2Fraw?sig=private%2f%2B&z=+"
	var s *fixture
	type requests struct{ create, parts, complete, sign, poll int }
	var calls requests
	var v1 bool
	brokenComplete := make(map[string]*badClose)
	reply := func(data string) *http.Response {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"code":0,"data":` + data + `}`))}
	}
	badReply := func(mode, data string) *http.Response {
		response := reply(data)
		body := &badClose{ReadCloser: response.Body, remaining: 1}
		brokenComplete[mode], response.Body = body, body
		return response
	}
	validPart := func(part []byte) bool {
		start := (calls.parts - 1) * 4
		return start < len(data) && string(part) == data[start:min(start+4, len(data))]
	}
	sliceClient := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		defer r.Body.Close()
		if r.Method != http.MethodPost || r.URL.String() != "https://openapi-upload.123pan.com/upload/v2/file/slice" || r.GetBody != nil ||
			r.Header.Get("Platform") != "open_platform" || r.Header.Get("Authorization") != "" || calls.create != 1 || calls.complete != 0 {
			return nil, errors.New("wrong recipient, authentication, or upload phase")
		}
		calls.parts++
		if err := r.ParseMultipartForm(4096); err != nil {
			return nil, err
		}
		defer r.MultipartForm.RemoveAll()
		file, _, err := r.FormFile("slice")
		if err != nil {
			return nil, err
		}
		part, err := io.ReadAll(file)
		err = errors.Join(err, file.Close())
		if err != nil || !validPart(part) ||
			r.FormValue("preuploadID") != session || r.FormValue("sliceNo") != strconv.Itoa(calls.parts) ||
			r.FormValue("sliceMD5") != fmt.Sprintf("%x", md5.Sum(part)) || len(r.MultipartForm.Value) != 3 {
			return nil, errors.New("wrong part, ordinal, or digest")
		}
		if s.state() == "slice-failure" {
			return nil, errors.New("slice confirmation lost for " + session)
		}
		return reply("null"), nil
	})}
	signedClient := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		calls.parts++
		if r.Method != http.MethodPut || r.GetBody != nil || r.URL.String() != signed ||
			r.Header.Get("Content-Type") != "application/octet-stream" || r.Header.Get("Authorization") != "" ||
			r.Header.Get("Platform") != "" || r.Header.Get("Cookie") != "" || calls.sign != calls.parts {
			return nil, errors.New("incorrect presigned PUT request")
		}
		defer r.Body.Close()
		part, err := io.ReadAll(r.Body)
		if err != nil || !validPart(part) || r.ContentLength != int64(len(part)) {
			return nil, errors.New("incorrect presigned PUT body")
		}
		response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}
		if s.state() == "redirect" {
			response.StatusCode = http.StatusFound
			response.Header.Set("Location", "https://other.example/")
		}
		return response, nil
	})}
	s, driver, storage, mount, options := setup(t, func(o *Options) { o.UploadClient, o.MaxUploadBytes = sliceClient, 4 })
	control := driver.api.Transport
	driver.api.Transport = wire(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet {
			return control.RoundTrip(r)
		}
		defer r.Body.Close()
		if r.Method != http.MethodPost || r.GetBody != nil || r.URL.Scheme != "https" || r.URL.Host != "open-api.123pan.com" ||
			r.Header.Get("Platform") != "open_platform" || r.Header.Get("Content-Type") != "application/json" {
			return nil, errors.New("incorrect upload control request")
		}
		var fields map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
			return nil, err
		}
		mode := s.state()
		prefix := "/upload/v2/file/"
		if v1 {
			prefix = "/upload/v1/file/"
		}
		switch strings.TrimPrefix(r.URL.Path, prefix) {
		case "create":
			calls.create++
			if len(fields) != 4 || string(fields["parentFileID"]) != "2" || string(fields["filename"]) != `"large%中"` ||
				string(fields["size"]) != "9" || mode != "wrong-view" && string(fields["etag"]) != fmt.Sprintf(`"%x"`, md5.Sum([]byte(data))) {
				return nil, errors.New("wrong parent, name, or complete source digest")
			}
			if mode == "reuse" {
				return reply(`{"reuse":true,"fileID":9007199254740993}`), nil
			}
			if v1 {
				return reply(fmt.Sprintf(`{"reuse":false,"fileID":0,"preuploadID":%q,"sliceSize":4}`, session)), nil
			}
			size, origin := 4, "https://openapi-upload.123pan.com"
			switch mode {
			case "part-budget":
				size = 5
			case "http-only":
				origin = "http://openapi-upload.123242.com"
			}
			return reply(fmt.Sprintf(`{"reuse":false,"fileID":0,"preuploadID":%q,"sliceSize":%d,"servers":[%q]}`, session, size, origin)), nil
		case "get_upload_url":
			calls.sign++
			if !v1 || len(fields) != 2 || string(fields["preuploadID"]) != `"private-session"` || string(fields["sliceNo"]) != strconv.Itoa(calls.sign) {
				return nil, errors.New("incorrect signed slice scope")
			}
			target := signed
			if mode == "http-only" {
				target = "http" + strings.TrimPrefix(target, "https")
			}
			return reply(fmt.Sprintf(`{"presignedURL":%q}`, target)), nil
		case "upload_complete", "upload_async_result":
			query := r.URL.Path == prefix+"upload_async_result"
			if query {
				calls.poll++
				if !v1 || calls.complete != 1 {
					return nil, errors.New("unexpected observation of upload result")
				}
			} else {
				calls.complete++
			}
			if len(fields) != 1 || string(fields["preuploadID"]) != `"private-session"` || calls.parts != 3 {
				return nil, errors.New("completion before the complete set of parts")
			}
			if !query {
				switch mode {
				case "lost-complete", "result-missing", "result-pending":
					return nil, errors.New("confirmation lost for " + session)
				case "gateway-complete":
					return &http.Response{StatusCode: http.StatusBadGateway, Body: io.NopCloser(strings.NewReader("gateway lost confirmation for " + session))}, nil
				case "broken-complete":
					return badReply(mode, "null"), nil
				case "async-close":
					return badReply(mode, `{"async":true,"completed":false,"fileID":0}`), nil
				case "refused-complete":
					return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"code":5113}`))}, nil
				}
			} else if mode == "result-missing" {
				return &http.Response{StatusCode: 404, Body: http.NoBody}, nil
			}
			if v1 && !query {
				if mode == "async" {
					return reply(`{"async":true,"completed":false,"fileID":0}`), nil
				}
				return reply(`{"async":false,"completed":true,"fileID":9007199254740993}`), nil
			}
			if v1 && calls.poll == 1 && (mode == "async" || mode == "async-close" || mode == "result-pending") {
				return reply(`{"completed":false,"fileID":0}`), nil
			}
			if !v1 && calls.complete == 1 {
				const pending = `{"completed":false,"fileID":""}`
				switch mode {
				case "cancel-poll":
					s.entered <- struct{}{}
				case "pending-close":
					return badReply(mode, pending), nil
				}
				return reply(pending), nil
			}
			return reply(`{"completed":true,"fileID":9007199254740993}`), nil
		default:
			return nil, errors.New("unexpected control route")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	options.PresignedMultipart = true
	options.ContentClient = signedClient
	options.Client = &http.Client{Transport: driver.api.Transport}
	presigned, err := Open(ctx, options)
	if err != nil {
		if presigned != nil {
			err = errors.Join(err, presigned.Close())
		}
		t.Fatal(err)
	}
	other, err := storage.Install("/v1", presigned)
	if err != nil {
		t.Fatal(errors.Join(err, presigned.Close()))
	}
	t.Cleanup(func() { _ = other.Retire(context.Background()) })
	write := func(input *partSource) (core.Change, error) {
		size := int64(len(data))
		mount := "/remote"
		if v1 {
			mount = "/v1"
		}
		return storage.Write(ctx, "u", core.Path(mount+"/sub/large%中"), input, &size, core.WriteOptions{})
	}
	for _, tc := range []struct {
		v1         bool
		mode, view string
		effect     core.Effect
		code       core.Code
		calls      requests
		ranges     int
	}{
		{false, "normal", data, core.Applied, "", requests{create: 1, parts: 3, complete: 2}, 2},
		{false, "pending-close", data, core.Applied, core.InternalError, requests{create: 1, parts: 3, complete: 2}, 2},
		{false, "reuse", data, core.Applied, "", requests{create: 1}, 0},
		{false, "wrong-view", "bcdefghij", core.Unknown, core.InvalidRequest, requests{create: 1}, 1},
		{false, "range-close", data, core.Unknown, core.InternalError, requests{create: 1}, 1},
		{false, "part-budget", data, core.Unknown, core.CapacityExceeded, requests{create: 1}, 0},
		{false, "http-only", data, core.Unknown, core.InvalidUpstreamResponse, requests{create: 1}, 0},
		{false, "long-input", data + "j", core.Unchanged, core.InvalidRequest, requests{}, 0},
		{false, "slice-failure", data, core.Unknown, core.UpstreamFailure, requests{create: 1, parts: 1}, 2},
		{false, "lost-complete", data, core.Unknown, core.UpstreamFailure, requests{create: 1, parts: 3, complete: 1}, 2},
		{true, "normal", data, core.Applied, "", requests{create: 1, parts: 3, complete: 1, sign: 3}, 2},
		{true, "async", data, core.Applied, "", requests{create: 1, parts: 3, complete: 1, sign: 3, poll: 2}, 2},
		{true, "async-close", data, core.Unknown, core.InternalError, requests{create: 1, parts: 3, complete: 1, sign: 3, poll: 1}, 2},
		{true, "http-only", data, core.Unknown, core.InvalidUpstreamResponse, requests{create: 1, sign: 1}, 2},
		{true, "redirect", data, core.Unknown, core.UpstreamFailure, requests{create: 1, parts: 1, sign: 1}, 2},
		{true, "lost-complete", data, core.Applied, "", requests{create: 1, parts: 3, complete: 1, sign: 3, poll: 1}, 2},
		{true, "gateway-complete", data, core.Applied, "", requests{create: 1, parts: 3, complete: 1, sign: 3, poll: 1}, 2},
		{true, "result-missing", data, core.Unknown, core.UpstreamFailure, requests{create: 1, parts: 3, complete: 1, sign: 3, poll: 1}, 2},
		{true, "result-pending", data, core.Unknown, core.UpstreamFailure, requests{create: 1, parts: 3, complete: 1, sign: 3, poll: 1}, 2},
		{true, "broken-complete", data, core.Applied, core.InternalError, requests{create: 1, parts: 3, complete: 1, sign: 3, poll: 1}, 2},
		{true, "refused-complete", data, core.Unknown, core.UpstreamAccessDenied, requests{create: 1, parts: 3, complete: 1, sign: 3}, 2},
	} {
		s.set(tc.mode)
		v1, calls = tc.v1, requests{}
		source := &partSource{Reader: strings.NewReader(tc.view), replay: data, failClose: tc.mode == "range-close"}
		change, err := write(source)
		if change.Effect != tc.effect || (tc.code == "") != (err == nil) || err != nil && core.CodeOf(err) != tc.code ||
			err != nil && strings.Contains(err.Error(), session) || calls != tc.calls || len(source.opened) != tc.ranges {
			t.Fatalf("%s (presigned=%v): change=%v err=%v calls=%+v ranges=%d", tc.mode, v1, change, err, calls, len(source.opened))
		}
		if tc.mode == "result-missing" && (!strings.Contains(err.Error(), "private HTTP I/O failed") || !strings.Contains(err.Error(), "HTTP 404")) {
			t.Fatalf("missing result lost the completion or query failure: %v", err)
		}
		if body := brokenComplete[tc.mode]; body != nil && (!errors.Is(err, io.ErrUnexpectedEOF) || body.calls != 1) {
			t.Fatalf("%s lost failed completion body cleanup: %v", tc.mode, err)
		}
		for _, b := range source.opened {
			if b.calls != 1 {
				t.Fatalf("%s: range closes=%d", tc.mode, b.calls)
			}
		}
	}
	if err := other.Retire(ctx); err != nil || brokenComplete["broken-complete"].calls != 2 || brokenComplete["async-close"].calls != 2 {
		t.Fatalf("retirement did not retry completion response cleanup: %v", err)
	}
	if got := s.count("/upload/v2/file/domain"); got != 0 {
		t.Fatalf("multipart fetched single-upload routing: %d", got)
	}
	s.set("cancel-poll")
	v1, calls = false, requests{}
	done := make(chan error, 1)
	go func() {
		change, err := write(&partSource{Reader: strings.NewReader(data), replay: data})
		if change.Effect != core.Unknown {
			t.Errorf("retirement lost the unknown file effect: %v", change)
		}
		done <- err
	}()
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal("completion poll did not start")
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if brokenComplete["pending-close"].calls != 2 {
		t.Fatal("retirement did not retry V2 completion response cleanup")
	}
	if err := <-done; !errors.Is(err, context.Canceled) || calls.complete != 1 {
		t.Fatal(err, calls.complete)
	}
}
