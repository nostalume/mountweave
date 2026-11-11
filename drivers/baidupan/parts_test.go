package baidupan

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"

	core "github.com/nostalume/mountweave"
)

type partTestInput struct {
	data        string
	read        int
	ranges      []int64
	wrong       bool
	readError   error
	failedRange *badClose
}

func (r *partTestInput) Read(p []byte) (int, error) {
	if r.readError != nil {
		return 0, r.readError
	}
	n := copy(p[:min(len(p), 31<<10)], r.data[r.read:])
	r.read += n
	if r.read == len(r.data) {
		return n, io.EOF
	}
	return n, nil
}

func (*partTestInput) SourceVersion() string { return "fixture" }

func (r *partTestInput) OpenRange(_ context.Context, offset, length int64) (io.ReadCloser, error) {
	r.ranges = append(r.ranges, offset)
	data := r.data[offset : offset+length]
	if r.wrong {
		data = strings.Repeat("x", len(data))
	}
	body := io.NopCloser(strings.NewReader(data))
	if r.failedRange != nil {
		r.failedRange.ReadCloser = body
		return r.failedRange, nil
	}
	return body, nil
}

func checkParts(t *testing.T, s *fixture, c *core.Core, options Options) {
	t.Helper()
	const directory, target, task = "/Archive/staged", "/Archive/staged/up%+中", "task%+/42"
	const partSize int64 = 16 << 20
	payload := strings.Repeat("a", int(partSize)) + "end"
	size := int64(len(payload))
	digests, _ := json.Marshal([]string{fmt.Sprintf("%x", md5.Sum([]byte(payload[:partSize]))), fmt.Sprintf("%x", md5.Sum([]byte("end")))})
	manifest := string(digests)
	base := options.Client.Transport
	var calls []string
	var madeDir, lostPart bool
	mode := ""
	options.PartSize, options.MaxBufferedBytes = partSize, partSize
	options.Client = &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		q := r.URL.Query()
		method := q.Get("method")
		if method != "create" && method != "precreate" && method != "locateupload" && method != "upload" {
			return base.RoundTrip(r)
		}
		calls = append(calls, method)
		if r.URL.Scheme != "https" || q.Get("access_token") != firstToken || len(q["access_token"]) != 1 || r.GetBody != nil {
			t.Error("incorrect credential receiver or replayable request", method)
		}
		if r.Body != nil {
			defer r.Body.Close()
		}
		response := httptest.NewRecorder()
		reply := map[string]any{"errno": 0}
		uploadHost := "upload.fixture"
		if lostPart {
			uploadHost = "retry-upload.fixture"
		}
		switch method {
		case "precreate", "create":
			if r.Method != "POST" || r.URL.Host != "pan.baidu.com" || r.URL.Path != "/rest/2.0/xpan/file" || r.ParseForm() != nil {
				t.Error("incorrect xpan control request", method)
			}
			form := r.PostForm
			if form.Get("isdir") == "1" {
				if method != "create" || form.Get("path") != directory || form.Get("rtype") != "0" || form.Has("uploadid") || form.Has("size") || form.Has("block_list") {
					t.Error("wrong directory create contract", form)
				}
				if madeDir {
					reply["errno"] = -8
				} else {
					madeDir = true
					reply["path"], reply["isdir"], reply["fs_id"] = directory, 1, uint64(0)
				}
				break
			}
			if !madeDir || form.Get("path") != target || form.Get("size") != strconv.FormatInt(size, 10) || form.Get("isdir") != "0" || form.Has("rtype") || form.Get("block_list") != manifest {
				t.Error("manifest or target changed between control stages", method)
			}
			if method == "precreate" {
				if form.Get("autoinit") != "1" || form.Has("uploadid") {
					t.Error("precreate did not initiate a fresh task", form)
				}
				if mode == "lost start" {
					return nil, io.ErrUnexpectedEOF
				}
				reply["path"], reply["uploadid"], reply["block_list"] = target, task, []int{1}
			} else {
				if form.Get("uploadid") != task {
					t.Error("commit changed its native task", form)
				}
				if mode == "lost commit" {
					return nil, io.ErrUnexpectedEOF
				}
				s.mu.Lock()
				s.uploaded[target] = payload
				s.mu.Unlock()
				reply["path"], reply["isdir"], reply["fs_id"], reply["size"] = target, 0, uint64(0), size
			}
		case "locateupload":
			if r.Method != "GET" || r.URL.Host != "d.pcs.baidu.com" || r.URL.Path != "/rest/2.0/pcs/file" || q.Get("path") != target || q.Get("uploadid") != task || q.Get("appid") != "250528" || q.Get("upload_version") != "2.0" {
				t.Error("part routing did not bind the native task", r.URL.Path)
			}
			reply = map[string]any{"error_code": 0, "servers": []map[string]string{{"server": "http://unsafe.fixture"}, {"server": "https://" + uploadHost + "/"}}}
		case "upload":
			if r.Method != "POST" || r.URL.Host != uploadHost || r.URL.Path != "/rest/2.0/pcs/superfile2" || q.Get("type") != "tmpfile" || q.Get("path") != target || q.Get("uploadid") != task || q.Get("partseq") != "1" {
				t.Error("upload changed its selected part or route", r.URL.Path)
			}
			parts, err := r.MultipartReader()
			if err != nil {
				return nil, err
			}
			part, err := parts.NextPart()
			if err != nil {
				return nil, err
			}
			data, err := io.ReadAll(part)
			if err != nil || part.FormName() != "file" || string(data) != "end" {
				t.Error("wrong tail part", data, err)
			}
			if !lostPart {
				lostPart = true
				return nil, io.ErrUnexpectedEOF
			}
			digest := fmt.Sprintf("%x", md5.Sum(data))
			if mode == "bad part receipt" {
				digest = "wrong"
			}
			reply = map[string]any{"md5": digest}
		}
		_ = json.NewEncoder(response).Encode(reply)
		return response.Result(), nil
	})}
	ctx := context.Background()
	d, err := Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := c.Install("/parts", d)
	if err != nil {
		t.Fatal(err)
	}
	if change, err := c.Mkdir(ctx, "u", core.Path("/parts/staged")); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	if change, err := c.Mkdir(ctx, "u", core.Path("/parts/staged")); core.CodeOf(err) != core.AlreadyExists || change.Effect != core.Unchanged {
		t.Fatal(change, err)
	}
	calls = nil
	tooLarge := singleUploadBytes + 1
	sourceErr := errors.New("source stopped before precreate")
	if change, err := c.Write(ctx, "u", core.Path("/parts/staged/probe"), &partTestInput{readError: sourceErr}, &tooLarge); !errors.Is(err, sourceErr) || change.Effect != core.Unchanged || len(calls) != 0 {
		t.Fatal("stable input dispatch performed native effects before validation", change, err, calls)
	}
	for _, setting := range []struct{ part, budget, max int64 }{
		{0, partBytes, 4 << 30},
		{partSize, partSize, 10 << 30},
		{32 << 20, 32 << 20, 20 << 30},
		{8 << 20, partSize, 0},
		{partSize, partBytes, 0},
	} {
		o := options
		o.PartSize, o.MaxBufferedBytes = setting.part, setting.budget
		driver, err := Open(ctx, o)
		if setting.max == 0 {
			if driver != nil || core.CodeOf(err) != core.InvalidRequest {
				t.Fatal(setting, driver, err)
			}
			continue
		}
		if err != nil {
			t.Fatal(setting, err)
		}
		// Prove dispatch and the one-byte capacity boundary without reading gigabytes.
		if change, err := driver.Create(ctx, core.NativePath("/probe"), &partTestInput{readError: sourceErr}, &setting.max); !errors.Is(err, sourceErr) || change.Effect != core.Unchanged {
			t.Fatal(setting, change, err)
		}
		above := setting.max + 1
		if change, err := driver.Create(ctx, core.NativePath("/probe"), &partTestInput{readError: sourceErr}, &above); core.CodeOf(err) != core.CapacityExceeded || change.Effect != core.Unchanged {
			t.Fatal(setting, change, err)
		}
		if err := driver.Close(); err != nil || len(calls) != 0 {
			t.Fatal(setting, err, calls)
		}
	}
	var failedRange badClose
	for _, attempt := range []struct {
		mode   string
		effect core.Effect
		calls  []string
	}{
		{"", core.Applied, []string{"precreate", "locateupload", "upload", "locateupload", "upload", "create"}},
		{"wrong range", core.Unchanged, []string{"precreate", "locateupload"}},
		{"close range", core.Unchanged, []string{"precreate", "locateupload"}},
		{"bad part receipt", core.Unchanged, []string{"precreate", "locateupload", "upload"}},
		{"lost start", core.Unknown, []string{"precreate"}},
		{"lost commit", core.Unknown, []string{"precreate", "locateupload", "upload", "create"}},
	} {
		mode, calls = attempt.mode, nil
		source := &partTestInput{data: payload, wrong: mode == "wrong range"}
		if mode == "close range" {
			source.failedRange = &failedRange
		}
		change, err := d.createParts(ctx, target, source, size)
		if change.Effect != attempt.effect || (err == nil) != (attempt.effect == core.Applied) || !slices.Equal(calls, attempt.calls) {
			t.Fatal(mode, change, err, calls)
		}
		if mode != "lost start" && !slices.Equal(source.ranges, []int64{partSize}) {
			t.Fatal("preexisting part was reread or a retry reopened the source", source.ranges)
		}
		if mode == "close range" && (!errors.Is(err, io.ErrUnexpectedEOF) || failedRange.calls != 1) {
			t.Fatal("range close failure was lost or the body closed more than once", err, failedRange.calls)
		}
	}
	data, _ := readOpened(t, c, "/parts/staged/up%+中", core.OpenOptions{Range: &core.Range{Start: partSize, End: size - 1}})
	if string(data) != "end" {
		t.Fatal("staged file could not be read back", string(data))
	}
	if err := mount.Retire(ctx); err != nil || failedRange.calls != 1 {
		t.Fatal("retirement retained an input range body", err, failedRange.calls)
	}
}
