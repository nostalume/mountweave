package pan123

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
)

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

func TestExclusiveUploadAndRetirement(t *testing.T) {
	var s *fixture
	var c *core.Core
	var mount *core.Mount
	var sent atomic.Int32
	var bad *badClose
	const success = `{"code":0,"message":"ok","data":{"fileID":9007199254740997,"completed":true}}`
	client := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		defer r.Body.Close()
		sent.Add(1)
		if r.Method != http.MethodPost || r.URL.String() != "https://openapi-upload.123pan.com/upload/v2/file/single/create" ||
			r.Header.Get("Platform") != "open_platform" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" ||
			r.GetBody != nil || r.ContentLength <= 6 {
			return nil, errors.New("unexpected upload request")
		}
		mode := s.state()
		if mode == "wait-upload" {
			s.entered <- struct{}{}
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		if err := r.ParseMultipartForm(4096); err != nil {
			return nil, err
		}
		defer r.MultipartForm.RemoveAll()
		file, header, err := r.FormFile("file")
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(file)
		err = errors.Join(err, file.Close())
		name := r.FormValue("filename")
		parent := "1"
		if name == "new%+中" {
			parent = "2"
		}
		if err != nil || string(data) != "abcdef" || r.FormValue("parentFileID") != parent ||
			r.FormValue("size") != "6" || r.FormValue("etag") != "e80b5017098950fc58aad83c8c14978e" ||
			header.Filename != name || len(r.MultipartForm.Value) != 4 {
			return nil, errors.New("incorrect native upload fields or body")
		}
		response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(success))}
		switch mode {
		case "lost-upload":
			return nil, errors.New("confirmation lost: https://upload.invalid?token=secret")
		case "failed-upload":
			response.Body = io.NopCloser(strings.NewReader(`{"code":5999,"message":"secret","data":null}`))
		case "pending-upload":
			response.Body = io.NopCloser(strings.NewReader(`{"code":0,"data":{"fileID":9007199254740997,"completed":false}}`))
		case "redirect-upload":
			response.StatusCode = http.StatusFound
			response.Header.Set("Location", "https://upload.invalid?token=secret")
		case "close-upload":
			bad = &badClose{ReadCloser: response.Body, remaining: 1}
			response.Body = bad
		}
		return response, nil
	})}
	s, _, c, mount, _ = setup(t, func(o *Options) { o.UploadClient, o.MaxUploadBytes = client, 6 })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	size := int64(6)
	write := func(name string, input io.Reader, length *int64) (core.Change, error) {
		return c.Write(ctx, "u", core.Path("/remote/"+name), input, length, core.WriteOptions{})
	}
	source := &badClose{ReadCloser: io.NopCloser(strings.NewReader("abcdef"))}
	change, err := write("sub/new%+中", source, &size)
	if err != nil || change.Effect != core.Applied || change.Before != core.TargetMissing || source.calls != 0 ||
		sent.Load() != 1 || s.count("/upload/v2/file/domain") != 1 {
		t.Fatal(change, err, source.calls, sent.Load())
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "source"), []byte("abcdef"), 0600); err != nil {
		t.Fatal(err)
	}
	localDriver, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	localMount, err := c.Install("/local", localDriver)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := localMount.Retire(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if change, err := c.Copy(ctx, "u", core.Path("/local/source"), core.Path("/remote/copied"), core.CopyOptions{}); err != nil || change.Effect != core.Applied {
		t.Fatal(change, err)
	}
	noRead := readerFunc(func([]byte) (int, error) { t.Error("rejected input was read"); return 0, io.EOF })
	tooLarge := size + 1
	for _, tc := range []struct {
		input io.Reader
		size  *int64
		code  core.Code
	}{
		{noRead, nil, core.UnsupportedOperation},
		{noRead, &tooLarge, core.CapacityExceeded},
		{strings.NewReader("abc"), &size, core.InvalidRequest},
		{strings.NewReader("abcdefg"), &size, core.InvalidRequest},
		{io.MultiReader(strings.NewReader("abcdef"), readerFunc(func([]byte) (int, error) { return 0, io.ErrClosedPipe })), &size, core.InvalidRequest},
	} {
		if change, err := write("invalid", tc.input, tc.size); change.Effect != core.Unchanged || core.CodeOf(err) != tc.code || sent.Load() != 2 || s.count("/upload/v2/file/domain") != 2 {
			t.Fatal(change, err, sent.Load())
		}
	}
	s.set("unsafe-upload")
	if change, err := write("unsafe", strings.NewReader("abcdef"), &size); change.Effect != core.Unchanged || core.CodeOf(err) != core.InvalidUpstreamResponse || sent.Load() != 2 {
		t.Fatal(change, err, sent.Load())
	}
	for _, mode := range []string{"lost-upload", "failed-upload", "pending-upload", "redirect-upload"} {
		s.set(mode)
		before := sent.Load()
		change, err := write("uncertain", strings.NewReader("abcdef"), &size)
		if change.Effect != core.Unknown || err == nil || strings.Contains(err.Error(), "secret") || sent.Load() != before+1 {
			t.Fatal(mode, change, err, sent.Load())
		}
	}
	s.set("close-upload")
	if change, err := write("committed", strings.NewReader("abcdef"), &size); change.Effect != core.Applied || !errors.Is(err, io.ErrUnexpectedEOF) || bad.calls != 1 {
		t.Fatal(change, err)
	}
	s.set("wait")
	lookup, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		change, err := c.Write(lookup, "u", core.Path("/remote/sub/waiting"), noRead, &size)
		if change.Effect != core.Unchanged {
			t.Errorf("canceled parent lookup changed the target: %v", change)
		}
		done <- err
	}()
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal("parent lookup did not start")
	}
	change, err = write("independent", strings.NewReader("abcdef"), &size)
	stop()
	if lookupErr := <-done; !errors.Is(lookupErr, context.Canceled) {
		t.Fatal(lookupErr)
	}
	if err != nil || change.Effect != core.Applied {
		t.Fatal("parent lookup exhausted independent upload capacity", change, err)
	}
	s.set("wait-upload")
	go func() {
		change, err := write("canceled", strings.NewReader("abcdef"), &size)
		if change.Effect != core.Unknown {
			t.Errorf("cancel changed receipt: %v", change)
		}
		done <- err
	}()
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal("upload did not start")
	}
	before := sent.Load()
	if change, err := write("capacity", noRead, &size); change.Effect != core.Unchanged || core.CodeOf(err) != core.CapacityExceeded || sent.Load() != before {
		t.Fatal(change, err, sent.Load())
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) || bad.calls != 2 {
		t.Fatal(err, bad.calls)
	}
}
