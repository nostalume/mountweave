package googlephotos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

type wire func(*http.Request) (*http.Response, error)

func (f wire) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type retryClose struct {
	io.ReadCloser
	url   string
	calls int
}

func (b *retryClose) Close() error {
	b.calls++
	if b.calls == 1 {
		return fmt.Errorf("closing %s: %w", b.url, io.EOF)
	}
	return b.ReadCloser.Close()
}

func TestAppCreatedAlbumExportThroughCore(t *testing.T) {
	const photoName, videoName = "Photo 版.jpg", "Clip.MOV"
	const photoBase = "https://lh3.googleusercontent.com/media%2Fphoto?sig=private%2b"
	const videoBase = "https://lh3.googleusercontent.com/media/video"
	const delegated = "https://photoslibrary.googleapis.com/delegated?sig=second"
	const token = "page +=/"
	var mode atomic.Value
	mode.Store("")
	var pages, bodies atomic.Int32
	var broken *retryClose
	started := make(chan struct{}, 1)
	reply := func(status int, data string) *http.Response {
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(data)), ContentLength: int64(len(data))}
	}
	control := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "photoslibrary.googleapis.com" || r.Header.Get("Accept") != "application/json" ||
			r.Header.Get("Range") != "" || r.Header.Get("Accept-Encoding") != "identity" {
			return nil, errors.New("credentialed request escaped the Library API")
		}
		r.Header.Set("Authorization", "Bearer sealed-library-token")
		state := mode.Load().(string)
		if r.Method == http.MethodGet && r.URL.Path == "/v1/albums/album" {
			id := "album"
			if state == "wrong-album" {
				id = "other"
			}
			return reply(200, fmt.Sprintf(`{"id":%q,"title":"My app album"}`, id)), nil
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/mediaItems:search" || r.URL.RawQuery != "" || r.Header.Get("Content-Type") != "application/json" {
			return nil, errors.New("wrong native album search route")
		}
		defer r.Body.Close()
		var args struct {
			AlbumID, PageToken string
			PageSize           int
		}
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil || args.AlbumID != "album" || args.PageSize != 100 {
			return nil, errors.New("search lost its album scope or page size")
		}
		pages.Add(1)
		switch args.PageToken {
		case "":
			link := photoBase
			if state == "unsafe" {
				link = "https://account:secret@lh3.googleusercontent.com/media/photo"
			}
			return reply(200, fmt.Sprintf(`{"mediaItems":[{"id":"photo","filename":%q,"mimeType":"image/jpeg","baseUrl":%q}],"nextPageToken":%q}`, photoName, link, token)), nil
		case token:
			next := "last"
			if state == "cycle" {
				next = token
			}
			return reply(200, fmt.Sprintf(`{"nextPageToken":%q}`, next)), nil
		case "last":
			if state == "duplicate" {
				return reply(200, fmt.Sprintf(`{"mediaItems":[{"id":"photo-two","filename":%q,"mimeType":"image/jpeg"}]}`, photoName)), nil
			}
			return reply(200, fmt.Sprintf(`{"mediaItems":[{"id":"video","filename":%q,"mimeType":"video/quicktime","baseUrl":%q,"mediaMetadata":{"video":{"status":"READY"}}},{"id":"pending","filename":"Pending.mp4","mimeType":"video/mp4","baseUrl":%q,"mediaMetadata":{"video":{"status":"PROCESSING"}}}]}`, videoName, videoBase, videoBase)), nil
		}
		return nil, errors.New("unexpected native page token")
	})}
	content := &http.Client{Transport: wire(func(r *http.Request) (*http.Response, error) {
		bodies.Add(1)
		if r.Method != http.MethodGet || r.URL.Scheme != "https" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" ||
			r.Header.Get("Referer") != "" || r.Header.Get("Accept-Encoding") != "identity" {
			return nil, errors.New("media request carried account context")
		}
		switch r.URL.String() {
		case "https://lh3.googleusercontent.com/media%2Fphoto=d?sig=private%2b":
			response := reply(http.StatusFound, "")
			response.Header.Set("Location", delegated)
			if mode.Load() == "failed-close" {
				broken = &retryClose{ReadCloser: response.Body, url: r.URL.String()}
				response.Body = broken
			}
			return response, nil
		case delegated:
			if mode.Load() == "wait" {
				started <- struct{}{}
				<-r.Context().Done()
				return nil, r.Context().Err()
			}
			if r.Header.Get("Range") == "bytes=1-3" {
				response := reply(206, "bcd")
				response.Header.Set("Content-Range", "bytes 1-3/6")
				return response, nil
			}
			return reply(200, "abcdef"), nil
		case videoBase + "=dv":
			return reply(200, "transcoded"), nil
		}
		return nil, errors.New("unexpected representation route")
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o := Options{AlbumID: "album", Client: control, ContentClient: content}
	source, err := Open(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	mode.Store("wrong-album")
	wrong, err := Open(ctx, o)
	if core.CodeOf(err) != core.InvalidUpstreamResponse || wrong == nil {
		t.Fatal("unrelated album was admitted", err)
	}
	if err := wrong.Close(); err != nil {
		t.Fatal(err)
	}
	mode.Store("")
	s, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	mount, err := s.Install("/album", source)
	if err != nil {
		t.Fatal(err)
	}
	q := core.ListRequest{Query: core.Children(core.Path("/album")), Limit: 1, CursorTTL: time.Minute}
	for k, expected := range []string{photoName, "", videoName, "Pending.mp4"} {
		page, err := s.List(ctx, "u", q)
		if err != nil || (k < 3) != (page.NextCursor != nil) || expected == "" && len(page.Items) != 0 ||
			expected != "" && (len(page.Items) != 1 || page.Items[0].Name != expected || page.Items[0].Size != nil) {
			t.Fatal(page, err)
		}
		if page.NextCursor != nil {
			q.Cursor = *page.NextCursor
		}
	}
	if pages.Load() != 3 {
		t.Fatal("Core pagination did not reuse the album page remainder", pages.Load())
	}
	photo := core.Path("/album/" + photoName)
	for _, check := range []struct {
		state, path string
		code        core.Code
	}{{"duplicate", photo.Value(), core.PathAmbiguous}, {"cycle", photo.Value(), core.InvalidUpstreamResponse},
		{"unsafe", photo.Value(), core.InvalidUpstreamResponse}, {"", "/album/Pending.mp4", core.UpstreamFailure}} {
		mode.Store(check.state)
		if _, err := s.Open(ctx, "u", core.Path(check.path), core.OpenOptions{}); core.CodeOf(err) != check.code || bodies.Load() != 0 {
			t.Fatal(check, err)
		}
	}
	mode.Store("")
	for _, check := range []struct {
		path core.Address
		want string
		o    core.OpenOptions
	}{{photo, "bcd", core.OpenOptions{Range: &core.Range{Start: 1, End: 3}}}, {photo, "abcdef", core.OpenOptions{}}, {core.Path("/album/" + videoName), "transcoded", core.OpenOptions{}}} {
		opened, err := s.Open(ctx, "u", check.path, check.o)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(opened.Body)
		if err := errors.Join(err, opened.Body.Close()); err != nil || string(data) != check.want ||
			check.o.Range != nil && (opened.Range == nil || opened.Range.Total != 6) || check.o.Range == nil && opened.Range != nil {
			t.Fatal(string(data), opened.Range, err)
		}
	}
	mode.Store("failed-close")
	before := bodies.Load()
	if _, err := s.Open(ctx, "u", photo, core.OpenOptions{}); !errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "sig=") || bodies.Load() != before+1 {
		t.Fatal("failed private close leaked or advanced the download", err)
	}
	mode.Store("wait")
	done := make(chan error, 1)
	go func() { _, err := s.Open(ctx, "u", photo, core.OpenOptions{}); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("native download did not start")
	}
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) || broken == nil || broken.calls != 2 {
		t.Fatal("retirement did not cancel and retry cleanup", err)
	}
}
