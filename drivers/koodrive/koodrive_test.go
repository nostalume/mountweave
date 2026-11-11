package koodrive

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSpacesThroughCore(t *testing.T) {
	const (
		user      = "user-id"
		container = "personal-space"
		root      = "native-ordinary-root"
		name      = "Report 版"
		data      = "abcdef"
		auth      = "HMAC-SHA256 sealed-control-auth"
		signature = "sealed-obs-signature"
		newDir    = "Drafts +% 版"
		newFile   = "Upload +% 版"
	)
	var mode atomic.Value
	mode.Store("")
	var spaces, rootPages, bodies, unexpected, creates, uploadStarts, partWrites, commits atomic.Int32
	var created, uploaded atomic.Bool
	started := make(chan struct{}, 1)
	obs := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodies.Add(1)
		if r.URL.Query().Get("X-Amz-Signature") != signature ||
			r.Header.Get("Authorization") != "" || r.Header.Get("X-User-Id") != "" ||
			r.Header.Get("X-Date") != "" || r.Header.Get("Referer") != "" || r.Header.Get("Cookie") != "" ||
			r.Header.Get("Accept-Encoding") != "identity" {
			unexpected.Add(1)
			http.Error(w, "unexpected OBS request", http.StatusBadRequest)
			return
		}
		if r.Method == http.MethodPut && r.URL.Path == "/upload" {
			operation := r.URL.Query().Get("operation")
			if operation != "upload-tail" {
				partWrites.Add(1)
			}
			content, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "incomplete upload", http.StatusBadRequest)
				return
			}
			expected := data
			if operation == "upload-empty" {
				expected = ""
			}
			if string(content) != expected || r.ContentLength != int64(len(expected)) ||
				r.Header.Get("Content-Length") == "" || len(r.TransferEncoding) != 0 {
				unexpected.Add(1)
				http.Error(w, "invalid upload body", http.StatusBadRequest)
				return
			}
			switch operation {
			case "upload-redirect":
				w.Header().Set("Location", "/object?X-Amz-Signature="+signature)
				w.WriteHeader(http.StatusTemporaryRedirect)
			case "upload-cancel":
				started <- struct{}{}
				<-r.Context().Done()
			default:
				w.WriteHeader(http.StatusOK)
			}
			return
		}
		if r.Method != http.MethodGet {
			unexpected.Add(1)
			http.Error(w, "unexpected content method", http.StatusBadRequest)
			return
		}
		if r.URL.Path == "/signed" {
			w.Header().Set("Location", "/object?X-Amz-Signature="+signature)
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		if r.URL.Path != "/object" {
			unexpected.Add(1)
			http.NotFound(w, r)
			return
		}
		switch {
		case mode.Load() == "wait":
			w.Header().Set("Content-Length", "6")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			started <- struct{}{}
			<-r.Context().Done()
		case mode.Load() == "wrong-size":
			_, _ = io.WriteString(w, "unexpected content")
		case r.Header.Get("Range") == "bytes=1-3":
			w.Header().Set("Content-Range", "bytes 1-3/6")
			w.Header().Set("Content-Length", "3")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(w, data[1:4])
		case r.Header.Get("Range") == "bytes=99-":
			w.Header().Set("Content-Range", "bytes */7")
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		case mode.Load() == "group":
			w.(http.Flusher).Flush()
			_, _ = io.WriteString(w, data+"x")
		default:
			_, _ = io.WriteString(w, data)
		}
	}))
	defer obs.Close()
	row := func(id, name, typ, parent string) map[string]any {
		f := map[string]any{"id": id, "fileName": name, "fileType": typ,
			"parentFolder": []string{parent}, "containers": []string{container}, "recycled": false,
			"editedTime": "2025-07-12T12:00:00.125Z"}
		if typ == "45" {
			f["size"] = len(data)
		}
		return f
	}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != auth || r.Header.Get("X-User-Id") != user ||
			r.Header.Get("X-Date") == "" || r.Header.Get("Accept-Encoding") != "identity" {
			unexpected.Add(1)
			http.Error(w, "unexpected control request", http.StatusBadRequest)
			return
		}
		var request struct {
			Container  string       `json:"containerId"`
			Parent     string       `json:"parentFileId"`
			IDs        []string     `json:"fileIds"`
			Parents    []string     `json:"parentFolder"`
			Name       string       `json:"fileName"`
			Type       string       `json:"fileType"`
			MIME       string       `json:"mimeType"`
			Rename     int          `json:"renameMode"`
			ID         string       `json:"fileId"`
			Upload     int          `json:"uploadType"`
			AutoRename int          `json:"autoRename"`
			Mode       string       `json:"uploadMode"`
			Length     *int64       `json:"length"`
			Parts      []uploadPart `json:"multiParts"`
			SHA256     string       `json:"sha256"`
			Proof      string       `json:"proofCode"`
			Page       struct {
				Cursor string `json:"pageCursor"`
			} `json:"pageInfo"`
		}
		if r.Method == http.MethodPost && (r.Header.Get("Content-Type") != "application/json" ||
			json.NewDecoder(r.Body).Decode(&request) != nil || request.ID == "" && request.Container != container) {
			unexpected.Add(1)
			http.Error(w, "invalid control POST", http.StatusBadRequest)
			return
		}
		switch r.Method + " " + r.URL.Path {
		case "GET /koodrive/ose/v1/space/container/" + container:
			spaces.Add(1)
			var typ any = 1
			owner := user
			switch mode.Load() {
			case "department":
				typ, owner = 0, "department-owner"
			case "group":
				typ, owner = "2", "group-owner"
			case "invalid-team-owner":
				typ, owner = 2, ".."
			case "wrong-owner":
				owner = "different-user"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{
				"type": typ, "ownerId": owner, "containerId": container, "bizRootFileId": root, "bizSboxFileId": "safe",
			}})
		case "POST /koodrive/ose/v1/files/0", "POST /koodrive/ose/v1/files/department-owner/0", "POST /koodrive/ose/v1/files/group-owner/0":
			expected := "/koodrive/ose/v1/files/0"
			switch mode.Load() {
			case "department":
				expected = "/koodrive/ose/v1/files/department-owner/0?team_type=0"
			case "group":
				expected = "/koodrive/ose/v1/files/group-owner/0?team_type=2"
			}
			if r.URL.RequestURI() != expected {
				unexpected.Add(1)
				http.Error(w, "wrong list scope", http.StatusBadRequest)
				return
			}
			files, next := []map[string]any{}, ""
			if request.Parent == "root" {
				rootPages.Add(1)
			}
			switch request.Parent + ":" + request.Page.Cursor {
			case "root:":
				parent := root
				if mode.Load() == "wrong-parent" {
					parent = "unrelated"
				}
				files = append(files, row("team-id", "Team", "10", parent), row("readme-id", "readme", "45", root),
					row("smart-id", "Smart", "11", root))
				next = "cursor+=1"
			case "root:cursor+=1":
				next = "cursor2"
				if mode.Load() == "cycle" {
					next = request.Page.Cursor
				}
			case "root:cursor2":
				files = append(files, row("notes-id", "notes", "45", root))
			case "team-id:":
				files = append(files, row("file-id", name, "45", "team-id"))
				if created.Load() {
					files = append(files, row("directory-id", newDir, "10", "team-id"))
				}
				if uploaded.Load() {
					files = append(files, row("upload-id", newFile, "45", "team-id"))
				}
			default:
				unexpected.Add(1)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0, "category": "drive#fileList", "files": files, "nextCursor": next,
			})
		case "POST /koodrive/ose/v1/files/download":
			if len(request.IDs) != 1 || request.IDs[0] != "file-id" {
				unexpected.Add(1)
			}
			id := "file-id"
			if mode.Load() == "wrong-receipt" {
				id = "unrelated-file"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "batchCode": 1, "downloads": []map[string]any{{
				"code": "0", "fileId": id, "fileName": name, "url": obs.URL + "/signed?X-Amz-Signature=" + signature,
			}}})
		case "POST /koodrive/ose/v1/files/create":
			uploadStarts.Add(1)
			size, wireParent, parent := int64(len(data)), "team-id", "team-id"
			if mode.Load() == "upload-empty" {
				size, wireParent, parent = 0, "root", root
			}
			if request.Container != container || len(request.Parents) != 1 || request.Parents[0] != wireParent ||
				request.Type != "45" || request.Name != newFile || request.Mode != "multipart" ||
				request.Upload != 1 || request.AutoRename != 3 || request.Length == nil || *request.Length != size ||
				len(request.Parts) != 1 || request.Parts[0].Number != 1 || request.Parts[0].Size == nil ||
				*request.Parts[0].Size != size || request.Parts[0].URL != "" || request.SHA256 != "" || request.Proof != "" {
				unexpected.Add(1)
				http.Error(w, "invalid file creation", http.StatusBadRequest)
				return
			}
			if mode.Load() == "upload-conflict" {
				_, _ = io.WriteString(w, `{"code":"9","existFileId":"other-id"}`)
				return
			}
			if mode.Load() == "upload-wrong-parent" {
				parent = "unrelated-id"
			}
			address := obs.URL + "/upload?X-Amz-Signature=" + signature + "&operation=" + mode.Load().(string)
			if mode.Load() == "upload-invalid-grant" {
				address += "#"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0, "fileId": "upload-id", "containerId": container, "fileName": newFile, "parentFolder": []string{parent},
				"multiParts": []map[string]any{{"partNumber": 1, "partSize": size, "uploadUrl": address}},
			})
		case "POST /koodrive/ose/v1/files/complete":
			commits.Add(1)
			if request.ID != "upload-id" {
				unexpected.Add(1)
				http.Error(w, "unexpected file identity", http.StatusBadRequest)
				return
			}
			if mode.Load() == "upload-lost" {
				_, _ = io.WriteString(w, `{"code":`)
				return
			}
			if mode.Load() == "upload-normal" {
				uploaded.Store(true)
			}
			_, _ = io.WriteString(w, `{"code":0}`)
		case "POST /koodrive/ose/v1/directory":
			creates.Add(1)
			parent, directoryID := "team-id", "directory-id"
			rootTarget := mode.Load() == "mkdir-root"
			if rootTarget {
				parent, directoryID = "root", "new-root-directory-id"
			}
			if len(request.Parents) != 1 || request.Parents[0] != parent || request.Name != newDir ||
				request.Type != "10" || request.MIME != folderMIME || request.Rename != 3 {
				unexpected.Add(1)
				http.Error(w, "invalid directory mutation", http.StatusBadRequest)
				return
			}
			switch mode.Load() {
			case "generic-conflict":
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"code":9}`)
			case "unconfirmed":
				_, _ = io.WriteString(w, `{"id":"directory-id","code":0,"fileName":null,`)
			case "create-redirect":
				w.Header().Set("Location", obs.URL+"/object")
				w.WriteHeader(http.StatusTemporaryRedirect)
			default:
				if !rootTarget && created.Swap(true) {
					_ = json.NewEncoder(w).Encode(map[string]any{"code": "9", "existFileId": directoryID})
				} else {
					_ = json.NewEncoder(w).Encode(map[string]any{
						"code": 0, "id": directoryID, "fileName": newDir, "mimeType": folderMIME,
					})
				}
			}
		default:
			unexpected.Add(1)
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	apiClient := api.Client()
	native := apiClient.Transport
	apiURL, err := url.Parse(api.URL)
	if err != nil {
		t.Fatal(err)
	}
	apiClient.Transport = transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "cn.myhuaweicloud-koodrive.com" {
			unexpected.Add(1)
			return nil, errors.New("wrong signed request recipient")
		}
		if (r.URL.Path == "/koodrive/ose/v1/directory" || r.URL.Path == "/koodrive/ose/v1/files/create" ||
			r.URL.Path == "/koodrive/ose/v1/files/complete") && r.GetBody != nil {
			unexpected.Add(1)
			return nil, errors.New("directory mutation request was replayable")
		}
		req := r.Clone(r.Context())
		req.URL.Host = apiURL.Host
		req.Header.Set("Authorization", auth)
		req.Header.Set("X-Date", "20261030T123000Z")
		return native.RoundTrip(req)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	o := Options{UserID: user, ContainerID: container, Client: apiClient, ContentClient: obs.Client()}
	for _, rejected := range []struct {
		mode string
		code core.Code
	}{{"group", core.UnsupportedOperation}, {"wrong-owner", core.InvalidUpstreamResponse}} {
		mode.Store(rejected.mode)
		d, err := Open(ctx, o)
		if d == nil || core.CodeOf(err) != rejected.code {
			t.Fatal(rejected, err)
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
	mode.Store("")
	readOnly, err := Open(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	if change, err := readOnly.Mkdir(ctx, core.NativePath("/Team/"+newDir)); core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged {
		t.Fatal("default mount admitted directory creation", change, err)
	}
	size := int64(len(data))
	if change, err := readOnly.Create(ctx, core.NativePath("/"+newFile), strings.NewReader(data), &size); core.CodeOf(err) != core.UnsupportedOperation || change.Effect != core.Unchanged {
		t.Fatal("default mount admitted file creation", change, err)
	}
	if err := readOnly.Close(); err != nil {
		t.Fatal(err)
	}
	o.AllowMkdir, o.AllowCreate = true, true
	driver, err := Open(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	s, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	mount, err := s.Install("/cloud", driver)
	if err != nil {
		t.Fatal(err)
	}
	q := core.ListRequest{Query: core.Children(core.Path("/cloud")), Limit: 1, CursorTTL: time.Minute}
	checks := [][]string{{"Team"}, {"readme"}, nil, {"notes"}}
	for index, expected := range checks {
		page, err := s.List(ctx, "u", q)
		if err != nil || len(page.Items) != len(expected) || (index < len(checks)-1) != (page.NextCursor != nil) ||
			len(expected) != 0 && page.Items[0].Name != expected[0] {
			t.Fatal(page, err)
		}
		if index == 1 {
			replay, err := s.List(ctx, "u", q)
			if err != nil || len(replay.Items) != 1 || replay.Items[0].Name != expected[0] || rootPages.Load() != 1 {
				t.Fatal("Core replay requested an additional native page", replay, err)
			}
		}
		if page.NextCursor != nil {
			q.Cursor = *page.NextCursor
		}
	}
	if rootPages.Load() != 3 {
		t.Fatal("native cursor did not traverse the empty continuation page", rootPages.Load())
	}
	if change, err := s.Mkdir(ctx, "u", core.Path("/cloud/Team/a?b")); core.CodeOf(err) != core.UnrepresentableName || change.Effect != core.Unchanged || creates.Load() != 0 {
		t.Fatal("unrepresentable name reached a mutation request", change, err)
	}
	folder := core.Path("/cloud/Team/" + newDir)
	for index, sample := range []struct {
		mode   string
		effect core.Effect
		before core.TargetPresence
		code   core.Code
	}{{"generic-conflict", core.Unknown, core.TargetUnknown, core.UpstreamFailure},
		{"unconfirmed", core.Unknown, core.TargetUnknown, core.InvalidUpstreamResponse},
		{"create-redirect", core.Unknown, core.TargetUnknown, core.UpstreamFailure},
		{"", core.Applied, core.TargetMissing, ""}, {"", core.Unchanged, core.TargetPresent, core.AlreadyExists}} {
		mode.Store(sample.mode)
		change, err := s.Mkdir(ctx, "u", folder)
		if change.Effect != sample.effect || change.Before != sample.before ||
			(err == nil) != (sample.code == "") || err != nil && core.CodeOf(err) != sample.code || creates.Load() != int32(index+1) {
			t.Fatal(sample, change, err, creates.Load())
		}
	}
	mode.Store("mkdir-root")
	if change, err := s.Mkdir(ctx, "u", core.Path("/cloud/"+newDir)); err != nil ||
		change.Effect != core.Applied || change.Before != core.TargetMissing {
		t.Fatal("root creation did not use the native root sentinel", change, err)
	}
	mode.Store("")
	if f, err := s.Stat(ctx, "u", folder); err != nil || f.Kind != core.Directory || f.Name != newDir || bodies.Load() != 0 {
		t.Fatal("created directory was not visible or redirect reached OBS", f, err, bodies.Load())
	}
	target := core.Path("/cloud/Team/" + name)
	for _, rejected := range []struct {
		mode string
		path core.Address
		code core.Code
	}{{"wrong-parent", target, core.InvalidUpstreamResponse}, {"cycle", target, core.InvalidUpstreamResponse},
		{"wrong-receipt", target, core.InvalidUpstreamResponse}, {"", core.Path("/cloud/Team/" + strings.ToLower(name)), core.NotFound}} {
		mode.Store(rejected.mode)
		if _, err := s.Open(ctx, "u", rejected.path, core.OpenOptions{}); core.CodeOf(err) != rejected.code || bodies.Load() != 0 {
			t.Fatal(rejected, err, bodies.Load())
		}
	}
	mode.Store("wrong-size")
	if _, err := s.Open(ctx, "u", target, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("mismatched content size was accepted", err)
	}
	mode.Store("")
	if f, err := s.Stat(ctx, "u", target); err != nil || f.Kind != core.File || f.Name != name || f.Size == nil || *f.Size != 6 {
		t.Fatal(f, err)
	}
	opened, err := s.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(bytes) != "bcd" || opened.Range == nil || opened.Range.Total != 6 {
		t.Fatal(string(bytes), opened.Range, err)
	}
	opened, err = s.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bytes, err = io.ReadAll(opened.Body)
	if err = errors.Join(err, opened.Body.Close()); err != nil || string(bytes) != data || opened.Range != nil {
		t.Fatal("complete original", string(bytes), err)
	}
	uploadTarget := core.Path("/cloud/Team/" + newFile)
	var empty int64
	oversize := int64(maxSinglePart) + 1
	for _, sample := range []struct {
		length *int64
		input  string
		code   core.Code
	}{{nil, data, core.UnsupportedOperation}, {&oversize, data, core.CapacityExceeded}, {&empty, data, core.InvalidRequest}} {
		change, err := s.Write(ctx, "u", uploadTarget, strings.NewReader(sample.input), sample.length)
		if core.CodeOf(err) != sample.code || change.Effect != core.Unchanged || uploadStarts.Load() != 0 {
			t.Fatal("invalid input reached initialization", change, err, uploadStarts.Load())
		}
	}
	for _, sample := range []struct {
		mode, input string
		effect      core.Effect
		code        core.Code
		puts, calls int32
	}{
		{"upload-conflict", data, core.Unchanged, core.AlreadyExists, 0, 0},
		{"upload-wrong-parent", data, core.Unknown, core.InvalidUpstreamResponse, 0, 0},
		{"upload-invalid-grant", data, core.Unknown, core.InvalidUpstreamResponse, 0, 0},
		{"upload-redirect", data, core.Unknown, core.UpstreamFailure, 1, 0},
		{"upload-tail", data + "!", core.Unknown, core.InvalidRequest, 0, 0},
		{"upload-lost", data, core.Unknown, core.InvalidUpstreamResponse, 1, 1},
		{"upload-normal", data, core.Applied, "", 1, 1},
		{"upload-empty", "", core.Applied, "", 1, 1},
	} {
		mode.Store(sample.mode)
		address, length := uploadTarget, &size
		if sample.mode == "upload-empty" {
			address, length = core.Path("/cloud/"+newFile), &empty
		}
		beforeStart, beforePut, beforeCommit := uploadStarts.Load(), partWrites.Load(), commits.Load()
		change, err := s.Write(ctx, "u", address, strings.NewReader(sample.input), length)
		if change.Effect != sample.effect || (err == nil) != (sample.code == "") ||
			err != nil && core.CodeOf(err) != sample.code || uploadStarts.Load()-beforeStart != 1 ||
			partWrites.Load()-beforePut != sample.puts || commits.Load()-beforeCommit != sample.calls {
			t.Fatal(sample.mode, change, err, uploadStarts.Load()-beforeStart, partWrites.Load()-beforePut, commits.Load()-beforeCommit)
		}
	}
	if f, err := s.Stat(ctx, "u", uploadTarget); err != nil || f.Kind != core.File || f.Name != newFile || f.Size == nil || *f.Size != size {
		t.Fatal("confirmed file is not visible", f, err)
	}
	mode.Store("upload-cancel")
	beforeCommit := commits.Load()
	writeCtx, stopWrite := context.WithCancel(ctx)
	defer stopWrite()
	written := make(chan error, 1)
	go func() {
		change, err := s.Write(writeCtx, "u", uploadTarget, strings.NewReader(data), &size)
		if change.Effect != core.Unknown {
			err = errors.Join(err, errors.New("canceled upload lost its possible effect"))
		}
		written <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("upload did not start")
	}
	stopWrite()
	if err := <-written; !errors.Is(err, context.Canceled) || commits.Load() != beforeCommit {
		t.Fatal("canceled upload reached completion", err, commits.Load())
	}

	teamOptions := o
	teamOptions.TeamSpace = true
	before := spaces.Load()
	for _, flags := range [][2]bool{{true, false}, {false, true}} {
		teamOptions.AllowMkdir, teamOptions.AllowCreate = flags[0], flags[1]
		team, err := Open(ctx, teamOptions)
		if team != nil || core.CodeOf(err) != core.InvalidRequest || spaces.Load() != before {
			t.Fatal("team mutation flags reached native construction", team != nil, err)
		}
	}
	teamOptions.AllowMkdir, teamOptions.AllowCreate = false, false
	mode.Store("invalid-team-owner")
	badTeam, err := Open(ctx, teamOptions)
	if badTeam == nil || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatal("invalid team owner passed construction", err)
	}
	if err := badTeam.Close(); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []string{"department", "group"} {
		mode.Store(scope)
		team, err := Open(ctx, teamOptions)
		if err != nil {
			t.Fatal(scope, err)
		}
		at := "/" + scope
		binding, err := s.Install(at, team)
		if err != nil {
			t.Fatal(scope, err)
		}
		target := core.Path(at + "/Team/" + name)
		opened, err := s.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 1, End: 3}})
		if err != nil {
			t.Fatal(scope, err)
		}
		content, readErr := io.ReadAll(opened.Body)
		if err := errors.Join(readErr, opened.Body.Close()); err != nil || string(content) != "bcd" {
			t.Fatal(scope, string(content), err)
		}
		if scope == "group" {
			opened, err = s.Open(ctx, "u", target, core.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			content, readErr = io.ReadAll(opened.Body)
			if err := errors.Join(readErr, opened.Body.Close()); string(content) != data || core.CodeOf(err) != core.InvalidUpstreamResponse {
				t.Fatal("unframed download size mismatch was accepted", string(content), err)
			}
			if _, err := s.Open(ctx, "u", target, core.OpenOptions{Range: &core.Range{Start: 99, End: -1}}); core.CodeOf(err) != core.InvalidUpstreamResponse {
				t.Fatal("unsatisfied range with a different total was accepted", err)
			}
		}
		if err := binding.Retire(ctx); err != nil {
			t.Fatal(scope, err)
		}
	}

	mode.Store("wait")
	opened, err = s.Open(ctx, "u", target, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("OBS read did not start")
	}
	done := make(chan error, 1)
	go func() { _, err := io.ReadAll(opened.Body); done <- err }()
	if err := mount.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) || unexpected.Load() != 0 {
		t.Fatal("unexpected request or stream did not cancel", unexpected.Load(), err)
	}
}
