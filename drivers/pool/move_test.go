package pool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
	"github.com/nostalume/mountweave/protocols/httpapi"
)

type moveTargetFixture struct {
	*Driver
	loseReceipt bool
}

func (d *moveTargetFixture) PublishForMove(ctx context.Context, p core.NativeAddress, parent core.Validator, r io.Reader, n *int64) (core.Change, error) {
	change, err := d.Driver.PublishForMove(ctx, p, parent, r, n)
	if d.loseReceipt && err == nil {
		return core.Change{Effect: core.Unknown}, core.Fail(core.UpstreamTimeout, nil)
	}
	return change, err
}

func TestCrossMountMove(t *testing.T) {
	source, _, _ := rig(t, 3)
	source.MaxBufferedBytes = 96
	_, files := attach(t, source)
	target, members, _ := rig(t, 3)
	target.MaxBufferedBytes = 96
	d, err := New(target)
	if err != nil {
		t.Fatal(err)
	}
	out := &moveTargetFixture{Driver: d}
	if _, err := files.Install("/other", out); err != nil {
		_ = out.Close()
		t.Fatal(err)
	}
	ctx := context.Background()
	const actor = "user"
	p := core.Path
	mkdir := func(name string) {
		t.Helper()
		if _, err := files.Mkdir(ctx, actor, p(name)); err != nil {
			t.Fatal(err)
		}
	}
	write := func(name string) error {
		_, err := files.Write(ctx, actor, p(name), strings.NewReader("payload"), nil, core.WriteOptions{Policy: core.Publish()})
		return err
	}
	check := func(name string, want core.Code) {
		t.Helper()
		_, err := files.Stat(ctx, actor, p(name))
		if (want == "" && err != nil) || (want != "" && core.CodeOf(err) != want) {
			t.Fatalf("%s: want %s, got %v", name, want, err)
		}
	}
	read := func(name string) {
		t.Helper()
		got, err := files.Open(ctx, actor, p(name), core.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(got.Body)
		if err := errors.Join(err, got.Body.Close()); err != nil || string(b) != "payload" {
			t.Fatal(string(b), err)
		}
	}
	tree := core.MoveOptions{Tree: core.TreeOptions{ExecutionOptions: core.ExecutionOptions{Concurrency: 2}, MaxEntries: 12, MaxPlanBytes: 256}}
	h, err := httpapi.New(files, httpapi.Options{
		Authenticate: func(context.Context, string) (core.Principal, error) { return actor, nil },
		CursorTTL:    time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	deliver := func(t *testing.T, kind string, input any, status int, result any) {
		t.Helper()
		body, err := json.Marshal(map[string]any{"kind": kind, "input": input})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/fs/operations", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer fixture")
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		h.ServeHTTP(response, req)
		if response.Code != status {
			t.Fatalf("status %d: %s", response.Code, response.Body.String())
		}
		if err := json.Unmarshal(response.Body.Bytes(), result); err != nil {
			t.Fatal(err)
		}
	}
	mkdir("/pool/tree")
	mkdir("/pool/tree/a")
	mkdir("/pool/tree/b")
	mkdir("/pool/tree/empty")
	for _, name := range []string{"/pool/tree/a/item", "/pool/tree/b/item"} {
		if err := write(name); err != nil {
			t.Fatal(err)
		}
	}
	refuse := tree
	refuse.NoCopy = true
	if _, err := files.Move(ctx, actor, p("/pool/tree"), p("/other/tree"), refuse); core.CodeOf(err) != core.UnsupportedOperation {
		t.Fatal(err)
	}
	tiny := tree
	tiny.Tree.MaxEntries = 1
	if _, err := files.Move(ctx, actor, p("/pool/tree"), p("/other/tree"), tiny); core.CodeOf(err) != core.CapacityExceeded {
		t.Fatal(err)
	}
	check("/other/tree", core.NotFound)
	var moved httpapi.FileResult
	deliver(t, "fs.move_batch", map[string]any{"items": []any{
		map[string]any{"source": "/pool/tree", "target": "/other/tree", "options": tree},
	}}, http.StatusOK, &moved)
	if len(moved.Items) != 1 {
		t.Fatal(moved)
	}
	item := moved.Items[0]
	if item.SourceEffect != "applied" || item.TargetEffect != "applied" || item.Tree == nil ||
		item.Tree.Effect != "applied" || len(item.Tree.Items) != 12 {
		t.Fatal(item)
	}
	check("/pool/tree", core.NotFound)
	check("/other/tree/empty", "")
	read("/other/tree/a/item")
	read("/other/tree/b/item")

	localDriver, err := local.New(t.TempDir(), local.Options{StagingDirectory: t.TempDir(), MaxStagingBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = files.Install("/local", localDriver); err != nil {
		_ = localDriver.Close()
		t.Fatal(err)
	}
	refused, err := files.Move(ctx, actor, p("/other/tree"), p("/local/tree"), tree)
	if core.CodeOf(err) != core.UnsupportedOperation ||
		refused.Source.Effect != core.Unchanged || refused.Target.Effect != core.Unchanged {
		t.Fatal(refused, err)
	}
	read("/other/tree/a/item")
	check("/local/tree", core.NotFound)
	var requests []core.MoveRequest
	for _, name := range []string{"first", "second"} {
		if err := write("/pool/" + name); err != nil {
			t.Fatal(err)
		}
		requests = append(requests, core.MoveRequest{Source: p("/pool/" + name), Target: p("/local/" + name)})
	}
	rows, err := files.MoveBatch(ctx, actor, requests, core.ExecutionOptions{Concurrency: 2})
	if err != nil || len(rows) != 2 {
		t.Fatal(rows, err)
	}
	for i, row := range rows {
		if row.Source.Effect != core.Applied || row.Target.Effect != core.Applied {
			t.Fatal(row)
		}
		check(requests[i].Source.Value(), core.NotFound)
		read(requests[i].Target.Value())
	}

	for _, tc := range []struct {
		name           string
		code           core.Code
		source, target core.Effect
	}{
		{"file-replaced", core.PreconditionFailed, core.Unchanged, core.Applied},
		{"empty-replaced", core.PreconditionFailed, core.Incomplete, core.Applied},
		{"target-replaced", core.PreconditionFailed, core.Unchanged, core.Incomplete},
		{"cancelled", "", core.Unchanged, core.Incomplete},
		{"lost-receipt", core.UpstreamTimeout, core.Unchanged, core.Unknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			from, to := "/pool/"+tc.name, "/other/"+tc.name
			mkdir(from)
			mkdir(from + "/empty")
			if err := write(from + "/leaf"); err != nil {
				t.Fatal(err)
			}
			call, cancel := context.WithCancel(ctx)
			defer cancel()
			replace := func(name string) error {
				if _, err := files.Move(ctx, actor, p(name), p(name+"-saved")); err != nil {
					return err
				}
				_, err := files.Mkdir(ctx, actor, p(name))
				return err
			}
			var once sync.Once
			var injection error
			for _, member := range members {
				member.create = func(ctx context.Context, name string, r io.Reader, n *int64) (core.Change, error) {
					once.Do(func() {
						switch tc.name {
						case "file-replaced":
							injection = write(from + "/leaf")
						case "empty-replaced":
							injection = replace(from + "/empty")
						case "target-replaced":
							injection = replace(to)
						case "cancelled":
							cancel()
						}
					})
					if injection != nil {
						return core.Change{Effect: core.Unchanged}, injection
					}
					return member.Driver.Create(ctx, core.NativePath(name), r, n)
				}
			}
			out.loseReceipt = tc.name == "lost-receipt"
			if out.loseReceipt {
				var problem struct {
					Code   string
					Result httpapi.FileResult
				}
				deliver(t, "fs.move", map[string]any{"source": from, "target": to, "options": tree}, http.StatusGatewayTimeout, &problem)
				receipt := problem.Result
				if problem.Code != "upstream_timeout" || receipt.SourceEffect != tc.source.String() ||
					receipt.TargetEffect != tc.target.String() || len(receipt.Items) == 0 {
					t.Fatal(problem)
				}
				last := receipt.Items[len(receipt.Items)-1]
				if last.Side != core.TargetSide || last.Phase != core.WritePhase ||
					last.RelativePath != "/leaf" || last.Effect != "unknown" || last.Error != problem.Code {
					t.Fatal(last)
				}
			} else {
				r, err := files.Move(call, actor, p(from), p(to), tree)
				if injection != nil || r.Source.Effect != tc.source || r.Target.Effect != tc.target ||
					(tc.name == "cancelled" && !errors.Is(err, context.Canceled)) ||
					(tc.name != "cancelled" && core.CodeOf(err) != tc.code) {
					t.Fatal(r, err, injection)
				}
			}
			check(from+"/empty", "")
			if tc.name == "empty-replaced" {
				check(from+"/empty-saved", "")
				check(from+"/leaf", core.NotFound)
			} else {
				read(from + "/leaf")
			}
			if tc.name == "target-replaced" {
				check(to+"-saved/leaf", core.NotFound)
			}
			if tc.target == core.Applied || tc.target == core.Unknown {
				read(to + "/leaf")
			} else {
				check(to+"/leaf", core.NotFound)
			}
		})
	}
}
