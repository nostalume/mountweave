package uc

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	core "github.com/nostalume/mountweave"
)

func TestWebsiteRecycleByID(t *testing.T) {
	ctx := context.Background()
	account := strings.Repeat("a", 64)
	folder, regular, size := 0, 1, int64(4)
	items := []ucListItem{
		{ID: "source-id", Name: "sample", Kind: &regular, Size: &size},
		{ID: "file-still-listed", Name: "unconfirmed", Kind: &regular, Size: &size},
		{ID: "no-task", Name: "malformed", Kind: &regular, Size: &size},
		{ID: "folder-id", Name: "folder", Kind: &folder},
	}
	recycles := 0
	call := func(_ context.Context, _ string, raw json.RawMessage) (json.RawMessage, error) {
		var q ucMetadataRequest
		if err := json.Unmarshal(raw, &q); err != nil {
			t.Fatal(err)
		}
		if q.Account != account {
			t.Fatalf("unbound operation: %+v", q)
		}
		reply := ucMetadataReply{Account: account, Status: 200}
		switch q.Operation {
		case "page":
			total := len(items)
			reply.Page, reply.Total, reply.Items = q.Page, &total, items
		case "recycle":
			recycles++
			switch q.FileID {
			case "source-id":
				items[0].ID = "peer-with-same-name"
				reply.Task = "confirmed-task"
			case "file-still-listed":
				reply.Task = "contradictory-task"
			case "no-task":
			default:
				t.Fatalf("unexpected recycle target: %s", q.FileID)
			}
		default:
			t.Fatalf("unexpected browser operation: %s", q.Operation)
		}
		return json.Marshal(reply)
	}
	d, err := OpenWebsite(ctx, WebsiteOptions{Tab: ctx, Call: call, Account: account, RootPath: "/", Release: func() error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(ctx) })
	if _, err := c.Install("/uc", d); err != nil {
		t.Fatal(err)
	}
	typed := core.RemoveOptions{Target: core.RemoveNonDirectory}
	typed.Disposition = core.RemoveTrash
	change, err := c.Remove(ctx, "writer", core.Path("/uc/sample"), typed)
	if err != nil || change.Effect != core.Applied || items[0].ID != "peer-with-same-name" {
		t.Fatalf("recycle: %+v, %v", change, err)
	}
	for _, path := range []string{"/uc/unconfirmed", "/uc/malformed"} {
		if got, err := c.Remove(ctx, "writer", core.Path(path), typed); got.Effect != core.Unknown || core.CodeOf(err) != core.InvalidUpstreamResponse {
			t.Fatalf("%s: %+v, %v", path, got, err)
		}
	}
	if got, err := c.Remove(ctx, "writer", core.Path("/uc/folder"), typed); got.Effect != core.Unchanged || core.CodeOf(err) != core.UnsupportedOperation || recycles != 3 {
		t.Fatalf("wrong type: %+v, %v, calls=%d", got, err, recycles)
	}
}
