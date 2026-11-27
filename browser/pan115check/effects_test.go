package pan115check

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

func Test115EffectRequiresFreshBoundList(t *testing.T) {
	old, _ := parse115Receipt(fileExchange115{operation: "list", loader: "doc", parent: "0", started: 1, completed: 2},
		[]byte(`{"state":true,"cid":0,"data":[{"fid":"1234","cid":0,"n":"old.zip"}]}`), "")
	for _, operation := range []struct {
		name, form, destination string
	}{
		{"rename", "format=json&files_new_name%5B1234%5D=new.zip", "0"},
		{"move", "format=json&fid%5B0%5D=1234&pid=2", "2"},
	} {
		exchange := fileExchange115{id: "change", operation: operation.name, loader: "doc", started: 4, completed: 6}
		change, ok := parse115Receipt(exchange, []byte(`{"state":true}`), operation.form)
		if !ok {
			t.Fatalf("fixture %s was not accepted", operation.name)
		}
		if rejected, ok := parse115Receipt(exchange, []byte(`{"state":false}`), operation.form); !ok || !rejected.rejected {
			t.Fatalf("%s business rejection was not identified", operation.name)
		}
		for _, scenario := range []struct {
			name, id, parent string
			started          cdp.MonotonicTime
			competing        cdp.MonotonicTime
		}{
			{"in flight before acceptance", "1234", operation.destination, 5, 0},
			{"same name on another file", "1235", operation.destination, 8, 0},
			{"same ID in another directory", "1234", "3", 8, 0},
			{"competing request with unknown result", "1234", operation.destination, 8, 7},
			{"earlier request still in flight at baseline", "1234", operation.destination, 8, 1},
		} {
			o := fileObservation115{}
			after, _ := parse115Receipt(fileExchange115{operation: "list", loader: "doc", parent: scenario.parent, started: scenario.started, completed: 9},
				fmt.Appendf(nil, `{"state":true,"cid":%q,"data":[{"fid":%q,"cid":%q,"n":"new.zip"}]}`, scenario.parent, scenario.id, scenario.parent), "")
			for _, receipt := range []fileReceipt115{old, change, after} {
				o.receipts.add(receipt)
			}
			if scenario.competing != 0 {
				o.writes.add(fileExchange115{id: "other", operation: "move", loader: "doc", started: scenario.competing})
			}
			if result := o.result(); !slices.Equal(result, []string{operation.name + "_unconfirmed"}) {
				t.Errorf("%s, %s: unexpected readback result %v", operation.name, scenario.name, result)
			}
		}
	}
	deleted, ok := parse115Receipt(fileExchange115{id: "delete", operation: "delete", loader: "doc", started: 4, completed: 6},
		[]byte(`{"state":true}`), "format=json&fid%5B0%5D=1234&pid=0&ignore_warn=0")
	if !ok {
		t.Fatal("fixture delete was not accepted")
	}
	for _, scenario := range []struct {
		name, extra, count, offset string
	}{
		{"filtered empty page", "&star=1", "0", "0"},
		{"truncated first page", "", "2", "0"},
		{"wrong page receipt", "", "0", "30"},
	} {
		query, err := url.ParseQuery("aid=1&cid=0&offset=0&limit=30&type=0&show_dir=1&count_folders=1" + scenario.extra)
		if err != nil {
			t.Fatal(err)
		}
		after, ok := parse115Receipt(fileExchange115{operation: "list", loader: "doc", parent: "0", started: 8, completed: 9, complete: fullList115(query)},
			fmt.Appendf(nil, `{"state":true,"cid":0,"offset":%s,"count":%s,"data":[]}`, scenario.offset, scenario.count), "")
		o := fileObservation115{}
		for _, receipt := range []fileReceipt115{old, deleted, after} {
			o.receipts.add(receipt)
		}
		if result := o.result(); !ok || !slices.Equal(result, []string{"delete_unconfirmed"}) {
			t.Errorf("%s: unexpected readback result %v", scenario.name, result)
		}
	}
	o := fileObservation115{ready: make(chan fileExchange115, 1)}
	o.statuses.add(fileStatus115{id: "list", code: http.StatusNotModified})
	o.finished.add(network.EventLoadingFinished{RequestID: "list", Timestamp: 9})
	o.enqueue(fileExchange115{id: "list", operation: "list", responseOp: "list", started: 7, acknowledged: 8})
	if len(o.ready) != 0 {
		t.Fatal("a revalidated browser cache entry was treated as a fresh response")
	}
}

func TestBrowserFileEffects(t *testing.T) {
	exe := os.Getenv("MWEAVE_TEST_BROWSER")
	if exe == "" {
		t.Skip("set MWEAVE_TEST_BROWSER to a Chromium executable for the loopback fixture")
	}
	const secret = "private-sample"
	var renamed, moved, deleted atomic.Bool
	transfer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		switch r.URL.Path {
		case "/files":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			name := "old.zip"
			if renamed.Load() {
				name = secret + ".zip"
			}
			directory := r.URL.Query().Get("cid")
			parent := "0"
			if moved.Load() {
				parent = "2"
			}
			if deleted.Load() || directory != parent {
				fmt.Fprintf(w, `{"state":true,"cid":%q,"offset":0,"count":0,"data":[]}`, directory)
				return
			}
			fmt.Fprintf(w, `{"state":true,"cid":%q,"offset":0,"count":1,"data":[{"fid":"1234","cid":%q,"n":%q}]}`, parent, parent, name)
		case "/files/batch_rename":
			w.Header().Set("Content-Type", "application/json")
			if r.Method == http.MethodPost && r.ParseForm() == nil && r.Form.Get("files_new_name[1234]") == secret+".zip" {
				renamed.Store(true)
				fmt.Fprint(w, `{"state":true,"data":{"1234":true}}`)
			} else {
				fmt.Fprint(w, `{"state":false}`)
			}
		case "/files/move":
			w.Header().Set("Content-Type", "application/json")
			if r.Method == http.MethodPost && r.ParseForm() == nil && r.PostForm.Get("fid[0]") == "1234" && r.PostForm.Get("pid") == "2" {
				moved.Store(true)
				fmt.Fprint(w, `{"state":true}`)
			} else {
				fmt.Fprint(w, `{"state":false}`)
			}
		case "/rb/delete":
			w.Header().Set("Content-Type", "application/json")
			accepted := r.Method == http.MethodPost && r.ParseForm() == nil && r.PostForm.Get("fid[0]") == "1234" &&
				r.PostForm.Get("pid") == "2" && r.PostForm.Get("ignore_warn") == "1"
			deleted.Store(accepted)
			fmt.Fprintf(w, `{"state":%t}`, accepted)
		default:
			http.NotFound(w, r)
		}
	}))
	defer transfer.Close()
	portal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<link rel="icon" href="data:,">`)
	}))
	defer portal.Close()
	entry, _ := url.Parse(portal.URL)
	service, _ := url.Parse(transfer.URL)
	opts := append(slices.Clone(chromedp.DefaultExecAllocatorOptions[:]), chromedp.ExecPath(exe), chromedp.UserDataDir(t.TempDir()))
	if strings.EqualFold(filepath.Base(exe), "vivaldi.exe") {
		opts = append(opts, chromedp.Flag("disable-vivaldi", true))
	}
	alloc, closeBrowser := chromedp.NewExecAllocator(t.Context(), opts...)
	defer closeBrowser()
	tab, closeTab := chromedp.NewContext(alloc)
	defer closeTab()
	ctx, cancel := context.WithTimeout(tab, 15*time.Second)
	defer cancel()
	if err := chromedp.Do(ctx); err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	if _, err := chromedp.Call(ctx, network.Enable, network.EnableParams{MaxPostDataSize: &zero}); err != nil {
		t.Fatal(err)
	}
	finish := observe(ctx, requestOrigin(entry), requestOrigin(service))
	if finish == nil {
		t.Fatal("loopback observer did not start")
	}
	defer finish()
	if err := chromedp.Do(ctx, chromedp.Navigate(entry.String())); err != nil {
		t.Fatal(err)
	}
	run := func(js string) {
		t.Helper()
		if err := chromedp.Do(ctx, chromedp.Evaluate[chromedp.Void](js, chromedp.EvalAwaitPromise)); err != nil {
			t.Fatal(err)
		}
	}
	run(fmt.Sprintf(`(async () => {
		const api = %q;
		const list = parent => fetch(api + "/files?aid=1&offset=0&limit=30&type=0&show_dir=1&count_folders=1&cid=" + parent).then(response => response.json());
		const change = (path, fields) => fetch(api + path, {method: "POST", body: new URLSearchParams({...fields, format: "json"})}).then(response => response.json());
		await list("0");
		await change("/files/batch_rename", {"files_new_name[1234]": %q});
		await list("0");
		await change("/files/move", {"fid[0]": "1234", pid: "2"});
		await list("0");
		await list("2");
		await change("/rb/delete", {"fid[0]": "1234", pid: "2", ignore_warn: "0"});
		await change("/rb/delete", {"fid[0]": "1234", pid: "2", ignore_warn: "1"});
		await list("2");
	})()`, transfer.URL, secret+".zip"))

	// Let the same passive listeners drain as the command does before exit.
	time.Sleep(300 * time.Millisecond)
	effects := finish()
	if !slices.Equal(effects, []string{"rename_confirmed", "move_confirmed", "delete_source_confirmed"}) {
		t.Fatalf("file effects not confirmed: %v", effects)
	}
	raw, err := json.Marshal(effects)
	if err != nil || strings.Contains(string(raw), secret) || strings.Contains(string(raw), transfer.URL) {
		t.Fatal("file effects exposed private data")
	}
}
