package pan115cdp

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/pan115"
)

func TestWebsitePageAndOpen(t *testing.T) {
	binary := os.Getenv("MWEAVE_TEST_BROWSER")
	if binary == "" {
		t.Skip("set MWEAVE_TEST_BROWSER to a local Chromium executable")
	}
	var lists atomic.Int32
	var wrongPage atomic.Bool
	var insecureRedirect atomic.Bool
	var originalRequests, recycleRequests atomic.Int32
	var applyRecycle, recycled atomic.Bool
	app := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Access-Control-Allow-Origin", Origin)
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Content-Type", "application/json")
		switch r.Host + r.URL.Path {
		case "f.115.com" + directoryPath:
			http.SetCookie(w, &http.Cookie{Name: "fixture", Value: "account", Domain: "115.com", Secure: true, HttpOnly: true, Path: "/"})
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			fmt.Fprint(w, paginationSite)
		case "passportapi.115.com" + accountPath:
			fmt.Fprint(w, `{"state":1,"data":{"user_id":19}}`)
		case "webapi.115.com/files":
			lists.Add(1)
			if r.URL.Query().Get("offset") == "2" && !wrongPage.Load() {
				id := 23
				if recycled.Load() {
					id = 24 // Another object can acquire the old name before confirmation.
				}
				fmt.Fprintf(w, `{"state":true,"offset":2,"limit":2,"cid":0,"count":3,"data":[{"fid":%d,"cid":0,"n":"third.txt","s":5}]}`, id)
			} else {
				fmt.Fprint(w, `{"state":true,"offset":0,"limit":2,"cid":0,"count":3,"data":[{"fid":21,"cid":0,"n":"first.txt","s":5},{"fid":22,"cid":0,"n":"second.txt","s":5}]}`)
			}
		case "webapi.115.com/rb/delete":
			recycleRequests.Add(1)
			cookie, err := r.Cookie("fixture")
			if err != nil || cookie.Value != "account" || r.Method != http.MethodPost || r.ParseForm() != nil ||
				len(r.PostForm) != 4 || r.PostForm.Get("fid[0]") != "23" || r.PostForm.Get("pid") != "0" ||
				r.PostForm.Get("format") != "json" || r.PostForm.Get("ignore_warn") != "0" {
				http.Error(w, "unexpected recycle request", http.StatusBadRequest)
				return
			}
			recycled.Store(applyRecycle.Load())
			fmt.Fprint(w, `{"state":true}`)
		case "body.115.invalid/22":
			if insecureRedirect.Load() {
				http.Redirect(w, r, "http://body.115.invalid/unexpected", http.StatusFound)
				return
			}
			http.Redirect(w, r, "https://cdn.115.invalid/original", http.StatusFound)
		case "cdn.115.invalid/original":
			originalRequests.Add(1)
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Disposition", `attachment; filename="second.txt"`)
			fmt.Fprint(w, "two22")
		default:
			http.NotFound(w, r)
		}
	}))
	defer app.Close()
	addr := app.Listener.Addr().String()
	mapping := "MAP f.115.com " + addr + ", MAP passportapi.115.com " + addr + ", MAP webapi.115.com " + addr + ", MAP body.115.invalid " + addr + ", MAP cdn.115.invalid " + addr + ", MAP * ~NOTFOUND"
	opts := append(chromedp.DefaultExecAllocatorOptions[:], chromedp.ExecPath(binary), chromedp.UserDataDir(t.TempDir()),
		chromedp.Flag("no-proxy-server", true), chromedp.Flag("ignore-certificate-errors", true), chromedp.Flag("host-resolver-rules", mapping))
	if strings.Contains(strings.ToLower(binary), "vivaldi") {
		opts = append(opts, chromedp.Flag("disable-vivaldi", true))
	}
	allocator, stop := chromedp.NewExecAllocator(t.Context(), opts...)
	defer stop()
	tab, closeTab := chromedp.NewContext(allocator)
	defer closeTab()
	if err := chromedp.Do(tab); err != nil {
		t.Fatal(err)
	}
	s := New(tab)
	account, err := s.Account(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for offset, id := range []string{"21", "22", "23"} {
		before := lists.Load()
		page, err := s.List(t.Context(), account, "0", int64(offset), 1)
		if err != nil || page.Offset != int64(offset) || len(page.Items) != 1 || page.Items[0].ID != id {
			t.Fatalf("unexpected item at %d: %v", offset, err)
		}
		if offset == 1 && lists.Load() != before {
			t.Fatal("slicing the same native page issued a network request")
		}
	}
	d, err := pan115.OpenWebsite(t.Context(), pan115.WebsiteOptions{Session: s, Account: account, RootID: "0", Release: func() error { closeTab(); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	c, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(t.Context())
	if _, err := c.Install("/115", d); err != nil {
		t.Fatal(err)
	}
	opened, err := c.Open(t.Context(), "reader", core.Path("/115/second.txt"), core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	value, err := io.ReadAll(opened.Body)
	if closeErr := opened.Body.Close(); err != nil || closeErr != nil || string(value) != "two22" {
		t.Fatalf("reopened file had wrong bytes or failed to close: %v, %v", err, closeErr)
	}
	insecureRedirect.Store(true)
	if opened, err := c.Open(t.Context(), "reader", core.Path("/115/second.txt"), core.OpenOptions{}); opened.Body != nil || core.CodeOf(err) != core.InvalidUpstreamResponse {
		t.Fatalf("the non-HTTPS redirect did not fail before body handoff: %v", err)
	}
	if originalRequests.Load() != 1 {
		t.Fatal("the verified attachment was requested again after a denied redirect")
	}
	wrongPage.Store(true)
	if page, err := s.List(t.Context(), account, "0", 2, 1); core.CodeOf(err) != core.InvalidUpstreamResponse || len(page.Items) != 0 {
		t.Fatalf("wrong offset was accepted: %v", err)
	}
	wrongPage.Store(false)
	file := core.Path("/115/third.txt")
	for _, policy := range []core.RemoveOptions{{Target: core.RemoveEmptyDirectory}, {When: core.IfPresent()}} {
		if change, err := c.Remove(t.Context(), "reader", file, policy); change.Effect != core.Unchanged ||
			core.CodeOf(err) != core.UnsupportedOperation || recycleRequests.Load() != 0 {
			t.Fatalf("unsupported policy was not refused before recycle: %v, %v", change, err)
		}
	}
	policy := core.RemoveOptions{Target: core.RemoveNonDirectory}
	if change, err := c.Remove(t.Context(), "reader", file, policy); change.Effect != core.Unknown ||
		core.CodeOf(err) != core.InvalidUpstreamResponse || recycleRequests.Load() != 1 {
		t.Fatalf("unconfirmed recycle response was treated as an effect or retried: %v, %v", change, err)
	}
	applyRecycle.Store(true)
	policy.Disposition = core.RemoveTrash
	if change, err := c.Remove(t.Context(), "reader", file, policy); change.Effect != core.Applied ||
		change.Before != core.TargetPresent || err != nil || recycleRequests.Load() != 2 {
		t.Fatalf("recycle did not confirm removal of the same file ID: %v, %v", change, err)
	}

}

const paginationSite = `<!doctype html><meta charset="utf-8"><style>#rows{height:90px;overflow:auto}#rows > div{height:120px}</style>
<div id="rows"></div><input id="page" type="text" placeholder="GO">
<button id="download" hidden>下载</button>
<div id="options" role="dialog" hidden><button id="ordinary" data-download-method="ordinary">普通下载</button></div>
<script>
const pager = document.querySelector('#page'), rows = document.querySelector('#rows');
const download = document.querySelector('#download'), options = document.querySelector('#options');
async function load(offset) {
	pager.disabled = true;
	const result = await fetch('https://webapi.115.com/files?aid=1&show_dir=1&type=0&format=json&cid=0&limit=2&offset='+offset).then(r=>r.json());
	localStorage.offset = result.offset;
	rows.replaceChildren(...result.data.map(item => {
		const row = document.createElement('div');
		row.dataset.fileId = item.fid;
		row.innerHTML = '<label><input type="checkbox"><span>'+item.n+'</span></label>';
		row.onchange = e => { download.hidden = !e.target.checked; download.dataset.id = item.fid; };
		return row;
	}));
	pager.disabled = false;
}
pager.onkeypress = e => { if (e.key === 'Enter') { load((Number(pager.value)-1)*2); pager.value=''; } };
download.onclick = () => { options.hidden = false; };
ordinary.onclick = () => { options.hidden = true; window.open('https://body.115.invalid/'+download.dataset.id); };
fetch('https://passportapi.115.com/app/1.0/web/26.0/user/base_info').then(r=>r.json());
load(Number(localStorage.offset || 2));
</script>`
