package pan123cdp

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/pan123"
)

func TestOrdinaryWebsiteDownload(t *testing.T) {
	binary := os.Getenv("MWEAVE_TEST_BROWSER")
	if binary == "" {
		t.Skip("set MWEAVE_TEST_BROWSER to a local Chromium executable")
	}
	var mode atomic.Int32
	var grants atomic.Int32
	app := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		switch r.URL.Path {
		case "/":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			fmt.Fprint(w, fixturePage)
		case accountPath:
			fmt.Fprint(w, `{"code":0,"data":{"UID":"19"}}`)
		case listPath:
			parent := r.URL.Query().Get("parentFileId")
			if parent != "0" && parent != "7" {
				http.Error(w, "unexpected fixture directory", 400)
				return
			}
			if r.URL.Query().Get("next") == "9" {
				fmt.Fprintf(w, `{"code":0,"data":{"InfoList":[{"FileId":10,"ParentFileId":%s,"FileName":"second.zip","Type":0,"Size":15}],"Next":-1}}`, parent)
				return
			}
			fmt.Fprintf(w, `{"code":0,"data":{"InfoList":[{"FileId":9,"ParentFileId":%s,"FileName":"sample.zip","Type":0,"Size":15}],"Next":"9"}}`, parent)
		case trafficPath:
			if r.Method != http.MethodPost {
				http.Error(w, "POST required", 405)
				return
			}
			fmt.Fprintf(w, `{"code":0,"data":{"isBlocked":true,"isTrafficExceeded":%t,"cashierEnv":"e1","hasOrdinaryAction":%t}}`, mode.Load() == 1, mode.Load() != 3)
		case grantPath:
			grants.Add(1)
			id := 9
			if mode.Load() == 2 {
				id = 10
			}
			fmt.Fprintf(w, `{"code":0,"data":{"fileId":%d,"downloadPath":"/sample.zip?signature=fixture","dispatchList":[{"prefix":"https://body.example"}]}}`, id)
		default:
			http.NotFound(w, r)
		}
	}))
	defer app.Close()
	opts := append(chromedp.DefaultExecAllocatorOptions[:], chromedp.ExecPath(binary),
		chromedp.UserDataDir(t.TempDir()), chromedp.Flag("ignore-certificate-errors", true))
	if strings.Contains(strings.ToLower(binary), "vivaldi") {
		opts = append(opts, chromedp.Flag("disable-vivaldi", true))
	}
	allocator, closeAllocator := chromedp.NewExecAllocator(t.Context(), opts...)
	defer closeAllocator()
	tab, closeTab := chromedp.NewContext(allocator)
	defer closeTab()
	if err := chromedp.Do(tab); err != nil {
		t.Fatal(err)
	}

	s := New(tab)
	s.origin = app.URL
	list, err := s.List(t.Context(), "7", "")
	if err != nil || list.Account != "19" || list.Next != "9" || len(list.Items) != 1 || list.Items[0].ParentID != "7" {
		t.Fatalf("first page not bound: %v", err)
	}
	list, err = s.List(t.Context(), "7", "9")
	if err != nil || list.Next != "" || len(list.Items) != 1 || list.Items[0].ID != "10" {
		t.Fatalf("second page not bound: %v", err)
	}
	file := pan123.WebsiteFile{Account: "19", ID: "9", Name: "sample.zip", Size: 15}
	grant, err := s.Download(t.Context(), "0", file)
	if err != nil || grant.Account != file.Account || grant.FileID != file.ID || grant.URL != "https://body.example/sample.zip?signature=fixture" {
		t.Fatalf("ordinary file grant not bound: %v", err)
	}
	mode.Store(1)
	if refused, err := s.Download(t.Context(), "0", file); refused.URL != "" || core.CodeOf(err) != core.UpstreamAccessDenied || grants.Load() != 1 {
		t.Fatalf("traffic limit was not enforced before grant: %v", err)
	}
	mode.Store(2)
	if wrong, err := s.Download(t.Context(), "0", file); wrong.URL != "" || core.CodeOf(err) != core.InvalidUpstreamResponse || grants.Load() != 2 {
		t.Fatalf("mismatched receipt was accepted: %v", err)
	}
	mode.Store(3)
	if closed, err := s.Download(t.Context(), "0", file); closed.URL != "" || core.CodeOf(err) != core.UpstreamAccessDenied || grants.Load() != 2 {
		t.Fatalf("an unlabeled continuation activated the close button: %v", err)
	}
}

const fixturePage = `<!doctype html><meta charset="utf-8"><style>
	.ant-table-body {height:100px;overflow:auto}
	.spacer {height:200px}
	.measuring-container {position:fixed;left:-1000px}
	#offer:not([hidden]) {animation: enter 250ms linear}
	@keyframes enter {from {transform:translateX(150px)} to {transform:translateX(0)}}
</style>
<div class="file-table-list-wrapper"><div id="scroller" class="ant-table-body">
	<div data-row-key="9"><label><input type="checkbox">sample.zip</label></div>
	<div class="spacer"></div>
</div></div>
<div class="table-head-title"><span id="selected"></span></div>
<div class="file-operator-group">
	<button id="download" class="file-operator-group-button" hidden>下载</button>
	<div class="measuring-container"><button class="file-operator-group-button">下载</button></div>
</div>
<div id="offer" class="scheme-e-vip-modal-wrap" hidden>
	<button id="close" class="site-modal-close"><svg width="20" height="20"><use href="#general_close_16_11"/></svg></button>
	<div id="continue" class="scheme-e-vip-modal-footer">继续普通下载</div>
</div>
<script>
	const parent = new URLSearchParams(location.search).get("homeFilePath") || "0";
	const list = marker => fetch("/b/api/file/list/new?parentFileId="+parent+"&next="+marker).then(r=>r.json());
	const post = (path, data) => fetch(path, {method:"POST", body:JSON.stringify(data)});
	fetch("/b/api/user/info").then(r=>r.json()); list("0");
	const scroller = document.getElementById("scroller");
	scroller.addEventListener("scroll", () => {
		if (scroller.scrollTop >= 100) list("9");
	}, {once:true});
	const checkbox = document.querySelector('input');
	const download = document.getElementById("download");
	const offer = document.getElementById("offer");
	checkbox.onchange = () => {
		document.getElementById("selected").textContent = checkbox.checked ? "已选择1项" : "";
		download.hidden = !checkbox.checked;
	};
	download.onclick = async () => {
		const reply = await post("/b/api/file/download/traffic/check", {fids:[9]});
		const traffic = await reply.json();
		offer.hidden = !traffic.data.isBlocked;
		document.getElementById("continue").hidden = !traffic.data.hasOrdinaryAction;
	};
	document.getElementById("continue").onclick = document.getElementById("close").onclick = () => post("/b/api/v2/file/download_info", {fileId:9}).then(r=>r.json());
</script>`
