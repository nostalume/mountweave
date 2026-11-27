package baiducdp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/baidupan"
)

func TestOrdinaryDownload(t *testing.T) {
	binary := os.Getenv("MWEAVE_TEST_BROWSER")
	if binary == "" {
		t.Skip("set MWEAVE_TEST_BROWSER to a local Chromium executable")
	}
	const id uint64 = 9007199254740993
	const data = "original sample"
	var installers atomic.Int32
	content := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/first":
			if r.URL.Query().Get("response-cache-control") != "private" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			http.Redirect(w, r, "/file", http.StatusFound)
		case "/file", "/installer":
			w.Header().Set("Content-Disposition", `attachment; filename="sample.zip"`)
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			if r.URL.Path == "/installer" {
				installers.Add(1)
				_, _ = io.WriteString(w, strings.Repeat("x", len(data)))
			} else {
				_, _ = io.WriteString(w, data)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer content.Close()
	var wrongGrant atomic.Bool
	var grants atomic.Int32
	app := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case drivePath:
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			fmt.Fprintf(w, `<!doctype html><div class="nd-download__guide-dialog" hidden>
				<span class="nd-download__guide-title-text" id="name">sample.zip</span>
				<div class="nd-download__guide-actions"><button class="is-normal" id="ordinary"><span class="nd-download__guide-action-item-title">普通下载</span></button></div>
				<button id="client"><span class="nd-download__guide-action-item-title">客户端高速下载</span></button>
				</div><div id="verify" class="nd-verify-code__body" hidden>请输入验证码：</div><button id="toolbar">下载</button>
				<table><tr class="wp-s-pan-table__body-row"><td><a class="wp-s-pan-list__file-name-title-text"> sample.zip </a>
				<button id="file-download" class="wp-s-agile-tool-bar__h-action-button" title="下载">下载</button></td></tr></table>
				<div class="wp-s-pan-table__body" style="height:10px;overflow:auto"><div style="height:100px"></div></div>
				<script>
				window.locals={isLogin:1,uk:"19"};
				const listBody = document.querySelector(".wp-s-pan-table__body");
				const ordinary = document.getElementById("ordinary");
				const dialog = ordinary.closest(".nd-download__guide-dialog");
				const verify = document.getElementById("verify");
				let verifyAfterClick = false;
				window.clientClicks = 0;
				window.fileClicks = 0;
				document.getElementById("client").onclick = () => window.clientClicks++;
				document.getElementById("toolbar").onclick = () => window.clientClicks++;
				window.testSurface = state => {
					dialog.hidden = state === "verify" || state === "unoffered";
					verify.hidden = state !== "verify";
					ordinary.hidden = state === "client";
					const large = state === "large";
					dialog.className = large ? "nd-download__big-file-dialog" : "nd-download__guide-dialog";
					const name = document.getElementById("name");
					name.className = large ? "nd-download__big-file-title-text" : "nd-download__guide-title-text";
					name.textContent = state === "wrong_file" ? "different.zip" : "sample.zip";
					ordinary.className = large ? "nd-download__big-file-btn" : "is-normal";
					ordinary.firstElementChild.textContent = large ? "使用浏览器下载" : "普通下载";
					verifyAfterClick = state === "verify_after_click";
				};
				document.getElementById("file-download").onclick = () => {
					window.fileClicks++;
					testSurface(sessionStorage.getItem("download-case") || "ordinary");
				};
				let currentDir, pageNumber;
				const list = () => {
					dialog.hidden = true;
					const query = new URLSearchParams({page:pageNumber,num:100});
					if (currentDir !== "/") query.set("dir",currentDir);
					return fetch("/api/list?"+query).then(response=>response.json());
				};
				const firstPage = () => {
					currentDir = new URLSearchParams(location.hash.split("?")[1]).get("path") || "/";
					pageNumber = 1;
					listBody.scrollTop = 0;
					list();
				};
				addEventListener("hashchange",firstPage);
				firstPage();
				listBody.onscroll = () => {
					if (pageNumber===1 && currentDir==="/many" && listBody.scrollTop>0) {
						pageNumber = 2;
						list();
					}
				};
				ordinary.onclick=async ()=>{
					dialog.hidden = true;
					if (verifyAfterClick) { verify.hidden = false; return; }
					const response=await fetch("/api/download?type=dlink&fidlist="+encodeURIComponent("[%d]"));
					const grant=await response.json();
					const iframe=document.createElement("iframe");
					iframe.src=grant.dlink[0].dlink+"&response-cache-control=private";
					document.body.appendChild(iframe);
				};
				</script>`, id)
		case listPath:
			dir, page := r.URL.Query().Get("dir"), r.URL.Query().Get("page")
			if r.Method != http.MethodGet || r.URL.Query().Get("num") != "100" ||
				(dir != "" && dir != "/many") || (page != "1" && (dir != "/many" || page != "2")) {
				http.Error(w, "unexpected directory request", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			if dir == "" {
				_, _ = io.WriteString(w, `{"errno":0,"list":[{"fs_id":101,"server_filename":"many","path":"/many","isdir":1}]}`)
				return
			}
			if dir == "/many" && page == "1" {
				_, _ = io.WriteString(w, `{"errno":0,"list":[`)
				for i := 1; i <= 100; i++ {
					if i != 1 {
						_, _ = io.WriteString(w, ",")
					}
					fmt.Fprintf(w, `{"fs_id":%d,"server_filename":"file-%d","path":"/many/file-%d","isdir":0,"size":0}`, i, i, i)
				}
				_, _ = io.WriteString(w, `]}`)
				return
			}
			fmt.Fprintf(w, `{"errno":0,"list":[{"fs_id":%d,"server_filename":"sample.zip","path":%q,"isdir":0,"size":%d}]}`,
				id, dir+"/sample.zip", len(data))
		case "/api/download":
			grants.Add(1)
			if r.URL.Query().Get("fidlist") != "[9007199254740993]" || r.URL.Query().Get("type") != "dlink" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			grantID := id
			if wrongGrant.Load() {
				grantID--
			}
			fmt.Fprintf(w, `{"errno":0,"dlink":[{"dlink":%q,"fs_id":%d}]}`, content.URL+"/first?t=fixture", grantID)
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
	allocator, stop := chromedp.NewExecAllocator(t.Context(), opts...)
	defer stop()
	browser, closeBrowser := chromedp.NewContext(allocator)
	defer closeBrowser()
	if err := chromedp.Do(browser); err != nil {
		t.Fatal(err)
	}
	tab, closeTab := chromedp.NewContext(browser, chromedp.WithNewBrowserContext())
	defer closeTab()
	run, done := context.WithTimeout(tab, 25*time.Second)
	defer done()
	if err := chromedp.Do(run, chromedp.Navigate(app.URL+drivePath)); err != nil {
		t.Fatal(err)
	}
	if chromedp.FromContext(tab).BrowserContextID == "" {
		t.Fatal("fixture did not initialize a nondefault browser context")
	}
	session := NewWithGuardedDownloads(tab)
	session.origin = app.URL
	account, err := session.Account(run)
	if err != nil || account == "" {
		t.Fatalf("account receipt invalid: %v", err)
	}
	first, err := session.ReadPage(run, "/many", 1)
	if err != nil || first.Directory != "/many" || first.Account != account || first.Number != 1 {
		t.Fatalf("first native list receipt unavailable: %v", err)
	}
	second, err := session.ReadPage(run, "/many", 2)
	if err != nil || second.Directory != "/many" || second.Account != account || second.Number != 2 ||
		!strings.Contains(string(second.Body), "/many/sample.zip") {
		t.Fatalf("second native list receipt unavailable: %v", err)
	}
	if _, err := session.ReadPage(run, "/many", 3); core.CodeOf(err) != core.InvalidRequest {
		t.Fatalf("short native page admitted a continuation: %v", err)
	}
	native, err := baidupan.OpenWebsite(run, baidupan.WebsiteOptions{
		Account: account, RootPath: "/",
		ReadPage: session.ReadPage,
		ReadFile: session.ReadOrdinary,
		Release:  func() error { closeTab(); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	storage, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, core.Address) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close(run)
	if _, err := storage.Install("/disk", native); err != nil {
		t.Fatal(err)
	}
	filePath := core.Path("/disk/many/sample.zip")
	opened, err := storage.Open(run, "test", filePath, core.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wait, cancel := context.WithTimeout(run, 100*time.Millisecond)
	_, listErr := session.ReadPage(wait, "/", 1)
	cancel()
	if !errors.Is(listErr, context.DeadlineExceeded) {
		t.Fatalf("directory navigation took the active response's tab: %v", listErr)
	}
	bytes, err := io.ReadAll(opened.Body)
	if err := errors.Join(err, opened.Body.Close()); err != nil || string(bytes) != data {
		t.Fatalf("complete original not delivered: %q, %v", bytes, err)
	}
	if calls, err := chromedp.Run(run, chromedp.Evaluate[[]int]("[fileClicks,clientClicks]")); err != nil ||
		len(calls) != 2 || calls[0] != 1 || calls[1] != 0 {
		t.Fatalf("automatic file action did not stay on the verified row: %v, %v", calls, err)
	}
	unavailable := core.Fail(core.UnsupportedOperation, errors.New("ordinary browser download unavailable"))
	file := baidupan.WebsiteFile{Account: account, ID: id, Path: "/many/sample.zip", Size: int64(len(data))}
	if err := chromedp.Do(run, chromedp.Evaluate[chromedp.Void](`void testSurface("ordinary")`)); err != nil {
		t.Fatal(err)
	}
	if download, err := New(tab).ReadOrdinary(run, file); download.Response != nil ||
		core.CodeOf(err) != core.UnsupportedOperation || grants.Load() != 1 {
		t.Fatalf("an unguarded session adopted the existing dialog: %v", err)
	}
	_, err = session.CaptureOriginal(run, file, func(context.Context) error { return unavailable })
	if !errors.Is(err, unavailable) {
		t.Fatalf("website outcome lost after the previous stream closed: %v", err)
	}
	if err := chromedp.Do(run, chromedp.Evaluate[chromedp.Void](`testSurface("unoffered"); sessionStorage.setItem("download-case","large")`)); err != nil {
		t.Fatal(err)
	}
	large, err := session.ReadOrdinary(run, file)
	if err != nil {
		t.Fatal(err)
	}
	bytes, err = io.ReadAll(large.Response.Body)
	if err := errors.Join(err, large.Response.Body.Close()); err != nil || string(bytes) != data {
		t.Fatalf("browser download choice did not return the original: %q, %v", bytes, err)
	}
	for _, branch := range []struct {
		state, reason string
		code          core.Code
	}{
		{"verify", "interactive download verification", core.UpstreamAccessDenied},
		{"verify_after_click", "interactive download verification", core.UpstreamAccessDenied},
		{"client", "desktop client download only", core.UnsupportedOperation},
		{"wrong_file", "different file or selection", core.InvalidUpstreamResponse},
	} {
		if err := chromedp.Do(run, chromedp.Evaluate[chromedp.Void](fmt.Sprintf(`testSurface("unoffered"); sessionStorage.setItem("download-case",%q)`, branch.state))); err != nil {
			t.Fatal(err)
		}
		if opened, err := session.ReadOrdinary(run, file); opened.Response != nil || core.CodeOf(err) != branch.code ||
			!strings.Contains(err.Error(), branch.reason) || grants.Load() != 2 {
			t.Fatalf("%s did not stop early with its own outcome: %v", branch.state, err)
		}
	}
	if count, err := chromedp.Run(run, chromedp.Evaluate[int]("window.clientClicks")); err != nil || count != 0 {
		t.Fatalf("desktop client choice was clicked: %d, %v", count, err)
	}
	// A failed client invocation can yield an unrelated attachment.
	read, stopRead := context.WithTimeout(run, 2*time.Second)
	installer, err := session.CaptureOriginal(read, file, func(ctx context.Context) error {
		return chromedp.Do(ctx, chromedp.Evaluate[chromedp.Void](fmt.Sprintf(`(()=>{
			const frame=document.createElement("iframe"); frame.src=%q; document.body.append(frame);
		})()`, content.URL+"/installer")))
	})
	stopRead()
	if !errors.Is(err, context.DeadlineExceeded) || installer.Response != nil || installers.Load() != 1 || grants.Load() != 2 {
		if installer.Response != nil {
			_ = installer.Response.Body.Close()
		}
		t.Fatalf("installer attachment was not distinguished from an original grant: %v", err)
	}
	if err := chromedp.Do(run, chromedp.Evaluate[chromedp.Void](`sessionStorage.removeItem("download-case")`)); err != nil {
		t.Fatal(err)
	}
	wrongGrant.Store(true)
	if failed, err := storage.Open(run, "test", filePath, core.OpenOptions{}); core.CodeOf(err) != core.InvalidUpstreamResponse || failed.Body != nil {
		t.Fatalf("wrong grant exposed a response: %v", err)
	}
}
