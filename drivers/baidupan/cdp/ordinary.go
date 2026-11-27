package baiducdp

import (
	"context"
	"errors"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/input"
	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
	transport "github.com/nostalume/mountweave/browser"
	"github.com/nostalume/mountweave/drivers/baidupan"
)

// OpenGuardedSession prepares a dedicated profile before starting the browser.
// The host supplies exclusive profile access and a launcher that applies the
// options and runs the check on the new tab before navigating to WebsiteURL.
// The returned release closes both the tab and its proxy.
func OpenGuardedSession(ctx context.Context, profile string, options []chromedp.ExecAllocatorOption,
	open func([]chromedp.ExecAllocatorOption, func(context.Context) error) (context.Context, func() error, error),
) (*Session, func() error, error) {
	if open == nil {
		return nil, nil, core.Fail(core.InvalidRequest, nil)
	}
	if err := transport.PrepareHTTPSOnlyProfile(profile); err != nil {
		return nil, nil, err
	}
	proxy, stopProxy, err := transport.StartHTTPSProxy(context.WithoutCancel(ctx))
	if err != nil {
		return nil, nil, err
	}
	options = append(slices.Clone(options), chromedp.Flag("proxy-server", proxy), chromedp.Flag("proxy-bypass-list", "<-loopback>"),
		chromedp.Flag("no-proxy-server", false), chromedp.Flag("disable-quic", true))
	tab, release, err := open(options, transport.CheckHTTPSOnlyNavigation)
	if err == nil && (tab == nil || release == nil) {
		err = core.Fail(core.InvalidRequest, errors.New("guarded browser launcher must return a tab and release"))
	}
	if err != nil {
		if release != nil {
			err = errors.Join(err, release())
		}
		return nil, nil, errors.Join(err, stopProxy())
	}
	return NewWithGuardedDownloads(tab), sync.OnceValue(func() error { return errors.Join(release(), stopProxy()) }), nil
}

// ReadOrdinary starts the site's file action in a guarded session and streams
// the exact original. The command must initiate its own download.
func (s *Session) ReadOrdinary(ctx context.Context, file baidupan.WebsiteFile) (baidupan.WebsiteDownload, error) {
	if core.ValidatePath(file.Path) != nil || file.Path == "/" {
		return baidupan.WebsiteDownload{}, core.Fail(core.InvalidRequest, nil)
	}
	if !s.guardedDownloads {
		return baidupan.WebsiteDownload{}, core.Fail(core.UnsupportedOperation, errors.New("Baidu website download requires a guarded session"))
	}
	name, dir := path.Base(file.Path), path.Dir(file.Path)
	var fragment string
	return s.captureOriginal(ctx, file, func(call context.Context) error {
		frame := topFrame(call)
		if !s.onDirectory(frame, dir) {
			return core.Fail(core.InvalidUpstreamResponse, errors.New("Baidu download action is outside the verified directory"))
		}
		fragment = strings.TrimPrefix(frame.URLFragment, "#")
		return s.startFileDownload(call, name, fragment)
	}, func(call context.Context) error {
		tick := time.NewTicker(125 * time.Millisecond)
		defer tick.Stop()
		selected := name
		for {
			state, err := s.ordinarySurface(call, selected, fragment)
			if err != nil {
				return err
			}
			if err := downloadBlocker(state); err != nil {
				return err
			}
			if state == "ordinary" {
				selected = ""
			}
			select {
			case <-call.Done():
				return call.Err()
			case <-tick.C:
			}
		}
	})
}

func downloadBlocker(state string) error {
	switch state {
	case "verify":
		return core.Fail(core.UpstreamAccessDenied, errors.New("Baidu requires interactive download verification"))
	case "client":
		return core.Fail(core.UnsupportedOperation, errors.New("Baidu offers desktop client download only"))
	case "wrong_file":
		return core.Fail(core.InvalidUpstreamResponse, errors.New("Baidu download dialog names a different file or selection"))
	}
	return nil
}

func (s *Session) ordinarySurface(ctx context.Context, name, fragment string) (string, error) {
	script := downloadSurface + "(" + strconv.Quote(s.origin) + "," + strconv.Quote(name) + "," + strconv.Quote(fragment) + ")"
	return chromedp.Run(ctx, chromedp.Evaluate[string](script))
}

func (s *Session) startFileDownload(ctx context.Context, name, fragment string) error {
	locate, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	args := strconv.Quote(s.origin) + "," + strconv.Quote(name) + "," + strconv.Quote(fragment)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		point, err := chromedp.Run(locate, chromedp.Evaluate[[]float64](fileDownloadAction+"("+args+",false)"))
		if err != nil {
			return err
		}
		if len(point) == 2 {
			if _, err := chromedp.Call(locate, input.DispatchMouseEvent, input.DispatchMouseEventParams{Type: input.DispatchMouseEventTypeMouseMoved, X: point[0], Y: point[1]}); err != nil {
				return err
			}
			clicked, err := chromedp.Run(locate, chromedp.Evaluate[bool](fileDownloadAction+"("+args+",true)"))
			if err != nil || clicked {
				return err
			}
		}
		select {
		case <-locate.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return core.Fail(core.UnsupportedOperation, errors.New("Baidu did not expose a download action for the verified file"))
		case <-tick.C:
		}
	}
}

const fileDownloadAction = `((origin, name, fragment, click) => {
	const absent = click ? false : [];
	if (location.origin !== origin || location.pathname !== "/disk/main" || location.hash.slice(1) !== fragment || window !== top) return absent;
	const visible = element => element.getClientRects().length && getComputedStyle(element).visibility === "visible" &&
		!element.closest('[hidden],[aria-hidden=true],[inert]');
	const titles = [...document.querySelectorAll(".wp-s-pan-table__body-row .wp-s-pan-list__file-name-title-text")]
		.filter(e => visible(e) && (e.title === name || e.getAttribute('aria-label') === name || e.textContent.trim() === name));
	if (titles.length !== 1) return absent;
	const title = titles[0];
	if (!click) {
		title.scrollIntoView({block:"nearest",inline:"nearest"});
		const bounds = title.getBoundingClientRect();
		return [bounds.x+bounds.width/2, bounds.y+bounds.height/2];
	}
	const actions = [...title.closest(".wp-s-pan-table__body-row").querySelectorAll('.wp-s-agile-tool-bar__h-action-button[title="下载"],.nd-file-list__line-action-item[title="下载"]')]
		.filter(e => visible(e) && !e.closest('[disabled],[aria-disabled=true]'));
	if (actions.length !== 1) return false;
	const button = actions[0];
	const bounds = button.getBoundingClientRect();
	if (!button.contains(document.elementFromPoint(bounds.x+bounds.width/2,bounds.y+bounds.height/2))) return false;
	button.click();
	return true;
})`

// With no expected file this is read-only. Otherwise match and activate the
// same element in one JS task so a rerender cannot move a client choice under
// a coordinate recorded by an earlier evaluation.
const downloadSurface = `((origin, expected, fragment) => {
	if (location.origin !== origin || location.pathname !== "/disk/main" || window !== window.top) return "";
	if (expected && location.hash.slice(1) !== fragment) return "wrong_file";
	const visible = e => e.getClientRects().length && getComputedStyle(e).visibility === "visible" &&
		!e.closest('[hidden],[aria-hidden=true],[inert]');
	if ([...document.querySelectorAll(".nd-verify-code__body")].some(visible)) return "verify";
	const dialogs = [...document.querySelectorAll(".nd-download__guide-dialog,.nd-download__big-file-dialog")].filter(visible);
	if (dialogs.length !== 1) return "";
	const dialog = dialogs[0];
	const titles = [...dialog.querySelectorAll(".nd-download__guide-title-text,.nd-download__big-file-title-text")].filter(visible);
	if (expected && (titles.length !== 1 || (titles[0].title !== expected && titles[0].textContent.trim() !== expected))) return "wrong_file";
	const options = [...dialog.querySelectorAll(".nd-download__guide-actions .is-normal,.nd-download__big-file-btn")].filter(visible);
	const ordinary = options.filter(e => {
		const label = e.querySelector(".nd-download__guide-action-item-title");
		return ["普通下载","使用浏览器下载"].includes(label?.textContent.trim());
	});
	if (!ordinary.length) {
		const client = [...dialog.querySelectorAll(".nd-download__guide-action-item-title")].filter(visible);
		if (client.some(e => ["客户端高速下载","使用客户端下载"].includes(e.textContent.trim()))) return "client";
		return "";
	}
	if (!expected || ordinary.length !== 1) return "";
	const button = ordinary[0];
	if (button.matches(":disabled,[aria-disabled=true]")) return "";
	const bounds = button.getBoundingClientRect();
	const X = bounds.left + bounds.width / 2, Y = bounds.top + bounds.height / 2;
	if (!button.contains(document.elementFromPoint(X,Y)) || typeof button.click !== "function") return "";
	button.click();
	return "ordinary";
})`
