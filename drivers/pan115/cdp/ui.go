package pan115cdp

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/kb"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/pan115"
)

type control struct {
	X, Y float64
}

func (p control) visible() bool { return p.X > 0 && p.Y > 0 }

func (p control) click(ctx context.Context) error {
	return chromedp.Do(ctx, chromedp.MouseClickXY(p.X, p.Y))
}

type fileSelection struct {
	Row, Toolbar control
	Selected     int
	Bound        bool
}

func selection(ctx context.Context, file pan115.WebsiteItem) (fileSelection, error) {
	script := selectFile + "(" + strconv.Quote(file.ID) + "," + strconv.Quote(file.Name) + ")"
	return chromedp.Run(ctx, chromedp.Evaluate[fileSelection](script))
}

func pauseForPage(ctx context.Context) error {
	timer := time.NewTimer(125 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func jumpToPage(ctx context.Context, page int64) error {
	for {
		point, err := chromedp.Run(ctx, chromedp.Evaluate[control](pageInput+"()"))
		if err != nil {
			return err
		}
		if point.visible() {
			return chromedp.Do(ctx, chromedp.MouseClickXY(point.X, point.Y), chromedp.KeyEvent(strconv.FormatInt(page, 10)+kb.Enter))
		}
		if err := pauseForPage(ctx); err != nil {
			return err
		}
	}
}

func prepareFile(ctx context.Context, file pan115.WebsiteItem, index int) (control, error) {
	clicked := false
	for {
		state, err := selection(ctx, file)
		if err != nil {
			return control{}, err
		}
		if state.Selected > 1 || state.Selected == 1 && !state.Bound {
			return control{}, core.Fail(core.InvalidUpstreamResponse, errors.New("115 selected a different file"))
		}
		if state.Bound && state.Selected == 1 && state.Toolbar.visible() {
			if err := state.Toolbar.click(ctx); err != nil {
				return control{}, err
			}
			break
		}
		if !clicked && state.Selected == 0 && state.Row.visible() {
			if err := state.Row.click(ctx); err != nil {
				return control{}, err
			}
			clicked = true
		} else if !clicked && state.Selected == 0 {
			script := revealFile + "(" + strconv.Quote(file.ID) + "," + strconv.Itoa(index) + ")"
			if _, err := chromedp.Run(ctx, chromedp.Evaluate[bool](script)); err != nil {
				return control{}, err
			}
		}
		if err := pauseForPage(ctx); err != nil {
			return control{}, err
		}
	}
	dismissed := false
	for {
		state, err := chromedp.Run(ctx, chromedp.Evaluate[struct {
			Ordinary, Offer control
			Denied          bool
		}](downloadOptions+"()"))
		if err != nil {
			return control{}, err
		}
		if state.Denied {
			return control{}, core.Fail(core.UpstreamAccessDenied, errors.New("115 reported a restriction on ordinary downloading"))
		}
		if state.Ordinary.visible() {
			chosen, err := selection(ctx, file)
			if err != nil {
				return control{}, err
			}
			if !chosen.Bound || chosen.Selected != 1 {
				return control{}, core.Fail(core.InvalidUpstreamResponse, errors.New("115 changed the file selection before downloading"))
			}
			return state.Ordinary, nil
		}
		if !dismissed && state.Offer.visible() {
			if err := state.Offer.click(ctx); err != nil {
				return control{}, err
			}
			dismissed = true
		}
		if err := pauseForPage(ctx); err != nil {
			return control{}, err
		}
	}
}

const controlPoint = `e => {
	if (!e || e.matches(":disabled,[aria-disabled=true]") || e.closest("[inert]")) return {};
	const r = e.getBoundingClientRect();
	for (const y of [0.5, 0.25, 0.75]) for (const x of [0.5, 0.25, 0.75]) {
		const X = Math.round(r.left + r.width*x), Y = Math.round(r.top + r.height*y);
		if (X>0 && Y>0 && e.contains(document.elementFromPoint(X,Y))) return {X,Y};
	}
	return {};
}`

// The site's desktop file-list pager uses a single GO field and accepts Enter.
const pageInput = `(() => {
	if (location.origin !== "` + Origin + `" || location.pathname !== "` + directoryPath + `" || window !== window.top) return {};
	const point = ` + controlPoint + `;
	const fields = [...document.querySelectorAll('input[type="text"][placeholder="GO"]')]
		.filter(e => !e.value).map(point).filter(p => p.X);
	return fields.length === 1 ? fields[0] : {};
})`

const revealFile = `((id, index) => {
	if (location.origin !== "` + Origin + `" || location.pathname !== "` + directoryPath + `" || window !== window.top) return false;
	const rows = [...document.querySelectorAll("[data-file-id]")];
	const row = rows.find(e => e.dataset.fileId === id);
	if (row) { row.scrollIntoView({block:"nearest"}); return true; }
	// The list view may only render rows around the current viewport.
	const [a,b] = rows.filter(e => e.matches(".file-list-item[data-index]"));
	if (!a || !b || b.dataset.index - a.dataset.index !== 1) return false;
	const stride = b.getBoundingClientRect().top - a.getBoundingClientRect().top;
	if (stride <= 0) return false;
	for (let scroller = a.parentElement; scroller; scroller = scroller.parentElement) {
		if (!/^(auto|scroll)$/.test(getComputedStyle(scroller).overflowY) || scroller.scrollHeight <= scroller.clientHeight) continue;
		scroller.scrollTop += a.getBoundingClientRect().top - scroller.getBoundingClientRect().top + stride * (index - Number(a.dataset.index));
		return true;
	}
	return false;
})`

const selectFile = `((id, name) => {
	if (location.origin !== "` + Origin + `" || location.pathname !== "/storage/allfiles" || window !== window.top) return {};
	const rows = [...document.querySelectorAll("[data-file-id]")];
	const found = rows.filter(e => e.dataset.fileId === id && [...e.querySelectorAll("[title],span,a")].some(
		n => n.getAttribute("title") === name || n.textContent.trim() === name));
	if (found.length !== 1) return {};
	const row = found[0], selector = 'input[type="checkbox"],[role="checkbox"]';
	const checked = e => e.checked === true || e.getAttribute("aria-checked") === "true" || e.getAttribute("data-state") === "checked";
	const checks = [...row.querySelectorAll(selector)];
	const point = ` + controlPoint + `;
	const options = checks.map(e => point(e.labels?.[0] || e)).filter(p => p.X);
	const toolbar = [...document.querySelectorAll("button,[role=button],a")].filter(e => !e.closest("[data-file-id]") &&
		(e.getAttribute("title") || e.getAttribute("aria-label") || e.innerText || "").trim() === "下载").map(point).filter(p => p.X);
	return {Row:options.length === 1 ? options[0] : {}, Toolbar:toolbar.length === 1 ? toolbar[0] : {},
		Bound:checks.some(checked), Selected:rows.filter(e => [...e.querySelectorAll(selector)].some(checked)).length};
})`

const downloadOptions = `(() => {
	if (location.origin !== "` + Origin + `" || location.pathname !== "/storage/allfiles" || window !== window.top) return {Denied:true};
	const point = ` + controlPoint + `, dialog = '[role="dialog"],dialog';
	const visible = e => e.getClientRects().length && getComputedStyle(e).visibility === "visible";
	const label = e => (e.getAttribute("aria-label") || e.getAttribute("title") || e.innerText || "").trim();
	const dialogs = [...document.querySelectorAll(dialog)].filter(visible);
	if (dialogs.some(e => /安全验证|验证码|身份验证|该功能需要VIP|下载额度不足/.test(e.innerText))) return {Denied:true};
	const choices = [...document.querySelectorAll('button[data-download-method="ordinary"]')].filter(e =>
		visible(e) && e.closest(dialog) && e.textContent.includes("普通下载"));
	const Ordinary = choices.length === 1 ? point(choices[0]) : {};
	if (Ordinary.X) return {Ordinary};
	const offers = dialogs.filter(e => label(e) === "VIP专享立减" ||
		e.querySelector("h1,h2,h3,h4,[role=heading]")?.innerText.trim() === "VIP专享立减");
	if (offers.length !== 1) return {};
	const closes = [...offers[0].querySelectorAll("button,[role=button]")].filter(e =>
		e.closest(dialog) === offers[0] && visible(e) && label(e) === "关闭").map(point).filter(p => p.X);
	return {Offer:closes.length === 1 ? closes[0] : {}};
})`
