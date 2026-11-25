package pan123cdp

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
)

type point struct {
	X, Y  float64
	Ready bool
}

func control(ctx context.Context, origin, action, id string) error {
	_, err := findControl(ctx, origin, action, id)
	return err
}

func click(ctx context.Context, origin, action, id string) error {
	p, err := findControl(ctx, origin, action, id)
	if err != nil {
		return err
	}
	if err := chromedp.Do(ctx, chromedp.MouseClickXY(p.X, p.Y)); err != nil {
		return core.Fail(core.UpstreamFailure, errors.New("123 Pan website action could not be invoked"))
	}
	return nil
}

func findControl(ctx context.Context, origin, action, id string) (point, error) {
	args, _ := json.Marshal([]string{origin, action, id})
	script := controlScript + "..." + string(args) + ")"
	wait, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var previous point
	for {
		p, err := chromedp.Run(wait, chromedp.Evaluate[point](script))
		// The promotional dialog moves as it opens. Wait for the hit point to
		// settle before sending the single ordinary-download click.
		stable := previous.Ready && math.Abs(previous.X-p.X) < 1 && math.Abs(previous.Y-p.Y) < 1
		if err == nil && p.Ready && (action != "ordinary" || stable) {
			return p, nil
		}
		previous = p
		if err != nil || chromedp.Do(wait, chromedp.Sleep(100*time.Millisecond)) != nil {
			if action == "ordinary" && ctx.Err() == nil && wait.Err() == context.DeadlineExceeded {
				return point{}, core.Fail(core.UpstreamAccessDenied, errors.New("123 Pan did not offer ordinary download"))
			}
			return point{}, core.Fail(core.UpstreamFailure, errors.New("123 Pan website control was not available: "+action))
		}
	}
}

const signInScript = `[...document.querySelectorAll("button,input,[role=button]")].some(el =>
	el.getClientRects().length && getComputedStyle(el).visibility === "visible" &&
	(el.type === "password" || /^(登录|立即登录|扫码登录)$/.test(el.textContent.trim())))`

const controlScript = `((origin, action, id) => {
	if (location.origin !== origin || window !== window.top) return {};
	const visible = el => el?.getClientRects().length && getComputedStyle(el).visibility === "visible";
	const text = el => el.textContent.replace(/\s/g,"");
	const hit = el => {
		if (!visible(el) || el.matches(":disabled,.disabled,[aria-disabled=true]")) return {};
		const r = el.getBoundingClientRect();
		const x = r.x + r.width/2, y = r.y + r.height/2;
		return el.contains(document.elementFromPoint(x,y)) ? {X:x,Y:y,Ready:true} : {};
	};
	const scroller = [...document.querySelectorAll(
		".file-table-list-wrapper .ant-table-tbody-virtual-holder, .file-table-list-wrapper .ant-table-body, .file-table-list-body"
	)].find(el => visible(el) && el.scrollHeight > el.clientHeight && /auto|scroll/.test(getComputedStyle(el).overflowY));
	if (action === "end") {
		if (!scroller) return {};
		scroller.scrollTop = scroller.scrollHeight;
		return {Ready:true};
	}
	if (action === "ordinary") {
		const ordinary = [...document.querySelectorAll(".scheme-e-vip-modal-footer")].filter(
			el => text(el) === "继续普通下载"
		).map(hit).filter(p => p.Ready);
		return ordinary.length === 1 ? ordinary[0] : {};
	}
	const row = document.querySelector('.file-table-list-wrapper [data-row-key="'+id+'"], [data-file="'+id+'"]');
	if (!visible(row)) {
		if (action === "checkbox" && scroller) {
			const bottom = scroller.scrollHeight - scroller.clientHeight;
			scroller.scrollTop = scroller.scrollTop >= bottom - 1 ? 0 : scroller.scrollTop + scroller.clientHeight * .8;
		}
		return {};
	}
	const checkbox = row.querySelector('input[type="checkbox"],[role="checkbox"]');
	if (!checkbox) return {};
	const checked = checkbox.checked || checkbox.getAttribute("aria-checked") === "true";
	if (action === "checkbox") {
		if (checked) return {};
		row.scrollIntoView({block:"nearest", inline:"nearest"});
		return hit(checkbox.closest("label") || checkbox);
	}
	if (action !== "download" || !checked) return {};
	const selected = [...document.querySelectorAll(".table-head-title > span, [class$='-check-all']")].some(
		el => visible(el) && text(el) === "已选择1项"
	);
	if (!selected) return {};
	const buttons = [...document.querySelectorAll(".file-operator-group > .file-operator-group-button")].filter(
		el => visible(el) && !el.closest(".measuring-container") && text(el) === "下载"
	);
	return buttons.length === 1 ? hit(buttons[0]) : {};
})(`
