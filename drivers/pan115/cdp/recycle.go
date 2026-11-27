package pan115cdp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"time"

	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/pan115"
)

// Recycle removes a regular file through the ordinary single-entry trash request.
func (s *Session) Recycle(ctx context.Context, account, parentID string, file pan115.WebsiteItem) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if account == "" || !validDirectoryID(parentID) || !validDirectoryID(file.ID) ||
		file.ID == "0" || file.ID == parentID || file.ParentID != parentID || file.Directory ||
		!core.ValidName(file.Name) || file.Size < 0 || file.Offset < 0 {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	life, release, err := s.borrow(ctx)
	if err != nil {
		return unchanged, err
	}
	defer release()
	current, err := s.showDirectory(life, account, parentID, file.Offset, true)
	if err != nil {
		return unchanged, err
	}
	if !slices.ContainsFunc(current.Items, func(row pan115.WebsiteItem) bool {
		return row.ID == file.ID && row.Name == file.Name && row.Size == file.Size && !row.Directory
	}) {
		return unchanged, badDirectory("115 website file changed after directory resolution")
	}
	if err := life.Err(); err != nil {
		return unchanged, err
	}

	// After dispatch, let the browser finish or abort its bounded request before
	// releasing the slot. A lost response may leave a remote effect.
	settle, cancel := context.WithTimeout(context.WithoutCancel(life), 6*time.Second)
	defer cancel()
	args, _ := json.Marshal([]string{parentID, file.ID})
	type result struct {
		Status int
		State  *bool
	}
	unknown := core.Change{Effect: core.Unknown}
	receipt, err := chromedp.Run(settle, chromedp.Evaluate[result](recycleScript+"..."+string(args)+")", chromedp.EvalAwaitPromise))
	if err != nil {
		return unknown, core.Fail(core.UpstreamFailure, errors.New("115 website recycle acknowledgement unavailable"))
	}
	switch receipt.Status {
	case -1:
		return unchanged, core.Fail(core.UpstreamAccessDenied, pan115.ErrWebsiteBindingMismatch)
	case http.StatusUnauthorized, http.StatusForbidden:
		return unknown, core.Fail(core.UpstreamAccessDenied, errors.New("115 website declined recycle"))
	case http.StatusTooManyRequests:
		return unknown, core.Fail(core.UpstreamThrottled, nil)
	case http.StatusOK:
		if receipt.State == nil {
			return unknown, badDirectory("115 website recycle response omitted the business result")
		}
		if !*receipt.State {
			return unknown, core.Fail(core.UpstreamFailure, errors.New("115 website declined recycle"))
		}
	default:
		return unknown, core.Fail(core.UpstreamFailure, errors.New("115 website recycle request failed"))
	}

	var count int64
	for offset := int64(0); ; {
		list, err := s.showDirectory(life, account, parentID, offset, offset == 0)
		if err != nil {
			return unknown, err
		}
		if offset == 0 {
			count = list.Count
		}
		if list.Count != count || slices.ContainsFunc(list.Items, func(row pan115.WebsiteItem) bool { return row.ID == file.ID }) {
			return unknown, badDirectory("115 website recycle could not be confirmed for the same file ID")
		}
		offset += int64(len(list.Items))
		if offset == count {
			return core.Change{Effect: core.Applied, Before: core.TargetPresent}, nil
		}
	}
}

const recycleScript = `(async (parent, id) => {
	if (window !== window.top || location.origin !== "` + Origin + `" || location.pathname !== "` + directoryPath + `" ||
		(new URLSearchParams(location.search).get("cid") ?? "0") !== parent) return {status: -1};
	const body = new URLSearchParams({"fid[0]": id, pid: parent, format: "json", ignore_warn: "0"});
	const response = await fetch("https://webapi.115.com/rb/delete", {
		method: "POST", body, credentials: "include", redirect: "error", cache: "no-store",
		headers: {"Content-Type":"application/x-www-form-urlencoded"},
		signal: AbortSignal.timeout(5000),
	});
	if (!response.ok) return {status: response.status};
	const reply = await response.json().catch(() => null);
	return {status: response.status, state: typeof reply?.state === "boolean" ? reply.state : null};
})(`
