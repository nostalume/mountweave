package pan123cdp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/pan123"
)

func (s *Session) Rename(ctx context.Context, parent string, file pan123.WebsiteFile, name string) (core.Change, error) {
	if !core.ValidName(name) || name == file.Name {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.InvalidRequest, nil)
	}
	_, change, err := s.mutate(ctx, parent, file, name, "")
	return change, err
}

func (s *Session) Recycle(ctx context.Context, parent string, file pan123.WebsiteFile) (core.Change, error) {
	_, change, err := s.mutate(ctx, parent, file, "", "")
	return change, err
}

func (s *Session) Move(ctx context.Context, parent string, file pan123.WebsiteFile, directory string) (string, core.Change, error) {
	if !validParent(directory) || directory == parent {
		return "", core.Change{Effect: core.Unchanged}, core.Fail(core.InvalidRequest, nil)
	}
	return s.mutate(ctx, parent, file, "", directory)
}

func (s *Session) mutate(ctx context.Context, parent string, file pan123.WebsiteFile, name, directory string) (actual string, change core.Change, err error) {
	change.Effect = core.Unchanged
	if !validParent(parent) || !validID(file.ID) || !validID(file.Account) || !core.ValidName(file.Name) || file.Size < 0 {
		return "", change, core.Fail(core.InvalidRequest, nil)
	}
	err = s.run(ctx, func(ctx context.Context, receipts *observation) error {
		view, err := s.openDirectory(ctx, receipts, parent)
		if err != nil {
			return err
		}
		if view.account != file.Account {
			return pan123.ErrWebsiteBindingMismatch
		}
		before, err := s.fileMutation(ctx, parent, file, file.Name, "", "check")
		if err != nil {
			return invalid("123 Pan could not verify the current file")
		}
		if before.State == "unavailable" {
			return core.Fail(core.UnsupportedOperation, errors.New("123 Pan website file client was not recognized"))
		}
		if before.State != "matched" {
			return invalid("123 Pan current file no longer matches its parent, name and type")
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		// A dropped browser response cannot establish that the POST was rolled back.
		change.Effect = core.Unknown
		receipt, err := s.fileMutation(ctx, parent, file, name, directory, "submit")
		if err != nil {
			return core.Fail(core.UpstreamFailure, errors.New("123 Pan mutation acknowledgement unavailable"))
		}
		if receipt.State == "unavailable" {
			change.Effect = core.Unchanged
			return core.Fail(core.UnsupportedOperation, errors.New("123 Pan website file client was not recognized"))
		}
		if name != "" && receipt.Code != nil && *receipt.Code == 5060 {
			change.Effect, change.Before = core.Unchanged, core.TargetPresent
			return core.Fail(core.AlreadyExists, nil)
		}
		if receipt.Code == nil {
			return invalid("123 Pan returned no mutation business result")
		}
		if *receipt.Code != 0 {
			return core.Fail(core.UpstreamFailure, fmt.Errorf("123 Pan website mutation result %d", *receipt.Code))
		}
		after, err := s.fileMutation(ctx, parent, file, name, directory, "confirm")
		if err != nil || after.State != "matched" || directory != "" && !core.ValidName(after.Name) {
			return invalid("123 Pan mutation could not be confirmed for the same file ID")
		}
		if name != "" || directory != "" {
			change.Before = core.TargetMissing
		}
		actual = after.Name
		change.Effect = core.Applied
		return nil
	})
	return actual, change, err
}

type mutationReceipt struct {
	State string
	Name  string
	Code  *int
}

func (s *Session) fileMutation(ctx context.Context, parent string, file pan123.WebsiteFile, name, directory, phase string) (mutationReceipt, error) {
	args, _ := json.Marshal([]string{s.origin, parent, file.ID, strconv.FormatInt(file.Size, 10), name, directory, phase})
	return chromedp.Run(ctx, chromedp.Evaluate[mutationReceipt](mutationScript+"..."+string(args)+")", chromedp.EvalAwaitPromise))
}

// The page's own request client keeps account credentials inside the browser.
// Recognize exports by their service calls so a rebuilt bundle need not keep
// its old module IDs. A client with an unknown shape is refused before sending.
const mutationScript = `((origin, parent, id, size, name, directory, phase) => {
	const unavailable = {State:"unavailable"};
	if (location.origin !== origin || window !== window.top) return unavailable;
	const number = Number(id), chunks = self.webpackChunkreact_family;
	if (!Number.isSafeInteger(number) || String(number) !== id || number <= 0 ||
		!Array.isArray(chunks) || chunks.push === Array.prototype.push) return unavailable;
	let require;
	chunks.push([["mountweave-"+crypto.randomUUID()], {}, runtime=>{require=runtime;}]);
	if (typeof require !== "function" || !require.m) return unavailable;
	const marker = path => "(" + JSON.stringify(path) + ",";
	const route = (path, companion) => {
		const markers = [marker(path), marker(companion)];
		const modules = Object.keys(require.m).filter(id => markers.every(key => String(require.m[id]).includes(key)));
		if (modules.length !== 1) return;
		const matches = Object.values(require(+modules[0])).filter(fn =>
			typeof fn === "function" && String(fn).includes(markers[0]));
		if (matches.length === 1) return matches[0];
	};
	if (phase === "submit") {
		let call, payload;
		if (directory) {
			const dest = Number(directory);
			if (!Number.isSafeInteger(dest) || String(dest) !== directory || dest < 0) return unavailable;
			call = route("file/mod_pid", "file/rename");
			payload = {fileIdList:[{FileId:number}],parentFileId:dest,event:"fileMove"};
		} else if (name) {
			call = route("file/rename", "file/mod_pid");
			payload = {driveId:0,fileId:number,fileName:name,duplicate:1};
		} else {
			call = route("file/trash", "restful/goapi/v1/file/safe_box/batches/delete");
			payload = {fileTrashInfoList:[{FileId:number}],operation:true,driveId:0,event:"intoRecycle"};
		}
		if (!call) return unavailable;
		// Begin this single request synchronously. No deferred write may be
		// started by an outstanding read after the Go caller is cancelled.
		const request = call(payload);
		return Promise.resolve(request).then(reply=>({Code:Number.isInteger(reply?.code)?reply.code:null}));
	}
	const info = route("file/info", "file/rename");
	if (!info) return unavailable;
	const expectTrash = phase === "confirm" && name === "" && !directory;
	return Promise.resolve(info({fileIdList:[{fileId:number}]})).then(reply => {
		const items = reply?.data?.infoList;
		const row = Array.isArray(items) && items.length === 1 ? items[0] : null;
		const trashed = [true, 1, "1"].includes(row?.Trashed);
		const active = [false, 0, "0"].includes(row?.Trashed);
		const matched = (reply?.code === 0 || reply?.code === 200) && row &&
			String(row.FileId) === id && String(row.Type) === "0" &&
			String(row.Size) === size && String(row.ParentFileId) === (directory || parent) &&
			(expectTrash ? trashed : active && (directory ? typeof row.FileName === "string" : row.FileName === name));
		return matched ? {State:"matched",Name:row.FileName} : {State:"mismatch"};
	});
})(`
