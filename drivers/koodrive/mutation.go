package koodrive

import (
	"context"
	"errors"
	"net/http"
	"path"
	"strings"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const folderMIME = "application/vnd.huawei-apps.folder"

// Mkdir creates a directory only if its native name is available.
func (d *Driver) Mkdir(ctx context.Context, address core.NativeAddress) (change core.Change, err error) {
	change.Effect = core.Unchanged
	if err := d.check(ctx, address); err != nil {
		return change, err
	}
	if !d.allowMkdir {
		return change, core.Fail(core.UnsupportedOperation, nil)
	}
	if address.Value() == "/" {
		change.Before = core.TargetPresent
		return change, core.Fail(core.AlreadyExists, nil)
	}
	name := path.Base(address.Value())
	if !validMutationName(name) {
		return change, core.Fail(core.UnrepresentableName, nil)
	}
	parent, err := d.resolve(ctx, core.NativePath(path.Dir(address.Value())))
	if err != nil {
		return change, err
	}
	if parent.meta.Kind != core.Directory {
		return change, core.Fail(core.NotDirectory, nil)
	}
	fields := struct {
		Container string   `json:"containerId"`
		Parents   []string `json:"parentFolder"`
		Name      string   `json:"fileName"`
		Type      string   `json:"fileType"`
		MIME      string   `json:"mimeType"`
		Rename    int      `json:"renameMode"`
	}{d.container, []string{d.parentParameter(parent.ID)}, name, "10", folderMIME, 3}
	response, change, err := d.beginMutation(ctx, "/directory", fields)
	if err != nil {
		return change, err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	var refusal error
	if response.StatusCode != http.StatusOK {
		refusal = statusError(response.StatusCode)
		if response.StatusCode != http.StatusConflict {
			return change, refusal
		}
	}
	var receipt struct {
		result
		ID       string `json:"id"`
		Existing string `json:"existFileId"`
		Name     string `json:"fileName"`
		MIME     string `json:"mimeType"`
	}
	if err := httpcontent.DecodeJSON(response, d.limit, &receipt); err != nil {
		return change, errors.Join(refusal, err)
	}
	if receipt.Existing != "" {
		if receipt.ID != "" && receipt.ID != receipt.Existing || receipt.Name != "" && receipt.Name != name {
			return change, core.Fail(core.InvalidUpstreamResponse, errors.New("inconsistent KooDrive directory conflict receipt"))
		}
		return core.Change{Effect: core.Unchanged, Before: core.TargetPresent}, core.Fail(core.AlreadyExists, nil)
	}
	if err := errors.Join(refusal, receipt.result.check()); err != nil {
		return change, err
	}
	if receipt.ID == "" || receipt.ID == parent.ID || receipt.ID == d.root || receipt.Name != name || receipt.MIME != folderMIME {
		return change, core.Fail(core.InvalidUpstreamResponse, errors.New("KooDrive did not confirm the exact new directory"))
	}
	return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
}

func validMutationName(name string) bool {
	return !strings.ContainsAny(name, `<>|:\"*?`) && utf8.RuneCountInString(name) <= 250
}

func (d *Driver) beginMutation(ctx context.Context, route string, fields any) (*http.Response, core.Change, error) {
	change := core.Change{Effect: core.Unchanged}
	req, err := d.controlRequest(ctx, http.MethodPost, route, fields)
	if err != nil {
		return nil, change, err
	}
	req.GetBody = nil
	response, dispatched, err := d.exchange(&d.api, req)
	if dispatched {
		change.Effect = core.Unknown
	}
	return response, change, err
}

var _ core.DirectoryMaker = (*Driver)(nil)
