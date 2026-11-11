package baidupan

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
)

// Mkdir creates one directory with the native failure-on-conflict policy.
func (d *Driver) Mkdir(ctx context.Context, p core.NativeAddress) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	full, err := d.address(p)
	if err != nil {
		return unchanged, err
	}
	if p.Value() == "/" {
		return unchanged, core.Fail(core.AlreadyExists, nil)
	}
	token, err := d.credential(ctx)
	if err != nil {
		return unchanged, err
	}
	return d.createEntry(ctx, full, token, url.Values{"rtype": {"0"}}, nil)
}

func (d *Driver) postXpan(ctx context.Context, method, token string, form url.Values) (*http.Response, bool, error) {
	query := url.Values{"method": {method}, "access_token": {token}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://pan.baidu.com/rest/2.0/xpan/file?"+query.Encode(), strings.NewReader(form.Encode()))
	if err != nil {
		return nil, false, core.Fail(core.InvalidRequest, privateError(err, token))
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return d.do(req, token)
}

// createEntry confirms a directory or a file assembled from temporary parts.
// Size is present only for a file. Each caller supplies its native form fields.
func (d *Driver) createEntry(ctx context.Context, full, token string, form url.Values, size *int64) (result core.Change, err error) {
	result.Effect = core.Unchanged
	var directory int
	form.Set("path", full)
	if size == nil {
		directory = 1
	} else {
		form.Set("size", strconv.FormatInt(*size, 10))
	}
	form.Set("isdir", strconv.Itoa(directory))
	response, entered, err := d.postXpan(ctx, "create", token, form)
	if entered {
		result.Effect = core.Unknown
	}
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, token), ctx.Err()) }()
	data, code, err := d.writeReply(response, token, true)
	if err != nil {
		if code == -8 || code == 31061 {
			return core.Change{Effect: core.Unchanged, Before: core.TargetPresent}, core.Fail(core.AlreadyExists, err)
		}
		return result, err
	}
	var receipt struct {
		ID        *uint64 `json:"fs_id"`
		Path      string  `json:"path"`
		Directory *int    `json:"isdir"`
		Size      *int64  `json:"size"`
		Status    *int    `json:"status"`
	}
	if err := json.Unmarshal(data, &receipt); err != nil {
		return result, core.Fail(core.InvalidUpstreamResponse, privateError(err, token))
	}
	if receipt.ID == nil || receipt.Path != full || receipt.Directory == nil || *receipt.Directory != directory ||
		receipt.Status != nil && *receipt.Status != 0 || size != nil && (receipt.Size == nil || *receipt.Size != *size) {
		return result, core.Fail(core.InvalidUpstreamResponse, errors.New("native create receipt contradicts the requested entry"))
	}
	return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
}

var _ core.DirectoryMaker = (*Driver)(nil)
