package yandex

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
)

func (d *Driver) Mkdir(ctx context.Context, p core.NativeAddress) (core.Change, error) {
	return d.mutate(ctx, p, "PUT", http.StatusCreated)
}

// RemoveSubtree uses native recursive deletion only after an explicit subtree
// grant. It retains native recycle-bin behavior rather than requesting purging.
func (d *Driver) RemoveSubtree(ctx context.Context, p core.NativeAddress, _ core.TreeOptions) (core.Change, error) {
	return d.mutate(ctx, p, "DELETE", http.StatusNoContent)
}

func (d *Driver) mutate(ctx context.Context, p core.NativeAddress, method string, success int) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	target, err := d.target(ctx, p)
	if err != nil {
		return unchanged, err
	}
	if p.Value() == "/" {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	response, err := d.request(ctx, method, d.address("/resources", url.Values{"path": {target}}))
	if err != nil {
		return core.Change{Effect: core.Unknown}, err
	}
	if response.StatusCode == success {
		return core.Change{Effect: core.Applied}, errors.Join(d.closeBody(response.Body), ctx.Err())
	}
	if response.StatusCode == http.StatusAccepted {
		// A 202 grants observation of this operation, not permission to resend it.
		var operation link
		if _, err = d.decode(response, &operation, http.StatusAccepted); err != nil {
			return core.Change{Effect: core.Unknown}, err
		}
		return d.waitOperation(ctx, operation)
	}
	result := core.Change{Effect: core.Unknown}
	if response.StatusCode >= 400 && response.StatusCode < 500 && response.StatusCode != 408 {
		result.Effect = core.Unchanged
	}
	var failure any
	_, err = d.decode(response, &failure)
	return result, errors.Join(err, ctx.Err())
}

func (d *Driver) waitOperation(ctx context.Context, operation link) (core.Change, error) {
	unknown := core.Change{Effect: core.Unknown}
	target, err := operation.target()
	prefix := d.endpoint.Path + "/operations/"
	if err != nil || target.Scheme != d.endpoint.Scheme || !strings.EqualFold(target.Host, d.endpoint.Host) ||
		!strings.HasPrefix(target.Path, prefix) || !core.ValidName(strings.TrimPrefix(target.Path, prefix)) || len(target.Path) == len(prefix) || target.RawQuery != "" {
		return unknown, core.Fail(core.InvalidUpstreamResponse, errors.New("operation capability left the control endpoint"))
	}
	for {
		response, err := d.request(ctx, "GET", target)
		if err != nil {
			return unknown, err
		}
		var out struct {
			Status string `json:"status"`
		}
		valid, err := d.decode(response, &out)
		if valid && out.Status == "success" {
			return core.Change{Effect: core.Applied}, errors.Join(err, ctx.Err())
		}
		if err != nil {
			return unknown, err
		}
		switch out.Status {
		case "failed":
			return unknown, core.Fail(core.UpstreamFailure, errors.New("native operation failed; partial effects are not excluded"))
		case "in-progress":
		default:
			return unknown, core.Fail(core.InvalidUpstreamResponse, errors.New("unknown native operation state"))
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return unknown, ctx.Err()
		case <-timer.C:
		}
	}
}

var _ core.DirectoryMaker = (*Driver)(nil)
var _ core.SubtreeRemover = (*Driver)(nil)
