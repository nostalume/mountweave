package tencentdocs

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

const (
	exportWait = 20 * time.Minute
	exportPoll = 2 * time.Second
)

func (d *Driver) Open(ctx context.Context, p core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" || !o.Selection.When.Empty() {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	f, err := d.resolve(ctx, p)
	if err != nil {
		return core.Opened{}, err
	}
	if f.directory() {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if !d.allowExport || f.Shortcut != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	switch f.Type {
	case "doc", "sheet", "slide", "pdf":
	default:
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	target, err := d.exportURL(ctx, f.ID)
	if err != nil {
		return core.Opened{}, err
	}
	resp, err := httpcontent.FollowDownload(target, func(target string, _ bool) (*http.Response, error) {
		if err := d.active(ctx); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		req.Header.Set("Accept-Encoding", "identity")
		response, err := d.body.Do(req)
		if err != nil {
			return nil, errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
		}
		return response, nil
	}, d.cleanup.CloseBody)
	if err != nil {
		return core.Opened{}, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return core.Opened{}, errors.Join(statusError(resp.StatusCode), d.cleanup.CloseBody(resp.Body))
	}
	return d.cleanup.OpenBody(resp, nil, f.Title)
}

func (d *Driver) exportURL(ctx context.Context, id string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, exportWait)
	defer cancel()
	route := "/files/" + url.PathEscape(id)
	started, err := call[struct {
		ID string `json:"operationID"`
	}](ctx, d, http.MethodPost, route+"/async-export")
	if err != nil {
		return "", err
	}
	if started.ID == "" || strings.TrimSpace(started.ID) != started.ID {
		return "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Tencent Docs export operation ID"))
	}
	status := route + "/export-progress?operationID=" + url.QueryEscape(started.ID)
	for {
		result, err := call[struct {
			Progress *int   `json:"progress"`
			URL      string `json:"url"`
		}](ctx, d, http.MethodGet, status)
		if err != nil {
			return "", err
		}
		if result.Progress == nil || *result.Progress < 0 || *result.Progress > 100 {
			return "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Tencent Docs export progress"))
		}
		if *result.Progress == 100 {
			target, err := url.Parse(result.URL)
			if err != nil || target.Scheme != "https" || target.Hostname() == "" || target.User != nil ||
				target.Opaque != "" || strings.ContainsRune(result.URL, '#') {
				return "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid Tencent Docs export URL"))
			}
			return result.URL, nil
		}
		timer := time.NewTimer(exportPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
}
