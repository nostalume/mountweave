package googledrive

import (
	"cmp"
	"context"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

func unavailableContent(mime string) string {
	switch mime {
	case folderType:
		return "native folder has no byte representation"
	case "application/vnd.google-apps.shortcut", "application/vnd.google-apps.drive-sdk":
		return "native shortcut has no selected byte representation"
	case "application/vnd.google-apps.vid":
		return "Google Vids requires its native long-running download"
	default:
		return ""
	}
}
func (d *Driver) confirmExports(ctx context.Context) error {
	var about struct {
		Formats map[string][]string `json:"exportFormats"`
	}
	if err := d.json(ctx, apiURL("/about", url.Values{"fields": {"exportFormats"}}), item{}, &about); err != nil {
		return err
	}
	for source, target := range d.exports {
		if !slices.Contains(about.Formats[source], target) {
			return core.Fail(core.InvalidRequest, errors.New("configured export MIME pair is not available"))
		}
	}
	return nil
}

func (d *Driver) Open(ctx context.Context, p core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if p.Namespace() != core.TreeNamespace || o.Mode != core.StreamRead || o.NoFollow || o.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := o.Selection.When.ForDomain(""); err != nil {
		return core.Opened{}, err
	}
	f, err := d.resolve(ctx, p.Value())
	if err != nil {
		return core.Opened{}, err
	}
	if f.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := o.Selection.Check(f.meta.Evidence); err != nil {
		return core.Opened{}, err
	}
	if reason := unavailableContent(f.MIME); reason != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, errors.New(reason))
	}
	if f.Capabilities != nil && f.Capabilities.CanDownload != nil && !*f.Capabilities.CanDownload {
		return core.Opened{}, core.Fail(core.UpstreamAccessDenied, errors.New("native file capability forbids downloading"))
	}
	route := "/files/" + url.PathEscape(f.ID)
	query := url.Values{"supportsAllDrives": {"true"}, "alt": {"media"}}
	wanted := o.Range
	if format := d.exports[f.MIME]; format != "" {
		route += "/export"
		query = url.Values{"mimeType": {format}}
		wanted = nil
	} else if nativeDocument(f.MIME) {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, errors.New("no export format was selected for this native MIME type"))
	}
	response, authenticated, err := d.body(ctx, apiURL(route, query), f, wanted)
	if err != nil {
		return core.Opened{}, err
	}
	if response.StatusCode != 200 && response.StatusCode != 206 && response.StatusCode != 416 {
		return core.Opened{}, errors.Join(d.responseFailure(response, authenticated), d.cleanup.CloseBody(response.Body))
	}
	return d.cleanup.OpenBody(response, wanted, f.Name)
}

func apiOrigin(u *url.URL) bool {
	return u.Scheme == "https" && strings.EqualFold(u.Hostname(), ControlHost) && (u.Port() == "" || u.Port() == "443")
}

// The ordinary no-redirect path performs one authenticated GET. After crossing
// the API origin, the chain remains on the independent unauthenticated client.
func (d *Driver) body(ctx context.Context, target *url.URL, f item, wanted *core.Range) (*http.Response, bool, error) {
	authenticated := true
	response, err := httpcontent.FollowDownload(target.String(), func(target string, _ bool) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		authenticated = authenticated && apiOrigin(req.URL)
		req.Header.Set("Accept-Encoding", "identity")
		if wanted != nil {
			req.Header.Set("Range", httpcontent.Header(wanted))
		}
		client := &d.content
		if authenticated {
			client = &d.api
			resourceHeader(req, f)
		}
		response, err := d.do(client, req, true)
		if err == nil {
			// An empty fragment is equivalent to no fragment for Drive redirects.
			if next, emptyFragment := strings.CutSuffix(response.Header.Get("Location"), "#"); emptyFragment {
				response.Header.Set("Location", cmp.Or(next, target))
			}
		}
		return response, err
	}, d.cleanup.CloseBody)
	return response, err == nil && authenticated, err
}
