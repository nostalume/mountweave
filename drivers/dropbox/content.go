package dropbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"unicode/utf16"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

func (d *Driver) Open(ctx context.Context, p core.NativeAddress, o core.OpenOptions) (core.Opened, error) {
	if err := o.Validate(); err != nil {
		return core.Opened{}, err
	}
	if p.Namespace() != core.TreeNamespace || o.Mode != core.StreamRead || o.NoFollow ||
		o.Selection.Version != "" || !o.Selection.When.Empty() {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := d.admit(ctx, p.Value()); err != nil {
		return core.Opened{}, err
	}
	if p.Value() == "/" {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	parent, err := d.walk(ctx, path.Dir(p.Value()))
	if err != nil {
		return core.Opened{}, err
	}
	if parent.meta.Kind != core.Directory {
		return core.Opened{}, core.Fail(core.NotDirectory, nil)
	}
	name := path.Base(p.Value())
	argument := map[string]any{"path": childPath(parent, name)}
	response, err := d.content(ctx, "/files/download", argument, o.Range)
	if err != nil {
		return core.Opened{}, err
	}
	exporting := false
	if response.StatusCode == http.StatusConflict {
		tag, nativeErr := d.failure(response, readFailure)
		if tag != "unsupported_file" || d.export == nil {
			return core.Opened{Body: response.Body}, nativeErr
		}
		if closeErr := d.closeBody(response.Body, false); closeErr != nil {
			return core.Opened{}, errors.Join(nativeErr, closeErr)
		}
		if d.export.Format != "" {
			argument["export_format"] = d.export.Format
		}
		response, err = d.content(ctx, "/files/export", argument, o.Range)
		if err != nil {
			return core.Opened{}, err
		}
		exporting = true
	}
	if response.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		// A 416 does not promise same-response leaf metadata. Verify the exact
		// name without attributing the earlier range total to a later object.
		opened := core.Opened{Body: response.Body}
		found, err := d.lookup(ctx, parent, name)
		if err != nil {
			return opened, err
		}
		if found.meta.Kind != core.File {
			return opened, core.Fail(core.NotFile, nil)
		}
		return opened, &core.Error{Code: core.RangeNotSatisfiable}
	}
	return d.opened(response, name, o.Range, exporting)
}

func (d *Driver) content(ctx context.Context, route string, argument any, wanted *core.Range) (*http.Response, error) {
	req, err := contentRequest(ctx, http.MethodGet, route, argument, nil, false)
	if err != nil {
		return nil, err
	}
	if wanted != nil {
		req.Header.Set("Range", httpcontent.Header(wanted))
	}
	response, _, err := d.do(req)
	return response, err
}

func contentRequest(ctx context.Context, method, route string, argument any, body io.Reader, private bool) (*http.Request, error) {
	payload, err := json.Marshal(argument)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	target := "https://content.dropboxapi.com/2" + route
	if !private {
		target += "?" + (url.Values{"arg": {string(payload)}}).Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	if private {
		req.Header.Set("Dropbox-API-Arg", headerJSON(payload))
	}
	return req, nil
}

// json.Marshal emits non-ASCII and DEL only inside JSON strings.
func headerJSON(payload []byte) string {
	var header strings.Builder
	header.Grow(len(payload))
	for _, r := range string(payload) {
		switch {
		case r < 0x7f:
			header.WriteByte(byte(r))
		case r <= 0xffff:
			fmt.Fprintf(&header, "\\u%04x", r)
		default:
			high, low := utf16.EncodeRune(r)
			fmt.Fprintf(&header, "\\u%04x\\u%04x", high, low)
		}
	}
	return header.String()
}

func (d *Driver) opened(response *http.Response, name string, wanted *core.Range, exporting bool) (core.Opened, error) {
	opened := core.Opened{Body: response.Body}
	if response.StatusCode != 200 && response.StatusCode != 206 {
		_, err := d.failure(response, readFailure)
		return opened, err
	}

	var original nativeMetadata
	var total *int64
	header := []byte(response.Header.Get("Dropbox-API-Result"))
	if exporting {
		var result struct {
			File   nativeMetadata `json:"file_metadata"`
			Export struct {
				Size *int64 `json:"size"`
			} `json:"export_metadata"`
		}
		if err := d.decode(header, &result); err != nil {
			return opened, err
		}
		original, total = result.File, result.Export.Size
	} else {
		if err := d.decode(header, &original); err != nil {
			return opened, err
		}
		total = original.Size
	}
	f, err := accept(original, true)
	if err != nil {
		return opened, err
	}
	if f.meta.Name != name {
		return opened, core.Fail(core.NotFound, nil)
	}
	if total == nil || *total < 0 {
		return opened, core.Fail(core.InvalidUpstreamResponse, errors.New("native body size is missing or invalid"))
	}
	opened, err = httpcontent.Open(response, wanted)
	opened.Metadata.Name = name
	if err != nil {
		return opened, err
	}
	if opened.Metadata.Size != nil && *opened.Metadata.Size != *total {
		return opened, core.Fail(core.InvalidUpstreamResponse, errors.New("native metadata and actual body size disagree"))
	}
	if response.StatusCode == http.StatusOK && opened.Length == nil {
		opened.Length = total
	}
	opened.Metadata.Size = total
	if f.meta.ModifiedAt != nil {
		opened.Metadata.ModifiedAt = f.meta.ModifiedAt
	}
	return opened, nil
}
