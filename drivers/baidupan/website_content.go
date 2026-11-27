package baidupan

import (
	"context"
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

// WebsiteFile identifies one original file listed in the active account.
type WebsiteFile struct {
	Account, Path string
	ID            uint64
	Size          int64
}

// WebsiteDownload carries an independently verified single-file selection and
// its final HTTPS response. The transport owns the selected iframe and verifies
// its grant and redirect ancestry before returning. Body must preserve the
// original bytes and verify EOF if provided through a browser stream. Ownership
// of any returned Body transfers even when the transport returns an error.
type WebsiteDownload struct {
	File     WebsiteFile
	Response *http.Response
}

func (d *Website) Open(ctx context.Context, address core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if d.openFile == nil || options.Mode != core.StreamRead || options.NoFollow ||
		options.Range != nil || options.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	selectionErr := options.Selection.When.Check(core.Evidence{}, true)
	if selectionErr != nil && core.CodeOf(selectionErr) != core.PreconditionFailed {
		return core.Opened{}, selectionErr
	}
	full, err := d.address(address)
	if err != nil {
		return core.Opened{}, err
	}
	node, err := d.lookup(ctx, full)
	if err != nil {
		return core.Opened{}, err
	}
	if node.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if selectionErr != nil {
		return core.Opened{}, selectionErr
	}
	wanted := WebsiteFile{Account: d.account, Path: full, ID: node.id, Size: *node.meta.Size}
	if err := ctx.Err(); err != nil {
		return core.Opened{}, err
	}
	download, err := d.openFile(ctx, wanted)
	response := download.Response
	var opened core.Opened
	if response != nil {
		opened.Body = response.Body
	}
	if err != nil {
		return opened, privateError(err, "")
	}
	if err := ctx.Err(); err != nil {
		return opened, err
	}
	if err := d.life.Err(); err != nil {
		return opened, err
	}
	if download.File.Account != "" && download.File.Account != wanted.Account {
		return opened, core.Fail(core.UpstreamAccessDenied, errors.New("website download account differs from the saved binding"))
	}
	if download.File != wanted || response == nil || response.Body == nil || response.Uncompressed ||
		response.ContentLength < -1 || response.Request == nil || response.Request.Method != http.MethodGet ||
		!httpcontent.IsHTTPS(response.Request.URL) {
		return opened, core.Fail(core.InvalidUpstreamResponse, errors.New("website original did not match the selected file"))
	}
	if response.StatusCode != http.StatusOK {
		return opened, core.Fail(core.InvalidUpstreamResponse, errors.New("website original did not return a complete response"))
	}
	disposition, _, err := mime.ParseMediaType(response.Header.Get("Content-Disposition"))
	if err != nil || !strings.EqualFold(disposition, "attachment") {
		return opened, core.Fail(core.InvalidUpstreamResponse, errors.New("website original did not return an attachment"))
	}
	opened, err = httpcontent.OpenSized(response, nil, wanted.Size)
	if err == nil {
		opened.Metadata = node.meta
	}
	return opened, err
}

// WebsiteDownloadTarget checks the single-file grant observed by the browser
// and returns the exact first iframe address used by the ordinary download UI.
// The returned address is only for correlating the existing browser request.
func WebsiteDownloadTarget(id uint64, requested, body []byte) (string, error) {
	var selection []json.Number
	if id == 0 || len(requested) > 64 || json.Unmarshal(requested, &selection) != nil || len(selection) != 1 ||
		string(selection[0]) != strconv.FormatUint(id, 10) {
		return "", core.Fail(core.InvalidUpstreamResponse, errors.New("website grant did not request the selected file"))
	}
	var result struct {
		Errno *int
		Dlink []struct {
			Link string      `json:"dlink"`
			ID   json.Number `json:"fs_id"`
		}
	}
	if len(body) > 1<<20 || json.Unmarshal(body, &result) != nil || result.Errno == nil {
		return "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid website grant"))
	}
	if *result.Errno != 0 {
		return "", nativeFailure(*result.Errno, reply{}, "")
	}
	if len(result.Dlink) != 1 {
		return "", core.Fail(core.InvalidUpstreamResponse, errors.New("website grant did not return one file"))
	}
	entry := result.Dlink[0]
	if entry.ID != "" && string(entry.ID) != string(selection[0]) {
		return "", core.Fail(core.InvalidUpstreamResponse, errors.New("website grant returned another file"))
	}
	address := entry.Link
	if prefix, ok := strings.CutPrefix(address, "http://"); ok {
		address = "https://" + prefix
	}
	target, err := url.Parse(address)
	if err != nil || !httpcontent.IsHTTPS(target) {
		return "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid website iframe address"))
	}
	return address + "&response-cache-control=private", nil
}
