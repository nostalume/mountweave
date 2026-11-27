package mobilepan

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

// WebsiteUploadOptions enables exclusive creation from an existing private
// staging directory. MaxStagingBytes bounds the active upload's local file.
type WebsiteUploadOptions struct {
	StagingDirectory string
	MaxStagingBytes  int64
}

func prepareWebsiteUpload(o *WebsiteUploadOptions) (*upload.Staging, error) {
	if o == nil {
		return nil, nil
	}
	return upload.NewStaging(o.StagingDirectory, o.MaxStagingBytes)
}

type websiteUploadPart struct {
	Number int   `json:"partNumber"`
	Size   int64 `json:"partSize"`
	Offset struct {
		Start int64 `json:"partOffset"`
	} `json:"parallelHashCtx"`
}

type websiteUploadGrant struct {
	ID        string `json:"fileId"`
	Parent    string `json:"parentFileId"`
	Name      string `json:"name"`
	Kind      string `json:"type"`
	UploadID  string `json:"uploadId"`
	Exists    bool   `json:"exist"`
	Rapid     bool   `json:"rapidUpload"`
	PartCount int    `json:"partCount"`
	Part      struct {
		Number int    `json:"partNumber"`
		Size   *int64 `json:"partSize"`
		URL    string `json:"uploadUrl"`
	} `json:"part"`
}

const websiteUploadType = "application/octet-stream"
const websiteUploadHash = "SHA256"

// Create prehashes the complete input before requesting an exclusive name.
func (d *Driver) Create(ctx context.Context, at core.NativeAddress, input io.Reader, length *int64) (result core.Change, err error) {
	result.Effect = core.Unchanged
	if d.upload == nil || at.Namespace() != core.TreeNamespace || at.Value() == "/" {
		return result, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(at.Value()); err != nil {
		return result, err
	}
	parent, err := d.resolve(ctx, core.NativePath(path.Dir(at.Value())))
	if err != nil {
		return result, err
	}
	if parent.metadata.Kind != core.Directory {
		return result, core.Fail(core.NotDirectory, nil)
	}
	if err := d.check(ctx); err != nil {
		return result, err
	}
	file, err := d.upload.Stage(ctx, input, length)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	size := file.Size
	if size == 0 {
		return result, core.Fail(core.UnsupportedOperation, errors.New("empty upload is not available"))
	}
	name := path.Base(at.Value())
	part := websiteUploadPart{Number: 1, Size: size}
	digest := file.SHA256
	fields := map[string]any{"parentFileId": parent.id, "name": name, "type": "file", "size": size,
		"fileRenameMode": "refuse", "contentType": websiteUploadType, "contentHashAlgorithm": websiteUploadHash,
		"contentHash": digest, "parallelUpload": false, "partInfos": []websiteUploadPart{part}}
	if err := d.check(ctx); err != nil {
		return result, err
	}
	uncertain := core.Change{Effect: core.Unknown}
	var grant websiteUploadGrant
	if err := d.call(ctx, "upload-grant", fields, &grant); err != nil {
		return uncertain, err
	}
	if !websiteUploadID(grant.ID) || grant.ID == parent.id || grant.Name != "" && grant.Name != name || grant.Parent != "" && grant.Parent != parent.id {
		return uncertain, invalidWebsiteUpload()
	}
	if grant.Exists {
		pages := d.resolvePages
		occupant, err := d.child(ctx, parent.id, name, &pages)
		if err != nil || occupant.id != grant.ID {
			return uncertain, errors.Join(invalidWebsiteUpload(), err)
		}
		return core.Change{Effect: core.Unchanged, Before: core.TargetPresent}, core.Fail(core.AlreadyExists, nil)
	}
	if grant.Kind != "file" || grant.Name != name || grant.Parent != parent.id {
		return uncertain, invalidWebsiteUpload()
	}
	var collision bool
	if !grant.Rapid {
		collision, err = d.completeWebsiteUpload(ctx, grant, file, size, digest)
		if err != nil {
			return uncertain, err
		}
	}
	pages := d.resolvePages
	confirmed, err := d.child(ctx, parent.id, name, &pages)
	if err != nil {
		return uncertain, errors.Join(invalidWebsiteUpload(), err)
	}
	if collision {
		if confirmed.id != "" && confirmed.id != grant.ID {
			return core.Change{Effect: core.Unchanged, Before: core.TargetPresent}, core.Fail(core.AlreadyExists, nil)
		}
		return uncertain, invalidWebsiteUpload()
	}
	if confirmed.id != grant.ID || confirmed.metadata.Kind != core.File || confirmed.metadata.Size == nil || *confirmed.metadata.Size != size {
		return uncertain, invalidWebsiteUpload()
	}
	return core.Change{Effect: core.Applied, Before: core.TargetMissing}, nil
}

func (d *Driver) completeWebsiteUpload(ctx context.Context, grant websiteUploadGrant, file io.ReaderAt, size int64, digest string) (bool, error) {
	// An untouched part can omit its current size. The requested size is fixed by the
	// grant request and the exact Content-Length of the PUT below.
	if !websiteUploadID(grant.UploadID) || grant.PartCount != 1 || grant.Part.Number != 1 || grant.Part.Size != nil && *grant.Part.Size != 0 && *grant.Part.Size != size {
		return false, invalidWebsiteUpload()
	}
	if err := d.putWebsitePart(ctx, grant.Part.URL, file, size); err != nil {
		return false, err
	}
	request := map[string]string{"fileId": grant.ID, "uploadId": grant.UploadID, "contentHash": digest, "contentHashAlgorithm": websiteUploadHash}
	var receipt struct {
		Accepted bool
		Code     string
		ID       string `json:"fileId"`
	}
	if err := d.call(ctx, "upload-complete", request, &receipt); err != nil {
		return false, err
	}
	if receipt.ID != "" && receipt.ID != grant.ID {
		return false, invalidWebsiteUpload()
	}
	if !receipt.Accepted && receipt.Code != "04010330" {
		return false, core.Fail(core.UpstreamFailure, errors.New("Mobile Cloud rejected upload completion"))
	}
	return !receipt.Accepted, nil
}

func (d *Driver) putWebsitePart(ctx context.Context, address string, file io.ReaderAt, size int64) error {
	u, err := url.Parse(address)
	if err != nil || !httpcontent.IsHTTPS(u) {
		return invalidWebsiteUpload()
	}
	if err := d.check(ctx); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), io.NewSectionReader(file, 0, size))
	if err != nil {
		return invalidWebsiteUpload()
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", websiteUploadType)
	response, err := d.content.Do(req)
	if err != nil {
		return errors.Join(httpcontent.UpstreamIOError(err), ctx.Err())
	}
	closeErr := d.cleanup.CloseBody(response.Body)
	if response.StatusCode != http.StatusOK {
		return errors.Join(requestFailure(response.StatusCode, ""), closeErr, ctx.Err())
	}
	return errors.Join(closeErr, ctx.Err())
}

func websiteUploadID(id string) bool { return id != "" && id != rootID && len(id) <= 64 }

func invalidWebsiteUpload() error {
	return core.Fail(core.InvalidUpstreamResponse, errors.New("Mobile Cloud did not confirm the upload receipt"))
}

var _ core.FileCreator = (*Driver)(nil)
