package koodrive

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

const maxSinglePart = 5 << 30

// Create uploads one part and confirms the newly created file by native ID.
func (d *Driver) Create(ctx context.Context, address core.NativeAddress, input io.Reader, length *int64) (change core.Change, err error) {
	change.Effect = core.Unchanged
	if err := d.check(ctx, address); err != nil {
		return change, err
	}
	if !d.allowCreate || length == nil {
		return change, core.Fail(core.UnsupportedOperation, nil)
	}
	size := *length
	if size < 0 || input == nil {
		return change, core.Fail(core.InvalidRequest, nil)
	}
	if size > maxSinglePart {
		return change, core.Fail(core.CapacityExceeded, nil)
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
	if size == 0 {
		// A zero-length HTTP request has no body for the transport to read.
		body := upload.NewBody(ctx, input, &size)
		var probe [1]byte
		_, err := body.Read(probe[:])
		if err != io.EOF {
			return change, err
		}
	}

	receipt, change, err := d.initializeUpload(ctx, parent.ID, name, size)
	if err != nil {
		return change, err
	}
	if err := d.putPart(ctx, receipt.Parts[0].URL, input, size); err != nil {
		return change, err
	}

	fields := struct {
		ID string `json:"fileId"`
	}{ID: receipt.ID}
	response, _, err := d.beginMutation(ctx, "/files/complete", fields)
	if err != nil {
		return change, err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	if response.StatusCode != http.StatusOK {
		return change, statusError(response.StatusCode)
	}
	var confirmation result
	if err := httpcontent.DecodeJSON(response, d.limit, &confirmation); err != nil {
		return change, err
	}
	if err := confirmation.check(); err != nil {
		return change, err
	}
	change.Effect = core.Applied
	return change, nil
}

type uploadPart struct {
	Number int    `json:"partNumber"`
	Size   *int64 `json:"partSize"`
	URL    string `json:"uploadUrl,omitempty"`
}

type uploadReceipt struct {
	result
	ID        string       `json:"fileId"`
	Container string       `json:"containerId"`
	Name      string       `json:"fileName"`
	UploadID  string       `json:"uploadId"`
	Parents   []string     `json:"parentFolder"`
	Parts     []uploadPart `json:"multiParts"`
	Existing  string       `json:"existFileId"`
	Rapid     bool         `json:"rapidUpload"`
}

func (d *Driver) initializeUpload(ctx context.Context, parent, name string, size int64) (receipt uploadReceipt, change core.Change, err error) {
	fields := struct {
		Container string       `json:"containerId"`
		Parents   []string     `json:"parentFolder"`
		Name      string       `json:"fileName"`
		Type      string       `json:"fileType"`
		Size      int64        `json:"length"`
		Mode      string       `json:"uploadMode"`
		Upload    int          `json:"uploadType"`
		Rename    int          `json:"autoRename"`
		Parts     []uploadPart `json:"multiParts"`
	}{
		Container: d.container, Parents: []string{d.parentParameter(parent)}, Name: name, Type: "45",
		Size: size, Mode: "multipart", Upload: 1, Rename: 3,
		Parts: []uploadPart{{Number: 1, Size: &size}},
	}
	response, change, err := d.beginMutation(ctx, "/files/create", fields)
	if err != nil {
		return receipt, change, err
	}
	defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	var refusal error
	if response.StatusCode != http.StatusOK {
		refusal = statusError(response.StatusCode)
		if response.StatusCode != http.StatusConflict {
			return receipt, change, refusal
		}
	}
	if err := httpcontent.DecodeJSON(response, d.limit, &receipt); err != nil {
		return receipt, change, errors.Join(refusal, err)
	}
	if receipt.Existing != "" {
		if receipt.ID != "" && receipt.ID != receipt.Existing ||
			receipt.Name != "" && receipt.Name != name || receipt.Container != "" && receipt.Container != d.container ||
			len(receipt.Parents) != 0 && receipt.Parents[0] != parent ||
			receipt.Rapid || receipt.UploadID != "" || len(receipt.Parts) != 0 {
			return receipt, change, core.Fail(core.InvalidUpstreamResponse, errors.New("inconsistent KooDrive upload conflict receipt"))
		}
		return receipt, core.Change{Effect: core.Unchanged, Before: core.TargetPresent}, core.Fail(core.AlreadyExists, nil)
	}
	if err := errors.Join(refusal, receipt.result.check()); err != nil {
		return receipt, change, err
	}
	if receipt.ID == "" || receipt.ID == parent || receipt.ID == d.root || receipt.Container != d.container ||
		receipt.Name != name || len(receipt.Parents) != 1 || receipt.Parents[0] != parent || receipt.Rapid ||
		len(receipt.Parts) != 1 || receipt.Parts[0].Number != 1 || receipt.Parts[0].Size == nil || *receipt.Parts[0].Size != size {
		return receipt, change, core.Fail(core.InvalidUpstreamResponse, errors.New("KooDrive did not confirm the requested upload and part"))
	}
	change.Before = core.TargetMissing
	return receipt, change, nil
}

func (d *Driver) putPart(ctx context.Context, address string, input io.Reader, size int64) (err error) {
	if err := checkContentURL(address); err != nil {
		return err
	}
	var body io.Reader
	if size != 0 {
		body = upload.NewBody(ctx, input, &size)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, address, body)
	if err != nil {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.ContentLength = size
	response, _, err := d.exchange(&d.content, req)
	if response != nil {
		defer func() { err = errors.Join(err, d.cleanup.CloseBody(response.Body), ctx.Err()) }()
	}
	if stream, ok := body.(*upload.Body); ok {
		complete, inputErr := stream.Finish()
		if inputErr != nil {
			return errors.Join(inputErr, err)
		}
		if err == nil && response.StatusCode == http.StatusOK && !complete {
			return core.Fail(core.InvalidUpstreamResponse, errors.New("KooDrive part acknowledged before input EOF"))
		}
	}
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return statusError(response.StatusCode)
	}
	return nil
}

var _ core.FileCreator = (*Driver)(nil)
