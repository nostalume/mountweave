package baidupan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

// The native documentation says 2 GB without a byte definition. This local
// 2 GiB payload guard bounds streaming; a stricter native limit still applies.
const singleUploadBytes int64 = 2 << 30

// Create streams one exclusive upload or sends numbered parts from a stable
// source above the single-request guard.
func (d *Driver) Create(ctx context.Context, p core.NativeAddress, input io.Reader, length *int64) (result core.Change, err error) {
	result.Effect = core.Unchanged
	var stable core.Replayable
	if input == nil || length != nil && *length < 0 {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	if length != nil {
		n := *length
		length = &n
		if n == 0 {
			return result, core.Fail(core.UnsupportedOperation, errors.New("native upload does not support empty files"))
		}
		if n > singleUploadBytes {
			stable, _ = input.(core.Replayable)
			if d.parts == nil || n > d.maxFileSize || stable == nil || stable.SourceVersion() == "" {
				return result, core.Fail(core.CapacityExceeded, errors.New("large upload requires a stable source within the native part window"))
			}
		}
	}
	full, err := d.address(p)
	if err != nil {
		return result, err
	}
	if p.Value() == "/" {
		return result, core.Fail(core.AlreadyExists, nil)
	}
	if stable != nil {
		return d.createParts(ctx, full, stable, *length)
	}
	token, err := d.credential(ctx)
	if err != nil {
		return result, err
	}
	target, err := d.uploadHost(ctx, token)
	if err != nil {
		return result, err
	}

	body := upload.NewBody(ctx, input, length, singleUploadBytes)
	defer body.Finish()
	target.Path = "/rest/2.0/pcs/file"
	target.RawQuery = url.Values{"method": {"upload"}, "ondup": {"fail"}, "path": {full}, "access_token": {token}}.Encode()
	request, err := multipartRequest(ctx, target, path.Base(full), body, length)
	if err != nil {
		return result, core.Fail(core.InvalidRequest, privateError(err, token))
	}
	response, entered, requestErr := d.do(request, token)
	complete, inputErr := body.Finish()
	read := body.PayloadBytes()
	if entered {
		result.Effect = core.Unknown
	}
	if requestErr != nil {
		return result, errors.Join(requestErr, privateError(inputErr, token))
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, token), ctx.Err()) }()
	data, code, failure := d.writeReply(response, token, false)
	if failure != nil {
		if code == -8 || code == 31061 {
			result.Effect, result.Before = core.Unchanged, core.TargetPresent
			failure = core.Fail(core.AlreadyExists, failure)
		}
		return result, errors.Join(failure, privateError(inputErr, token))
	}
	var receipt struct {
		ID   *uint64 `json:"fs_id"`
		Path string  `json:"path"`
		Size *int64  `json:"size"`
	}
	if decodeErr := json.Unmarshal(data, &receipt); decodeErr != nil {
		return result, core.Fail(core.InvalidUpstreamResponse, errors.Join(privateError(decodeErr, token), privateError(inputErr, token)))
	}
	if receipt.ID == nil || receipt.Path != full || receipt.Size == nil || *receipt.Size < 0 || *receipt.Size > read {
		return result, core.Fail(core.InvalidUpstreamResponse, errors.Join(errors.New("native upload receipt is missing or contradicts the submitted file"), privateError(inputErr, token)))
	}
	if inputErr != nil {
		result.Effect, result.Before = core.Incomplete, core.TargetMissing
		return result, privateError(inputErr, token)
	}
	if !complete || *receipt.Size != read {
		return result, core.Fail(core.InvalidUpstreamResponse, errors.New("native receipt does not confirm the complete input"))
	}
	if read == 0 {
		return result, core.Fail(core.UnsupportedOperation, errors.New("native upload does not support empty files"))
	}
	result.Effect, result.Before = core.Applied, core.TargetMissing
	return result, nil
}

// writeReply admits the response envelope before any success-only fields.
// The caller closes the response after recording its native effect.
func (d *Driver) writeReply(response *http.Response, token string, xpan bool) ([]byte, int, error) {
	data, err := d.readBody(response, token)
	if err != nil {
		return nil, 0, err
	}
	var ack struct {
		Errno        *int            `json:"errno"`
		ErrorCode    *int            `json:"error_code"`
		Message      string          `json:"errmsg"`
		ErrorMessage string          `json:"error_msg"`
		Show         string          `json:"show_msg"`
		RequestID    json.RawMessage `json:"request_id"`
	}
	if err := json.Unmarshal(data, &ack); err != nil {
		if response.StatusCode != http.StatusOK {
			return nil, 0, errors.Join(d.httpFailure(response, data, token), privateError(err, token))
		}
		return nil, 0, core.Fail(core.InvalidUpstreamResponse, privateError(err, token))
	}
	for _, code := range []*int{ack.Errno, ack.ErrorCode} {
		if code != nil && *code != 0 {
			return nil, *code, nativeFailure(*code, reply{Message: ack.Message + " " + ack.ErrorMessage, Show: ack.Show, RequestID: ack.RequestID}, token)
		}
	}
	if response.StatusCode != http.StatusOK {
		return nil, 0, d.httpFailure(response, data, token)
	}
	if xpan && ack.Errno == nil {
		return nil, 0, core.Fail(core.InvalidUpstreamResponse, errors.New("native response omitted errno"))
	}
	return data, 0, nil
}

// multipartRequest buffers the framing and borrows the payload as a single body.
func multipartRequest(ctx context.Context, target *url.URL, name string, body io.ReadCloser, length *int64) (*http.Request, error) {
	var framing bytes.Buffer
	writer := multipart.NewWriter(&framing)
	if _, err := writer.CreateFormFile("file", name); err != nil {
		return nil, err
	}
	prefix := framing.Len()
	if err := writer.Close(); err != nil {
		return nil, err
	}
	frame := framing.Bytes()
	payload := struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(frame[:prefix]), body, bytes.NewReader(frame[prefix:])), body}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), payload)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.ContentLength = -1
	if length != nil {
		if *length > math.MaxInt64-int64(len(frame)) {
			return nil, core.Fail(core.CapacityExceeded, nil)
		}
		req.ContentLength = int64(len(frame)) + *length
	}
	return req, nil
}

// This public routing variant is also used by the official browser client.
// It grants no account access and needs no pre-upload task or uploadid.
func (d *Driver) uploadHost(ctx context.Context, token string) (target *url.URL, err error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://d.pcs.baidu.com/rest/2.0/pcs/file?method=locateupload", nil)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, err)
	}
	response, _, err := d.do(request, token)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, token)) }()
	data, err := d.readBody(response, token)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, d.httpFailure(response, data, token)
	}
	var routing struct {
		Code      *int            `json:"error_code"`
		Message   string          `json:"error_msg"`
		RequestID json.RawMessage `json:"request_id"`
		Servers   []string        `json:"server"`
	}
	if err := json.Unmarshal(data, &routing); err != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, privateError(err, token))
	}
	if routing.Code == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("native upload routing omitted its result"))
	}
	if *routing.Code != 0 {
		return nil, nativeFailure(*routing.Code, reply{Message: routing.Message, RequestID: routing.RequestID}, token)
	}
	for _, host := range routing.Servers {
		u, parseErr := url.Parse("https://" + host)
		if parseErr == nil && contentURL(u) && u.Hostname() != "" && u.Host == host && u.Path == "" && u.RawQuery == "" && !u.ForceQuery {
			return u, nil
		}
	}
	return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("native upload routing returned no usable host"))
}

var _ core.FileCreator = (*Driver)(nil)
