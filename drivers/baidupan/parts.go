package baidupan

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

const partBytes int64 = 4 << 20

func (d *Driver) createParts(ctx context.Context, full string, source core.Replayable, size int64) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if d.closed.Load() {
		return unchanged, core.Fail(core.MountUnavailable, nil)
	}
	lease, err := d.parts.Acquire(ctx, min(size, d.partSize), 1)
	if err != nil {
		if errors.Is(err, upload.ErrCapacity) {
			err = core.Fail(core.CapacityExceeded, err)
		}
		return unchanged, err
	}
	defer lease.Release()
	buffer := lease.Buffer()
	digests, err := blockManifest(ctx, source, size, buffer)
	if err != nil {
		return unchanged, err
	}
	manifest := make([]string, len(digests))
	for i, digest := range digests {
		manifest[i] = hex.EncodeToString(digest.md5[:])
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return unchanged, err
	}
	token, err := d.credential(ctx)
	if err != nil {
		return unchanged, err
	}
	task, prepared, err := d.precreate(ctx, full, token, size, string(encoded))
	if err != nil {
		return prepared, err
	}
	missing := *task.Missing
	if len(missing) == 0 {
		missing = []int{0}
	}
	seen := make([]bool, len(manifest))
	for _, part := range missing {
		if part < 0 || part >= len(seen) || seen[part] {
			return unchanged, core.Fail(core.InvalidUpstreamResponse, errors.New("native precreate returned an invalid part sequence"))
		}
		seen[part] = true
	}
	host, err := d.taskUploadHost(ctx, full, token, task.ID)
	if err != nil {
		return unchanged, err
	}
	for _, part := range missing {
		offset := int64(part) * d.partSize
		data := buffer[:int(min(d.partSize, size-offset))]
		if err := upload.ReadRange(ctx, source, offset, data); err != nil {
			// Keep input error types unless redaction or EOF normalization is needed.
			message := err.Error()
			if errors.Is(err, io.EOF) || safeText(message, token) != message {
				err = privateError(err, token)
			}
			return unchanged, err
		}
		if sha256.Sum256(data) != digests[part].sha256 {
			return unchanged, core.Fail(core.InvalidRequest, errors.New("replayed range differs from the submitted input"))
		}
		if err := lease.Send(ctx, func() error {
			retry, err := d.sendPart(ctx, host, full, token, task.ID, part, data, manifest[part])
			if retry && ctx.Err() == nil {
				next, routeErr := d.taskUploadHost(ctx, full, token, task.ID)
				if routeErr != nil {
					return errors.Join(err, routeErr)
				}
				host = next
				_, second := d.sendPart(ctx, host, full, token, task.ID, part, data, manifest[part])
				if second == nil {
					return nil
				}
				err = errors.Join(err, second)
			}
			return err
		}); err != nil {
			return unchanged, err
		}
	}
	return d.createEntry(ctx, full, token, url.Values{"uploadid": {task.ID}, "block_list": {string(encoded)}}, &size)
}

type partDigest struct {
	md5    [md5.Size]byte
	sha256 [sha256.Size]byte
}

func blockManifest(ctx context.Context, input io.Reader, size int64, buffer []byte) ([]partDigest, error) {
	body := upload.NewBody(ctx, input, &size)
	defer body.Finish()
	partSize := int64(len(buffer))
	digests := make([]partDigest, 0, (size+partSize-1)/partSize)
	for {
		n, err := body.Read(buffer)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n != 0 {
			digests = append(digests, partDigest{md5.Sum(buffer[:n]), sha256.Sum256(buffer[:n])})
		}
		if err == io.EOF {
			return digests, nil
		}
	}
}

type partTask struct {
	ID      string `json:"uploadid"`
	Path    string `json:"path"`
	Missing *[]int `json:"block_list"`
}

func (d *Driver) precreate(ctx context.Context, full, token string, size int64, manifest string) (task partTask, change core.Change, err error) {
	change.Effect = core.Unchanged
	form := url.Values{"path": {full}, "size": {strconv.FormatInt(size, 10)}, "isdir": {"0"}, "autoinit": {"1"}, "block_list": {manifest}}
	response, entered, err := d.postXpan(ctx, "precreate", token, form)
	if entered {
		change.Effect = core.Unknown
	}
	if err != nil {
		return task, change, err
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, token), ctx.Err()) }()
	data, code, err := d.writeReply(response, token, true)
	if err != nil {
		if code == -8 || code == 31061 {
			change = core.Change{Effect: core.Unchanged, Before: core.TargetPresent}
			err = core.Fail(core.AlreadyExists, err)
		}
		return task, change, err
	}
	if err := json.Unmarshal(data, &task); err != nil {
		return task, change, core.Fail(core.InvalidUpstreamResponse, privateError(err, token))
	}
	if task.ID == "" || task.Path != full {
		return task, change, core.Fail(core.InvalidUpstreamResponse, errors.New("native precreate did not confirm the requested upload task"))
	}
	change.Effect = core.Unchanged
	if task.Missing == nil {
		return task, change, core.Fail(core.InvalidUpstreamResponse, errors.New("native precreate omitted its part list"))
	}
	return task, change, nil
}

func (d *Driver) taskUploadHost(ctx context.Context, full, token, id string) (target *url.URL, err error) {
	query := url.Values{"method": {"locateupload"}, "appid": {"250528"}, "upload_version": {"2.0"}, "access_token": {token}, "path": {full}, "uploadid": {id}}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://d.pcs.baidu.com/rest/2.0/pcs/file?"+query.Encode(), nil)
	if err != nil {
		return nil, core.Fail(core.InvalidRequest, privateError(err, token))
	}
	response, _, err := d.do(request, token)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, token), ctx.Err()) }()
	data, _, err := d.writeReply(response, token, false)
	if err != nil {
		return nil, err
	}
	var routing struct {
		Code    *int `json:"error_code"`
		Servers []struct {
			URL string `json:"server"`
		} `json:"servers"`
	}
	if err := json.Unmarshal(data, &routing); err != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, privateError(err, token))
	}
	if routing.Code != nil && *routing.Code == 0 {
		for _, server := range routing.Servers {
			u, parseErr := url.Parse(server.URL)
			if parseErr == nil && contentURL(u) && u.Hostname() != "" && (u.Path == "" || u.Path == "/") && u.RawQuery == "" && !u.ForceQuery {
				return u, nil
			}
		}
	}
	return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("native task routing returned no usable host"))
}

func (d *Driver) sendPart(ctx context.Context, host *url.URL, full, token, id string, part int, data []byte, digest string) (retry bool, err error) {
	target := *host
	target.Path = "/rest/2.0/pcs/superfile2"
	target.RawPath = ""
	target.RawQuery = url.Values{"method": {"upload"}, "type": {"tmpfile"}, "path": {full}, "access_token": {token}, "uploadid": {id}, "partseq": {strconv.Itoa(part)}}.Encode()
	body := upload.Borrow(data)
	defer body.Close()
	size := int64(len(data))
	request, err := multipartRequest(ctx, &target, path.Base(full), body, &size)
	if err != nil {
		return false, core.Fail(core.InvalidRequest, privateError(err, token))
	}
	response, entered, err := d.do(request, token)
	if err != nil {
		return entered, err
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, token), ctx.Err()) }()
	data, _, err = d.writeReply(response, token, false)
	if err != nil {
		return false, err
	}
	var receipt struct {
		MD5 string `json:"md5"`
	}
	if err := json.Unmarshal(data, &receipt); err != nil {
		return false, core.Fail(core.InvalidUpstreamResponse, privateError(err, token))
	}
	if !strings.EqualFold(receipt.MD5, digest) {
		return false, core.Fail(core.InvalidUpstreamResponse, errors.New("native part receipt disagrees with the manifest"))
	}
	return false, nil
}
