package pan123

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

// The native multipart limit is documented as 10 GB.
const maxUploadFileBytes int64 = 10_000_000_000

type partSession struct {
	ID        string   `json:"preuploadID"`
	FileID    nativeID `json:"fileID"`
	Reuse     *bool    `json:"reuse"`
	SliceSize int64    `json:"sliceSize"`
	Servers   []string `json:"servers"`
}

func (d *Driver) createParts(ctx context.Context, dir, name string, source core.Replayable, size int64) (core.Change, error) {
	result := core.Change{Effect: core.Unchanged}
	parent, err := d.directoryID(ctx, dir)
	if err != nil {
		return result, err
	}
	native, strong := md5.New(), sha256.New()
	lease, err := d.reserveUpload(ctx, min(d.singleLimit, 32<<10))
	if err != nil {
		return result, err
	}
	err = hashUpload(ctx, source, size, lease.Buffer(), io.MultiWriter(native, strong))
	lease.Release()
	if err != nil {
		return result, err
	}
	fields := struct {
		Parent json.Number `json:"parentFileID"`
		Name   string      `json:"filename"`
		MD5    string      `json:"etag"`
		Size   int64       `json:"size"`
	}{json.Number(parent), name, hex.EncodeToString(native.Sum(nil)), size}
	base := "/upload/v2/file/"
	if d.presigned {
		base = "/upload/v1/file/"
	}
	req, err := controlPost(ctx, base+"create", fields)
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if d.closed.Load() {
		return result, core.Fail(core.MountUnavailable, nil)
	}
	result.Effect = core.Unknown
	var session partSession
	decoded, err := d.request(&d.api, req, &session)
	if !decoded {
		return result, err
	}
	if session.Reuse != nil && *session.Reuse && session.FileID != "" && session.FileID != "0" {
		return core.Change{Effect: core.Applied, Before: core.TargetMissing}, err
	}
	if session.Reuse == nil || *session.Reuse || session.FileID != "" && session.FileID != "0" ||
		session.ID == "" || session.SliceSize <= 0 {
		return result, errors.Join(core.Fail(core.InvalidUpstreamResponse, errors.New("123 returned an invalid upload session")), err)
	}
	if err != nil {
		return result, err
	}
	if err := d.sendParts(ctx, source, name, size, session, strong.Sum(nil)); err != nil {
		return result, err
	}
	return d.finishParts(ctx, base, session.ID)
}

func hashUpload(ctx context.Context, input io.Reader, size int64, buffer []byte, digest io.Writer) error {
	body := upload.NewBody(ctx, input, &size)
	defer body.Finish()
	if _, err := io.CopyBuffer(digest, body, buffer); err != nil {
		return core.Fail(core.InvalidRequest, err)
	}
	return nil
}

func openReplay(ctx context.Context, source core.Replayable, size int64) (io.ReadCloser, error) {
	body, err := source.OpenRange(ctx, 0, size)
	if err != nil {
		if body != nil {
			err = errors.Join(err, body.Close())
		}
		return nil, err
	}
	if body == nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("source omitted its range body"))
	}
	return body, nil
}

func (d *Driver) sendParts(ctx context.Context, source core.Replayable, name string, size int64, session partSession, expected []byte) error {
	var target string
	if !d.presigned {
		var err error
		target, err = uploadTarget(session.Servers, "/upload/v2/file/slice")
		if err != nil {
			return err
		}
	}
	count := (size-1)/session.SliceSize + 1
	if count > int64(math.MaxInt) {
		return core.Fail(core.CapacityExceeded, upload.ErrParts)
	}
	lease, err := d.reserveUpload(ctx, min(session.SliceSize, size))
	if err != nil {
		return err
	}
	defer lease.Release()
	// The stream may be a subview of the source. Check its replay coordinates
	// before sending any part, using the same buffer as the subsequent upload.
	check, err := openReplay(ctx, source, size)
	if err != nil {
		return err
	}
	strong := sha256.New()
	if err := errors.Join(hashUpload(ctx, check, size, lease.Buffer(), strong), check.Close()); err != nil {
		return err
	}
	if !bytes.Equal(strong.Sum(nil), expected) {
		return core.Fail(core.InvalidRequest, errors.New("replayed bytes differ from the supplied input"))
	}
	replay, err := openReplay(ctx, source, size)
	if err != nil {
		return err
	}
	_, _, err = lease.Run(ctx, replay, &size, int(count), func(call context.Context, index int, part []byte) error {
		if d.closed.Load() {
			return core.Fail(core.MountUnavailable, nil)
		}
		if d.presigned {
			return d.sendPresignedPart(call, session.ID, index, part)
		}
		input := upload.Borrow(part)
		defer input.Close()
		digest := md5.Sum(part)
		fields := [][2]string{{"preuploadID", session.ID}, {"sliceNo", strconv.Itoa(index)}, {"sliceMD5", hex.EncodeToString(digest[:])}}
		req, err := multipartRequest(call, target, fields, "slice", name, int64(len(part)), input)
		if err != nil {
			return core.Fail(core.InvalidRequest, err)
		}
		_, err = d.request(d.uploadClient, req, nil)
		return err
	})
	return errors.Join(err, replay.Close())
}

func (d *Driver) sendPresignedPart(ctx context.Context, sessionID string, index int, part []byte) (err error) {
	fields := struct {
		ID    string `json:"preuploadID"`
		Index int    `json:"sliceNo"`
	}{sessionID, index}
	req, err := controlPost(ctx, "/upload/v1/file/get_upload_url", fields)
	if err != nil {
		return err
	}
	var capability struct {
		URL string `json:"presignedURL"`
	}
	if _, err := d.request(&d.api, req, &capability); err != nil {
		return err
	}
	u, err := url.Parse(capability.URL)
	if err != nil || !httpcontent.IsHTTPS(u) {
		return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid presigned upload address"))
	}
	input := upload.Borrow(part)
	defer input.Close()
	req, err = http.NewRequestWithContext(ctx, http.MethodPut, u.String(), input)
	if err != nil {
		return core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.ContentLength = int64(len(part))
	req.Header.Set("Content-Type", "application/octet-stream")
	response, err := d.exchange(&d.content, req, true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.closeBody(response.Body, true)) }()
	if response.StatusCode != http.StatusOK {
		return core.Fail(core.UpstreamFailure, fmt.Errorf("123 presigned slice returned HTTP %d", response.StatusCode))
	}
	return nil
}

func (d *Driver) finishParts(ctx context.Context, base, sessionID string) (core.Change, error) {
	result := core.Change{Effect: core.Unknown}
	fields := struct {
		ID string `json:"preuploadID"`
	}{sessionID}
	complete, query := base+"upload_complete", base+"upload_async_result"
	route := complete
	var unconfirmed, cleanup error
	for {
		if err := ctx.Err(); err != nil {
			return result, errors.Join(unconfirmed, cleanup, err)
		}
		if d.closed.Load() {
			return result, errors.Join(unconfirmed, cleanup, core.Fail(core.MountUnavailable, nil))
		}
		req, err := controlPost(ctx, route, fields)
		if err != nil {
			return result, errors.Join(unconfirmed, cleanup, err)
		}
		var receipt struct {
			uploadReceipt
			Async *bool `json:"async"`
		}
		v1Commit := d.presigned && route == complete
		decoded, lost, err, closeErr := d.requestReply(&d.api, req, &receipt)
		// A decoded pending receipt permits one more observation after a close failure.
		stopPolling := cleanup != nil
		cleanup = errors.Join(cleanup, closeErr)
		if !decoded {
			if v1Commit && lost {
				// This V1 endpoint only observes the same upload. Do not replay completion.
				unconfirmed, route = err, query
				continue
			}
			return result, errors.Join(unconfirmed, err, cleanup)
		}
		if v1Commit && receipt.Async != nil && *receipt.Async {
			if receipt.Completed == nil || *receipt.Completed {
				return result, errors.Join(core.Fail(core.InvalidUpstreamResponse, errors.New("invalid 123 async upload receipt")), cleanup)
			}
			route = query
			continue
		}
		if (!v1Commit || receipt.Async != nil) && receipt.confirmsFile() {
			return core.Change{Effect: core.Applied, Before: core.TargetMissing}, cleanup
		}
		if unconfirmed != nil {
			return result, errors.Join(unconfirmed, cleanup, errors.New("123 upload result did not confirm completion"))
		}
		if v1Commit || receipt.Completed == nil || *receipt.Completed {
			return result, errors.Join(core.Fail(core.InvalidUpstreamResponse, errors.New("123 upload did not confirm a completed file ID")), cleanup)
		}
		if stopPolling {
			return result, cleanup
		}
		wait := time.NewTimer(time.Second)
		select {
		case <-wait.C:
		case <-ctx.Done():
			wait.Stop()
			return result, errors.Join(cleanup, ctx.Err())
		}
	}
}
