package pan123

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

// The native single-request limit is documented as 1 GB.
const singleUploadBytes int64 = 1_000_000_000

// Create sends a validated, bounded payload to the native exclusive upload.
func (d *Driver) Create(ctx context.Context, address core.NativeAddress, input io.Reader, length *int64) (core.Change, error) {
	result := core.Change{Effect: core.Unchanged}
	if err := d.check(ctx, address); err != nil {
		return result, err
	}
	if d.uploadBudget == nil || length == nil {
		return result, core.Fail(core.UnsupportedOperation, errors.New("native upload requires configuration and a known length"))
	}
	size := *length
	if size < 0 || input == nil {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	if size > maxUploadFileBytes {
		return result, core.Fail(core.CapacityExceeded, errors.New("native upload exceeds 10 GB"))
	}
	if address.Value() == "/" {
		result.Before = core.TargetPresent
		return result, core.Fail(core.AlreadyExists, nil)
	}
	name := path.Base(address.Value())
	if strings.TrimSpace(name) == "" || utf8.RuneCountInString(name) >= 255 || strings.ContainsAny(name, `":*?|><`) {
		return result, core.Fail(core.UnrepresentableName, nil)
	}
	if size > d.singleLimit {
		source, ok := input.(core.Replayable)
		if !ok || source.SourceVersion() == "" {
			return result, core.Fail(core.CapacityExceeded, errors.New("uploads above the single-request capacity require replayable input"))
		}
		return d.createParts(ctx, path.Dir(address.Value()), name, source, size)
	}
	parent, err := d.directoryID(ctx, path.Dir(address.Value()))
	if err != nil {
		return result, err
	}
	lease, err := d.reserveUpload(ctx, max(size, 1))
	if err != nil {
		return result, err
	}
	defer lease.Release()
	data := lease.Buffer()[:size]
	if err := upload.ReadExact(ctx, input, data); err != nil {
		return result, core.Fail(core.InvalidRequest, err)
	}
	var domains []string
	if err := d.call(ctx, "/upload/v2/file/domain", nil, &domains); err != nil {
		return result, err
	}
	target, err := uploadTarget(domains, "/upload/v2/file/single/create")
	if err != nil {
		return result, err
	}
	body := upload.Borrow(data)
	defer body.Close()
	digest := md5.Sum(data)
	// Omitting duplicate preserves the documented no-duplicate name rule.
	fields := [][2]string{{"parentFileID", parent}, {"filename", name}, {"etag", hex.EncodeToString(digest[:])}, {"size", strconv.FormatInt(size, 10)}}
	req, err := multipartRequest(ctx, target, fields, "file", name, size, body)
	if err != nil {
		return result, core.Fail(core.InvalidRequest, err)
	}
	err = lease.Send(ctx, func() error {
		if d.closed.Load() {
			return core.Fail(core.MountUnavailable, nil)
		}
		result.Effect = core.Unknown
		var receipt uploadReceipt
		decoded, err := d.request(d.uploadClient, req, &receipt)
		if !decoded {
			return err
		}
		if !receipt.confirmsFile() {
			return errors.Join(core.Fail(core.InvalidUpstreamResponse, errors.New("123 upload did not confirm a completed file ID")), err)
		}
		result.Effect, result.Before = core.Applied, core.TargetMissing
		return err
	})
	return result, err
}

func (d *Driver) reserveUpload(ctx context.Context, size int64) (*upload.Lease, error) {
	lease, err := d.uploadBudget.Acquire(ctx, size, 1)
	if errors.Is(err, upload.ErrCapacity) {
		return nil, core.Fail(core.CapacityExceeded, err)
	}
	return lease, err
}

func uploadTarget(domains []string, route string) (string, error) {
	for _, domain := range domains {
		u, err := url.Parse(domain)
		if err == nil && httpcontent.IsHTTPS(u) && u.RawQuery == "" && !u.ForceQuery && u.RawPath == "" && (u.Path == "" || u.Path == "/") {
			u.Path = route
			return u.String(), nil
		}
	}
	return "", core.Fail(core.InvalidUpstreamResponse, errors.New("123 returned no HTTPS upload origin"))
}

type uploadReceipt struct {
	ID        json.RawMessage `json:"fileID"`
	Completed *bool           `json:"completed"`
}

func (r uploadReceipt) confirmsFile() bool {
	var id nativeID
	return r.Completed != nil && *r.Completed && json.Unmarshal(r.ID, &id) == nil && id != "0"
}

func multipartRequest(ctx context.Context, target string, fields [][2]string, fieldName, name string, size int64, body io.ReadCloser) (*http.Request, error) {
	var framing bytes.Buffer
	form := multipart.NewWriter(&framing)
	for _, field := range fields {
		if err := form.WriteField(field[0], field[1]); err != nil {
			return nil, err
		}
	}
	if _, err := form.CreateFormFile(fieldName, name); err != nil {
		return nil, err
	}
	prefix := framing.Len()
	if err := form.Close(); err != nil {
		return nil, err
	}
	frame := framing.Bytes()
	payload := struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(frame[:prefix]), body, bytes.NewReader(frame[prefix:])), body}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, payload)
	if err != nil {
		return nil, err
	}
	req.ContentLength = size + int64(len(frame))
	req.Header.Set("Content-Type", form.FormDataContentType())
	return req, nil
}

var _ core.FileCreator = (*Driver)(nil)
