package azureblob

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

func (d *Driver) Create(ctx context.Context, p core.NativeAddress, input io.Reader, length *int64) (core.Change, error) {
	return d.Publish(ctx, p, input, length, core.PublicationOptions{When: core.IfAbsent()})
}

func (d *Driver) Publish(ctx context.Context, p core.NativeAddress, input io.Reader, length *int64, o core.PublicationOptions) (result core.Change, err error) {
	result.Effect = core.Unchanged
	access, err := conditions(o.When, true)
	if err != nil {
		return result, err
	}
	if input == nil || length != nil && *length < 0 {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	if err = d.admit(ctx, p, false); err != nil {
		return result, err
	}
	if d.buffer == nil {
		return result, core.Fail(core.UnsupportedOperation, errors.New("upload buffering is not configured"))
	}
	size, parts := d.part, 0
	if length != nil {
		n := *length
		length = &n
		if n > 0 && (n-1)/d.part >= blockblob.MaxBlocks {
			return result, core.Fail(core.CapacityExceeded, upload.ErrParts)
		}
		parts = max(1, int((max(1, n)-1)/d.part+1))
		if parts == 1 {
			size = max(1, n)
		}
	}
	before := core.TargetUnknown
	if o.ObserveTarget && !o.When.IsAbsent() && o.When.Match() == "" {
		_, e := d.Stat(ctx, p)
		before = core.TargetPresent
		if e != nil {
			if core.CodeOf(e) != core.NotFound {
				return result, e
			}
			before = core.TargetMissing
		}
	}
	defer func() {
		if result.Effect == core.Applied {
			if o.When.IsAbsent() {
				before = core.TargetMissing
			} else if o.When.Match() != "" {
				before = core.TargetPresent
			}
		}
		result.Before = before
	}()
	lease, err := d.buffer.Acquire(ctx, size, parts)
	if err != nil {
		return result, inputError(err)
	}
	defer lease.Release()
	b := d.client.NewBlockBlobClient(d.prefix + p.Value())
	if length != nil && parts == 1 {
		data := lease.Buffer()[:int(*length)]
		if err = upload.ReadExact(ctx, input, data); err != nil {
			return result, inputError(err)
		}
		body := upload.Borrow(data)
		defer body.Close()
		err = lease.Send(ctx, func() error {
			if e := ctx.Err(); e != nil {
				return e
			}
			var raw *http.Response
			out, e := b.Upload(noReplay(ctx, &raw), body, &blockblob.UploadOptions{AccessConditions: access})
			result, e = mutationResult(ctx, raw, e, o.When, http.StatusCreated)
			if result.Effect == core.Applied {
				result.Evidence = evidence(out.ETag, out.VersionID)
			}
			return e
		})
		return result, err
	}
	var nonce [20]byte
	if _, err = rand.Read(nonce[:16]); err != nil {
		return result, err
	}
	var mu sync.Mutex
	var ids []string
	_, _, err = lease.Run(ctx, input, length, blockblob.MaxBlocks, func(call context.Context, index int, data []byte) error {
		id := nonce
		binary.BigEndian.PutUint32(id[16:], uint32(index))
		encoded := base64.StdEncoding.EncodeToString(id[:])
		body := upload.Borrow(data)
		defer body.Close()
		_, e := b.StageBlock(call, encoded, body, nil)
		if e != nil {
			return translate(e)
		}
		mu.Lock()
		for len(ids) < index {
			ids = append(ids, "")
		}
		ids[index-1] = encoded
		mu.Unlock()
		return nil
	})
	if err != nil {
		return result, inputError(err)
	}
	err = lease.Send(ctx, func() error {
		if e := ctx.Err(); e != nil {
			return e
		}
		var raw *http.Response
		out, e := b.CommitBlockList(noReplay(ctx, &raw), ids, &blockblob.CommitBlockListOptions{AccessConditions: access})
		result, e = mutationResult(ctx, raw, e, o.When, http.StatusCreated)
		if result.Effect == core.Applied {
			result.Evidence = evidence(out.ETag, out.VersionID)
		}
		return e
	})
	return result, err
}

func noReplay(ctx context.Context, raw **http.Response) context.Context {
	return policy.WithCaptureResponse(policy.WithRetryOptions(ctx, policy.RetryOptions{MaxRetries: -1}), raw)
}

func mutationResult(ctx context.Context, raw *http.Response, err error, when core.Condition, expected int) (core.Change, error) {
	if err == nil {
		return core.Change{Effect: core.Applied}, ctx.Err()
	}
	if raw != nil && raw.StatusCode == expected {
		return core.Change{Effect: core.Applied}, errors.Join(translate(err), ctx.Err())
	}
	result := core.Change{Effect: core.Unknown}
	var response *azcore.ResponseError
	if errors.As(err, &response) && response.ErrorCode != "" && response.StatusCode >= 400 && response.StatusCode < 500 && response.StatusCode != 408 {
		result.Effect = core.Unchanged
		if response.StatusCode == 412 {
			if when.IsAbsent() {
				return result, errors.Join(core.Fail(core.AlreadyExists, err), ctx.Err())
			}
			return result, errors.Join(conditionFailure(when, err), ctx.Err())
		}
	}
	return result, errors.Join(translate(err), ctx.Err())
}

func inputError(err error) error {
	var native *core.Error
	if errors.As(err, &native) {
		return err
	}
	switch {
	case errors.Is(err, upload.ErrCapacity), errors.Is(err, upload.ErrParts):
		return core.Fail(core.CapacityExceeded, err)
	case errors.Is(err, upload.ErrSize), errors.Is(err, io.ErrUnexpectedEOF):
		return core.Fail(core.InvalidRequest, err)
	default:
		return err
	}
}

func (d *Driver) Remove(ctx context.Context, p core.NativeAddress, policies ...core.RemoveOptions) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if len(policies) > 1 {
		return unchanged, core.Fail(core.InvalidRequest, nil)
	}
	var o core.RemoveOptions
	if len(policies) == 1 {
		o = policies[0]
	}
	// Blob Delete has no atomic resource-type guard for hierarchical namespaces.
	if o.Target != "" {
		return unchanged, core.Fail(core.UnsupportedOperation, nil)
	}
	access, err := conditions(o.When, true)
	if err != nil {
		return unchanged, err
	}
	if err = d.admit(ctx, p, false); err != nil {
		return unchanged, err
	}
	var raw *http.Response
	_, err = d.client.NewBlobClient(d.prefix+p.Value()).Delete(noReplay(ctx, &raw), &blob.DeleteOptions{AccessConditions: access})
	result, err := mutationResult(ctx, raw, err, o.When, http.StatusAccepted)
	if core.CodeOf(err) == core.AlreadyExists {
		err = conditionFailure(o.When, err)
	}
	return result, err
}

var _ core.FileCreator = (*Driver)(nil)
var _ core.FilePublisher = (*Driver)(nil)
var _ core.Remover = (*Driver)(nil)
