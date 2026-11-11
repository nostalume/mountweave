package smb

import (
	"context"
	"errors"
	"io"
	"math"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

// Create makes a visible new file at FILE_CREATE's native exclusion point.
// The caller owns input; partial or unknown writes never trigger name-based undo.
func (d *Driver) Create(ctx context.Context, p core.NativeAddress, input io.Reader, length *int64) (result core.Change, err error) {
	result.Effect = core.Unchanged
	if input == nil || length != nil && *length < 0 {
		return result, core.Fail(core.InvalidRequest, nil)
	}
	if err = d.admit(p); err != nil {
		return result, err
	}
	if p.Value() == "/" {
		return result, core.Fail(core.AlreadyExists, nil)
	}
	c, err := d.borrow(ctx)
	if err != nil {
		return result, err
	}
	end := c.scope(ctx)
	defer func() { err = errors.Join(err, end(), ctx.Err(), d.put(c)) }()
	size := int64(d.options.BlockBytes)
	if length != nil {
		size = max(1, min(size, *length))
	}
	lease, err := d.buffer.Acquire(ctx, size, 1)
	if err != nil {
		return result, core.Fail(core.CapacityExceeded, err)
	}
	defer lease.Release()
	body := upload.NewBody(ctx, input, length)
	defer body.Finish()
	if length != nil && *length == 0 {
		if _, err = body.Read(make([]byte, 1)); err != io.EOF {
			return result, err
		}
	}
	target, _, err := d.locate(ctx, c, p.Value(), true)
	if err != nil {
		return result, err
	}
	h, _, err := c.open(ctx, target, false, true)
	if err != nil {
		if !createRefused(err) {
			result.Effect = core.Unknown
		}
		_, closeErr := d.closeHandle(c, h)
		return result, errors.Join(err, closeErr)
	}
	result.Effect = core.Incomplete
	var offset int64
	for {
		n, inputErr := upload.ReadChunk(ctx, body, lease.Buffer())
		if inputErr != nil && inputErr != io.EOF {
			err = inputErr
			break
		}
		if int64(n) > math.MaxInt64-offset {
			err = core.Fail(core.CapacityExceeded, errors.New("SMB input exceeds supported offsets"))
			break
		}
		for data := lease.Buffer()[:n]; len(data) != 0; {
			written, failure := h.writeAt(ctx, data, offset)
			offset += int64(written)
			data = data[written:]
			if failure != nil {
				result.Effect = core.Unknown
				err = failure
				break
			}
			if written == 0 {
				err = io.ErrShortWrite
				break
			}
		}
		if err != nil || inputErr == io.EOF {
			break
		}
	}
	complete, inputErr := body.Finish()
	err = errors.Join(err, inputErr)
	contentConfirmed := complete && err == nil
	ack, closeErr := d.closeHandle(c, h)
	err = errors.Join(err, closeErr)
	if !ack {
		result.Effect = core.Unknown
		if err == nil {
			err = core.Fail(core.UpstreamFailure, errors.New("SMB file close was not confirmed"))
		}
	} else if contentConfirmed {
		result.Effect = core.Applied
	}
	return result, err
}

func (d *Driver) Mkdir(ctx context.Context, p core.NativeAddress) (result core.Change, err error) {
	result.Effect = core.Unchanged
	if err = d.admit(p); err != nil {
		return result, err
	}
	if p.Value() == "/" {
		return result, core.Fail(core.AlreadyExists, nil)
	}
	c, err := d.borrow(ctx)
	if err != nil {
		return result, err
	}
	end := c.scope(ctx)
	defer func() { err = errors.Join(err, end(), ctx.Err(), d.put(c)) }()
	target, _, err := d.locate(ctx, c, p.Value(), true)
	if err != nil {
		return result, err
	}
	h, _, err := c.open(ctx, target, true, true)
	if err == nil {
		result.Effect = core.Applied
	} else if !createRefused(err) {
		result.Effect = core.Unknown
	}
	_, closeErr := d.closeHandle(c, h)
	return result, errors.Join(err, closeErr)
}

var (
	_ core.Driver         = (*Driver)(nil)
	_ core.FileCreator    = (*Driver)(nil)
	_ core.DirectoryMaker = (*Driver)(nil)
)
