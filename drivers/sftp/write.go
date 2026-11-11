package sftp

import (
	"context"
	"errors"
	"io"
	"math"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
	sshfx "github.com/pkg/sftp/v2/encoding/ssh/filexfer"
)

// Generic v3 FAILURE can follow a visible create (for example, exhaustion while
// allocating its handle). Only an explicit refusal proves no effect.
func refused(err error) bool {
	var status *sshfx.StatusPacket
	if !errors.As(err, &status) {
		return false
	}
	switch status.StatusCode {
	case sshfx.StatusNoSuchFile, sshfx.StatusPermissionDenied, sshfx.StatusOpUnsupported:
		return true
	}
	return false
}

// Create makes a new visible file with native CREATE|EXCL. Input is borrowed
// synchronously; failed input or transport never triggers deletion by name.
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
	defer func() { err = errors.Join(err, end(), d.put(c)) }()
	lease, err := d.buffer.Acquire(ctx, windowBytes(d.options.MaxRequests, length), 1)
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
	target, _, err := d.resolve(c, p.Value(), true)
	if err != nil {
		return result, err
	}
	handle, err := c.wire.handle(&sshfx.OpenPacket{Filename: target, PFlags: sshfx.FlagWrite | sshfx.FlagCreate | sshfx.FlagExclusive})
	if err != nil {
		if !refused(err) {
			result.Effect = core.Unknown
		}
		return result, err
	}
	result.Effect = core.Incomplete
	var offset int64
	for {
		n, readErr := upload.ReadChunk(ctx, body, lease.Buffer())
		if readErr != nil && readErr != io.EOF {
			err = readErr
			break
		}
		if int64(n) > math.MaxInt64-offset {
			err = core.Fail(core.CapacityExceeded, errors.New("SFTP input exceeds supported offset range"))
			break
		}
		if n > 0 {
			written, failure := c.wire.writeAt(handle, lease.Buffer()[:n], offset, d.options.MaxRequests)
			offset += int64(written)
			if failure != nil || written != n {
				err = failure
				if err == nil {
					err = io.ErrShortWrite
				}
				break
			}
		}
		if readErr == io.EOF {
			break
		}
	}
	complete, inputErr := body.Finish()
	err = errors.Join(err, inputErr)
	owned := &handle
	acknowledged, closeErr := closeHandle(c, &owned, d.options.CleanupTimeout)
	err = errors.Join(err, closeErr)
	if !acknowledged && err == nil {
		err = core.Fail(core.UpstreamFailure, errors.New("SFTP file close was not confirmed"))
	}
	if c.wire.broken || !acknowledged {
		result.Effect = core.Unknown
	} else if err == nil && complete {
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
	defer func() { err = errors.Join(err, end(), d.put(c)) }()
	target, _, err := d.resolve(c, p.Value(), true)
	if err != nil {
		return result, err
	}
	err = c.wire.status(&sshfx.MkdirPacket{Path: target})
	if err == nil {
		result.Effect = core.Applied
	} else {
		if !refused(err) {
			result.Effect = core.Unknown
		}
	}
	return result, err
}

var (
	_ core.FileCreator    = (*Driver)(nil)
	_ core.DirectoryMaker = (*Driver)(nil)
)
