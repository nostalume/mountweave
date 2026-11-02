package local

import (
	"context"
	"errors"
	"os"
	"runtime"
	"syscall"

	core "github.com/nostalume/mountweave"
)

func publicationName(string) error { return nil }

func sameFilesystem(from, to *os.File) error {
	a, err := from.Stat()
	if err != nil {
		return localError(err)
	}
	b, err := to.Stat()
	if err != nil {
		return localError(err)
	}
	if a.Sys().(*syscall.Stat_t).Dev != b.Sys().(*syscall.Stat_t).Dev {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	return nil
}

func (d *Driver) publishNative(ctx context.Context, _ *os.File, to *os.File, oldName, newName string) (core.Change, error) {
	if err := ctx.Err(); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	err := syscall.Renameat(int(d.stagingParent.Fd()), oldName, int(to.Fd()), newName)
	runtime.KeepAlive(d.stagingParent)
	runtime.KeepAlive(to)
	if errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.ENOTDIR) {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.NotFile, err)
	}
	if errors.Is(err, syscall.EXDEV) {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UpstreamFailure, err)
	}
	return nativeChange(err, core.NotFile)
}
