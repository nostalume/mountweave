package local

import (
	"context"
	"errors"
	"os"
	"runtime"
	"syscall"
	"unsafe"

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

func (d *Driver) publishNative(ctx context.Context, _ *os.File, to *os.File, oldName, newName string, absent bool) (core.Change, bool, error) {
	if err := ctx.Err(); err != nil {
		return core.Change{Effect: core.Unchanged}, false, err
	}
	var err error
	if absent {
		old, e := syscall.BytePtrFromString(oldName)
		if e != nil {
			return core.Change{Effect: core.Unchanged}, false, e
		}
		next, e := syscall.BytePtrFromString(newName)
		if e != nil {
			return core.Change{Effect: core.Unchanged}, false, e
		}
		// linkat publishes the private regular file exclusively on every Linux
		// architecture. The original private link remains owned by stage.cleanup.
		_, _, errno := syscall.Syscall6(syscall.SYS_LINKAT, d.stagingParent.Fd(), uintptr(unsafe.Pointer(old)), to.Fd(), uintptr(unsafe.Pointer(next)), 0, 0)
		if errno != 0 {
			err = errno
		}
	} else {
		err = syscall.Renameat(int(d.stagingParent.Fd()), oldName, int(to.Fd()), newName)
	}
	runtime.KeepAlive(d.stagingParent)
	runtime.KeepAlive(to)
	if errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.ENOTDIR) {
		return core.Change{Effect: core.Unchanged}, false, core.Fail(core.NotFile, err)
	}
	if errors.Is(err, syscall.EXDEV) {
		return core.Change{Effect: core.Unchanged}, false, core.Fail(core.UpstreamFailure, err)
	}
	conflict := core.NotFile
	if absent {
		conflict = core.AlreadyExists
	}
	change, err := nativeChange(err, conflict)
	return change, !absent && change.Effect == core.Applied, err
}
