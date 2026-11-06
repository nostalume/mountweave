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

func (d *Driver) removeChild(ctx context.Context, parent *os.File, name string, target core.RemoveTarget) (change core.Change, err error) {
	change.Effect = core.Unchanged
	unlock, err := d.names.lock(ctx, parent, nil)
	if err != nil {
		return change, err
	}
	defer unlock()
	file, info, err := d.openChild(parent, name, deleteChild)
	if err != nil {
		return change, localError(err)
	}
	marked := false
	defer func() {
		closeErr := d.closeFile(file)
		if marked && closeErr != nil {
			change.Effect = core.Unknown
		}
		err = errors.Join(err, localError(closeErr))
	}()
	if target == core.RemoveEmptyDirectory && !info.IsDir() {
		return change, core.Fail(core.NotDirectory, nil)
	}
	if target == core.RemoveNonDirectory && info.IsDir() {
		return change, core.Fail(core.NotFile, nil)
	}
	if err = ctx.Err(); err != nil {
		return change, err
	}
	flags := uint32(3) // FILE_DISPOSITION_DELETE | FILE_DISPOSITION_POSIX_SEMANTICS.
	var statusBlock [2]uintptr
	status, _, _ := setFileInformation.Call(file.Fd(), uintptr(unsafe.Pointer(&statusBlock[0])),
		uintptr(unsafe.Pointer(&flags)), unsafe.Sizeof(flags), 64) // FileDispositionInformationEx.
	runtime.KeepAlive(file)
	runtime.KeepAlive(flags)
	if status == 0 {
		marked = true
		change.Effect = core.Applied // Confirmed only after the deferred handle close.
		return change, nil
	}
	code, _, _ := statusToError.Call(status)
	nativeErr := syscall.Errno(code)
	switch code {
	case 1, 50, 87, 124:
		return change, core.Fail(core.UnsupportedOperation, nativeErr)
	case 145:
		return change, core.Fail(core.DirectoryNotEmpty, nativeErr)
	default:
		return nativeChange(nativeErr, core.DirectoryNotEmpty)
	}
}
