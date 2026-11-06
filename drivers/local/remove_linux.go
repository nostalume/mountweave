package local

import (
	"context"
	"os"
	"runtime"
	"syscall"
	"unsafe"

	core "github.com/nostalume/mountweave"
)

func (d *Driver) removeChild(ctx context.Context, parent *os.File, name string, target core.RemoveTarget) (core.Change, error) {
	encoded, err := syscall.BytePtrFromString(name)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.InvalidPath, err)
	}
	var flags uintptr
	if target == core.RemoveEmptyDirectory {
		flags = 0x200 // AT_REMOVEDIR: only remove an empty directory.
	}
	_, _, errno := syscall.Syscall(syscall.SYS_UNLINKAT, parent.Fd(), uintptr(unsafe.Pointer(encoded)), flags)
	if target == "" && errno == syscall.EISDIR {
		if err := ctx.Err(); err != nil {
			return core.Change{Effect: core.Unchanged}, err
		}
		_, _, errno = syscall.Syscall(syscall.SYS_UNLINKAT, parent.Fd(), uintptr(unsafe.Pointer(encoded)), 0x200)
	}
	runtime.KeepAlive(parent)
	runtime.KeepAlive(encoded)
	switch errno {
	case 0:
		return nativeChange(nil, core.DirectoryNotEmpty)
	case syscall.ENOSYS, syscall.EOPNOTSUPP:
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, errno)
	case syscall.ENOTEMPTY, syscall.EEXIST:
		return core.Change{Effect: core.Unchanged}, core.Fail(core.DirectoryNotEmpty, errno)
	case syscall.ENOTDIR, syscall.EISDIR:
		code := core.NotFile
		if target == core.RemoveEmptyDirectory || target == "" {
			code = core.NotDirectory
		}
		return core.Change{Effect: core.Unchanged}, core.Fail(code, errno)
	default:
		return nativeChange(errno, core.DirectoryNotEmpty)
	}
}
