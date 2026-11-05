package local

import (
	"context"
	"os"
	"runtime"
	"syscall"
	"unsafe"

	core "github.com/nostalume/mountweave"
)

func (d *Driver) removeChild(_ context.Context, parent *os.File, name string, kind core.Kind) (core.Change, error) {
	encoded, err := syscall.BytePtrFromString(name)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.InvalidPath, err)
	}
	var flags uintptr
	if kind == core.Directory {
		flags = 0x200 // AT_REMOVEDIR: only remove an empty directory.
	}
	_, _, errno := syscall.Syscall(syscall.SYS_UNLINKAT, parent.Fd(), uintptr(unsafe.Pointer(encoded)), flags)
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
		if kind == core.Directory {
			code = core.NotDirectory
		}
		return core.Change{Effect: core.Unchanged}, core.Fail(code, errno)
	default:
		return nativeChange(errno, core.DirectoryNotEmpty)
	}
}
