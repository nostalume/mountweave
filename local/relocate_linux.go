//go:build linux && amd64

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

func (d *Driver) relocateNative(_ context.Context, from, to *os.File, oldName, newName string) (core.Change, error) {
	old, err := syscall.BytePtrFromString(oldName)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	next, err := syscall.BytePtrFromString(newName)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	// Linux x86-64 syscall ABI: renameat2=316, RENAME_NOREPLACE=1.
	_, _, errno := syscall.Syscall6(316, from.Fd(), uintptr(unsafe.Pointer(old)), to.Fd(), uintptr(unsafe.Pointer(next)), 1, 0)
	runtime.KeepAlive(from)
	runtime.KeepAlive(to)
	if errno == 0 {
		return relocationResult(nil)
	}
	if errors.Is(errno, syscall.ENOSYS) || errors.Is(errno, syscall.EINVAL) ||
		errors.Is(errno, syscall.EOPNOTSUPP) || errors.Is(errno, syscall.EXDEV) {
		return relocationResult(errors.ErrUnsupported)
	}
	return relocationResult(errno)
}
