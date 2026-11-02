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

var setFileInformation = syscall.NewLazyDLL("ntdll.dll").NewProc("NtSetInformationFile")
var statusToError = syscall.NewLazyDLL("ntdll.dll").NewProc("RtlNtStatusToDosError")

func (d *Driver) relocateNative(ctx context.Context, from, to *os.File, oldName, newName string) (change core.Change, err error) {
	change.Effect = core.Unchanged
	owned, info, err := d.openChild(from, oldName, relocateChild)
	if err != nil {
		return change, localError(err)
	}
	defer func() { err = errors.Join(err, d.closeFile(owned)) }()
	if !info.Mode().IsRegular() && !info.IsDir() {
		return change, core.Fail(core.UnsupportedOperation, nil)
	}
	return renameFile(ctx, owned, to, newName, 0, 10)
}

func renameFile(ctx context.Context, owned, to *os.File, newName string, flags, class uint32) (core.Change, error) {
	change := core.Change{Effect: core.Unchanged}
	name, err := syscall.UTF16FromString(newName)
	if err != nil {
		return change, core.Fail(core.InvalidPath, err)
	}
	name = name[:len(name)-1]
	type renameInfo struct {
		flags uint32
		root  syscall.Handle
		size  uint32
		name  [1]uint16
	}
	buffer := make([]byte, int(unsafe.Sizeof(renameInfo{}))+len(name)*2)
	request := (*renameInfo)(unsafe.Pointer(&buffer[0]))
	request.flags = flags
	request.root = syscall.Handle(to.Fd())
	request.size = uint32(len(name) * 2)
	copy(unsafe.Slice(&request.name[0], len(name)), name)
	if err := ctx.Err(); err != nil {
		return change, err
	}
	// The caller selects non-replacing relocation or complete publication.
	var statusBlock [2]uintptr
	status, _, _ := setFileInformation.Call(owned.Fd(), uintptr(unsafe.Pointer(&statusBlock[0])), uintptr(unsafe.Pointer(&buffer[0])), uintptr(len(buffer)), uintptr(class))
	runtime.KeepAlive(owned)
	runtime.KeepAlive(to)
	runtime.KeepAlive(buffer)
	if status == 0 {
		return core.Change{Effect: core.Applied}, nil
	}
	winError, _, _ := statusToError.Call(status)
	callErr := syscall.Errno(winError)
	if class == 10 {
		if callErr == syscall.Errno(1) || callErr == syscall.Errno(50) || callErr == syscall.Errno(17) {
			return relocationResult(errors.ErrUnsupported)
		}
		return relocationResult(callErr)
	}
	// Capability refusals are checked before reading input. A runtime filesystem
	// rejection after transfer is an operation failure, without a fallback.
	if callErr == syscall.Errno(1) || callErr == syscall.Errno(50) || callErr == syscall.Errno(17) || callErr == syscall.Errno(87) {
		return core.Change{Effect: core.Unchanged}, core.Fail(core.UpstreamFailure, callErr)
	}
	return nativeChange(callErr, core.NotFile)
}
