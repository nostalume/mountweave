package local

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"unsafe"

	core "github.com/nostalume/mountweave"
)

const directoryScopesSupported = true

var ntCreateFile = syscall.NewLazyDLL("ntdll.dll").NewProc("NtCreateFile")
var finalPathByHandle = syscall.NewLazyDLL("kernel32.dll").NewProc("GetFinalPathNameByHandleW")

func exactDirectory(*os.File) error { return nil } // Each opened child verifies its actual name.

func (d *Driver) mkdirAt(ctx context.Context, parent *os.File, name string) (core.Change, error) {
	unlock, err := d.names.lock(ctx, parent, nil)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	file, _, err := openChild(parent, name, mkdirChild)
	unlock()
	if file == nil {
		return nativeChange(err, core.AlreadyExists)
	}
	return core.Change{Effect: core.Applied}, d.closeFile(file)
}

func openChild(parent *os.File, name string, mode childMode, editFlags ...int) (*os.File, os.FileInfo, error) {
	if !filepath.IsLocal(name) {
		return nil, nil, core.Fail(core.UnrepresentableName, nil)
	}
	encoded, err := syscall.UTF16FromString(name)
	if err != nil {
		return nil, nil, err
	}
	type unicodeName struct {
		length, maximum uint16
		buffer          *uint16
	}
	nativeName := unicodeName{length: uint16((len(encoded) - 1) * 2), maximum: uint16(len(encoded) * 2), buffer: &encoded[0]}
	type objectAttributes struct {
		length            uint32
		root              syscall.Handle
		name              *unicodeName
		attributes        uint32
		security, quality uintptr
	}
	attrs := objectAttributes{root: syscall.Handle(parent.Fd()), name: &nativeName}
	attrs.length = uint32(unsafe.Sizeof(attrs))
	// Each call uses one child name. OPEN_REPARSE_POINT prevents following that
	// child; only leaf observation of a recognized link or a non-directory
	// unlink may proceed on a reparse point.
	access, options := uint32(0x100080), uint32(0x00200020) // SYNCHRONIZE, READ_ATTRIBUTES; synchronous, no-follow.
	switch mode {
	case directoryChild, readChild:
		access = 0x120089
	case relocateChild, deleteChild:
		access |= 0x10000 // DELETE without a second reopen.
	}
	if mode == directoryChild || mode == mkdirChild {
		options |= 1
	}
	disposition := uint32(1) // FILE_OPEN.
	if mode == stageChild {
		options |= 0x40 // FILE_NON_DIRECTORY_FILE.
	}
	if mode == stageChild {
		access = 0x130196 // GENERIC_WRITE, READ_ATTRIBUTES and DELETE.
		disposition = 2   // FILE_CREATE: exclusive ownership of a new name.
	}
	if mode == editChild {
		access = 0x120196 // FILE_GENERIC_WRITE plus READ_ATTRIBUTES.
		if editFlags[0]&os.O_RDWR != 0 {
			access |= 1
		}
		options |= 0x40
		if editFlags[0]&os.O_EXCL != 0 {
			disposition = 2
		}
	}
	if mode == mkdirChild {
		disposition = 2
	}
	share := uintptr(7)
	if mode == relocateChild || mode == deleteChild {
		share = 3 // Prevent another rename/delete of the checked entry before commit.
	}
	var handle syscall.Handle
	var statusBlock [2]uintptr
	status, _, _ := ntCreateFile.Call(uintptr(unsafe.Pointer(&handle)), uintptr(access), uintptr(unsafe.Pointer(&attrs)),
		uintptr(unsafe.Pointer(&statusBlock[0])), 0, 0, share, uintptr(disposition), uintptr(options), 0, 0)
	runtime.KeepAlive(parent)
	runtime.KeepAlive(encoded)
	if status != 0 {
		code, _, _ := statusToError.Call(status)
		if disposition == 2 && (status == 0xc00000ba || status == 0xc0000103) {
			return nil, nil, os.ErrExist
		}
		if status == 0xc00000ba { // STATUS_FILE_IS_A_DIRECTORY
			return nil, nil, core.Fail(core.NotFile, syscall.Errno(code))
		}
		if code == 267 {
			return nil, nil, core.Fail(core.NotDirectory, syscall.Errno(code))
		}
		return nil, nil, syscall.Errno(code)
	}
	file := os.NewFile(uintptr(handle), name)
	if mode == mkdirChild || mode == stageChild {
		return file, nil, nil
	}
	if disposition == 1 {
		if err := exactChildName(handle, name); err != nil {
			return file, nil, err
		}
	}
	info, err := file.Stat()
	if err != nil {
		return file, nil, err
	}
	fileAttrs := info.Sys().(*syscall.Win32FileAttributeData).FileAttributes
	if fileAttrs&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0 &&
		!((mode == leafChild || mode == deleteChild) && info.Mode()&os.ModeSymlink != 0) {
		return file, info, core.Fail(core.UnsupportedOperation, nil)
	}
	if mode == directoryChild && fileAttrs&syscall.FILE_ATTRIBUTE_DIRECTORY == 0 {
		return file, info, core.Fail(core.NotDirectory, nil)
	}
	return file, info, nil
}

func exactChildName(handle syscall.Handle, name string) error {
	var local [512]uint16
	buf := local[:]
	for {
		n, _, err := finalPathByHandle.Call(uintptr(handle), uintptr(unsafe.Pointer(&buf[0])), uintptr(len(buf)), 4) // normalized name, no volume
		if n == 0 {
			return core.Fail(core.UnsupportedOperation, err)
		}
		if n < uintptr(len(buf)) {
			if filepath.Base(syscall.UTF16ToString(buf[:n])) != name {
				return core.Fail(core.NotFound, nil)
			}
			return nil
		}
		if n > 32768 {
			return core.Fail(core.UnrepresentableName, nil)
		}
		buf = make([]uint16, n+1)
	}
}
