package local

import (
	"os"
	"runtime"
	"syscall"
	"unsafe"

	core "github.com/nostalume/mountweave"
)

const directoryScopesSupported = true

var ntCreateFile = syscall.NewLazyDLL("ntdll.dll").NewProc("NtCreateFile")

func openChild(parent *os.File, name string, mode childMode, editFlags ...int) (*os.File, os.FileInfo, error) {
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
	// child; reparse points are refused except for a non-directory unlink.
	access, options := uint32(0x100080), uint32(0x00200020) // SYNCHRONIZE, READ_ATTRIBUTES; synchronous, no-follow.
	switch mode {
	case directoryChild, readChild:
		access = 0x120089
	case relocateChild, unlinkChild, removeDirectoryChild:
		access |= 0x10000 // DELETE without a second reopen.
	}
	if mode == directoryChild || mode == removeDirectoryChild {
		options |= 1
	}
	disposition := uint32(1) // FILE_OPEN.
	if mode == unlinkChild || mode == stageChild {
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
	var handle syscall.Handle
	var statusBlock [2]uintptr
	status, _, _ := ntCreateFile.Call(uintptr(unsafe.Pointer(&handle)), uintptr(access), uintptr(unsafe.Pointer(&attrs)),
		uintptr(unsafe.Pointer(&statusBlock[0])), 0, 0, 7, uintptr(disposition), uintptr(options), 0, 0)
	runtime.KeepAlive(parent)
	runtime.KeepAlive(encoded)
	if status != 0 {
		code, _, _ := statusToError.Call(status)
		if code == 267 {
			return nil, nil, core.Fail(core.NotDirectory, syscall.Errno(code))
		}
		return nil, nil, syscall.Errno(code)
	}
	file := os.NewFile(uintptr(handle), name)
	var info os.FileInfo
	var fileAttrs uint32
	if mode == inspectChild || mode == readChild || mode == relocateChild || mode == editChild {
		info, err = file.Stat()
		if err == nil {
			fileAttrs = info.Sys().(*syscall.Win32FileAttributeData).FileAttributes
		}
	} else {
		// These modes need only attributes; no caller projects full metadata.
		var native syscall.ByHandleFileInformation
		err = syscall.GetFileInformationByHandle(handle, &native)
		fileAttrs = native.FileAttributes
	}
	if err != nil {
		return file, nil, err
	}
	if fileAttrs&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0 && mode != unlinkChild {
		return file, info, core.Fail(core.UnsupportedOperation, nil)
	}
	if mode == directoryChild && fileAttrs&syscall.FILE_ATTRIBUTE_DIRECTORY == 0 {
		return file, info, core.Fail(core.NotDirectory, nil)
	}
	return file, info, nil
}
