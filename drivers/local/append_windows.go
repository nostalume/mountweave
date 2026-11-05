package local

import (
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

var ntWriteFile = syscall.NewLazyDLL("ntdll.dll").NewProc("NtWriteFile")

func appendFile(file *os.File, p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if uint64(len(p)) > 0xffffffff {
		p = p[:0xffffffff]
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return 0, err
	}
	var nativeErr error
	err = raw.Control(func(handle uintptr) {
		var receipt [2]uintptr
		offset := int64(-1) // FILE_WRITE_TO_END_OF_FILE, not a preceding size observation.
		status, _, _ := ntWriteFile.Call(handle, 0, 0, 0, uintptr(unsafe.Pointer(&receipt[0])), uintptr(unsafe.Pointer(&p[0])), uintptr(len(p)), uintptr(unsafe.Pointer(&offset)), 0)
		n = int(receipt[1])
		if status != 0 {
			code, _, _ := statusToError.Call(status)
			nativeErr = syscall.Errno(code)
		}
	})
	runtime.KeepAlive(file)
	runtime.KeepAlive(p)
	if err == nil {
		err = nativeErr
	}
	return n, err
}
