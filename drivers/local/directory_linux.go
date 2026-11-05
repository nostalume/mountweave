package local

import (
	"errors"
	"os"
	"runtime"
	"syscall"

	core "github.com/nostalume/mountweave"
)

const directoryScopesSupported = true

func openChild(parent *os.File, name string, mode childMode, editFlags ...int) (*os.File, os.FileInfo, error) {
	flags := syscall.O_CLOEXEC | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
	if mode == inspectChild {
		flags |= 0x200000
	} // Linux O_PATH: inspect without opening a device.
	if mode == directoryChild {
		flags |= syscall.O_DIRECTORY
	}
	if mode == stageChild {
		flags |= syscall.O_RDWR | syscall.O_CREAT | syscall.O_EXCL
	}
	if mode == editChild {
		flags |= editFlags[0]
	}
	fd, err := syscall.Openat(int(parent.Fd()), name, flags, 0600)
	runtime.KeepAlive(parent)
	if err != nil {
		if errors.Is(err, syscall.ENOTDIR) {
			return nil, nil, core.Fail(core.NotDirectory, err)
		}
		if errors.Is(err, syscall.ELOOP) {
			return nil, nil, core.Fail(core.UnsupportedOperation, err)
		}
		return nil, nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	info, err := file.Stat()
	if err == nil && info.Mode()&os.ModeSymlink != 0 {
		err = core.Fail(core.UnsupportedOperation, nil)
	}
	if err == nil && mode == directoryChild && !info.IsDir() {
		err = core.Fail(core.NotDirectory, nil)
	}
	return file, info, err
}
