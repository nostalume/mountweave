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

const directoryScopesSupported = true

// Name-based Linux commits require exact kernel lookup. The admitted
// filesystems expose case folding through the flags of the bound directory.
func exactDirectory(dir *os.File) error {
	var fs syscall.Statfs_t
	fd := dir.Fd()
	defer runtime.KeepAlive(dir)
	if err := syscall.Fstatfs(int(fd), &fs); err != nil {
		return localError(err)
	}
	switch uint32(fs.Type) {
	case 0xef53, 0x01021994: // ext, tmpfs
	default:
		return core.Fail(core.UnsupportedOperation, errors.New("exact directory lookup is unverified on this filesystem"))
	}
	var flags uint32
	const getFlags = 0x80006601 | (unsafe.Sizeof(uintptr(0)) << 16) // FS_IOC_GETFLAGS
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, getFlags, uintptr(unsafe.Pointer(&flags)))
	if errno != 0 {
		return core.Fail(core.UnsupportedOperation, errno)
	}
	if flags&0x40000000 != 0 { // FS_CASEFOLD_FL
		return core.Fail(core.UnsupportedOperation, errors.New("directory uses case folding"))
	}
	return nil
}

func (d *Driver) mkdirAt(_ context.Context, parent *os.File, name string) (core.Change, error) {
	err := syscall.Mkdirat(int(parent.Fd()), name, 0700)
	runtime.KeepAlive(parent)
	return nativeChange(err, core.AlreadyExists)
}

func openChild(parent *os.File, name string, mode childMode, editFlags ...int) (*os.File, os.FileInfo, error) {
	flags := syscall.O_CLOEXEC | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
	if mode == inspectChild || mode == leafChild {
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
	if err == nil && info.Mode()&os.ModeSymlink != 0 && mode != leafChild {
		err = core.Fail(core.UnsupportedOperation, nil)
	}
	if err == nil && mode == directoryChild && !info.IsDir() {
		err = core.Fail(core.NotDirectory, nil)
	}
	return file, info, err
}
