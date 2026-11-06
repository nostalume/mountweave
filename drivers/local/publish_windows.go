package local

import (
	"context"
	"errors"
	"os"
	"runtime"
	"strings"
	"syscall"

	core "github.com/nostalume/mountweave"
)

func publicationName(name string) error {
	if strings.ContainsRune(name, ':') {
		return core.Fail(core.UnrepresentableName, nil)
	}
	return nil
}

func sameFilesystem(from, to *os.File) error {
	var a, b syscall.ByHandleFileInformation
	err := syscall.GetFileInformationByHandle(syscall.Handle(from.Fd()), &a)
	if err == nil {
		err = syscall.GetFileInformationByHandle(syscall.Handle(to.Fd()), &b)
	}
	runtime.KeepAlive(from)
	runtime.KeepAlive(to)
	if err != nil {
		return localError(err)
	}
	if a.VolumeSerialNumber != b.VolumeSerialNumber {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	return nil
}

func (d *Driver) publishNative(ctx context.Context, file, to *os.File, _, newName string, absent bool) (change core.Change, moved bool, err error) {
	unlock, err := d.names.lock(ctx, to, nil)
	if err != nil {
		return core.Change{Effect: core.Unchanged}, false, err
	}
	defer unlock()
	// Recheck the exact name after streaming, within the managed commit.
	if !absent {
		target, info, openErr := d.openChild(to, newName, leafChild)
		if openErr == nil {
			if closeErr := d.closeFile(target); closeErr != nil {
				return core.Change{Effect: core.Unchanged}, false, closeErr
			}
			if !info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
				return core.Change{Effect: core.Unchanged}, false, core.Fail(core.NotFile, nil)
			}
		} else if errors.Is(openErr, os.ErrNotExist) {
			absent = true
		} else {
			return core.Change{Effect: core.Unchanged}, false, localError(openErr)
		}
	}
	flags, class := uint32(3), uint32(65)
	if absent {
		flags, class = 0, 10
	}
	change, err = renameFile(ctx, file, to, newName, flags, class)
	return change, change.Effect == core.Applied, err
}
