package local

import (
	"context"
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

func (d *Driver) publishNative(ctx context.Context, file, to *os.File, _, newName string) (core.Change, error) {
	// FileRenameInformationEx: replace a non-directory name, retaining existing
	// readers of its old object. No copy/delete or delayed-publication fallback.
	return renameFile(ctx, file, to, newName, 3, 65)
}
