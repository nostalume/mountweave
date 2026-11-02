//go:build !linux && !windows

package local

import (
	"context"
	"os"

	core "github.com/nostalume/mountweave"
)

func publicationName(string) error            { return core.Fail(core.UnsupportedOperation, nil) }
func sameFilesystem(*os.File, *os.File) error { return core.Fail(core.UnsupportedOperation, nil) }
func (d *Driver) publishNative(context.Context, *os.File, *os.File, string, string) (core.Change, error) {
	return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
}
