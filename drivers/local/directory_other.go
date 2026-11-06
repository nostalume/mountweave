//go:build !linux && !windows

package local

import (
	"context"
	core "github.com/nostalume/mountweave"
	"os"
)

const directoryScopesSupported = false

func exactDirectory(*os.File) error { return core.Fail(core.UnsupportedOperation, nil) }
func (*Driver) mkdirAt(context.Context, *os.File, string) (core.Change, error) {
	return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
}

func openChild(*os.File, string, childMode, ...int) (*os.File, os.FileInfo, error) {
	return nil, nil, core.Fail(core.UnsupportedOperation, nil)
}
