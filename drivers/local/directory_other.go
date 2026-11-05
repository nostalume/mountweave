//go:build !linux && !windows

package local

import (
	core "github.com/nostalume/mountweave"
	"os"
)

const directoryScopesSupported = false

func openChild(*os.File, string, childMode, ...int) (*os.File, os.FileInfo, error) {
	return nil, nil, core.Fail(core.UnsupportedOperation, nil)
}
