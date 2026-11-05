//go:build !linux && !windows

package local

import (
	core "github.com/nostalume/mountweave"
	"os"
)

func appendFile(*os.File, []byte) (int, error) { return 0, core.Fail(core.UnsupportedOperation, nil) }
