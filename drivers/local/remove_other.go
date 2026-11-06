//go:build !linux && !windows

package local

import (
	"context"
	"os"

	core "github.com/nostalume/mountweave"
)

func (d *Driver) removeChild(context.Context, *os.File, string, core.RemoveTarget) (core.Change, error) {
	return core.Change{Effect: core.Unchanged}, core.Fail(core.UnsupportedOperation, nil)
}
