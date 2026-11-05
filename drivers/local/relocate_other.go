//go:build !windows && (!linux || !amd64)

package local

import (
	"context"
	"errors"
	"os"

	core "github.com/nostalume/mountweave"
)

func (d *Driver) relocateNative(context.Context, *os.File, *os.File, string, string) (core.Change, error) {
	return relocationResult(errors.ErrUnsupported)
}
