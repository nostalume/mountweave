//go:build !windows

package local

import (
	"context"
	"os"
)

type nameLocks struct{}

func (nameLocks) lock(ctx context.Context, _, _ *os.File) (func(), error) {
	return func() {}, ctx.Err()
}
