//go:build !linux

package local

import "os"

func readFlags() int { return os.O_RDONLY }
