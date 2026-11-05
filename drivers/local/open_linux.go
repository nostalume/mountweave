package local

import (
	"os"
	"syscall"
)

func readFlags() int { return os.O_RDONLY | syscall.O_NONBLOCK }
