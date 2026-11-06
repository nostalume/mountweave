package local

import (
	"context"
	"os"
	"runtime"
	"sync"
	"syscall"
)

// Namespace changes on this Driver coordinate only their native commits.
// Windows replacement by name needs the exact-target check and rename to
// exclude another managed mutation of the parent between those two calls.
type nameLocks struct {
	once  sync.Once
	gates [32]chan struct{}
}

func directoryKey(dir *os.File) (uint32, error) {
	var info syscall.ByHandleFileInformation
	err := syscall.GetFileInformationByHandle(syscall.Handle(dir.Fd()), &info)
	runtime.KeepAlive(dir)
	return info.VolumeSerialNumber ^ info.FileIndexHigh ^ info.FileIndexLow, localError(err)
}

func (l *nameLocks) lock(ctx context.Context, from, to *os.File) (func(), error) {
	key, err := directoryKey(from)
	if err != nil {
		return nil, err
	}
	i := int(key % uint32(len(l.gates)))
	j := i
	if to != nil {
		key, err = directoryKey(to)
		if err != nil {
			return nil, err
		}
		j = int(key % uint32(len(l.gates)))
	}
	if j < i {
		i, j = j, i
	}
	l.once.Do(func() {
		for k := range l.gates {
			l.gates[k] = make(chan struct{}, 1)
		}
	})
	select {
	case l.gates[i] <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if i != j {
		select {
		case l.gates[j] <- struct{}{}:
		case <-ctx.Done():
			<-l.gates[i]
			return nil, ctx.Err()
		}
	}
	unlock := func() {
		if i != j {
			<-l.gates[j]
		}
		<-l.gates[i]
	}
	if err := ctx.Err(); err != nil {
		unlock()
		return nil, err
	}
	return unlock, nil
}
