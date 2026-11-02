package local

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path"
	"sync"

	core "github.com/nostalume/mountweave"
)

// Publish streams into private storage and publishes one complete file at a
// captured parent. Concurrent calls have no version or cross-call order promise.
func (d *Driver) Publish(ctx context.Context, rel string, input io.Reader, length *int64) (change core.Change, err error) {
	change.Effect = core.Unchanged
	if input == nil || (length != nil && *length < 0) {
		return change, core.Fail(core.InvalidRequest, nil)
	}
	if err = core.ValidatePath(rel); err != nil {
		return change, err
	}
	if rel == "/" {
		return change, core.Fail(core.InvalidRequest, nil)
	}
	if d.staging == nil || !directoryScopesSupported {
		return change, core.Fail(core.UnsupportedOperation, nil)
	}
	if err = publicationName(path.Base(rel)); err != nil {
		return change, err
	}
	parent, err := d.OpenDirectory(ctx, path.Dir(rel))
	if err != nil {
		return change, err
	}
	defer func() { err = errors.Join(err, parent.Close()) }()
	to := parent.(*directoryScope).file
	if err = sameFilesystem(d.stagingParent, to); err != nil {
		return change, err
	}
	// Observed directories and links are refused. This is not a version guard:
	// a compatible replacement can race, and the final name is never followed.
	existing, info, checkErr := d.openChild(to, path.Base(rel), inspectChild)
	if checkErr == nil {
		nameErr := findChild(ctx, to, path.Base(rel))
		closeErr := d.closeFile(existing)
		if nameErr != nil {
			return change, errors.Join(nameErr, closeErr)
		}
		if !info.Mode().IsRegular() {
			return change, errors.Join(core.Fail(core.NotFile, nil), closeErr)
		}
		if closeErr != nil {
			return change, closeErr
		}
	} else if !errors.Is(checkErr, os.ErrNotExist) {
		return change, localError(checkErr)
	}
	stage := &stagedFile{driver: d, name: "mountweave-" + rand.Text(), fixed: length != nil}
	if length != nil {
		if err = d.reserveStaging(*length); err != nil {
			return change, err
		}
		stage.reserved = *length
	}
	stage.file, _, err = openChild(d.stagingParent, stage.name, stageChild)
	if stage.file == nil {
		d.releaseStaging(stage.reserved)
		return change, localError(err)
	}
	defer func() {
		err = errors.Join(err, stage.cleanup(change.Effect == core.Applied))
		if cause := ctx.Err(); cause != nil && !errors.Is(err, cause) {
			err = errors.Join(err, cause)
		}
	}()
	if err != nil {
		return change, localError(err)
	}
	stop := context.AfterFunc(ctx, func() { _ = stage.close() })
	err = copyInput(ctx, stage, input, length)
	stop()
	if err != nil {
		return change, localError(err)
	}
	if err = ctx.Err(); err != nil {
		return change, err
	}
	if d.commits != nil {
		select {
		case d.commits <- struct{}{}:
			defer func() { <-d.commits }()
		case <-ctx.Done():
			return change, ctx.Err()
		}
		// Acquisition can race cancellation. Never publish merely because a
		// permit became available at the same time as the stop request.
		if err = ctx.Err(); err != nil {
			return change, err
		}
	}
	return d.publishNative(ctx, stage.file, to, stage.name, path.Base(rel))
}

type stagedFile struct {
	driver   *Driver
	file     *os.File
	name     string
	reserved int64
	fixed    bool
	once     sync.Once
	closeErr error
}

func (d *Driver) reserveStaging(n int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if n > d.stagingLimit-d.stagingBytes {
		return core.Fail(core.CapacityExceeded, errors.New("staging byte budget exhausted"))
	}
	d.stagingBytes += n
	return nil
}
func (d *Driver) releaseStaging(n int64) {
	d.mu.Lock()
	d.stagingBytes -= n
	d.mu.Unlock()
}
func (s *stagedFile) Write(data []byte) (int, error) {
	if !s.fixed {
		n := int64(len(data))
		if err := s.driver.reserveStaging(n); err != nil {
			return 0, err
		}
		s.reserved += n
	}
	return s.file.Write(data)
}
func (s *stagedFile) close() error {
	s.once.Do(func() { s.closeErr = s.driver.closeFile(s.file) })
	return s.closeErr
}
func (s *stagedFile) cleanup(published bool) error {
	closeErr := s.close()
	var removeErr error
	if !published {
		removeErr = s.driver.staging.Remove(s.name)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
	}
	err := errors.Join(closeErr, removeErr)
	if err != nil {
		s.driver.mu.Lock()
		s.driver.failedStages = append(s.driver.failedStages, s)
		s.driver.cleanupErr = errors.Join(s.driver.cleanupErr, err)
		s.driver.mu.Unlock()
	}
	// Published bytes no longer belong to the temporary-space budget. Failed
	// cleanup of an unpublished object retains its conservative reservation.
	if removeErr == nil && (published || closeErr == nil) {
		s.driver.releaseStaging(s.reserved)
	}
	return localError(err)
}

var _ core.FilePublisher = (*Driver)(nil)
