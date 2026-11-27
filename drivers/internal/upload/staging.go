package upload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

// Staging owns one local upload spool at a time under a host-supplied directory.
type Staging struct {
	directory string
	maxBytes  int64
	lease     chan struct{}
}

// NewStaging accepts an existing absolute directory. The host owns its privacy.
func NewStaging(directory string, maxBytes int64) (*Staging, error) {
	if !filepath.IsAbs(directory) || maxBytes <= 0 {
		return nil, core.Fail(core.InvalidRequest, errors.New("uploads require an absolute private staging directory and a positive byte budget"))
	}
	info, err := os.Stat(directory)
	if err != nil || !info.IsDir() {
		return nil, core.Fail(core.InvalidRequest, errors.New("upload staging directory is unavailable"))
	}
	return &Staging{directory: directory, maxBytes: maxBytes, lease: make(chan struct{}, 1)}, nil
}

// StagedFile holds the verified size and SHA-256 of a seekable upload spool.
// The caller owns it until Close releases the file and its staging lease.
type StagedFile struct {
	Size   int64
	SHA256 string
	file   *os.File
	close  func() error
}

func (f *StagedFile) ReadAt(p []byte, offset int64) (int, error) {
	n, err := f.file.ReadAt(p, offset)
	if err == io.EOF {
		return n, err
	}
	return n, httpcontent.PrivateIOError(err)
}

func (f *StagedFile) Close() error { return f.close() }

// Stage borrows input until return and validates its normal EOF. On failure
// Stage closes and removes its temporary file and releases its lease.
func (s *Staging) Stage(ctx context.Context, input io.Reader, length *int64) (_ *StagedFile, err error) {
	select {
	case s.lease <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-s.lease
		return nil, err
	}
	file, err := os.CreateTemp(s.directory, "mountweave-upload-*")
	if err != nil {
		<-s.lease
		return nil, httpcontent.PrivateIOError(err)
	}
	staged := &StagedFile{file: file, close: sync.OnceValue(func() error {
		defer func() { <-s.lease }()
		return errors.Join(httpcontent.PrivateIOError(file.Close()), httpcontent.PrivateIOError(os.Remove(file.Name())))
	})}
	defer func() {
		if err != nil {
			err = errors.Join(err, staged.Close())
		}
	}()

	body := NewBody(ctx, input, length, s.maxBytes)
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(stagingSink{file}, hash), body)
	complete, inputErr := body.Finish()
	if !errors.Is(copyErr, inputErr) {
		copyErr = errors.Join(copyErr, inputErr)
	}
	if copyErr != nil {
		return nil, copyErr
	}
	if !complete {
		return nil, io.ErrUnexpectedEOF
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	staged.Size, staged.SHA256 = size, hex.EncodeToString(hash.Sum(nil))
	return staged, nil
}

// Sanitize local file errors before io.Copy can combine them with input causes.
type stagingSink struct{ file *os.File }

func (w stagingSink) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	return n, httpcontent.PrivateIOError(err)
}
