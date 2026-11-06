package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/input"
)

// A native range can cross client part boundaries. Each selected sealed part
// owns integrity for its slice, and only the current body stays open.
type partStream struct {
	ctx            context.Context
	inputs         *input.Store
	parts          []partRecord
	skip, unopened int64
	body           io.ReadCloser
	closed         bool
}

func (s *partStream) Read(p []byte) (int, error) {
	if s.closed {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if s.body == nil {
			if s.unopened == 0 {
				return 0, io.EOF
			}
			for len(s.parts) > 0 && s.skip >= s.parts[0].Size {
				s.skip -= s.parts[0].Size
				s.parts = s.parts[1:]
			}
			if len(s.parts) == 0 {
				return 0, io.ErrUnexpectedEOF
			}
			part := s.parts[0]
			ref, err := s.inputs.AcquireHeld(s.ctx, part.ID, part.Owner)
			if err != nil {
				return 0, err
			}
			record := ref.Record()
			if record.Version != part.Version || record.Offset != part.Size {
				return 0, errors.Join(input.ErrCorrupt, ref.Close())
			}
			length := min(s.unopened, part.Size-s.skip)
			body, err := ref.OpenRange(s.ctx, s.skip, length)
			closeErr := ref.Close()
			if err != nil {
				return 0, errors.Join(err, closeErr)
			}
			if closeErr != nil {
				return 0, errors.Join(closeErr, body.Close())
			}
			s.body, s.parts = body, s.parts[1:]
			s.skip = 0
			s.unopened -= length
		}
		n, err := s.body.Read(p)
		if err != io.EOF {
			return n, err
		}
		if err := s.closeBody(); err != nil {
			return n, err
		}
		if n > 0 {
			return n, nil
		}
	}
}

func (s *partStream) closeBody() error {
	if s.body == nil {
		return nil
	}
	err := s.body.Close()
	s.body = nil
	return err
}

func (s *partStream) Close() error {
	s.closed = true
	return s.closeBody()
}

type manifestSource struct {
	*partStream
	version string
	parts   []partRecord
	ends    []int64
	size    int64
}

func newManifestSource(stream *partStream, id string, parts []partRecord, size int64) *manifestSource {
	h := sha256.New()
	fmt.Fprintf(h, "mountweave-s3-multipart-v1:%d:%s", len(id), id)
	ends := make([]int64, len(parts))
	var end int64
	for i, p := range parts {
		fmt.Fprintf(h, ":%d:%s:%d:%s:%d", len(p.ID), p.ID, len(p.Version), p.Version, p.Size)
		end += p.Size
		ends[i] = end
	}
	return &manifestSource{partStream: stream, version: hex.EncodeToString(h.Sum(nil)), parts: parts, ends: ends, size: size}
}

func (s *manifestSource) SourceVersion() string { return s.version }

func (s *manifestSource) OpenRange(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 || length < 0 || offset > s.size || length > s.size-offset {
		return nil, core.Fail(core.InvalidRange, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	i := sort.Search(len(s.ends), func(i int) bool { return s.ends[i] > offset })
	if i > 0 {
		offset -= s.ends[i-1]
	}
	return &partStream{ctx: ctx, inputs: s.inputs, parts: s.parts[i:], skip: offset, unopened: length}, nil
}

var _ core.Replayable = (*manifestSource)(nil)
