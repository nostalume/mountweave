package webdav

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"io"

	core "github.com/nostalume/mountweave"
)

type putDigest struct {
	io.Reader
	hash hash.Hash
	size int64
}

func newPutDigest(input io.Reader) *putDigest {
	return &putDigest{Reader: input, hash: sha256.New()}
}

func (b *putDigest) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.size += int64(n)
	_, _ = b.hash.Write(p[:n])
	return n, err
}

func (h *Handler) confirmPut(ctx context.Context, p core.Principal, a core.Address, input *putDigest, length *int64) (err error) {
	conflict := core.Fail(core.AlreadyExists, nil)
	existing, err := h.files.Open(ctx, p, a, core.OpenOptions{})
	if core.CodeOf(err) == core.NotFound || core.CodeOf(err) == core.NotFile {
		return conflict
	}
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, existing.Body.Close()) }()
	if existing.Length == nil || length != nil && *length != *existing.Length || input.size > *existing.Length {
		return conflict
	}
	if _, err = io.CopyN(io.Discard, input, *existing.Length-input.size); err != nil {
		if err == io.EOF {
			return conflict
		}
		return err
	}
	var extra [1]byte
	if _, err = io.ReadFull(input, extra[:]); err != io.EOF {
		if err != nil {
			return err
		}
		return conflict
	}
	digest := sha256.New()
	if _, err = io.CopyN(digest, existing.Body, *existing.Length); err != nil {
		return core.Fail(core.InvalidUpstreamResponse, err)
	}
	if _, err = io.ReadFull(existing.Body, extra[:]); err != io.EOF {
		return core.Fail(core.InvalidUpstreamResponse, err)
	}
	if !bytes.Equal(input.hash.Sum(nil), digest.Sum(nil)) {
		return conflict
	}
	return ctx.Err()
}
