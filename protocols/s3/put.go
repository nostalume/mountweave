package s3

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/protocols/internal/httpwire"
)

func (h *Handler) put(w http.ResponseWriter, r *http.Request, actor core.Principal, address core.Address, payload string) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	conditions, err := httpwire.ParseRawConditions(r.Header)
	if err != nil {
		return unchanged, err
	}
	body, length, err := h.putBody(w, r, payload)
	if err != nil {
		return unchanged, err
	}
	change, err := h.files.Write(r.Context(), actor, address, body, length, core.WriteOptions{Policy: core.Publish(conditions.When)})
	if err != nil {
		var limit *http.MaxBytesError
		if errors.As(err, &limit) {
			return change, fault(http.StatusRequestEntityTooLarge, "EntityTooLarge", err)
		}
		return change, err
	}
	evidence(w.Header(), change.Evidence)
	w.WriteHeader(http.StatusOK)
	return change, nil
}

func (h *Handler) putBody(w http.ResponseWriter, r *http.Request, payload string) (io.Reader, *int64, error) {
	if r.Body == nil {
		return nil, nil, fault(http.StatusBadRequest, "InvalidRequest")
	}
	encoding := r.Header.Get("Content-Encoding")
	crc := r.Header.Get("X-Amz-Checksum-Crc32")
	algorithm := r.Header.Get("X-Amz-Sdk-Checksum-Algorithm")
	trailer := r.Header.Get("X-Amz-Trailer")
	decoded := r.Header.Get("X-Amz-Decoded-Content-Length")
	unsupported := fault(http.StatusNotImplemented, "NotImplemented")
	if algorithm != "" && algorithm != "CRC32" {
		return nil, nil, unsupported
	}
	if payload == trailerPayload {
		if r.TLS == nil || encoding != "aws-chunked" || trailer != "x-amz-checksum-crc32" || crc != "" {
			return nil, nil, unsupported
		}
		length, err := strconv.ParseUint(decoded, 10, 63)
		if err != nil {
			return nil, nil, fault(http.StatusBadRequest, "InvalidRequest", err)
		}
		if int64(length) > h.options.MaxPutBytes {
			return nil, nil, fault(http.StatusRequestEntityTooLarge, "EntityTooLarge")
		}
		size := int64(length)
		return &trailerReader{wire: bufio.NewReaderSize(r.Body, 128), left: size, crc: crc32.NewIEEE()}, &size, nil
	}
	if encoding != "" || trailer != "" || decoded != "" || algorithm != "" && crc == "" {
		return nil, nil, unsupported
	}
	if r.ContentLength > h.options.MaxPutBytes {
		return nil, nil, fault(http.StatusRequestEntityTooLarge, "EntityTooLarge")
	}
	var length *int64
	if r.ContentLength >= 0 {
		n := r.ContentLength
		length = &n
	}
	body := &checkedBody{source: http.MaxBytesReader(w, r.Body, h.options.MaxPutBytes)}
	switch {
	case isDigest(payload):
		body.sha = sha256.New()
		body.wantSHA = strings.ToLower(payload)
	case payload == unsignedPayload && r.TLS != nil:
	default:
		return nil, nil, unsupported
	}
	if crc != "" {
		if !validCRC(crc) {
			return nil, nil, fault(http.StatusBadRequest, "InvalidDigest")
		}
		body.crc = crc32.NewIEEE()
		body.wantCRC = crc
	}
	return body, length, nil
}

func validCRC(s string) bool {
	v, err := base64.StdEncoding.Strict().DecodeString(s)
	return err == nil && len(v) == 4 && base64.StdEncoding.EncodeToString(v) == s
}

type checkedBody struct {
	source           io.Reader
	sha, crc         hash.Hash
	wantSHA, wantCRC string
	end              error
}

func (b *checkedBody) Read(p []byte) (int, error) {
	if b.end != nil {
		return 0, b.end
	}
	n, err := b.source.Read(p)
	if b.sha != nil {
		_, _ = b.sha.Write(p[:n])
	}
	if b.crc != nil {
		_, _ = b.crc.Write(p[:n])
	}
	if err == io.EOF {
		switch {
		case b.sha != nil && hex.EncodeToString(b.sha.Sum(nil)) != b.wantSHA:
			err = fault(http.StatusBadRequest, "XAmzContentSHA256Mismatch")
		case b.crc != nil && base64.StdEncoding.EncodeToString(b.crc.Sum(nil)) != b.wantCRC:
			err = fault(http.StatusBadRequest, "BadDigest")
		}
	}
	b.end = err
	return n, err
}

// trailerReader decodes the TLS unsigned AWS framing in a fixed buffer. The
// terminating checksum and wire EOF are part of successful decoded EOF.
type trailerReader struct {
	wire        *bufio.Reader
	crc         hash.Hash32
	left, chunk int64
	separator   bool
	end         error
}

func (b *trailerReader) Read(p []byte) (int, error) {
	if b.end != nil {
		return 0, b.end
	}
	if len(p) == 0 {
		return 0, nil
	}
	n, err := b.read(p)
	if err != nil && err != io.EOF {
		var declared *wireError
		if !errors.As(err, &declared) {
			err = fault(http.StatusBadRequest, "InvalidRequest", err)
		}
	}
	b.end = err
	return n, err
}

func (b *trailerReader) read(p []byte) (int, error) {
	if b.chunk == 0 {
		if b.separator {
			line, err := b.line()
			if err != nil || line != "" {
				return 0, errors.Join(errors.New("missing chunk separator"), err)
			}
		}
		line, err := b.line()
		if err != nil {
			return 0, err
		}
		size, err := strconv.ParseUint(line, 16, 63)
		if err != nil || int64(size) > b.left {
			return 0, errors.Join(errors.New("invalid chunk length"), err)
		}
		if size == 0 {
			return 0, b.finish()
		}
		b.chunk = int64(size)
		b.separator = true
	}
	n, err := b.wire.Read(p[:min(int64(len(p)), b.chunk)])
	b.chunk -= int64(n)
	b.left -= int64(n)
	_, _ = b.crc.Write(p[:n])
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

func (b *trailerReader) line() (string, error) {
	v, err := b.wire.ReadSlice('\n')
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		return "", err
	}
	if len(v) < 2 || v[len(v)-2] != '\r' {
		return "", errors.New("invalid chunk line ending")
	}
	return string(v[:len(v)-2]), nil
}

func (b *trailerReader) finish() error {
	if b.left != 0 {
		return io.ErrUnexpectedEOF
	}
	line, err := b.line()
	if err != nil {
		return err
	}
	value, ok := strings.CutPrefix(line, "x-amz-checksum-crc32:")
	if !ok || !validCRC(value) {
		return fault(http.StatusBadRequest, "InvalidDigest")
	}
	if base64.StdEncoding.EncodeToString(b.crc.Sum(nil)) != value {
		return fault(http.StatusBadRequest, "BadDigest")
	}
	line, err = b.line()
	if err != nil || line != "" {
		return errors.Join(errors.New("missing trailer terminator"), err)
	}
	if _, err = b.wire.ReadByte(); err != io.EOF {
		return errors.Join(errors.New("unexpected data after trailer"), err)
	}
	return io.EOF
}
