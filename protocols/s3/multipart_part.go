package s3

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/input"
	bolt "go.etcd.io/bbolt"
)

const maxPartNumber = 10000
const maxPartBytes = 5 << 30
const minPartBytes = 5 << 20
const unlinkedInput = "?"

func partKey(number int) []byte { return binary.BigEndian.AppendUint16(nil, uint16(number)) }

func (h *Handler) uploadPart(w http.ResponseWriter, r *http.Request, a uploadAccess, address core.Address, number int, payload string) (err error) {
	m := h.multipart
	body, size, err := h.putBody(w, r, payload)
	if err != nil {
		return err
	}
	if size == nil {
		return fault(http.StatusLengthRequired, "MissingContentLength")
	}
	if *size > maxPartBytes || *size > m.MaxObjectBytes {
		return fault(http.StatusRequestEntityTooLarge, "EntityTooLarge")
	}
	var record uploadRecord
	if err := m.DB.View(func(tx *bolt.Tx) error {
		_, rec, err := a.read(tx)
		record = rec
		if err != nil {
			return err
		}
		return allowPart(rec)
	}); err != nil {
		return err
	}
	if _, err := m.bound(r.Context(), record, address); err != nil {
		return err
	}

	ctx, done := m.partContext(r.Context(), a.id)
	defer done()
	owner := "s3-multipart/" + a.id + "/" + rand.Text()
	if err := m.DB.Update(func(tx *bolt.Tx) error {
		b, rec, err := a.read(tx)
		if err != nil {
			return err
		}
		if err := allowPart(rec); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		root := tx.Bucket(manifestRoot)
		if count(root, pendingCount) >= m.MaxPendingParts {
			return multipartCapacity()
		}
		if err := b.Bucket(pendingTable).Put([]byte(owner), []byte(unlinkedInput)); err != nil {
			return err
		}
		return addCount(root, pendingCount, 1)
	}); err != nil {
		return err
	}

	reclaim := owner
	defer func() {
		if reclaim == "" {
			return
		}
		if cleanupErr := m.discard(context.WithoutCancel(ctx), a.id, reclaim, nil); cleanupErr != nil {
			if err != nil {
				err = errors.Join(err, cleanupErr)
			} else {
				h.observe(r, cleanupErr, nil)
			}
		}
	}()
	// Abort and Complete must interrupt a request that is blocked receiving a
	// part. Wait for the callback before returning the ResponseWriter.
	stopped := make(chan struct{})
	interrupt := context.AfterFunc(ctx, func() {
		defer close(stopped)
		if http.NewResponseController(w).SetReadDeadline(time.Now()) != nil {
			_ = r.Body.Close()
		}
	})
	defer func() {
		if !interrupt() {
			<-stopped
		}
	}()

	part, err := m.stage(ctx, a.id, owner, body, *size, payload)
	if err != nil {
		return err
	}
	part, reclaim, err = m.installPart(ctx, a, number, part)
	if err != nil {
		reclaim = owner
		return err
	}
	w.Header().Set("ETag", part.Tag)
	if part.CRC != "" {
		w.Header().Set("X-Amz-Checksum-Crc32", part.CRC)
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func allowPart(r uploadRecord) error {
	if r.Phase == committing {
		return uploadConflict()
	}
	if r.Phase != receiving || !time.Now().Before(r.Expires) {
		return noUpload()
	}
	return nil
}

func (m *multipart) stage(ctx context.Context, upload, owner string, body io.Reader, size int64, payload string) (partRecord, error) {
	in, err := m.Inputs.Create(ctx, owner)
	if err != nil {
		return partRecord{}, partInputError(err)
	}
	if err := m.DB.Update(func(tx *bolt.Tx) error {
		b, _, err := readUpload(tx, upload)
		if err != nil {
			return err
		}
		return b.Bucket(pendingTable).Put([]byte(owner), []byte(in.ID))
	}); err != nil {
		return partRecord{}, err
	}

	// A verified signed digest can also be the strong part identity.
	var stream = body
	digest := sha256.New()
	signedDigest := isDigest(payload)
	if !signedDigest {
		stream = io.TeeReader(body, digest)
	}
	block := m.Inputs.Limits().MaxAppendBytes
	for offset := int64(0); offset < size; {
		n := min(size-offset, block)
		if _, err := m.Inputs.Append(ctx, in.ID, offset, io.LimitReader(stream, n), n); err != nil {
			return partRecord{}, partInputError(err)
		}
		offset += n
	}
	if n, err := io.CopyN(io.Discard, stream, 1); n != 0 || err != io.EOF {
		var declared *wireError
		if errors.As(err, &declared) {
			return partRecord{}, err
		}
		return partRecord{}, fault(http.StatusBadRequest, "InvalidRequest", err)
	}
	in, err = m.Inputs.Seal(ctx, in.ID, size)
	if err != nil {
		return partRecord{}, partInputError(err)
	}
	if err := m.Inputs.Hold(ctx, in.ID, owner); err != nil {
		return partRecord{}, partInputError(err)
	}
	if _, err := m.Inputs.Release(ctx, in.ID); err != nil {
		return partRecord{}, err
	}

	tag := strings.ToLower(payload)
	if !signedDigest {
		tag = hex.EncodeToString(digest.Sum(nil))
	}
	crc := ""
	switch body := body.(type) {
	case *checkedBody:
		crc = body.wantCRC
	case *trailerReader:
		crc = base64.StdEncoding.EncodeToString(body.crc.Sum(nil))
	}
	return partRecord{ID: in.ID, Owner: owner, Version: in.Version, Size: size, Tag: "\"" + tag + "\"", CRC: crc}, nil
}

func partInputError(err error) error {
	var limit *http.MaxBytesError
	switch {
	case errors.As(err, &limit):
		return fault(http.StatusRequestEntityTooLarge, "EntityTooLarge", err)
	case errors.Is(err, input.ErrCapacity):
		return fault(http.StatusServiceUnavailable, "SlowDown", err)
	case errors.Is(err, io.ErrUnexpectedEOF):
		return fault(http.StatusBadRequest, "IncompleteBody", err)
	default:
		return err
	}
}

func (m *multipart) installPart(ctx context.Context, a uploadAccess, number int, part partRecord) (out partRecord, reclaim string, err error) {
	out = part
	err = m.DB.Update(func(tx *bolt.Tx) error {
		b, rec, err := a.read(tx)
		if err != nil {
			return err
		}
		if err := allowPart(rec); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		parts, pending := b.Bucket(partTable), b.Bucket(pendingTable)
		key := partKey(number)
		var previous partRecord
		if old := parts.Get(key); old != nil {
			if err := json.Unmarshal(old, &previous); err != nil {
				return err
			}
		}
		now := time.Now().UTC()
		rec.Expires = now.Add(m.Retention)
		if err := putRecord(b, uploadMeta, rec); err != nil {
			return err
		}
		if previous.ID != "" && previous.Tag == part.Tag && previous.Size == part.Size {
			// The CRC is a property of verified bytes. A retry can provide it
			// for a part previously accepted without an explicit CRC.
			if previous.CRC == "" && part.CRC != "" {
				previous.CRC = part.CRC
				if err := putRecord(parts, key, previous); err != nil {
					return err
				}
			}
			out, reclaim = previous, part.Owner
			return nil
		}
		part.Modified = &now
		out = part
		if err := putRecord(parts, key, part); err != nil {
			return err
		}
		if err := pending.Delete([]byte(part.Owner)); err != nil {
			return err
		}
		if previous.ID != "" {
			reclaim = previous.Owner
			return pending.Put([]byte(previous.Owner), []byte(previous.ID))
		}
		return addCount(tx.Bucket(manifestRoot), pendingCount, -1)
	})
	return
}
