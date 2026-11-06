package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/protocols/internal/httpwire"
	bolt "go.etcd.io/bbolt"
)

type completeRequest struct {
	Parts []selectedPart
	When  core.Condition
	Size  *int64
}

type selectedPart struct {
	Number   int
	Tag, CRC string
}

func (h *Handler) parseComplete(w http.ResponseWriter, r *http.Request, payload string) (completeRequest, error) {
	const maxXML = 4 << 20
	body, length, err := h.putBody(w, r, payload)
	if err != nil {
		return completeRequest{}, err
	}
	if length != nil && *length > maxXML {
		return completeRequest{}, fault(http.StatusRequestEntityTooLarge, "EntityTooLarge")
	}
	data, err := io.ReadAll(io.LimitReader(body, maxXML+1))
	if err != nil {
		return completeRequest{}, partInputError(err)
	}
	if len(data) > maxXML {
		return completeRequest{}, fault(http.StatusRequestEntityTooLarge, "EntityTooLarge")
	}
	type extra struct{ XMLName xml.Name }
	var request struct {
		XMLName xml.Name `xml:"CompleteMultipartUpload"`
		Parts   []struct {
			Number []int    `xml:"PartNumber"`
			Tag    []string `xml:"ETag"`
			CRC    []string `xml:"ChecksumCRC32"`
			Extra  []extra  `xml:",any"`
		} `xml:"Part"`
		Extra []extra `xml:",any"`
	}
	dec := xml.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&request); err != nil {
		return completeRequest{}, fault(http.StatusBadRequest, "MalformedXML", err)
	}
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		text, ok := tok.(xml.CharData)
		if err != nil || !ok || strings.TrimSpace(string(text)) != "" {
			return completeRequest{}, fault(http.StatusBadRequest, "MalformedXML", err)
		}
	}
	if len(request.Parts) == 0 || len(request.Parts) > maxPartNumber {
		return completeRequest{}, fault(http.StatusBadRequest, "InvalidPart")
	}
	if len(request.Extra) != 0 {
		return completeRequest{}, fault(http.StatusNotImplemented, "NotImplemented")
	}

	var out completeRequest
	last := 0
	for _, p := range request.Parts {
		if len(p.Extra) != 0 {
			return out, fault(http.StatusNotImplemented, "NotImplemented")
		}
		if len(p.Number) != 1 || p.Number[0] < 1 || p.Number[0] > maxPartNumber || len(p.Tag) != 1 || len(p.CRC) > 1 {
			return out, fault(http.StatusBadRequest, "InvalidPart")
		}
		if p.Number[0] <= last {
			return out, fault(http.StatusBadRequest, "InvalidPartOrder")
		}
		last = p.Number[0]
		part := selectedPart{Number: last, Tag: p.Tag[0]}
		if len(p.CRC) == 1 {
			if !validCRC(p.CRC[0]) {
				return out, fault(http.StatusBadRequest, "InvalidPart")
			}
			part.CRC = p.CRC[0]
		}
		out.Parts = append(out.Parts, part)
	}
	if value := r.Header.Get("X-Amz-Mp-Object-Size"); value != "" {
		n, err := strconv.ParseUint(value, 10, 63)
		if err != nil {
			return out, fault(http.StatusBadRequest, "InvalidRequest", err)
		}
		size := int64(n)
		out.Size = &size
	}
	conditions, err := httpwire.ParseRawConditions(r.Header)
	out.When = conditions.When
	return out, err
}

func (h *Handler) completeUpload(w http.ResponseWriter, r *http.Request, a uploadAccess, address core.Address, payload string) (*core.Change, error) {
	m := h.multipart
	ctx := r.Context()
	req, err := h.parseComplete(w, r, payload)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	var record uploadRecord
	if err := m.DB.View(func(tx *bolt.Tx) error {
		_, rec, err := a.read(tx)
		record = rec
		return err
	}); err != nil {
		return nil, err
	}
	if record.Phase == completed {
		if record.Digest != digest {
			return nil, uploadConflict()
		}
		h.replyComplete(w, r, a, record.Change)
		return &record.Change, nil
	}
	if err := allowComplete(record, digest, false); err != nil {
		return nil, err
	}
	selected, err := m.bound(ctx, record, address)
	if err != nil {
		return nil, err
	}
	attempt, err := m.freeze(ctx, a, req, digest, false)
	if err != nil {
		return nil, err
	}
	if attempt.previous != nil {
		h.replyComplete(w, r, a, *attempt.previous)
		return attempt.previous, nil
	}
	change, runErr := h.writeFrozen(ctx, a.id, record, selected, req, attempt, false)
	// Persist the observed effect even if the HTTP caller stopped waiting.
	change, persistErr := m.finish(a.id, change, false)
	if persistErr != nil {
		return &change, errors.Join(runErr, persistErr)
	}
	if change.Effect != core.Applied {
		if runErr == nil {
			runErr = core.Fail(core.InvalidUpstreamResponse, nil)
		}
		return &change, runErr
	}
	err = errors.Join(runErr, m.clean(context.WithoutCancel(ctx), a.id, true, nil))
	if err != nil {
		h.observe(r, err, &change)
	}
	h.replyComplete(w, r, a, change)
	return &change, nil
}

func allowComplete(r uploadRecord, digest [32]byte, abort bool) error {
	switch r.Phase {
	case committing:
		return uploadConflict()
	case paused, unknown:
		if !r.Native || !r.hasFrozen || r.Digest != digest || (r.AbortRequested && !abort) {
			return uploadConflict()
		}
		return nil
	default:
		if abort {
			return uploadConflict()
		}
		return allowPart(r)
	}
}

type frozenWrite struct {
	parts      []partRecord
	total      int64
	checkpoint []byte
	previous   *core.Change
}

func (m *multipart) freeze(ctx context.Context, a uploadAccess, req completeRequest, digest [32]byte, abort bool) (out frozenWrite, err error) {
	err = m.DB.Update(func(tx *bolt.Tx) error {
		b, rec, err := a.read(tx)
		if err != nil {
			return err
		}
		if rec.Phase == completed {
			if abort || rec.Digest != digest {
				return uploadConflict()
			}
			out.previous = &rec.Change
			return nil
		}
		if err := allowComplete(rec, digest, abort); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if rec.Native {
			if rec.Phase != receiving {
				out.checkpoint, err = loadCheckpoint(b)
				if err != nil {
					return err
				}
				if len(out.checkpoint) == 0 {
					return uploadConflict()
				}
			} else if _, err = b.CreateBucket(nativeLog); err != nil {
				return err
			}
		}
		for i, want := range req.Parts {
			raw := b.Bucket(partTable).Get(partKey(want.Number))
			if raw == nil {
				return fault(http.StatusBadRequest, "InvalidPart")
			}
			var p partRecord
			if err := json.Unmarshal(raw, &p); err != nil {
				return err
			}
			if p.Tag != want.Tag || want.CRC != "" && p.CRC != want.CRC {
				return fault(http.StatusBadRequest, "InvalidPart")
			}
			if i < len(req.Parts)-1 && p.Size < minPartBytes {
				return fault(http.StatusBadRequest, "EntityTooSmall")
			}
			if rec.Phase == receiving && p.Size > m.MaxObjectBytes-out.total {
				return fault(http.StatusRequestEntityTooLarge, "EntityTooLarge")
			}
			out.parts = append(out.parts, p)
			out.total += p.Size
		}
		if req.Size != nil && out.total != *req.Size {
			return fault(http.StatusBadRequest, "InvalidRequest")
		}
		if rec.Phase == receiving {
			if err := putRecord(b, uploadFrozen, req); err != nil {
				return err
			}
			rec.Change = core.Change{Effect: core.Unchanged}
		}
		rec.Phase, rec.Digest = committing, digest
		rec.AbortRequested = abort
		return putRecord(b, uploadMeta, rec)
	})
	return
}

func (h *Handler) writeFrozen(ctx context.Context, id string, record uploadRecord, selected *core.Selection, req completeRequest, attempt frozenWrite, abort bool) (core.Change, error) {
	m := h.multipart
	if err := m.drain(ctx, id); err != nil {
		return core.Change{Effect: core.Unchanged}, err
	}
	stream := &partStream{ctx: ctx, inputs: m.Inputs, parts: attempt.parts, unopened: attempt.total}
	var source io.Reader = stream
	options := core.WriteOptions{Policy: core.Publish(req.When)}
	if record.Native {
		source = newManifestSource(stream, id, attempt.parts, attempt.total)
		options.Continuation = &core.WriteContinuation{Checkpoint: attempt.checkpoint, Abort: abort,
			Append: func(ctx context.Context, delta []byte) error { return m.appendCheckpoint(ctx, id, delta) }}
	}
	change, err := h.files.Write(selected.Context(ctx), record.Actor, record.Address, source, &attempt.total, options)
	return change, errors.Join(err, stream.Close())
}

func (m *multipart) finish(id string, change core.Change, confirmedAbort bool) (core.Change, error) {
	err := m.DB.Update(func(tx *bolt.Tx) error {
		b, rec, err := readUpload(tx, id)
		if err != nil {
			return err
		}
		if rec.Phase != committing {
			return uploadConflict()
		}
		rec.Expires = time.Now().Add(m.Retention)
		switch {
		case change.Effect == core.Applied:
			rec.Phase = completed
		case confirmedAbort:
			rec.Phase = aborted
		case change.Effect != core.Unchanged || rec.Change.Effect == core.Unknown:
			rec.Phase = unknown
			change.Effect = core.Unknown
		case checkpointBytes(b) > 0:
			// The object has not changed, but a private native session can
			// still exist. Only this frozen request or its Abort may use it.
			rec.Phase = paused
		default:
			rec.Phase = receiving
		}
		rec.Change = change
		if rec.Phase == receiving || rec.Phase == completed || rec.Phase == aborted {
			if err := b.Delete(uploadFrozen); err != nil {
				return err
			}
			rec.AbortRequested = false
			if b.Bucket(nativeLog) != nil {
				if err := b.DeleteBucket(nativeLog); err != nil {
					return err
				}
			}
		}
		return putRecord(b, uploadMeta, rec)
	})
	return change, err
}

func (h *Handler) replyComplete(w http.ResponseWriter, r *http.Request, a uploadAccess, change core.Change) {
	evidence(w.Header(), change.Evidence)
	h.multipartXML(w, r, struct {
		XMLName     xml.Name `xml:"CompleteMultipartUploadResult"`
		Bucket, Key string
		ETag        string `xml:",omitempty"`
	}{Bucket: a.bucket, Key: a.key, ETag: change.Evidence.Validator.Tag}, &change)
}
