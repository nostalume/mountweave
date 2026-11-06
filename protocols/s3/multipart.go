package s3

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	bolt "go.etcd.io/bbolt"
)

type multipartOperation uint8

const (
	noMultipart multipartOperation = iota
	createMultipart
	putPart
	listMultipartParts
	completeMultipart
	abortMultipart
)

func multipartRoute(r *http.Request, q url.Values) (multipartOperation, error) {
	if q.Has("uploads") {
		if r.Method == http.MethodPost && q.Get("uploads") == "" && !q.Has("uploadId") && !q.Has("partNumber") {
			return createMultipart, nil
		}
		return noMultipart, fault(http.StatusBadRequest, "InvalidArgument")
	}
	if !q.Has("uploadId") && !q.Has("partNumber") {
		return noMultipart, nil
	}
	if id := q.Get("uploadId"); id == "" || len(id) > 64 {
		return noMultipart, noUpload()
	}
	switch r.Method {
	case http.MethodGet:
		if !q.Has("partNumber") {
			return listMultipartParts, nil
		}
	case http.MethodPut:
		if q.Has("partNumber") {
			return putPart, nil
		}
	case http.MethodPost:
		if !q.Has("partNumber") {
			return completeMultipart, nil
		}
	case http.MethodDelete:
		if !q.Has("partNumber") {
			return abortMultipart, nil
		}
	}
	return noMultipart, fault(http.StatusBadRequest, "InvalidArgument")
}

func (h *Handler) serveMultipart(w http.ResponseWriter, r *http.Request, a uploadAccess, address core.Address, op multipartOperation, q url.Values, payload string) {
	m := h.multipart
	if m == nil {
		h.fail(w, r, core.Fail(core.UnsupportedOperation, nil), nil)
		return
	}
	m.gate.RLock()
	defer m.gate.RUnlock()
	var err error
	var change *core.Change
	switch op {
	case createMultipart:
		id, createErr := m.create(r.Context(), a, address)
		if createErr == nil {
			h.multipartXML(w, r, struct {
				XMLName               xml.Name `xml:"InitiateMultipartUploadResult"`
				Bucket, Key, UploadId string
			}{Bucket: a.bucket, Key: a.key, UploadId: id}, nil)
			return
		}
		err = createErr
	case putPart:
		n, numberErr := strconv.Atoi(q.Get("partNumber"))
		if numberErr != nil || n < 1 || n > maxPartNumber {
			err = fault(http.StatusBadRequest, "InvalidArgument", numberErr)
		} else {
			err = h.uploadPart(w, r, a, address, n, payload)
		}
	case listMultipartParts:
		err = h.listParts(w, r, a, address, q)
	case completeMultipart:
		change, err = h.completeUpload(w, r, a, address, payload)
	case abortMultipart:
		change, err = h.abortUpload(r.Context(), a, address)
		if err == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	if err != nil {
		h.fail(w, r, err, change)
	}
}

func (h *Handler) abortUpload(ctx context.Context, a uploadAccess, address core.Address) (*core.Change, error) {
	m := h.multipart
	m.mu.Lock()
	if m.stops[a.id] {
		m.mu.Unlock()
		return nil, uploadConflict()
	}
	m.stops[a.id] = true
	m.mu.Unlock()
	defer func() { m.mu.Lock(); delete(m.stops, a.id); m.mu.Unlock() }()
	var record uploadRecord
	var frozen *completeRequest
	if err := m.DB.Update(func(tx *bolt.Tx) error {
		b, rec, err := a.read(tx)
		if err != nil {
			return err
		}
		switch rec.Phase {
		case aborted:
			return nil
		case receiving:
			if err := ctx.Err(); err != nil {
				return err
			}
			rec.Phase, rec.Expires = aborted, time.Now().Add(m.Retention)
			return putRecord(b, uploadMeta, rec)
		case paused, unknown:
			if !rec.Native || !rec.hasFrozen || checkpointBytes(b) == 0 {
				return uploadConflict()
			}
			var req completeRequest
			if err := json.Unmarshal(b.Get(uploadFrozen), &req); err != nil {
				return err
			}
			record, frozen = rec, &req
			return nil
		case committing:
			return uploadConflict()
		default:
			return noUpload()
		}
	}); err != nil {
		return nil, err
	}
	if frozen == nil {
		if err := m.drain(ctx, a.id); err != nil {
			return nil, err
		}
		return nil, m.clean(ctx, a.id, true, nil)
	}

	selected, err := m.bound(ctx, record, address)
	if err != nil {
		return nil, err
	}
	req := *frozen
	attempt, err := m.freeze(ctx, a, req, record.Digest, true)
	if err != nil {
		return nil, err
	}
	change, runErr := h.writeFrozen(ctx, a.id, record, selected, req, attempt, true)
	confirmed := runErr == nil && change.Effect == core.Unchanged
	change, persistErr := m.finish(a.id, change, confirmed)
	if persistErr != nil {
		return &change, errors.Join(runErr, persistErr)
	}
	if change.Effect == core.Applied {
		return &change, errors.Join(uploadConflict(), runErr, m.clean(context.WithoutCancel(ctx), a.id, true, nil))
	}
	if !confirmed {
		if runErr == nil {
			runErr = uploadConflict()
		}
		return &change, runErr
	}
	return &change, m.clean(ctx, a.id, true, nil)
}

func (h *Handler) multipartXML(w http.ResponseWriter, r *http.Request, value any, change *core.Change) {
	w.Header().Set("Content-Type", "application/xml")
	if err := xml.NewEncoder(w).Encode(value); err != nil {
		h.observe(r, err, change)
	}
}

func validXMLName(s string) bool {
	for _, c := range s {
		if c < 32 && !strings.ContainsRune("\t\n\r", c) || c == 0xfffe || c == 0xffff {
			return false
		}
	}
	return true
}
