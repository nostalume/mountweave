package s3

import (
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	core "github.com/nostalume/mountweave"
	bolt "go.etcd.io/bbolt"
)

const maxPartsPerPage = 1000

type listedPart struct {
	PartNumber    int
	LastModified  *time.Time `xml:",omitempty"`
	ETag          string
	Size          int64
	ChecksumCRC32 string `xml:",omitempty"`
}

func (h *Handler) listParts(w http.ResponseWriter, r *http.Request, a uploadAccess, address core.Address, q url.Values) error {
	page := struct {
		XMLName               xml.Name `xml:"ListPartsResult"`
		Bucket, Key, UploadId string
		PartNumberMarker      int
		NextPartNumberMarker  int `xml:",omitempty"`
		MaxParts              int
		IsTruncated           bool
		Parts                 []listedPart `xml:"Part"`
	}{Bucket: a.bucket, Key: a.key, UploadId: a.id, MaxParts: maxPartsPerPage}
	if q.Has("max-parts") {
		limit, err := strconv.Atoi(q.Get("max-parts"))
		if err != nil || limit < 1 || limit > maxPartsPerPage {
			return fault(http.StatusBadRequest, "InvalidArgument", err)
		}
		page.MaxParts = limit
	}
	if q.Has("part-number-marker") {
		marker, err := strconv.Atoi(q.Get("part-number-marker"))
		if err != nil || marker < 0 || marker > maxPartNumber {
			return fault(http.StatusBadRequest, "InvalidArgument", err)
		}
		page.PartNumberMarker = marker
	}

	ctx := r.Context()
	m := h.multipart
	var record uploadRecord
	// The read transaction owns one page snapshot. Rebind outside it so host
	// authorization cannot hold the database read lock while it runs.
	err := m.DB.View(func(tx *bolt.Tx) error {
		b, rec, err := a.read(tx)
		if err != nil {
			return err
		}
		record = rec
		switch rec.Phase {
		case receiving:
			if !time.Now().Before(rec.Expires) {
				return noUpload()
			}
		case committing, paused, unknown:
			if rec.AbortRequested {
				return uploadConflict()
			}
		default:
			return noUpload()
		}
		cursor := b.Bucket(partTable).Cursor()
		for key, raw := cursor.Seek(partKey(page.PartNumberMarker + 1)); key != nil; key, raw = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(page.Parts) == page.MaxParts {
				page.IsTruncated = true
				page.NextPartNumberMarker = page.Parts[len(page.Parts)-1].PartNumber
				break
			}
			if len(key) != 2 {
				return errors.New("invalid multipart part key")
			}
			var part partRecord
			if err := json.Unmarshal(raw, &part); err != nil {
				return err
			}
			page.Parts = append(page.Parts, listedPart{
				PartNumber: int(binary.BigEndian.Uint16(key)), LastModified: part.Modified,
				ETag: part.Tag, Size: part.Size, ChecksumCRC32: part.CRC,
			})
		}
		return ctx.Err()
	})
	if err != nil {
		return err
	}
	if _, err := m.bound(ctx, record, address); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	h.multipartXML(w, r, page, nil)
	return nil
}
