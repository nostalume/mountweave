package s3

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
)

type bucketPage struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	XMLNS                 string   `xml:"xmlns,attr"`
	Name, Prefix          string
	Delimiter             string `xml:",omitempty"`
	EncodingType          string `xml:",omitempty"`
	StartAfter            string `xml:",omitempty"`
	MaxKeys, KeyCount     int
	IsTruncated           bool
	ContinuationToken     string        `xml:",omitempty"`
	NextContinuationToken string        `xml:",omitempty"`
	Contents              []objectEntry `xml:"Contents"`
	CommonPrefixes        []prefixEntry `xml:"CommonPrefixes"`
}

type objectEntry struct {
	Key          string
	Size         *int64 `xml:",omitempty"`
	ETag         string `xml:",omitempty"`
	LastModified string `xml:",omitempty"`
}

type prefixEntry struct{ Prefix string }

func (h *Handler) list(w http.ResponseWriter, r *http.Request, actor core.Principal, name, space string, q url.Values) error {
	maxKeys := 1000
	if q.Has("max-keys") {
		n, err := strconv.ParseUint(q.Get("max-keys"), 10, 31)
		if err != nil {
			return fault(http.StatusBadRequest, "InvalidArgument", err)
		}
		maxKeys = int(n)
	}
	prefix, delimiter, after := q.Get("prefix"), q.Get("delimiter"), q.Get("start-after")
	encoding := q.Get("encoding-type")
	if (encoding != "" && encoding != "url") || q.Get("fetch-owner") != "" && q.Get("fetch-owner") != "false" {
		return fault(http.StatusNotImplemented, "NotImplemented")
	}
	for _, value := range []string{prefix, delimiter, after} {
		if len(value) > 1024 || !utf8.ValidString(value) {
			return fault(http.StatusBadRequest, "InvalidArgument")
		}
	}
	token := q.Get("continuation-token")
	cursor := ""
	if q.Has("continuation-token") {
		var originalAfter string
		var valid bool
		cursor, originalAfter, valid = h.openToken(name, token)
		if !valid || q.Has("start-after") && after != originalAfter {
			return fault(http.StatusBadRequest, "InvalidArgument")
		}
		after = originalAfter
	}
	req := core.ListRequest{Query: core.Keys(space, prefix, delimiter).After(after), Limit: min(max(maxKeys, 1), core.MaxPage), Cursor: cursor}
	if cursor == "" {
		req.CursorTTL = h.options.CursorTTL
	}
	page, err := h.files.List(r.Context(), actor, req)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if keep {
			return
		}
		if page.NextCursor != nil {
			cursor = *page.NextCursor
		}
		if cursor != "" {
			if err := h.files.ReleaseCursor(context.WithoutCancel(r.Context()), actor, cursor); err != nil && core.CodeOf(err) != core.CursorUnavailable {
				h.observe(r, err, nil)
			}
		}
	}()
	result := bucketPage{XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/", Name: name, MaxKeys: maxKeys, EncodingType: encoding, ContinuationToken: token}
	err = result.project(page, prefix, delimiter, after, encoding == "url", maxKeys)
	if err != nil {
		return err
	}
	if page.NextCursor != nil && maxKeys != 0 {
		result.IsTruncated = true
		result.NextContinuationToken = h.sealToken(name, *page.NextCursor, after)
	}
	w.Header().Set("Content-Type", "application/xml")
	if err := xml.NewEncoder(w).Encode(result); err != nil {
		h.observe(r, err, nil)
		panic(http.ErrAbortHandler)
	}
	keep = result.IsTruncated && r.Context().Err() == nil
	return nil
}

func (p *bucketPage) project(page core.Page, prefix, delimiter, after string, encode bool, maxKeys int) error {
	project := func(s string) (string, error) {
		if len(s) > 1024 || !utf8.ValidString(s) {
			return "", core.Fail(core.UnrepresentableName, nil)
		}
		if encode {
			return strings.ReplaceAll(url.QueryEscape(s), "+", "%20"), nil
		}
		if !validXMLName(s) {
			return "", core.Fail(core.UnrepresentableName, nil)
		}
		return s, nil
	}
	var err error
	if p.Prefix, err = project(prefix); err != nil {
		return err
	}
	if p.Delimiter, err = project(delimiter); err != nil {
		return err
	}
	if p.StartAfter, err = project(after); err != nil {
		return err
	}
	if maxKeys == 0 {
		return nil
	}
	for _, entry := range page.Items {
		key, err := project(entry.Address.Value())
		if err != nil {
			return err
		}
		if entry.Kind == core.PrefixGroup {
			p.CommonPrefixes = append(p.CommonPrefixes, prefixEntry{Prefix: key})
		} else {
			e := objectEntry{Key: key, Size: entry.Size, ETag: entry.Evidence.Validator.Tag}
			if entry.ModifiedAt != nil {
				e.LastModified = entry.ModifiedAt.UTC().Format("2006-01-02T15:04:05.000Z")
			}
			p.Contents = append(p.Contents, e)
		}
		p.KeyCount++
	}
	return nil
}

// The envelope binds the opaque Core cursor to the wire bucket and retains
// StartAfter when a continuation request omits it. Core still owns the cursor.
func (h *Handler) sealToken(bucket, cursor, after string) string {
	data := []byte(cursor + "\n" + after)
	return base64.RawURLEncoding.EncodeToString(append(data, h.tokenMAC(bucket, data)...))
}

func (h *Handler) openToken(bucket, token string) (string, string, bool) {
	if len(token) > 2048 {
		return "", "", false
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(raw) <= sha256.Size {
		return "", "", false
	}
	data, mac := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	if !hmac.Equal(mac, h.tokenMAC(bucket, data)) {
		return "", "", false
	}
	cursor, after, ok := strings.Cut(string(data), "\n")
	return cursor, after, ok && cursor != ""
}

func (h *Handler) tokenMAC(bucket string, data []byte) []byte {
	mac := hmac.New(sha256.New, h.tokenKey[:])
	mac.Write([]byte(bucket + "\n"))
	mac.Write(data)
	return mac.Sum(nil)
}
