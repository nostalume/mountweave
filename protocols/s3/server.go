// Package s3 serves a path-style S3 object profile backed by Core key bindings.
package s3

import (
	"context"
	"crypto/rand"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/protocols"
	"github.com/nostalume/mountweave/protocols/internal/httpwire"
)

// Bucket exports an existing Core key space. UnversionedDelete requires the
// host to maintain unversioned name-removal semantics for this binding.
type Bucket struct {
	Space             string
	UnversionedDelete bool
}

type Options struct {
	Region         string
	Buckets        map[string]Bucket
	LookupIdentity IdentityLookup
	Multipart      *MultipartOptions
	// CursorTTL bounds the lifetime of an enumeration across requests.
	CursorTTL time.Duration
	// MaxPutBytes caps decoded object bytes, including chunked uploads.
	MaxPutBytes int64
	// OnFailure observes original errors and mutation receipts. It can run concurrently.
	OnFailure func(protocols.Failure)
}

type Handler struct {
	files     *core.Core
	options   Options
	tokenKey  [32]byte
	multipart *multipart
}

// New snapshots routing policy and recovers multipart metadata when enabled.
// Route the original request target directly to the handler, without path cleaning.
func New(files *core.Core, options Options) (*Handler, error) {
	if files == nil || options.Region == "" || options.LookupIdentity == nil || options.CursorTTL <= 0 ||
		options.MaxPutBytes <= 0 || len(options.Buckets) == 0 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	for name, bucket := range options.Buckets {
		if !validBucket(name) || core.ValidatePath(bucket.Space) != nil {
			return nil, core.Fail(core.InvalidRequest, nil)
		}
	}
	options.Buckets = maps.Clone(options.Buckets)
	h := &Handler{files: files, options: options}
	if _, err := rand.Read(h.tokenKey[:]); err != nil {
		return nil, err
	}
	if options.Multipart != nil {
		m, err := newMultipart(*options.Multipart)
		if err != nil {
			return nil, err
		}
		h.multipart = m
		if err := h.RecoverMultipart(context.Background()); err != nil {
			return nil, err
		}
	}
	return h, nil
}

func validBucket(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.') {
			return false
		}
	}
	return name[0] != '.' && name[0] != '-' && name[len(name)-1] != '.' && name[len(name)-1] != '-'
}

func requestTarget(r *http.Request) (*url.URL, url.Values, string, string, error) {
	target, err := url.ParseRequestURI(r.RequestURI)
	rawPath, _, _ := strings.Cut(r.RequestURI, "?")
	if err != nil || target.IsAbs() || !strings.HasPrefix(rawPath, "/") || target.EscapedPath() != rawPath {
		return nil, nil, "", "", fault(http.StatusBadRequest, "InvalidURI")
	}
	rawBucket, rawKey, _ := strings.Cut(rawPath[1:], "/")
	// The bucket is unescaped ASCII, so the decoded path has the same boundary.
	if !validBucket(rawBucket) {
		return nil, nil, "", "", fault(http.StatusBadRequest, "InvalidBucketName")
	}
	key := target.Path[len(rawBucket)+1:]
	if rawKey != "" {
		key = key[1:]
	} else {
		key = ""
	}
	query, err := url.ParseQuery(target.RawQuery)
	if err != nil || len(key) > 1024 {
		return nil, nil, "", "", fault(http.StatusBadRequest, "InvalidArgument", err)
	}
	return target, query, rawBucket, key, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target, query, name, key, err := requestTarget(r)
	if err != nil {
		h.fail(w, r, err, nil)
		return
	}
	actor, payload, err := h.authenticate(r, target, time.Now())
	if err != nil {
		h.fail(w, r, err, nil)
		return
	}
	bucket, ok := h.options.Buckets[name]
	if !ok {
		h.fail(w, r, fault(http.StatusNotFound, "NoSuchBucket"), nil)
		return
	}
	op, err := multipartRoute(r, query)
	if err != nil {
		h.fail(w, r, err, nil)
		return
	}
	if err := validateRequest(r, query, key, payload, op); err != nil {
		h.fail(w, r, err, nil)
		return
	}
	if key == "" {
		if err := h.list(w, r, actor, name, bucket.Space, query); err != nil {
			h.fail(w, r, err, nil)
		}
		return
	}
	address := core.Key(bucket.Space, key)
	if op != noMultipart {
		if !validXMLName(key) || address.Validate() != nil {
			h.fail(w, r, fault(http.StatusBadRequest, "InvalidArgument"), nil)
			return
		}
		h.serveMultipart(w, r, uploadAccess{id: query.Get("uploadId"), bucket: name, key: key, actor: actor}, address, op, query, payload)
		return
	}
	selection := core.ReadSelection{Version: core.Version(query.Get("versionId"))}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		err = h.read(w, r, actor, address, selection)
	case http.MethodPut:
		change, err := h.put(w, r, actor, address, payload)
		if err != nil {
			h.fail(w, r, err, &change)
		}
		return
	case http.MethodDelete:
		change, err := h.remove(w, r, actor, address, bucket.UnversionedDelete)
		if err != nil {
			h.fail(w, r, err, &change)
		}
		return
	default:
		err = fault(http.StatusNotImplemented, "NotImplemented")
	}
	if err != nil {
		h.fail(w, r, err, nil)
	}
}

func validateRequest(r *http.Request, q url.Values, key, payload string, op multipartOperation) error {
	listing := key == "" && r.Method == http.MethodGet && q.Get("list-type") == "2"
	for name, values := range q {
		if len(values) != 1 {
			return fault(http.StatusBadRequest, "InvalidArgument")
		}
		switch name {
		case "x-id":
		case "uploads", "uploadId", "partNumber":
			if op == noMultipart {
				return fault(http.StatusNotImplemented, "NotImplemented")
			}
		case "list-type", "prefix", "delimiter", "start-after", "continuation-token", "max-keys", "encoding-type", "fetch-owner":
			if !listing {
				return fault(http.StatusNotImplemented, "NotImplemented")
			}
		case "max-parts", "part-number-marker":
			if op != listMultipartParts {
				return fault(http.StatusNotImplemented, "NotImplemented")
			}
		case "versionId":
			if op != noMultipart || listing || (r.Method != http.MethodGet && r.Method != http.MethodHead) || values[0] == "" {
				return fault(http.StatusNotImplemented, "NotImplemented")
			}
		default:
			return fault(http.StatusNotImplemented, "NotImplemented")
		}
	}
	if key == "" && !listing {
		return fault(http.StatusNotImplemented, "NotImplemented")
	}
	if r.Method != http.MethodPut && op != completeMultipart && (r.ContentLength != 0 || payload != emptyPayload && !(payload == unsignedPayload && r.TLS != nil)) {
		return fault(http.StatusBadRequest, "InvalidRequest")
	}
	for key, values := range r.Header {
		name := strings.ToLower(key)
		if listing && (strings.HasPrefix(name, "if-") || name == "range") ||
			op != noMultipart && op != completeMultipart && strings.HasPrefix(name, "if-") {
			return fault(http.StatusNotImplemented, "NotImplemented")
		}
		if (protectedHeader(name) || name == "x-amz-content-sha256") && len(values) != 1 {
			return fault(http.StatusBadRequest, "InvalidArgument")
		}
		switch name {
		case "x-amz-date", "x-amz-content-sha256", "x-amz-security-token", "x-amz-user-agent", "x-amz-checksum-mode":
		case "x-amz-mp-object-size":
			if op != completeMultipart {
				return fault(http.StatusNotImplemented, "NotImplemented")
			}
		case "content-encoding", "x-amz-trailer", "x-amz-decoded-content-length", "x-amz-checksum-crc32", "x-amz-sdk-checksum-algorithm":
			if r.Method != http.MethodPut {
				return fault(http.StatusNotImplemented, "NotImplemented")
			}
		case "content-type":
			if r.Method == http.MethodPut && values[0] != "" && values[0] != "application/octet-stream" {
				return fault(http.StatusNotImplemented, "NotImplemented")
			}
		case "range":
			if op != noMultipart || r.Method != http.MethodGet {
				return fault(http.StatusNotImplemented, "NotImplemented")
			}
		case "if-range", "content-md5", "content-disposition", "cache-control", "content-language", "expires":
			return fault(http.StatusNotImplemented, "NotImplemented")
		default:
			if strings.HasPrefix(name, "x-amz-") {
				return fault(http.StatusNotImplemented, "NotImplemented")
			}
		}
	}
	return nil
}

func (h *Handler) read(w http.ResponseWriter, r *http.Request, actor core.Principal, address core.Address, sel core.ReadSelection) error {
	conditions, err := httpwire.ParseRawConditions(r.Header)
	if err != nil {
		return err
	}
	sel.When = conditions.When
	if r.Method == http.MethodHead {
		entry, err := h.files.Stat(r.Context(), actor, address, sel)
		if err != nil {
			return err
		}
		evidence(w.Header(), entry.Evidence)
		metadata(w.Header(), entry.Size, entry.ModifiedAt)
		w.WriteHeader(http.StatusOK)
		return nil
	}
	span, err := httpwire.ParseRange(r.Header)
	if err != nil {
		return err
	}
	if span == nil && r.Header.Get("Range") != "" {
		return fault(http.StatusNotImplemented, "NotImplemented")
	}
	result, err := h.files.Open(r.Context(), actor, address, core.OpenOptions{Selection: sel, Range: span})
	if err != nil {
		return err
	}
	evidence(w.Header(), result.Entry.Evidence)
	metadata(w.Header(), result.Length, result.Entry.ModifiedAt)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")
	status := http.StatusOK
	if result.Range != nil {
		cr := result.Range
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", cr.Start, cr.End, cr.Total))
		status = http.StatusPartialContent
	}
	w.WriteHeader(status)
	_, err = io.Copy(w, result.Body)
	if err := errors.Join(err, result.Body.Close()); err != nil {
		h.observe(r, err, nil)
	}
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	return nil
}

func evidence(h http.Header, e core.Evidence) {
	if e.Validator.Valid() {
		h.Set("ETag", e.Validator.Tag)
	}
	if e.Version != "" {
		h.Set("X-Amz-Version-Id", string(e.Version))
	}
}

func metadata(h http.Header, size *int64, modified *time.Time) {
	if size != nil {
		h.Set("Content-Length", strconv.FormatInt(*size, 10))
	}
	if modified != nil {
		h.Set("Last-Modified", modified.UTC().Format(http.TimeFormat))
	}
}

type wireError struct {
	status int
	code   string
	cause  error
}

func (e *wireError) Error() string { return e.code }
func (e *wireError) Unwrap() error { return e.cause }
func fault(status int, code string, causes ...error) error {
	return &wireError{status: status, code: code, cause: errors.Join(causes...)}
}

func (h *Handler) observe(r *http.Request, err error, change *core.Change) {
	if h.options.OnFailure == nil {
		return
	}
	f := protocols.Failure{Operation: r.Method, Phase: "request", Err: err}
	if change != nil {
		f.HasChange, f.Change = true, *change
	}
	h.options.OnFailure(f)
}

func (h *Handler) fail(w http.ResponseWriter, r *http.Request, err error, change *core.Change) {
	h.observe(r, err, change)
	status, code := http.StatusInternalServerError, "InternalError"
	var wire *wireError
	if errors.As(err, &wire) {
		status, code = wire.status, wire.code
	} else {
		switch core.CodeOf(err) {
		case core.NotFound:
			status, code = http.StatusNotFound, "NoSuchKey"
		case core.PermissionDenied, core.Unauthenticated, core.UpstreamAccessDenied:
			status, code = http.StatusForbidden, "AccessDenied"
		case core.PreconditionFailed:
			status, code = http.StatusPreconditionFailed, "PreconditionFailed"
			var failed *core.ConditionFailure
			if errors.As(err, &failed) {
				evidence(w.Header(), failed.Evidence)
				if failed.Negative && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
					status = http.StatusNotModified
				}
			}
		case core.UnsupportedOperation:
			status, code = http.StatusNotImplemented, "NotImplemented"
		case core.InvalidRequest, core.InvalidPath, core.CursorUnavailable, core.UnrepresentableName:
			status, code = http.StatusBadRequest, "InvalidArgument"
		case core.InvalidRange, core.RangeNotSatisfiable:
			status, code = http.StatusRequestedRangeNotSatisfiable, "InvalidRange"
			var f *core.Error
			if errors.As(err, &f) && f.Total != nil {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", *f.Total))
			}
		case core.CapacityExceeded, core.UpstreamThrottled, core.MountUnavailable:
			status, code = http.StatusServiceUnavailable, "ServiceUnavailable"
		case core.UpstreamTimeout:
			status, code = http.StatusGatewayTimeout, "RequestTimeout"
		}
	}
	if r.Method == http.MethodHead || status == http.StatusNotModified {
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(struct {
		XMLName       xml.Name `xml:"Error"`
		Code, Message string
	}{Code: code, Message: http.StatusText(status)})
}

func (h *Handler) remove(w http.ResponseWriter, r *http.Request, actor core.Principal, address core.Address, unversioned bool) (core.Change, error) {
	unchanged := core.Change{Effect: core.Unchanged}
	if !unversioned || r.Header.Get("If-None-Match") != "" {
		return unchanged, fault(http.StatusNotImplemented, "NotImplemented")
	}
	conditions, err := httpwire.ParseRawConditions(r.Header)
	if err != nil {
		return unchanged, err
	}
	change, err := h.files.Remove(r.Context(), actor, address, core.RemoveOptions{When: conditions.When})
	absent := conditions.When.Empty() && change.Effect == core.Unchanged && change.Before == core.TargetMissing &&
		core.CodeOf(err) == core.NotFound && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
	if err != nil && !absent {
		return change, err
	}
	w.WriteHeader(http.StatusNoContent)
	return change, nil
}
