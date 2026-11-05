// Package webdav exposes bounded tree file workflows through an HTTP handler.
// The host owns authentication and the server. All file effects enter Core.
// This basic endpoint does not advertise a WebDAV compliance class or locking.
//
// Handler directly exposes OPTIONS, PROPFIND, GET/HEAD, PUT
// MKCOL, COPY, MOVE and DELETE through the same Core. Its PUT selects complete publication and
// reports observed creation/replacement; If-None-Match:* selects IfAbsent.
// The host supplies identity, route/root, XML/list/tree budgets and cursor lifetime.
// COPY maps Depth 0/infinity to Self/Recursive, and Overwrite T/F maps to
// Replace/CreateNew. MOVE and DELETE accept infinity. Destination must stay in
// this export and authority; no remote URL is fetched. Actual tree failures use
// 207 with failed hrefs, omitting default success and inferred dependency rows.
// List responses reuse existing cursor cleanup. No WebDAV class, locking or
// user-property transaction is advertised.
package webdav

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/protocols/internal/httpwire"
)

type Limits struct {
	XMLBytes    int64
	ListEntries int
	// ListNameBytes bounds retained name, address and evidence string bytes.
	ListNameBytes int
}

type Options struct {
	Prefix string
	Root   core.Address
	// Authenticate writes its own challenge/response on failure and returns false.
	Authenticate func(http.ResponseWriter, *http.Request) (core.Principal, bool)
	CursorTTL    time.Duration
	Limits       Limits
	// Tree bounds each COPY/MOVE/DELETE discovery plan. Both MaxEntries and
	// MaxPlanBytes must be positive; Concurrency defaults to serial execution.
	Tree core.TreeOptions
}

type Handler struct {
	files   *core.Core
	options Options
}

func New(files *core.Core, options Options) (*Handler, error) {
	prefix := strings.TrimSuffix(options.Prefix, "/")
	if prefix == "" {
		prefix = "/"
	}
	if files == nil || options.Authenticate == nil || options.Root.Namespace() != core.TreeNamespace ||
		core.ValidatePath(options.Root.Value()) != nil || options.Prefix == "" || core.ValidatePath(prefix) != nil ||
		options.CursorTTL <= 0 || options.Limits.XMLBytes <= 0 || options.Limits.ListEntries <= 0 || options.Limits.ListNameBytes <= 0 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if options.Tree.MaxEntries <= 0 || options.Tree.MaxPlanBytes <= 0 || options.Tree.Concurrency < 0 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	options.Prefix = prefix
	return &Handler{files: files, options: options}, nil
}

const methods = "OPTIONS, PROPFIND, GET, HEAD, PUT, MKCOL, COPY, MOVE, DELETE"

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p, ok := h.options.Authenticate(w, r)
	if !ok {
		return
	}
	if p == "" {
		fail(w, http.StatusForbidden, "forbidden")
		return
	}
	a, err := h.address(r.URL)
	if err != nil {
		h.failure(w, r, err)
		return
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	switch r.Method {
	case "OPTIONS":
		w.Header().Set("Allow", methods)
		w.WriteHeader(http.StatusNoContent)
	case "PROPFIND":
		h.propfind(w, r, p, a)
	case http.MethodGet, http.MethodHead:
		h.read(w, r, p, a)
	case http.MethodPut:
		h.put(w, r, p, a)
	case "COPY", "MOVE", http.MethodDelete:
		h.manage(w, r, p, a)
	case "MKCOL":
		if !noConditions(w, r) {
			return
		}
		var one [1]byte
		input := r.Body
		if input == nil {
			input = http.NoBody
		}
		_, err := io.ReadFull(input, one[:])
		if err != io.EOF {
			fail(w, http.StatusUnsupportedMediaType, "unsupported-body")
			return
		}
		_, err = h.files.Mkdir(r.Context(), p, a)
		if core.CodeOf(err) == core.AlreadyExists && err != nil {
			fail(w, http.StatusMethodNotAllowed, "already-exists")
			return
		}
		if err != nil {
			h.failure(w, r, err)
			return
		}
		w.WriteHeader(http.StatusCreated)
	default:
		w.Header().Set("Allow", methods)
		fail(w, http.StatusMethodNotAllowed, "unsupported-method")
	}
}

// URL.Path was decoded by net/http. Never decode it a second time or clean an
// invalid path into an accepted one; Root and Prefix have different ownership.
func (h *Handler) address(u *url.URL) (core.Address, error) {
	escaped := strings.ToLower(u.EscapedPath())
	if u.RawQuery != "" || strings.Contains(escaped, "%2f") || strings.Contains(escaped, "%5c") {
		return core.Address{}, core.Fail(core.InvalidPath, nil)
	}
	p := u.Path
	if h.options.Prefix != "/" {
		if p != h.options.Prefix && !strings.HasPrefix(p, h.options.Prefix+"/") {
			return core.Address{}, core.Fail(core.NotFound, nil)
		}
		p = strings.TrimPrefix(p, h.options.Prefix)
	}
	if p == "" || p == "/" {
		return h.options.Root, nil
	}
	p = strings.TrimSuffix(p, "/")
	if err := core.ValidatePath(p); err != nil {
		return core.Address{}, err
	}
	root := strings.TrimSuffix(h.options.Root.Value(), "/")
	return core.Path(root + p), nil
}

func noConditions(w http.ResponseWriter, r *http.Request) bool {
	for _, name := range []string{"If", "If-Match", "If-None-Match", "If-Unmodified-Since", "If-Modified-Since", "If-Range", "Range", "Content-Range"} {
		if len(r.Header.Values(name)) != 0 {
			fail(w, http.StatusNotImplemented, "unsupported-condition")
			return false
		}
	}
	return true
}

func (h *Handler) put(w http.ResponseWriter, r *http.Request, p core.Principal, a core.Address) {
	conditions, parseErr := httpwire.ParseConditions(r.Header)
	if parseErr != nil {
		h.failure(w, r, parseErr)
		return
	}
	// Core and its native publisher own the actual predicate observation.
	policy := core.Publish(conditions.When)
	for _, name := range []string{"If-Range", "Range", "Content-Range"} {
		if len(r.Header.Values(name)) != 0 {
			fail(w, 501, "unsupported-condition")
			return
		}
	}
	if encoding := r.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
		fail(w, 415, "unsupported-encoding")
		return
	}
	var length *int64
	if r.ContentLength >= 0 {
		n := r.ContentLength
		length = &n
	}
	input := r.Body
	if input == nil {
		input = http.NoBody
	}
	change, err := h.files.Write(r.Context(), p, a, input, length, core.WriteOptions{Policy: policy, ObserveTarget: true})
	httpwire.EvidenceHeaders(w, change.Evidence)
	if err != nil {
		h.failure(w, r, err)
		return
	}
	switch change.Before {
	case core.TargetMissing:
		w.WriteHeader(http.StatusCreated)
	case core.TargetPresent:
		w.WriteHeader(http.StatusNoContent)
	default:
		fail(w, http.StatusBadGateway, "missing-publication-observation")
	}
}

func (h *Handler) read(w http.ResponseWriter, r *http.Request, p core.Principal, a core.Address) {
	conditions, parseErr := httpwire.ParseConditions(r.Header)
	if parseErr != nil {
		h.failure(w, r, parseErr)
		return
	}
	if r.Method == http.MethodHead {
		entry, err := h.files.Inspect(r.Context(), p, core.ReadAction, a, core.ReadSelection{When: conditions.When})
		if err != nil {
			h.failure(w, r, err)
			return
		}
		if entry.Kind != core.File {
			fail(w, 405, "not-file")
			return
		}
		contentHeaders(w, entry, entry.Size)
		w.WriteHeader(http.StatusOK)
		return
	}
	var wanted *core.Range
	var err error
	// If-Range uses the permitted full-representation fallback until conditional
	// native reading is available; there is no racy metadata preflight.
	if len(r.Header.Values("If-Range")) == 0 {
		wanted, err = httpwire.ParseRange(r.Header)
	}
	if err != nil {
		h.failure(w, r, err)
		return
	}
	opened, err := h.files.Open(r.Context(), p, a, core.OpenOptions{Range: wanted, Selection: core.ReadSelection{When: conditions.When}})
	if err != nil {
		h.failure(w, r, err)
		return
	}
	contentHeaders(w, opened.Entry, opened.Length)
	status := http.StatusOK
	if opened.Range != nil {
		status = http.StatusPartialContent
		actual := opened.Range
		w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(actual.Start, 10)+"-"+strconv.FormatInt(actual.End, 10)+"/"+strconv.FormatInt(actual.Total, 10))
	}
	w.WriteHeader(status)
	_, copyErr := io.Copy(w, opened.Body)
	closeErr := opened.Body.Close()
	if copyErr != nil || closeErr != nil {
		panic(http.ErrAbortHandler)
	}
}

func contentHeaders(w http.ResponseWriter, entry core.Entry, length *int64) {
	httpwire.EvidenceHeaders(w, entry.Evidence)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")
	if length != nil {
		w.Header().Set("Content-Length", strconv.FormatInt(*length, 10))
	}
	if entry.ModifiedAt != nil {
		w.Header().Set("Last-Modified", entry.ModifiedAt.UTC().Format(http.TimeFormat))
	}
}

func (h *Handler) failure(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return
	}
	if status, ok := httpwire.ConditionStatus(w, r, err); ok {
		if status == 304 {
			w.WriteHeader(status)
		} else {
			fail(w, status, string(core.CodeOf(err)))
		}
		return
	}
	status := operationStatus(err)
	var operation *core.OperationError
	if errors.As(err, &operation) && operation.Side == core.TargetSide && core.CodeOf(err) == core.NotFound {
		status = 409
	}
	if core.CodeOf(err) == core.NotFound && (r.Method == "PUT" || r.Method == "MKCOL") {
		status = 409
	}
	if status == 416 {
		var detail *core.Error
		if errors.As(err, &detail) && detail.Total != nil {
			w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(*detail.Total, 10))
		}
	}
	fail(w, status, string(core.CodeOf(err)))
}

func fail(w http.ResponseWriter, status int, code string) {
	// Domain codes are safe identifiers. Native errors, paths and credentials
	// never enter the wire body.
	w.Header().Del("Content-Length")
	http.Error(w, code, status)
}

func (h *Handler) entries(ctx context.Context, p core.Principal, root core.Entry, children bool) (entries []core.Entry, err error) {
	remaining := h.options.Limits.ListNameBytes
	add := func(e core.Entry) error {
		n := len(e.Name) + len(e.Address.Value()) + len(e.Evidence.Validator.Domain) + len(e.Evidence.Validator.Tag) + len(e.Evidence.Version)
		if len(entries) >= h.options.Limits.ListEntries || n > remaining {
			return core.Fail(core.ResponseBudgetExceeded, nil)
		}
		remaining -= n
		entries = append(entries, e)
		return nil
	}
	if err = add(root); err != nil || !children || root.Kind != core.Directory {
		return
	}
	cursor := ""
	defer func() {
		if cursor != "" {
			cleanupErr := h.files.ReleaseCursor(ctx, p, cursor)
			if core.CodeOf(cleanupErr) != core.CursorUnavailable {
				err = errors.Join(err, cleanupErr)
			}
		}
	}()
	for {
		page, listErr := h.files.List(ctx, p, core.ListRequest{Query: core.Children(root.Address), Limit: min(core.MaxPage, h.options.Limits.ListEntries), Cursor: cursor, CursorTTL: h.options.CursorTTL})
		if listErr != nil {
			return nil, listErr
		}
		cursor = ""
		if page.NextCursor != nil {
			cursor = *page.NextCursor
		}
		for _, e := range page.Items {
			if err = add(e); err != nil {
				return nil, err
			}
		}
		if cursor == "" {
			return
		}
	}
}
