package webdav

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/protocols/internal/httpwire"
)

// destination only resolves a local URL; it never fetches or forwards one.
func (h *Handler) destination(r *http.Request) (core.Address, error) {
	values := r.Header.Values("Destination")
	if len(values) != 1 || values[0] == "" {
		return core.Address{}, core.Fail(core.InvalidRequest, nil)
	}
	u, err := url.Parse(values[0])
	if err != nil || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return core.Address{}, core.Fail(core.InvalidPath, err)
	}
	if u.Host != "" || u.Scheme != "" {
		scheme := r.URL.Scheme
		if scheme == "" {
			scheme = "http"
			if r.TLS != nil {
				scheme = "https"
			}
		}
		if !strings.EqualFold(u.Host, r.Host) || !strings.EqualFold(u.Scheme, scheme) {
			return core.Address{}, core.Fail(core.UpstreamFailure, nil)
		}
	}
	if !strings.HasPrefix(u.Path, "/") {
		return core.Address{}, core.Fail(core.InvalidPath, nil)
	}
	return h.address(u)
}

func (h *Handler) manage(w http.ResponseWriter, r *http.Request, p core.Principal, source core.Address) {
	conditions, err := httpwire.ParseConditions(r.Header)
	if err != nil {
		h.failure(w, r, err)
		return
	}
	for _, name := range []string{"If-Range", "Range", "Content-Range"} {
		if len(r.Header.Values(name)) != 0 {
			fail(w, 400, "invalid-range")
			return
		}
	}
	input := r.Body
	if input == nil {
		input = http.NoBody
	}
	var one [1]byte
	if _, err := io.ReadFull(input, one[:]); err != io.EOF {
		fail(w, 415, "unsupported-body")
		return
	}
	depth := r.Header.Get("Depth")
	if len(r.Header.Values("Depth")) > 1 || (depth != "" && depth != "0" && depth != "1" && depth != "infinity") {
		fail(w, 400, "invalid-depth")
		return
	}
	if r.Method != "COPY" && depth != "" && depth != "infinity" {
		fail(w, 400, "invalid-depth")
		return
	}
	if r.Method == http.MethodDelete {
		if source == h.options.Root {
			fail(w, 403, "protected-root")
			return
		}
		result, err := h.files.RemoveTree(r.Context(), p, source, h.options.Tree, core.DirectoryRemoveOptions{Scope: core.Subtree, When: conditions.When})
		h.treeResponse(w, r, source, source, result, err, false)
		return
	}
	target, err := h.destination(r)
	if err != nil {
		h.failure(w, r, err)
		return
	}
	if source == target || target == h.options.Root || r.Method == "MOVE" && source == h.options.Root {
		fail(w, 403, "protected-root")
		return
	}
	overwrite := r.Header.Get("Overwrite")
	if len(r.Header.Values("Overwrite")) > 1 || overwrite != "" && overwrite != "T" && overwrite != "F" {
		fail(w, 400, "invalid-overwrite")
		return
	}
	existing := core.Replace
	if overwrite == "F" {
		existing = core.CreateNew
	}
	if r.Method == "COPY" {
		selection := core.Recursive
		if depth == "0" {
			selection = core.Self
		} else if depth == "1" {
			selection = core.TreeDepth("1")
		}
		result, err := h.files.CopyTree(r.Context(), p, source, target, h.options.Tree, core.DirectoryCopyOptions{
			Existing: existing, Depth: selection, Source: core.ReadSelection{When: conditions.When}, WriteOptions: core.WriteOptions{Policy: core.Publish()},
		})
		h.treeResponse(w, r, source, target, result, err, overwrite == "F")
		return
	}
	result, err := h.files.Move(r.Context(), p, source, target, core.MoveOptions{Source: conditions.When, Existing: existing, Tree: h.options.Tree})
	if err != nil {
		if result.Removed != nil && len(result.Removed.Items) != 0 {
			rows := result.Removed.Items
			rows = append(rows, core.TreeItemResult{RelativePath: "/", Side: core.SourceSide, Phase: core.RemovePhase, Change: result.Source, Err: err})
			h.treeResponse(w, r, source, target, core.TreeResult{Change: core.Change{Effect: core.Incomplete}, Items: rows}, err, overwrite == "F")
		} else if overwrite == "F" && core.CodeOf(err) == core.AlreadyExists {
			fail(w, 412, "precondition-failed")
		} else {
			h.failure(w, r, err)
		}
		return
	}
	status := http.StatusCreated
	if result.Target.Before == core.TargetPresent {
		status = http.StatusNoContent
	}
	w.WriteHeader(status)
}

func (h *Handler) href(a core.Address, relative string, directory bool) string {
	p := a.Value()
	if relative != "/" {
		p = strings.TrimSuffix(p, "/") + relative
	}
	p = strings.TrimPrefix(p, strings.TrimSuffix(h.options.Root.Value(), "/"))
	p = strings.TrimSuffix(h.options.Prefix, "/") + p
	if p == "" {
		p = "/"
	}
	if directory && !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return (&url.URL{Path: p}).EscapedPath()
}

type operationResponse struct {
	Href   string `xml:"DAV: href"`
	Status string `xml:"DAV: status"`
}

func (h *Handler) treeResponse(w http.ResponseWriter, r *http.Request, source, target core.Address, result core.TreeResult, err error, exclusive bool) {
	if r.Context().Err() != nil {
		return
	}
	if err == nil {
		status := http.StatusNoContent
		if r.Method != http.MethodDelete && result.Change.Before == core.TargetMissing {
			status = http.StatusCreated
		}
		if r.Method != http.MethodDelete && result.Change.Before == core.TargetUnknown {
			h.failure(w, r, core.Fail(core.InvalidUpstreamResponse, nil))
			return
		}
		httpwire.EvidenceHeaders(w, result.Change.Evidence)
		w.WriteHeader(status)
		return
	}
	var responses []operationResponse
	var rootFailure error
	rootOnly := true
	multiple := false
	for _, item := range result.Items {
		if item.RelativePath != "/" || item.Phase == core.RemovePhase && r.Method != http.MethodDelete {
			multiple = true
		}
		if item.Err == nil {
			continue
		}
		if item.RelativePath != "/" {
			rootOnly = false
		}
		a := source
		side := item.Side
		if item.ErrorSide != "" {
			side = item.ErrorSide
		} else {
			var failure *core.OperationError
			if errors.As(item.Err, &failure) {
				side = failure.Side
			}
		}
		if side == core.TargetSide {
			a = target
		}
		if rootFailure == nil {
			rootFailure = &core.OperationError{Side: side, Phase: item.Phase, Cause: item.Err}
		}
		status := operationStatus(item.Err)
		if side == core.TargetSide && core.CodeOf(item.Err) == core.NotFound {
			status = 409
		}
		if core.CodeOf(item.Err) == core.UnsupportedOperation && (r.Header.Get("If-Match") != "" || r.Header.Get("If-None-Match") != "") {
			status = 501
		}
		if exclusive && core.CodeOf(item.Err) == core.AlreadyExists {
			status = 412
		}
		responses = append(responses, operationResponse{h.href(a, item.RelativePath, false), "HTTP/1.1 " + strconv.Itoa(status) + " " + http.StatusText(status)})
	}
	if len(responses) == 0 || rootOnly && !multiple {
		if rootFailure != nil {
			err = rootFailure
		}
		if exclusive && core.CodeOf(err) == core.AlreadyExists {
			fail(w, 412, "precondition-failed")
		} else {
			h.failure(w, r, err)
		}
		return
	}
	// Successful/default and inferred dependency statuses are omitted per RFC
	// 4918. Core retains its full attempted-prefix and phase receipts.
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	message := struct {
		XMLName   xml.Name            `xml:"DAV: multistatus"`
		Responses []operationResponse `xml:"DAV: response"`
	}{Responses: responses}
	if err := xml.NewEncoder(w).Encode(message); err != nil {
		panic(http.ErrAbortHandler)
	}
}

func operationStatus(err error) int {
	if core.CodeOf(err) == core.PreconditionFailed {
		return 412
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return 413
	}
	switch core.CodeOf(err) {
	case core.InvalidRequest, core.InvalidPath, core.InvalidRange, core.UnrepresentableName:
		return 400
	case core.Unauthenticated, core.PermissionDenied:
		return 403
	case core.NotFound:
		return 404
	case core.AlreadyExists, core.NotDirectory, core.DirectoryNotEmpty, core.PathAmbiguous:
		return 409
	case core.NotFile, core.UnsupportedOperation:
		return 405
	case core.CapacityExceeded, core.ResponseBudgetExceeded, core.ResolutionBudgetExceeded:
		return 507
	case core.RangeNotSatisfiable:
		return 416
	case core.MountUnavailable, core.CursorUnavailable, core.UpstreamThrottled:
		return 503
	case core.UpstreamTimeout:
		return 504
	case core.UpstreamFailure, core.InvalidUpstreamResponse, core.UpstreamAccessDenied:
		return 502
	default:
		return 500
	}
}
