package core

import (
	"context"
	"errors"
	"fmt"
)

type Code string

const (
	UnsupportedOperation     Code = "unsupported_operation"
	PreconditionFailed       Code = "precondition_failed"
	AlreadyExists            Code = "already_exists"
	DirectoryNotEmpty        Code = "directory_not_empty"
	InvalidRequest           Code = "invalid_request"
	InvalidPath              Code = "invalid_path"
	InvalidRange             Code = "invalid_range"
	Unauthenticated          Code = "unauthenticated"
	PermissionDenied         Code = "permission_denied"
	NotFound                 Code = "not_found"
	PathAmbiguous            Code = "path_ambiguous"
	NotDirectory             Code = "not_directory"
	NotFile                  Code = "not_file"
	UnrepresentableName      Code = "unrepresentable_name"
	CursorUnavailable        Code = "cursor_unavailable"
	RangeNotSatisfiable      Code = "range_not_satisfiable"
	CapacityExceeded         Code = "capacity_exceeded"
	MountUnavailable         Code = "mount_unavailable"
	InvalidUpstreamResponse  Code = "invalid_upstream_response"
	UpstreamFailure          Code = "upstream_failure"
	UpstreamAccessDenied     Code = "upstream_access_denied"
	UpstreamTimeout          Code = "upstream_timeout"
	InternalError            Code = "internal_error"
	ResolutionBudgetExceeded Code = "resolution_budget_exceeded"
	ResponseBudgetExceeded   Code = "response_budget_exceeded"
	UpstreamThrottled        Code = "upstream_throttled"
)

// Error preserves an internal cause without making it part of the wire response.
type Error struct {
	Code  Code
	Cause error
	Total *int64
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Code, e.Cause)
	}
	return string(e.Code)
}

func (e *Error) Unwrap() error { return e.Cause }

func Fail(code Code, cause error) error { return &Error{Code: code, Cause: cause} }

func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return UpstreamTimeout
	}
	if errors.Is(err, context.Canceled) {
		return MountUnavailable
	}
	return InternalError
}

// Driver is rooted when constructed. Tree addresses start at /; key addresses
// preserve literal keys. Metadata.Name is a child name for tree listings and a
// complete root-relative key for key listings/reads. PrefixGroup is listing-only.
// Provider IDs and physical paths remain inside it. Tree operations use exact
// names in the service's exported namespace, without client-invented aliases or
// redirects. Ordinary access does not prove how the service implements a name.
// Explicit leaf observation and NoFollow retain their stronger native promises.
// Open transfers every returned Body/File to the caller, including on error.
// BoundRead/LiveEdit return File only. They must not retain the opening call's context;
// subsequent calls and Close own the resource lifetime. LiveEdit must report
// opening Change even on error. StreamRead keeps Body.
// NoFollow must reject native links/redirects in every component; a Driver
// unable to guarantee that mode refuses it rather than silently following one.
type Driver interface {
	Stat(context.Context, NativeAddress, ...ReadSelection) (Metadata, error)
	Enumerate(context.Context, NativeQuery) (Iterator, error)
	Open(context.Context, NativeAddress, OpenOptions) (Opened, error)
	Close() error
}

// LeafStatter optionally observes the entry itself at a rooted tree address.
// It must refuse intermediate links/redirects and may report SymbolicLink for
// the final entry without inspecting its target. A namespace with no links can
// use the same lookup as Stat. No native handles are transferred.
type LeafStatter interface {
	Lstat(context.Context, NativeAddress) (Metadata, error)
}

const (
	DefaultPage     = 100
	MaxPage         = 200
	MaxPathBytes    = 4096
	MaxPathSegments = 64
)
