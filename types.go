package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"
)

type Principal string
type Action string

const (
	StatAction   Action = "stat"
	ListAction   Action = "list"
	ReadAction   Action = "read"
	CreateAction Action = "create"
	WriteAction  Action = "write" // Explicit create-or-replace permission.
	RemoveAction Action = "remove"
)

type Code string

const (
	UnsupportedOperation     Code = "unsupported_operation"
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
func (e *Error) Unwrap() error          { return e.Cause }
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

type Kind string

const (
	File      Kind = "file"
	Directory Kind = "directory"
)

// Metadata contains observations, not an object identity or an owned handle.
type Metadata struct {
	Name       string
	Kind       Kind
	Size       *int64
	ModifiedAt *time.Time
}
type Entry struct {
	Path       string     `json:"path"`
	Name       string     `json:"name"`
	Kind       Kind       `json:"kind"`
	Size       *int64     `json:"size"`
	ModifiedAt *time.Time `json:"modified_at"`
	Virtual    bool       `json:"virtual"`
}

// Iterator owns a native enumeration. Next returns a batch and may return io.EOF
// together with its final entries. Context applies to the current call only.
type Iterator interface {
	Next(context.Context, int) ([]Metadata, error)
	Close() error
}

// Driver is rooted when constructed. Paths use slash-separated mount-relative
// syntax beginning at /. Provider IDs and physical paths remain inside it.
// Open transfers every returned Body to the caller, including on error.
type Driver interface {
	Stat(context.Context, string) (Metadata, error)
	Enumerate(context.Context, string) (Iterator, error)
	Open(context.Context, string, *Range) (Opened, error)
	Close() error
}

type Opened struct {
	Metadata Metadata
	Body     io.ReadCloser
	Length   *int64
	Range    *ContentRange
}
type Content struct {
	Entry  Entry
	Body   io.ReadCloser
	Length *int64
	Range  *ContentRange
}
type Authorizer func(context.Context, Principal, Action, string) error

const (
	DefaultPage     = 100
	MaxPage         = 200
	MaxPathBytes    = 4096
	MaxPathSegments = 64
)

func ValidatePath(p string) error {
	if p == "" || len(p) > MaxPathBytes || !utf8.ValidString(p) || p[0] != '/' {
		return Fail(InvalidPath, nil)
	}
	if p == "/" {
		return nil
	}
	parts := strings.Split(p[1:], "/")
	if len(parts) > MaxPathSegments {
		return Fail(InvalidPath, nil)
	}
	for _, part := range parts {
		if !ValidName(part) {
			return Fail(InvalidPath, nil)
		}
	}
	return nil
}
func ValidName(s string) bool {
	if s == "" || s == "." || s == ".." || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r == '/' || r == '\\' || r < 32 || r == 127 {
			return false
		}
	}
	return true
}
func Within(p, root string) bool { return root == "/" || p == root || strings.HasPrefix(p, root+"/") }
func childPath(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}
func relative(p, root string) string {
	if root == "/" {
		return p
	}
	if p == root {
		return "/"
	}
	return strings.TrimPrefix(p, root)
}
func virtualEntry(p string) Entry {
	name := ""
	if p != "/" {
		name = p[strings.LastIndexByte(p, '/')+1:]
	}
	return Entry{Path: p, Name: name, Kind: Directory, Virtual: true}
}
func project(p string, m Metadata) (Entry, error) {
	if m.Kind != File && m.Kind != Directory {
		return Entry{}, Fail(InvalidUpstreamResponse, nil)
	}
	if m.Size != nil && (*m.Size < 0 || m.Kind == Directory) {
		return Entry{}, Fail(InvalidUpstreamResponse, nil)
	}
	e := virtualEntry(p)
	e.Kind = m.Kind
	e.Virtual = false
	if m.Size != nil {
		n := *m.Size
		e.Size = &n
	}
	if m.ModifiedAt != nil {
		t := m.ModifiedAt.UTC()
		if t.Year() < 1 || t.Year() > 9999 {
			return Entry{}, Fail(InvalidUpstreamResponse, errors.New("invalid metadata timestamp"))
		}
		e.ModifiedAt = &t
	}
	return e, nil
}
