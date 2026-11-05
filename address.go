package core

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"unicode/utf8"
)

// Namespace selects name semantics, not a provider or a second resource owner.
type Namespace uint8

const (
	TreeNamespace Namespace = iota
	KeyNamespace
)

// Address is a visible tree path, an exact key in a binding, or a key prefix.
// Constructors preserve bytes. Core validates at admission; prefixes select
// listings/ranges and cannot be used as file mutation targets.
type Address struct {
	namespace    Namespace
	prefix       bool
	space, value string
}

func Path(value string) Address { return Address{value: value} }

func Key(space, value string) Address {
	return Address{namespace: KeyNamespace, space: space, value: value}
}

func Prefix(space, value string) Address { a := Key(space, value); a.prefix = true; return a }

func (a Address) Namespace() Namespace { return a.namespace }

func (a Address) Space() string { return a.space }

func (a Address) Value() string { return a.value }

func (a Address) IsPrefix() bool { return a.prefix }

func (a Address) Validate() error {
	if a.namespace == TreeNamespace {
		return ValidatePath(a.value)
	}
	if a.namespace != KeyNamespace || ValidatePath(a.space) != nil || !utf8.ValidString(a.value) || (!a.prefix && a.value == "") {
		return Fail(InvalidRequest, nil)
	}
	return nil
}

func (a Address) MarshalJSON() ([]byte, error) {
	if a.namespace == TreeNamespace {
		return json.Marshal(struct {
			Namespace string `json:"namespace"`
			Path      string `json:"path"`
		}{"tree", a.value})
	}
	kind := "key"
	if a.prefix {
		kind = "prefix"
	}
	return json.Marshal(struct {
		Namespace string `json:"namespace"`
		Space     string `json:"space"`
		Kind      string `json:"kind"`
		Key       string `json:"key"`
	}{"keys", a.space, kind, a.value})
}

func (a *Address) UnmarshalJSON(data []byte) error {
	var v struct {
		Namespace string  `json:"namespace"`
		Path      *string `json:"path"`
		Space     *string `json:"space"`
		Kind      *string `json:"kind"`
		Key       *string `json:"key"`
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if !utf8.Valid(data) {
		return Fail(InvalidRequest, nil)
	}
	if err := d.Decode(&v); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return Fail(InvalidRequest, err)
	}
	var next Address
	switch v.Namespace {
	case "tree":
		if v.Path == nil || v.Space != nil || v.Kind != nil || v.Key != nil {
			return Fail(InvalidRequest, nil)
		}
		next = Path(*v.Path)
	case "keys":
		if v.Path != nil || v.Space == nil || v.Key == nil || v.Kind == nil {
			return Fail(InvalidRequest, nil)
		}
		switch *v.Kind {
		case "key":
			next = Key(*v.Space, *v.Key)
		case "prefix":
			next = Prefix(*v.Space, *v.Key)
		default:
			return Fail(InvalidRequest, nil)
		}
	default:
		return Fail(InvalidRequest, nil)
	}
	if err := next.Validate(); err != nil {
		return err
	}
	*a = next
	return nil
}

// NativeAddress is rooted in the prepared Driver. It has no public binding ID.
type NativeAddress struct {
	namespace Namespace
	value     string
}

func NativePath(value string) NativeAddress { return NativeAddress{value: value} }

func NativeKey(value string) NativeAddress {
	return NativeAddress{namespace: KeyNamespace, value: value}
}

func (a NativeAddress) Namespace() Namespace { return a.namespace }

func (a NativeAddress) Value() string { return a.value }

// NamespaceSupport is optional. A Driver without it accepts tree names only.
// Actual operation capabilities, authorization and retirement still apply.
type NamespaceSupport interface{ SupportsNamespace(Namespace) bool }

func supportsNamespace(d Driver, n Namespace) bool {
	if support, ok := d.(NamespaceSupport); ok {
		return support.SupportsNamespace(n)
	}
	return n == TreeNamespace
}

// Query selects immediate tree children or native keys/prefix groups.
// Delimiter and After are literal key-space values; Core cursors bind all fields.
type Query struct {
	address          Address
	delimiter, after string
}

func Children(address Address) Query { return Query{address: address} }

func Keys(space, prefix, delimiter string) Query {
	return Query{address: Prefix(space, prefix), delimiter: delimiter}
}

func (q Query) After(key string) Query { q.after = key; return q }

func (q Query) Address() Address { return q.address }

func (q Query) Delimiter() string { return q.delimiter }

func (q Query) StartAfter() string { return q.after }

func (q Query) validate() error {
	if q.address.namespace == TreeNamespace {
		if q.delimiter != "" || q.after != "" {
			return Fail(InvalidRequest, nil)
		}
	} else if !q.address.prefix || !utf8.ValidString(q.delimiter) || !utf8.ValidString(q.after) {
		return Fail(InvalidRequest, nil)
	}
	return nil
}

// NativeQuery carries the same selection after binding/root projection.
type NativeQuery struct {
	address          NativeAddress
	delimiter, after string
}

func NativeChildren(address NativeAddress) NativeQuery { return NativeQuery{address: address} }

func NativeKeys(prefix, delimiter string) NativeQuery {
	return NativeQuery{address: NativeKey(prefix), delimiter: delimiter}
}

func (q NativeQuery) After(key string) NativeQuery { q.after = key; return q }

func (q NativeQuery) Address() NativeAddress { return q.address }

func (q NativeQuery) Delimiter() string { return q.delimiter }

func (q NativeQuery) StartAfter() string { return q.after }

// KeyExtent maps a tree view to exact native keys and/or a literal prefix.
// This pure projection lets shared views schedule together without metadata I/O.
// Exact, when present, is this name's canonical native key, not a conservative
// extra point. Prefix may overapproximate its affected descendants.
// Extents cover possible effects, not authorization, persistent object identity
// or content versions. Missing/empty projection is conservative.
type KeyExtent struct {
	Exact, Prefix       string
	HasExact, HasPrefix bool
}

type KeyProjection interface {
	// KeyRoot gives the exact key prefix for a tree directory's contents.
	// False refuses key access through that alias; an overlap extent alone
	// is not sufficient evidence for routing or authorization projection.
	KeyRoot(NativeAddress) (string, bool)
	KeyExtent(NativeAddress) KeyExtent
}

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

// Names and separators are checked at construction. Projection only changes
// length and depth; it does not require parsing every ancestor name again.
func pathExtent(p string) bool {
	return len(p) <= MaxPathBytes && (p == "/" || strings.Count(p, "/") <= MaxPathSegments)
}

func childRoom(parent string) int {
	if parent != "/" && strings.Count(parent, "/") >= MaxPathSegments {
		return -1
	}
	if parent == "/" {
		return MaxPathBytes - 1
	}
	return MaxPathBytes - len(parent) - 1
}
