package core

import (
	"bytes"
	"encoding/json"
	"io"
	"sort"
	"strings"
	"unicode/utf8"
)

// Validator is an opaque entity tag in a declared comparison domain. Tag uses
// quoted entity-tag syntax; it is not necessarily a hash or an object identity.
// An empty Domain means comparison at the operation's already selected address.
type Validator struct {
	Domain string `json:"domain,omitempty"`
	Tag    string `json:"tag"`
}

// Version selects an immutable native revision; it is not an entity tag.
type Version string

// Evidence contains only observations the native operation actually returned.
type Evidence struct {
	Validator Validator `json:"validator,omitzero"`
	Version   Version   `json:"version,omitempty"`
}

func (v Validator) Valid() bool {
	s := strings.TrimPrefix(v.Tag, "W/")
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return false
	}
	for i := 1; i < len(s)-1; i++ {
		if s[i] < 0x21 || s[i] == '"' || s[i] == 0x7f {
			return false
		}
	}
	return true
}

// Condition holds at most one positive and one negative tag/presence test.
// Canonical strings keep it immutable, comparable and allocation-free for a
// single tag. Its zero value is unconditional; it is not an expression tree.
type Condition struct {
	match, none, domain string
	invalid             bool
}

func IfPresent() Condition { return Condition{match: "*"} }

func IfAbsent() Condition { return Condition{none: "*"} }

func IfMatch(tags ...Validator) Condition { return tagCondition(false, tags) }

func IfNotMatch(tags ...Validator) Condition { return tagCondition(true, tags) }

func (c Condition) IsAbsent() bool { return c == IfAbsent() }

func (c Condition) IsPresent() bool { return c == IfPresent() }

func (c Condition) Empty() bool { return c == (Condition{}) }

func (c Condition) Match() string { return c.match }

func (c Condition) NotMatch() string { return c.none }

func (c Condition) Domain() string { return c.domain }

func tagCondition(negative bool, tags []Validator) Condition {
	c := Condition{}
	if len(tags) == 0 {
		c.invalid = true
		return c
	}
	c.domain = tags[0].Domain
	var values []string
	for _, tag := range tags {
		if !tag.Valid() || tag.Domain != c.domain {
			c.invalid = true
			return c
		}
	}
	value := tags[0].Tag
	if len(tags) > 1 {
		values = make([]string, len(tags))
		for i, tag := range tags {
			values[i] = tag.Tag
		}
		sort.Strings(values)
		n := 0
		for _, v := range values {
			if n == 0 || values[n-1] != v {
				values[n] = v
				n++
			}
		}
		value = strings.Join(values[:n], ", ")
	}
	if negative {
		c.none = value
	} else {
		c.match = value
	}
	return c
}

// All combines the positive and negative tests; different duplicate tests are
// rejected instead of creating a general-purpose predicate language.
func All(conditions ...Condition) Condition {
	var out Condition
	for _, c := range conditions {
		if c.invalid || out.domain != "" && c.domain != "" && out.domain != c.domain {
			out.invalid = true
		}
		if c.domain != "" {
			out.domain = c.domain
		}
		if c.match != "" {
			if out.match != "" && out.match != c.match {
				out.invalid = true
			}
			out.match = c.match
		}
		if c.none != "" {
			if out.none != "" && out.none != c.none {
				out.invalid = true
			}
			out.none = c.none
		}
	}
	return out
}

func (c Condition) Validate() error {
	if c.invalid {
		return Fail(InvalidRequest, nil)
	}
	return nil
}

func (c Condition) ForDomain(domain string) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if c.domain != "" && c.domain != domain {
		return Fail(UnsupportedOperation, nil)
	}
	return nil
}

// ConditionFailure preserves which test failed and its actual observation.
// Protocols may project a negative GET/HEAD failure as 304; mutations use 412.
type ConditionFailure struct {
	Negative bool
	Evidence Evidence
}

func (*ConditionFailure) Error() string { return "condition failed" }

func (c Condition) Check(e Evidence, exists bool) error {
	if err := c.Validate(); err != nil {
		return err
	}
	for i, raw := range []string{c.match, c.none} {
		if raw == "" {
			continue
		}
		matched, err := c.matches(raw, e.Validator, exists, i == 1)
		if err != nil {
			return err
		}
		if matched == (i == 1) {
			return Fail(PreconditionFailed, &ConditionFailure{Negative: i == 1, Evidence: e})
		}
	}
	return nil
}

func (c Condition) matches(raw string, observed Validator, exists, weak bool) (bool, error) {
	if raw == "*" {
		return exists, nil
	}
	if !exists {
		return false, nil
	}
	if !observed.Valid() {
		return false, Fail(UnsupportedOperation, nil)
	}
	if err := c.ForDomain(observed.Domain); err != nil {
		return false, err
	}
	for raw != "" {
		tag, rest := nextTag(raw)
		if weak {
			if strings.TrimPrefix(tag, "W/") == strings.TrimPrefix(observed.Tag, "W/") {
				return true, nil
			}
		} else if !strings.HasPrefix(tag, "W/") && !strings.HasPrefix(observed.Tag, "W/") && tag == observed.Tag {
			return true, nil
		}
		raw = rest
	}
	return false, nil
}

func nextTag(raw string) (string, string) {
	start := 1
	if strings.HasPrefix(raw, "W/") {
		start = 3
	}
	end := strings.IndexByte(raw[start:], '"') + start + 1
	return raw[:end], strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw[end:]), ","))
}

func (c Condition) MarshalJSON() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	// JSON must never silently replace bytes in a durable expected validator.
	if !utf8.ValidString(c.match) || !utf8.ValidString(c.none) || !utf8.ValidString(c.domain) {
		return nil, Fail(UnsupportedOperation, nil)
	}
	return json.Marshal(struct {
		Match  string `json:"match,omitempty"`
		None   string `json:"not_match,omitempty"`
		Domain string `json:"domain,omitempty"`
	}{c.match, c.none, c.domain})
}

func (c *Condition) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return Fail(InvalidRequest, nil)
	}
	var wire struct {
		Match  json.RawMessage `json:"match"`
		None   json.RawMessage `json:"not_match"`
		Domain string          `json:"domain"`
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&wire); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return Fail(InvalidRequest, nil)
	}
	parse := func(data json.RawMessage, negative bool) Condition {
		if len(data) == 0 {
			return Condition{}
		}
		var raw string
		if json.Unmarshal(data, &raw) != nil || raw == "" {
			return Condition{invalid: true}
		}
		if raw == "*" {
			if negative {
				return IfAbsent()
			}
			return IfPresent()
		}
		var tags []Validator
		for raw != "" {
			start := 1
			if strings.HasPrefix(raw, "W/") {
				start = 3
			}
			if len(raw) <= start || raw[start-1] != '"' {
				return Condition{invalid: true}
			}
			end := strings.IndexByte(raw[start:], '"')
			if end < 0 {
				return Condition{invalid: true}
			}
			end += start + 1
			tags = append(tags, Validator{Domain: wire.Domain, Tag: raw[:end]})
			raw = strings.TrimSpace(raw[end:])
			if raw != "" {
				if raw[0] != ',' || strings.TrimSpace(raw[1:]) == "" {
					return Condition{invalid: true}
				}
				raw = strings.TrimSpace(raw[1:])
			}
		}
		return tagCondition(negative, tags)
	}
	*c = All(parse(wire.Match, false), parse(wire.None, true))
	return c.Validate()
}

// ReadSelection applies to the metadata or body actually returned by a read.
type ReadSelection struct {
	When    Condition `json:"when"`
	Version Version   `json:"version,omitempty"`
}

func (s ReadSelection) Check(e Evidence) error {
	if s.Version != "" && s.Version != e.Version {
		return Fail(NotFound, nil)
	}
	return s.When.Check(e, true)
}
