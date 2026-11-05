package httpwire

import (
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
)

type Conditions struct{ When core.Condition }

// ParseConditions owns HTTP syntax and precedence, not file observations.
func ParseConditions(h http.Header) (Conditions, error) {
	if len(h.Values("If")) != 0 ||
		len(h.Values("If-Modified-Since")) != 0 && len(h.Values("If-None-Match")) == 0 ||
		len(h.Values("If-Unmodified-Since")) != 0 && len(h.Values("If-Match")) == 0 {
		return Conditions{}, core.Fail(core.UnsupportedOperation, nil)
	}
	var terms []core.Condition
	for _, name := range []string{"If-Match", "If-None-Match"} {
		values := h.Values(name)
		if len(values) == 0 {
			continue
		}
		raw := strings.TrimSpace(strings.Join(values, ","))
		negative := name == "If-None-Match"
		if raw == "*" {
			if negative {
				terms = append(terms, core.IfAbsent())
			} else {
				terms = append(terms, core.IfPresent())
			}
			continue
		}
		var tags []core.Validator
		for raw != "" {
			start := 1
			if strings.HasPrefix(raw, "W/") {
				start = 3
			}
			if len(raw) <= start || raw[start-1] != '"' {
				return Conditions{}, core.Fail(core.InvalidRequest, nil)
			}
			end := strings.IndexByte(raw[start:], '"')
			if end < 0 {
				return Conditions{}, core.Fail(core.InvalidRequest, nil)
			}
			end += start + 1
			tag, err := ParseValidator(raw[:end])
			if err != nil {
				return Conditions{}, err
			}
			if len(tags) != 0 && tags[0].Domain != tag.Domain {
				return Conditions{}, core.Fail(core.UnsupportedOperation, nil)
			}
			tags = append(tags, tag)
			raw = strings.TrimSpace(raw[end:])
			if raw != "" {
				if raw[0] != ',' || strings.TrimSpace(raw[1:]) == "" {
					return Conditions{}, core.Fail(core.InvalidRequest, nil)
				}
				raw = strings.TrimSpace(raw[1:])
			}
		}
		if negative {
			terms = append(terms, core.IfNotMatch(tags...))
		} else {
			terms = append(terms, core.IfMatch(tags...))
		}
	}
	when := core.All(terms...)
	return Conditions{When: when}, when.Validate()
}

// WireValidator preserves the native comparison domain in an opaque HTTP tag.
// The original tag is recovered once at admission, without hashing content or
// adding a namespace/address to the credential-free evidence.
func WireValidator(v core.Validator) string {
	if !v.Valid() {
		return ""
	}
	prefix := ""
	if strings.HasPrefix(v.Tag, "W/") {
		prefix = "W/"
	}
	return prefix + `"mw.` + base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(len(v.Domain))+":"+v.Domain+strings.TrimPrefix(v.Tag, "W/"))) + `"`
}
func ParseValidator(tag string) (core.Validator, error) {
	v := core.Validator{Tag: tag}
	if !v.Valid() {
		return v, core.Fail(core.InvalidRequest, nil)
	}
	raw := strings.TrimPrefix(tag, "W/")
	if !strings.HasPrefix(raw, `"mw.`) {
		return v, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(raw[4 : len(raw)-1])
	if err != nil {
		return v, core.Fail(core.InvalidRequest, err)
	}
	size, rest, ok := strings.Cut(string(data), ":")
	n, sizeErr := strconv.Atoi(size)
	if !ok || sizeErr != nil || n < 0 || n > len(rest) {
		return v, core.Fail(core.InvalidRequest, nil)
	}
	domain, native := rest[:n], rest[n:]
	if strings.HasPrefix(tag, "W/") {
		native = "W/" + native
	}
	v = core.Validator{Domain: domain, Tag: native}
	if !v.Valid() {
		return v, core.Fail(core.InvalidRequest, nil)
	}
	return v, nil
}
func EvidenceHeaders(w http.ResponseWriter, e core.Evidence) {
	if tag := WireValidator(e.Validator); tag != "" {
		w.Header().Set("ETag", tag)
	}
}
func ConditionStatus(w http.ResponseWriter, r *http.Request, err error) (int, bool) {
	if core.CodeOf(err) == core.UnsupportedOperation {
		for _, name := range []string{"If", "If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since"} {
			if len(r.Header.Values(name)) != 0 {
				return http.StatusNotImplemented, true
			}
		}
	}
	if core.CodeOf(err) != core.PreconditionFailed {
		return 0, false
	}
	status := http.StatusPreconditionFailed
	var failed *core.ConditionFailure
	if errors.As(err, &failed) {
		EvidenceHeaders(w, failed.Evidence)
		if failed.Negative && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
			status = http.StatusNotModified
		}
	}
	return status, true
}
