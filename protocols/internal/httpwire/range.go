// Package httpwire contains shared HTTP representation parsing, not file execution.
package httpwire

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
)

// ParseRange selects a supported single byte range. Unknown units and multiple
// ranges are ignored, preserving the ordinary full-representation response.
func ParseRange(h http.Header) (*core.Range, error) {
	values := h.Values("Range")
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) != 1 {
		return nil, core.Fail(core.InvalidRange, nil)
	}
	raw := strings.TrimSpace(values[0])
	unit, spec, ok := strings.Cut(raw, "=")
	if !ok {
		return nil, core.Fail(core.InvalidRange, nil)
	}
	if !strings.EqualFold(strings.TrimSpace(unit), "bytes") {
		return nil, nil
	}
	if strings.Contains(spec, ",") {
		return nil, nil
	}
	first, last, ok := strings.Cut(strings.TrimSpace(spec), "-")
	if !ok {
		return nil, core.Fail(core.InvalidRange, nil)
	}
	number := func(s string) (int64, error) {
		if s == "" {
			return 0, errors.New("missing integer")
		}
		for _, r := range s {
			if r < '0' || r > '9' {
				return 0, errors.New("invalid integer")
			}
		}
		return strconv.ParseInt(s, 10, 64)
	}
	r := core.Range{End: -1}
	if first == "" {
		n, err := number(last)
		if err != nil {
			return nil, core.Fail(core.InvalidRange, err)
		}
		r.Start = n
		r.Suffix = true
	} else {
		n, err := number(first)
		if err != nil {
			return nil, core.Fail(core.InvalidRange, err)
		}
		r.Start = n
		if last != "" {
			n, err = number(last)
			if err != nil {
				return nil, core.Fail(core.InvalidRange, err)
			}
			r.End = n
		}
	}
	if !r.Valid() {
		return nil, core.Fail(core.InvalidRange, nil)
	}
	return &r, nil
}
