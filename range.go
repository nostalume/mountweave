package core

// Range is either an inclusive start/end (End=-1 means open-ended), or a suffix.
type Range struct {
	Start, End int64
	Suffix     bool
}
type ContentRange struct{ Start, End, Total int64 }

func (r Range) Valid() bool {
	if r.Suffix {
		return r.Start >= 0 && r.End == -1
	}
	return r.Start >= 0 && (r.End == -1 || r.End >= r.Start)
}
func (r Range) Resolve(total int64) (ContentRange, error) {
	if !r.Valid() {
		return ContentRange{}, Fail(InvalidRange, nil)
	}
	if total < 0 {
		return ContentRange{}, Fail(InvalidUpstreamResponse, nil)
	}
	if total == 0 || (!r.Suffix && r.Start >= total) || (r.Suffix && r.Start == 0) {
		return ContentRange{}, &Error{Code: RangeNotSatisfiable, Total: &total}
	}
	start, end := r.Start, r.End
	if r.Suffix {
		start = total - r.Start
		if start < 0 {
			start = 0
		}
		end = total - 1
	} else if end == -1 || end >= total {
		end = total - 1
	}
	return ContentRange{Start: start, End: end, Total: total}, nil
}
