package core

import (
	"bytes"
	"encoding/json"
	"io"
)

// WritePolicy distinguishes direct exclusive creation from complete publication.
// The zero value retains streaming creation, which may leave partial new content.
type WritePolicy struct {
	mode uint8
	when Condition
}

// Publish selects complete publication, creating or replacing a file by default.
// At most one predicate is accepted. Invalid policies fail before input is read.
func Publish(when ...Condition) WritePolicy {
	p := WritePolicy{mode: 1}
	if len(when) > 1 {
		p.mode = 255
	} else if len(when) == 1 {
		p.when = when[0]
	}
	return p
}

func (p WritePolicy) Complete() bool { return p.mode == 1 }

func (p WritePolicy) When() Condition { return p.when }

func (p WritePolicy) Validate() error {
	if p.mode > 1 || p.mode == 0 && !p.when.Empty() {
		return Fail(InvalidRequest, nil)
	}
	return p.when.Validate()
}

func (p WritePolicy) MarshalJSON() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	mode := "create_new"
	if p.Complete() {
		mode = "publish"
	}
	return json.Marshal(struct {
		Mode string    `json:"mode"`
		When Condition `json:"when"`
	}{mode, p.when})
}

func (p *WritePolicy) UnmarshalJSON(data []byte) error {
	var wire struct {
		Mode string    `json:"mode"`
		When Condition `json:"when"`
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&wire); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return Fail(InvalidRequest, nil)
	}
	switch wire.Mode {
	case "create_new":
		*p = WritePolicy{when: wire.When}
	case "publish":
		*p = Publish(wire.When)
	default:
		return Fail(InvalidRequest, nil)
	}
	return p.Validate()
}

// TargetPresence is a native observation made while preparing or committing a
// mutation. It is not a version guard or a promise about concurrent writers.
type TargetPresence uint8

const (
	TargetUnknown TargetPresence = iota
	TargetMissing
	TargetPresent
)

// PublicationOptions is shared by ordinary and continuable native publication.
// ObserveTarget requests a presence observation for response classification.
// It does not strengthen When; ordinary publication need not perform that I/O.
type PublicationOptions struct {
	When          Condition
	ObserveTarget bool
}
