package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	core "github.com/nostalume/mountweave"
)

// TypedFactory adapts a native settings struct to Factory. Config is a JSON
// object, with omitted Config treated as an empty object. check validates
// settings and references without acquiring resources. build resolves the
// references and constructs the Driver under the usual Factory ownership rules.
func TypedFactory[T any](check func(Definition, T) error, build func(context.Context, Definition, T) (core.Driver, error)) Factory {
	if build == nil {
		return Factory{}
	}
	settings := func(ctx context.Context, d Definition) (T, error) {
		var value T
		if err := ctx.Err(); err != nil {
			return value, err
		}
		raw := bytes.TrimSpace(d.Config)
		if len(d.Config) == 0 {
			raw = []byte("{}")
		}
		if len(raw) == 0 || raw[0] != '{' {
			return value, errors.New("native configuration must be a JSON object")
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&value); err != nil {
			return value, fmt.Errorf("invalid native configuration: %w", err)
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return value, errors.Join(errors.New("native configuration must contain one JSON object"), err)
		}
		if check != nil {
			if err := check(d, value); err != nil {
				return value, err
			}
		}
		return value, ctx.Err()
	}
	return Factory{
		Validate: func(ctx context.Context, d Definition) error {
			_, err := settings(ctx, d)
			return err
		},
		Build: func(ctx context.Context, d Definition) (core.Driver, error) {
			value, err := settings(ctx, d)
			if err != nil {
				return nil, err
			}
			return build(ctx, d, value)
		},
	}
}
