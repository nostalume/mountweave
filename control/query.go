package control

import (
	"context"

	core "github.com/nostalume/mountweave"
)

const (
	LoadAction Action = "load"
	ListAction Action = "list"
)

// Load returns desired input, independently of runtime activation. Callers own
// redaction for their audience; the control library does not interpret Config.
func (c *Controller) Load(ctx context.Context, p core.Principal, id string) (Definition, error) {
	if err := c.access(ctx, p, LoadAction, id); err != nil {
		return Definition{}, err
	}
	d, err := c.opts.Store.Load(ctx, id)
	if err == nil && d.ID != id {
		return Definition{}, ErrNotFound
	}
	return clone(d), err
}

// List requires list authority and filters each item through load authority.
// Reading definitions remains available while runtime components are stopped.
func (c *Controller) List(ctx context.Context, p core.Principal) ([]Definition, error) {
	if err := c.access(ctx, p, ListAction, ""); err != nil {
		return nil, err
	}
	definitions, err := c.opts.Store.List(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]Definition, 0, len(definitions))
	for _, d := range definitions {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if c.access(ctx, p, LoadAction, d.ID) == nil {
			result = append(result, clone(d))
		}
	}
	return result, ctx.Err()
}
