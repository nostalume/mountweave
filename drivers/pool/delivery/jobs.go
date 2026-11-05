// Package delivery optionally adapts Pool maintenance to the shared HTTP job
// resource. The base Pool package has no protocol or scheduler dependency.
package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"

	core "github.com/nostalume/mountweave"
	pool "github.com/nostalume/mountweave/drivers/pool"
	"github.com/nostalume/mountweave/protocols/httpapi"
	"github.com/nostalume/mountweave/task"
)

type Action string

const (
	Create  Action = "create"
	List    Action = "list"
	Observe Action = "observe"
	Run     Action = "run"
	Forget  Action = "forget"
)

// Options binds one durable Pool identity, never a replacement selected by path.
// Resolve borrows the host's restored owner; it must reject a changed resource.
// Scope must be stable and shared by all registrations of this Pool. Policies
// authorize resource administration; Pool jobs do not invent a user owner.
type Options struct {
	Scope     string
	Resolve   func(context.Context) (*pool.Driver, error)
	Authorize func(context.Context, core.Principal, Action, string) error
}
type request struct {
	Kind   pool.MaintenanceKind `json:"kind"`
	Member string               `json:"member,omitempty"`
}
type details struct {
	Request request `json:"request"`
	Done    bool    `json:"done"`
	Visited int     `json:"visited"`
	Changed int     `json:"changed"`
	Bytes   int64   `json:"bytes"`
	More    bool    `json:"more"`
}

func view(job pool.MaintenanceJob) httpapi.JobView {
	state := "pending"
	if job.Done {
		state = "completed"
	}
	return httpapi.JobView{JobRef: httpapi.JobRef{ID: job.ID, Generation: job.Generation}, State: state, Details: details{request{job.Request.Kind, job.Request.Member}, job.Done, job.Last.Visited, job.Last.Changed, job.Last.Bytes, job.Last.More}}
}
func decode(raw []byte, v any) error {
	if !utf8.Valid(raw) {
		return core.Fail(core.InvalidRequest, nil)
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return core.Fail(core.InvalidRequest, err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return core.Fail(core.InvalidRequest, err)
	}
	return nil
}

// Jobs reuses TaskKind for direct and queued execution. Preparing work performs
// no metadata/native lookup; conditional commands run inside Pool's own owner.
func Jobs(o Options) (httpapi.JobSource, error) {
	if o.Scope == "" || o.Resolve == nil || o.Authorize == nil {
		return httpapi.JobSource{}, errors.New("Pool jobs require stable scope, resolver and policy")
	}
	access := func(ctx context.Context, p core.Principal, a Action, id string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := o.Authorize(ctx, p, a, id); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return core.Fail(core.PermissionDenied, err)
		}
		return ctx.Err()
	}
	resolve := func(ctx context.Context) (*pool.Driver, error) {
		d, err := o.Resolve(ctx)
		if err == nil && d == nil {
			err = core.Fail(core.MountUnavailable, nil)
		}
		return d, err
	}
	source := httpapi.JobSource{Kind: "pool.maintenance", Version: 1, Actions: make(map[string]httpapi.TaskKind),
		Create: func(ctx context.Context, p core.Principal, id string, raw json.RawMessage) (httpapi.JobView, error) {
			var i request
			if err := decode(raw, &i); err != nil {
				return httpapi.JobView{}, err
			}
			if err := access(ctx, p, Create, id); err != nil {
				return httpapi.JobView{}, err
			}
			d, err := resolve(ctx)
			if err != nil {
				return httpapi.JobView{}, err
			}
			change, err := d.PlanMaintenance(ctx, id, pool.MaintenanceRequest{Kind: i.Kind, Member: i.Member})
			if err != nil {
				return httpapi.JobView{JobRef: httpapi.JobRef{ID: id}, Details: map[string]string{"record_effect": change.Effect.String()}}, err
			}
			job, err := d.MaintenanceStatus(ctx, id)
			return view(job), err
		},
		Get: func(ctx context.Context, p core.Principal, id string) (httpapi.JobView, error) {
			if err := access(ctx, p, Observe, id); err != nil {
				return httpapi.JobView{}, err
			}
			d, err := resolve(ctx)
			if err != nil {
				return httpapi.JobView{}, err
			}
			job, err := d.MaintenanceStatus(ctx, id)
			return view(job), err
		},
		List: func(ctx context.Context, p core.Principal, after string, limit int) (httpapi.JobPage, error) {
			if err := access(ctx, p, List, ""); err != nil {
				return httpapi.JobPage{}, err
			}
			d, err := resolve(ctx)
			if err != nil {
				return httpapi.JobPage{}, err
			}
			jobs, err := d.MaintenanceJobs(ctx, after, limit)
			if err != nil {
				return httpapi.JobPage{}, err
			}
			page := httpapi.JobPage{Items: make([]httpapi.JobView, 0, len(jobs))}
			for _, job := range jobs {
				if err := access(ctx, p, Observe, job.ID); err != nil {
					if ctx.Err() != nil {
						return page, ctx.Err()
					}
					continue
				}
				page.Items = append(page.Items, view(job))
			}
			if len(jobs) == limit {
				page.Next = jobs[len(jobs)-1].ID
			}
			return page, nil
		},
	}
	for _, action := range []Action{Run, Forget} {
		source.Actions[string(action)] = httpapi.TaskKind{
			Prepare: func(_ context.Context, p core.Principal, raw json.RawMessage) (task.Work, error) {
				var i struct {
					ID         string `json:"id"`
					Generation string `json:"generation"`
					Limit      int    `json:"limit,omitempty"`
				}
				if err := decode(raw, &i); err != nil {
					return task.Work{}, err
				}
				if i.ID == "" || i.Generation == "" || (action == Run && i.Limit < 1) || (action == Forget && i.Limit != 0) {
					return task.Work{}, core.Fail(core.InvalidRequest, nil)
				}
				return task.Work{Key: httpapi.JobKey{Resource: o.Scope}, Run: func(ctx context.Context, _ func(any)) (any, error) {
					if err := access(ctx, p, action, i.ID); err != nil {
						return nil, err
					}
					d, err := resolve(ctx)
					if err != nil {
						return nil, err
					}
					options := pool.MaintenanceOptions{ExpectedGeneration: i.Generation}
					result := httpapi.JobResult{JobRef: httpapi.JobRef{ID: i.ID, Generation: i.Generation}, Action: string(action)}
					if action == Run {
						job, e := d.RunMaintenance(ctx, i.ID, i.Limit, options)
						if job.ID != "" {
							result.Receipt = view(job).Details
						}
						err = e
					} else {
						change, e := d.ForgetMaintenance(ctx, i.ID, options)
						result.Receipt = map[string]string{"record_effect": change.Effect.String()}
						err = e
					}
					return result, err
				}}, nil
			}, Project: func(s task.Snapshot) (httpapi.TaskValues, error) { return httpapi.TaskValues{Result: s.Result}, nil },
		}
	}
	return source, nil
}
