package httpapi

import (
	"context"
	"encoding/json"
	"errors"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/task"
	"github.com/nostalume/mountweave/transfer"
)

type transferTarget struct {
	DefinitionID string        `json:"definition_id"`
	Revision     uint64        `json:"revision"`
	Path         *string       `json:"path,omitempty"`
	Address      *core.Address `json:"address,omitempty"`
}
type transferInput struct {
	InputID string            `json:"input_id"`
	Target  transferTarget    `json:"target"`
	Replace *bool             `json:"replace,omitempty"`
	Policy  *core.WritePolicy `json:"policy,omitempty"`
}
type transferDetails struct {
	Request           transferInput `json:"request"`
	Revision          uint64        `json:"revision"`
	Effect            string        `json:"effect"`
	CleanupPending    bool          `json:"cleanup_pending"`
	RetentionReleased bool          `json:"retention_released"`
}

func transferView(r transfer.Record) JobView {
	target := transferTarget{DefinitionID: r.Request.Target.DefinitionID, Revision: r.Request.Target.Revision}
	a := r.Request.Target.Address
	if a.Namespace() == core.TreeNamespace {
		p := a.Value()
		target.Path = &p
	} else {
		target.Address = &a
	}
	return JobView{JobRef: JobRef{ID: r.ID, Generation: r.Generation}, State: string(r.State), Owner: r.Owner, Details: transferDetails{
		Request:  transferInput{InputID: r.Request.InputID, Target: target, Policy: &r.Request.Policy},
		Revision: r.Revision, Effect: r.Change.Effect.String(), CleanupPending: r.CleanupPending(), RetentionReleased: r.RetentionReleased,
	}}
}

// TransferJobs exposes metadata and commands for the original transfer owner.
// A Store without bounded listing remains usable directly but cannot register
// complete recovery discovery. The service and its dependencies stay host-owned.
func TransferJobs(s *transfer.Service) (JobSource, error) {
	if s == nil || !s.SupportsListing() {
		return JobSource{}, errors.New("transfer job discovery requires a metadata lister")
	}
	source := JobSource{Kind: "transfer", Version: 2,
		Create: func(ctx context.Context, p core.Principal, id string, raw json.RawMessage) (JobView, error) {
			var i transferInput
			if err := decode(raw, &i); err != nil {
				return JobView{}, err
			}
			if (i.Target.Address == nil) == (i.Target.Path == nil) {
				return JobView{}, core.Fail(core.InvalidRequest, nil)
			}
			if i.Policy != nil && i.Replace != nil {
				return JobView{}, core.Fail(core.InvalidRequest, nil)
			}
			var policy core.WritePolicy
			if i.Policy != nil {
				policy = *i.Policy
			} else if i.Replace != nil {
				policy = core.Publish(core.IfAbsent())
				if *i.Replace {
					policy = core.Publish()
				}
			}
			var address core.Address
			if i.Target.Address != nil {
				address = *i.Target.Address
			} else {
				address = core.Path(*i.Target.Path)
			}
			r, err := s.Create(ctx, p, id, transfer.Request{InputID: i.InputID, Target: transfer.Target{DefinitionID: i.Target.DefinitionID, Revision: i.Target.Revision, Address: address}, Policy: policy})
			return transferView(r), err
		},
		Get: func(ctx context.Context, p core.Principal, id string) (JobView, error) {
			r, err := s.Status(ctx, p, id)
			return transferView(r), err
		},
		List: func(ctx context.Context, p core.Principal, after string, limit int) (JobPage, error) {
			page, err := s.List(ctx, p, after, limit)
			out := JobPage{Items: make([]JobView, len(page.Items)), Next: page.Next}
			for i, r := range page.Items {
				out.Items[i] = transferView(r)
			}
			return out, err
		}, Actions: make(map[string]TaskKind),
	}
	for _, action := range []string{"run", "abort"} {
		source.Actions[action] = TaskKind{
			Prepare: func(_ context.Context, p core.Principal, raw json.RawMessage) (task.Work, error) {
				var i struct {
					ID         string `json:"id"`
					Generation string `json:"generation"`
				}
				if err := decode(raw, &i); err != nil {
					return task.Work{}, err
				}
				if i.ID == "" || i.Generation == "" {
					return task.Work{}, core.Fail(core.InvalidRequest, nil)
				}
				return task.Work{Key: JobKey{Resource: s, ID: i.ID}, Run: func(ctx context.Context, _ func(any)) (any, error) {
					options := transfer.ExecutionOptions{ExpectedGeneration: i.Generation}
					var r transfer.Record
					var err error
					if action == "run" {
						r, err = s.Run(ctx, p, i.ID, options)
					} else {
						r, err = s.Abort(ctx, p, i.ID, options)
					}
					var receipt any
					if r.ID != "" {
						receipt = map[string]any{"state": r.State, "details": transferView(r).Details}
					}
					return JobResult{JobRef: JobRef{ID: i.ID, Generation: i.Generation}, Action: action, Receipt: receipt}, err
				}}, nil
			}, Project: func(s task.Snapshot) (TaskValues, error) { return TaskValues{Result: s.Result}, nil },
		}
	}
	return source, nil
}
