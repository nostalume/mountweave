package httpapi

import (
	"context"
	"encoding/json"
	"errors"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/task"
)

// OperationRequest is shared by synchronous file operations and queued work.
// Additional task kinds are supplied by the host, outside the scheduler.
type OperationRequest struct {
	Kind  string          `json:"kind"`
	Input json.RawMessage `json:"input"`
}

type copyInput struct {
	Source  string            `json:"source"`
	Target  string            `json:"target"`
	Replace bool              `json:"replace,omitempty"`
	Options *core.CopyOptions `json:"options,omitempty"`
}

func (i copyInput) copyOptions() (core.CopyOptions, error) {
	if i.Options != nil {
		if i.Replace {
			return core.CopyOptions{}, core.Fail(core.InvalidRequest, nil)
		}
		return *i.Options, nil
	}
	return core.CopyOptions{Target: fileWriteOptions(i.Replace)}, nil
}

type pathInput struct {
	Path string `json:"path"`
}
type moveInput struct {
	Source  string           `json:"source"`
	Target  string           `json:"target"`
	Options core.MoveOptions `json:"options,omitzero"`
}

type removeInput struct {
	Path string         `json:"path"`
	When core.Condition `json:"when,omitzero"`
}
type copyBatchInput struct {
	Items       []copyInput `json:"items"`
	Concurrency int         `json:"concurrency,omitempty"`
}
type moveBatchInput struct {
	Items       []moveInput `json:"items"`
	Concurrency int         `json:"concurrency,omitempty"`
}
type removeBatchInput struct {
	Paths       []string `json:"paths"`
	Concurrency int      `json:"concurrency,omitempty"`
}
type treeInput struct {
	Concurrency  int `json:"concurrency,omitempty"`
	MaxEntries   int `json:"max_entries"`
	MaxPlanBytes int `json:"max_plan_bytes"`
}

func (i treeInput) options() core.TreeOptions {
	return core.TreeOptions{ExecutionOptions: core.ExecutionOptions{Concurrency: i.Concurrency}, MaxEntries: i.MaxEntries, MaxPlanBytes: i.MaxPlanBytes}
}

type copyTreeInput struct {
	copyInput
	treeInput
	MergeDirectories bool               `json:"merge_directories,omitempty"`
	Existing         core.TargetPolicy  `json:"existing,omitempty"`
	Depth            core.TreeDepth     `json:"depth,omitempty"`
	SourceSelection  core.ReadSelection `json:"source_selection,omitzero"`
	Policy           *core.WritePolicy  `json:"policy,omitempty"`
}
type removeTreeInput struct {
	pathInput
	treeInput
	Scope        core.RemovalScope `json:"scope,omitempty"`
	ContentsOnly bool              `json:"contents_only,omitempty"`
	When         core.Condition    `json:"when,omitzero"`
}

// FileResult preserves endpoint effects and attempted-prefix receipts on errors.
// Error fields are stable codes; upstream causes never enter the wire value.
type FileResult struct {
	Effect              string              `json:"effect,omitempty"`
	SourceEffect        string              `json:"source_effect,omitempty"`
	TargetEffect        string              `json:"target_effect,omitempty"`
	Items               []FileItem          `json:"items,omitempty"`
	CleanupError        string              `json:"cleanup_error,omitempty"`
	Before              core.TargetPresence `json:"before,omitempty"`
	TargetRemovedEffect string              `json:"target_removed_effect,omitempty"`
}
type FileItem struct {
	Index        int                 `json:"index"`
	RelativePath string              `json:"relative_path,omitempty"`
	Effect       string              `json:"effect,omitempty"`
	SourceEffect string              `json:"source_effect,omitempty"`
	TargetEffect string              `json:"target_effect,omitempty"`
	Error        string              `json:"error,omitempty"`
	Side         core.ResultSide     `json:"side,omitempty"`
	Phase        core.OperationPhase `json:"phase,omitempty"`
	Removed      *FileResult         `json:"removed,omitempty"`
	ErrorSide    core.ResultSide     `json:"error_side,omitempty"`
}

func errorCode(err error) string {
	if err == nil {
		return ""
	}
	var fileError *core.Error
	if errors.As(err, &fileError) {
		_, code := problemStatus(err)
		return string(code)
	}
	switch {
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		_, code := domainStatus(err)
		return code
	}
}
func changeResult(c core.Change) FileResult { return FileResult{Effect: c.Effect.String()} }
func moveResult(r core.MoveResult) FileResult {
	var result FileResult
	if r.Removed != nil {
		result = treeResult(*r.Removed)
		result.TargetRemovedEffect = r.Removed.Change.Effect.String()
	}
	result.Effect = ""
	result.SourceEffect, result.TargetEffect, result.Before = r.Source.Effect.String(), r.Target.Effect.String(), r.Target.Before
	return result
}
func itemResult(items []core.ItemResult) FileResult {
	r := FileResult{Items: make([]FileItem, len(items))}
	for i, item := range items {
		r.Items[i] = FileItem{Index: i, Effect: item.Change.Effect.String(), Error: errorCode(item.Err)}
	}
	return r
}
func treeResult(result core.TreeResult) FileResult {
	r := FileResult{Effect: result.Change.Effect.String(), Before: result.Change.Before, Items: make([]FileItem, len(result.Items))}
	for i, item := range result.Items {
		r.Items[i] = FileItem{Index: i, RelativePath: item.RelativePath, Effect: item.Change.Effect.String(), Error: errorCode(item.Err), Side: item.Side, Phase: item.Phase, ErrorSide: item.ErrorSide}
	}
	return r
}

type fileRun func(context.Context, core.Principal) (FileResult, error)

// FileTaskKinds prepares only logical bindings, never open bodies or uses.
// Both HTTP delivery modes invoke the returned Work.Run without another file API.
func FileTaskKinds(c *core.Core) map[string]TaskKind {
	wrap := func(prepare func(json.RawMessage) ([]core.AddressAccess, fileRun, error)) TaskKind {
		return TaskKind{
			Prepare: func(ctx context.Context, p core.Principal, input json.RawMessage) (task.Work, error) {
				accesses, run, err := prepare(input)
				if err != nil {
					return task.Work{}, err
				}
				var selected *core.Selection
				if len(accesses) > 0 {
					selected, err = c.Capture(ctx, accesses...)
					if err != nil {
						return task.Work{}, err
					}
				}
				return task.Work{Key: selected, Run: func(ctx context.Context, report func(any)) (any, error) {
					if selected != nil {
						ctx = selected.Context(ctx)
					}
					report("running")
					result, err := run(ctx, p)
					return result, err
				}}, nil
			},
			Project: func(s task.Snapshot) (TaskValues, error) {
				return TaskValues{Progress: s.Progress, Result: s.Result}, nil
			},
		}
	}
	paths := func(source, target string, move bool) []core.AddressAccess {
		return []core.AddressAccess{{Address: core.Path(source), Write: move}, {Address: core.Path(target), Write: true}}
	}
	return map[string]TaskKind{
		"fs.copy": wrap(func(raw json.RawMessage) ([]core.AddressAccess, fileRun, error) {
			var i copyInput
			err := decode(raw, &i)
			if err != nil {
				return nil, nil, err
			}
			options, err := i.copyOptions()
			return paths(i.Source, i.Target, false), func(ctx context.Context, p core.Principal) (FileResult, error) {
				r, err := c.Copy(ctx, p, core.Path(i.Source), core.Path(i.Target), options)
				return changeResult(r), err
			}, err
		}),
		"fs.move": wrap(func(raw json.RawMessage) ([]core.AddressAccess, fileRun, error) {
			var i moveInput
			err := decode(raw, &i)
			return paths(i.Source, i.Target, true), func(ctx context.Context, p core.Principal) (FileResult, error) {
				r, err := c.Move(ctx, p, core.Path(i.Source), core.Path(i.Target), i.Options)
				return moveResult(r), err
			}, err
		}),
		"fs.remove": wrap(func(raw json.RawMessage) ([]core.AddressAccess, fileRun, error) {
			var i removeInput
			err := decode(raw, &i)
			return []core.AddressAccess{{Address: core.Path(i.Path), Write: true}}, func(ctx context.Context, p core.Principal) (FileResult, error) {
				r, err := c.Remove(ctx, p, core.Path(i.Path), core.RemoveOptions{When: i.When})
				return changeResult(r), err
			}, err
		}),
		"fs.mkdir": wrap(func(raw json.RawMessage) ([]core.AddressAccess, fileRun, error) {
			var i pathInput
			err := decode(raw, &i)
			return []core.AddressAccess{{Address: core.Path(i.Path), Write: true}}, func(ctx context.Context, p core.Principal) (FileResult, error) {
				r, err := c.Mkdir(ctx, p, core.Path(i.Path))
				return changeResult(r), err
			}, err
		}),
		"fs.copy_batch": wrap(func(raw json.RawMessage) ([]core.AddressAccess, fileRun, error) {
			var i copyBatchInput
			if err := decode(raw, &i); err != nil {
				return nil, nil, err
			}
			if i.Items == nil || i.Concurrency < 0 {
				return nil, nil, core.Fail(core.InvalidRequest, nil)
			}
			var a []core.AddressAccess
			requests := make([]core.CopyRequest, len(i.Items))
			for n, item := range i.Items {
				a = append(a, paths(item.Source, item.Target, false)...)
				options, err := item.copyOptions()
				if err != nil {
					return nil, nil, err
				}
				requests[n] = core.CopyRequest{Source: core.Path(item.Source), Target: core.Path(item.Target), Options: options}
			}
			return a, func(ctx context.Context, p core.Principal) (FileResult, error) {
				r, err := c.CopyBatch(ctx, p, requests, core.ExecutionOptions{Concurrency: i.Concurrency})
				return itemResult(r), err
			}, nil
		}),
		"fs.move_batch": wrap(func(raw json.RawMessage) ([]core.AddressAccess, fileRun, error) {
			var i moveBatchInput
			if err := decode(raw, &i); err != nil {
				return nil, nil, err
			}
			if i.Items == nil || i.Concurrency < 0 {
				return nil, nil, core.Fail(core.InvalidRequest, nil)
			}
			var a []core.AddressAccess
			requests := make([]core.MoveRequest, len(i.Items))
			for n, item := range i.Items {
				a = append(a, paths(item.Source, item.Target, true)...)
				requests[n] = core.MoveRequest{Source: core.Path(item.Source), Target: core.Path(item.Target), Options: item.Options}
			}
			return a, func(ctx context.Context, p core.Principal) (FileResult, error) {
				r, err := c.MoveBatch(ctx, p, requests, core.ExecutionOptions{Concurrency: i.Concurrency})
				out := FileResult{Items: make([]FileItem, len(r))}
				for n, item := range r {
					out.Items[n] = FileItem{Index: n, SourceEffect: item.Source.Effect.String(), TargetEffect: item.Target.Effect.String(), Error: errorCode(item.Err)}
					if item.Removed != nil {
						removed := treeResult(*item.Removed)
						out.Items[n].Removed = &removed
					}
				}
				return out, err
			}, nil
		}),
		"fs.remove_batch": wrap(func(raw json.RawMessage) ([]core.AddressAccess, fileRun, error) {
			var i removeBatchInput
			if err := decode(raw, &i); err != nil {
				return nil, nil, err
			}
			if i.Paths == nil || i.Concurrency < 0 {
				return nil, nil, core.Fail(core.InvalidRequest, nil)
			}
			a := make([]core.AddressAccess, len(i.Paths))
			for n, path := range i.Paths {
				a[n] = core.AddressAccess{Address: core.Path(path), Write: true}
			}
			return a, func(ctx context.Context, p core.Principal) (FileResult, error) {
				targets := make([]core.Address, len(i.Paths))
				for n, path := range i.Paths {
					targets[n] = core.Path(path)
				}
				r, err := c.RemoveBatch(ctx, p, targets, core.ExecutionOptions{Concurrency: i.Concurrency})
				return itemResult(r), err
			}, nil
		}),
		"fs.copy_tree": wrap(func(raw json.RawMessage) ([]core.AddressAccess, fileRun, error) {
			var i copyTreeInput
			if err := decode(raw, &i); err != nil {
				return nil, nil, err
			}
			if i.Options != nil {
				return nil, nil, core.Fail(core.UnsupportedOperation, nil)
			}
			if i.MergeDirectories {
				if i.Existing != "" {
					return nil, nil, core.Fail(core.InvalidRequest, nil)
				}
				i.Existing = core.Merge
			}
			write := fileWriteOptions(i.Replace)
			if i.Policy != nil {
				if i.Replace {
					return nil, nil, core.Fail(core.InvalidRequest, nil)
				}
				write.Policy = *i.Policy
			}
			if i.MaxEntries < 0 || i.MaxPlanBytes < 0 || i.Concurrency < 0 {
				return nil, nil, core.Fail(core.InvalidRequest, nil)
			}
			return paths(i.Source, i.Target, false), func(ctx context.Context, p core.Principal) (FileResult, error) {
				r, err := c.CopyTree(ctx, p, core.Path(i.Source), core.Path(i.Target), i.options(), core.DirectoryCopyOptions{WriteOptions: write, Existing: i.Existing, Depth: i.Depth, Source: i.SourceSelection})
				return treeResult(r), err
			}, nil
		}),
		"fs.remove_tree": wrap(func(raw json.RawMessage) ([]core.AddressAccess, fileRun, error) {
			var i removeTreeInput
			if err := decode(raw, &i); err != nil {
				return nil, nil, err
			}
			if i.MaxEntries < 0 || i.MaxPlanBytes < 0 || i.Concurrency < 0 {
				return nil, nil, core.Fail(core.InvalidRequest, nil)
			}
			if i.Scope != "" && i.Scope != core.Discovered && i.Scope != core.Subtree {
				return nil, nil, core.Fail(core.InvalidRequest, nil)
			}
			return []core.AddressAccess{{Address: core.Path(i.Path), Write: true}}, func(ctx context.Context, p core.Principal) (FileResult, error) {
				r, err := c.RemoveTree(ctx, p, core.Path(i.Path), i.options(), core.DirectoryRemoveOptions{Scope: i.Scope, ContentsOnly: i.ContentsOnly, When: i.When})
				return treeResult(r), err
			}, nil
		}),
	}
}

// FileTaskConflicts can be composed with host policies for other key types.
func FileTaskConflicts(a, b any) bool {
	left, _ := a.(*core.Selection)
	right, _ := b.(*core.Selection)
	return left.Conflicts(right)
}
