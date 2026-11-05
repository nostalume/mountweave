package core

import (
	"errors"
)

type ResultSide string

const (
	SourceSide ResultSide = "source"
	TargetSide ResultSide = "target"
)

type OperationPhase string

const (
	ReadPhase   OperationPhase = "read"
	CreatePhase OperationPhase = "create"
	WritePhase  OperationPhase = "write"
	RemovePhase OperationPhase = "remove"
)

// TreeResult separates whole-operation effects from attempted phase receipts.
// Applied means the selected operation completed, not that every item changed.
type TreeResult struct {
	Change Change
	Items  []TreeItemResult
}

type TreeItemResult struct {
	RelativePath string // "/" is the operation root.
	Side         ResultSide
	Phase        OperationPhase
	// ErrorSide distinguishes a source failure during a target write/cleanup.
	ErrorSide ResultSide
	Change    Change
	Err       error
}

// OperationError preserves failure provenance without changing its domain code
// or its independent effect receipt. Native causes never enter protocol output.
type OperationError struct {
	Side  ResultSide
	Phase OperationPhase
	Cause error
}

func (e *OperationError) Error() string { return e.Cause.Error() }

func (e *OperationError) Unwrap() error { return e.Cause }

// treeEffect never turns a confirmed removal into a successful later creation.
func treeEffect(items []TreeItemResult, complete bool) Effect {
	changed := false
	partial := false
	for _, item := range items {
		switch item.Change.Effect {
		case Unknown:
			return Unknown
		case Incomplete:
			partial = true
		case Applied:
			changed = true
		}
	}
	if partial {
		return Incomplete
	}
	if complete {
		return Applied
	}
	if changed {
		return Incomplete
	}
	return Unchanged
}

func fulfilled(items []TreeItemResult, phase OperationPhase, count int) bool {
	n := 0
	for _, item := range items {
		if item.Phase == ReadPhase {
			continue
		}
		if phase != "" && item.Phase == RemovePhase {
			continue
		}
		if item.Change.Effect != Applied && !(item.Change.Effect == Unchanged && item.Err == nil) {
			return false
		}
		n++
	}
	return n == count
}

func projectFailures(items []TreeItemResult) {
	for i := range items {
		var failure *OperationError
		if errors.As(items[i].Err, &failure) {
			items[i].ErrorSide = failure.Side
		}
	}
}
