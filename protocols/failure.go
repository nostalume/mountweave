// Package protocols defines observations shared by the file protocol adapters.
package protocols

import (
	"context"
	"errors"

	core "github.com/nostalume/mountweave"
)

// CreateModeConflict means the requested logical permissions do not contain
// the export's creation defaults. The request ended before any file change.
const CreateModeConflict core.Code = "create_mode_conflict"

// Failure describes a failed file request or transfer. Err retains private
// causes for the host. A mutation's Change describes the target, and Source
// additionally describes the source of a rename. A read has HasChange false.
type Failure struct {
	Operation string
	Phase     string
	RequestID uint32 // SFTP request ID; zero for FTP.
	Err       error
	Change    core.Change
	HasChange bool
	Source    *core.Change
}

// Effect summarizes the effects of the failed operation. Unknown
// takes precedence over confirmed changes on another side of a rename.
func (f Failure) Effect() (core.Effect, bool) {
	effect := f.Change.Effect
	if f.Source != nil && effect != f.Source.Effect {
		if effect == core.Unknown || f.Source.Effect == core.Unknown {
			effect = core.Unknown
		} else {
			effect = core.Incomplete
		}
	}
	return effect, f.HasChange
}

// Message returns bounded public text. Native error strings, paths and
// credentials are kept in Err and are never copied to a protocol response.
func (f Failure) Message() string {
	message := "Operation failed"
	switch core.CodeOf(f.Err) {
	case core.PermissionDenied:
		message = "Service policy denied the operation"
	case core.UpstreamAccessDenied:
		message = "Storage denied the operation"
	case core.Unauthenticated:
		message = "Authentication required"
	case core.NotFound:
		message = "Target not found"
	case core.AlreadyExists:
		message = "Target already exists"
	case core.NotDirectory:
		message = "Target is not a directory"
	case core.NotFile:
		message = "Target is not a file"
	case core.DirectoryNotEmpty:
		message = "Directory is not empty"
	case core.UnsupportedOperation:
		switch f.Phase {
		case "request":
			message = "Request or attributes not supported"
		case "edit-open":
			message = "Requested in-place open is unsupported"
		case "lstat":
			message = "Safe leaf observation is unsupported"
		default:
			message = "Requested operation is unsupported"
		}
	case core.InvalidRequest, core.InvalidPath, core.InvalidRange:
		message = "Invalid request"
	case core.PreconditionFailed:
		message = "Operation condition failed"
	case CreateModeConflict:
		message = "Create mode conflicts with export defaults (create_mode_conflict)"
	case core.CapacityExceeded, core.ResolutionBudgetExceeded, core.ResponseBudgetExceeded:
		message = "Request exceeds its resource limit"
	case core.CursorUnavailable:
		message = "Directory cursor is unavailable"
	case core.MountUnavailable:
		message = "Mount is unavailable"
	case core.UpstreamTimeout:
		message = "Storage operation timed out"
	case core.UpstreamThrottled:
		message = "Storage throttled the operation"
	case core.PathAmbiguous:
		message = "Target name is ambiguous"
	case core.RangeNotSatisfiable:
		message = "Requested offset is unavailable"
	}
	if f.Phase == "connect" {
		message = "Data connection could not be established"
	} else if f.Phase == "cleanup" {
		message = "Resource cleanup failed"
	} else if f.Phase == "reply" {
		message = "Reply could not be delivered"
	} else if errors.Is(f.Err, context.Canceled) {
		var primary *core.Error
		if !errors.As(f.Err, &primary) {
			message = "Operation canceled"
		}
	}
	if effect, ok := f.Effect(); ok {
		switch effect {
		case core.Unchanged:
			message += ". No changes made"
		case core.Applied:
			message += ". Change applied"
		case core.Incomplete:
			message += ". Partial changes remain"
		default:
			message += ". Effect unknown"
		}
	}
	return message
}
