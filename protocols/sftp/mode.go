package sftp

import (
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/protocols"
	sshfx "github.com/pkg/sftp/v2/encoding/ssh/filexfer"
)

// ModeView selects the logical mode advertised by this export. The host must
// derive it and its Authorizer from the same policy. In a ReadOnlyView the
// Authorizer must reject mutations for every principal using the export.
type ModeView uint8

const (
	OmitMode ModeView = iota
	ReadOnlyView
	ReadWriteView
)

func (v ModeView) permissions(kind core.Kind) sshfx.FileMode {
	var mode sshfx.FileMode = 0400
	if v == ReadWriteView {
		mode |= 0200
	}
	if kind == core.Directory {
		mode |= 0100
	}
	return mode
}

func (v ModeView) attrs(e core.Entry) sshfx.Attributes {
	var a sshfx.Attributes
	if e.Kind == core.SymbolicLink {
		a.SetPermissions(sshfx.ModeSymlink)
		return a
	}
	if e.Size != nil {
		a.SetSize(uint64(*e.Size))
	}
	if v != OmitMode {
		switch e.Kind {
		case core.File:
			a.SetPermissions(sshfx.ModeRegular | v.permissions(e.Kind))
		case core.Directory:
			a.SetPermissions(sshfx.ModeDir | v.permissions(e.Kind))
		}
	}
	return a
}

// A create request may bound the logical mode. After admission only the
// original creation intent crosses into Core, so native defaults still apply.
func (v ModeView) checkCreate(a sshfx.Attributes, kind core.Kind) error {
	if a.Flags == 0 {
		return nil
	}
	if v == OmitMode || a.Flags != sshfx.AttrPermissions || a.Permissions & ^sshfx.FileMode(0777) != 0 {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	mode := v.permissions(kind)
	if a.Permissions&mode != mode {
		return core.Fail(protocols.CreateModeConflict, nil)
	}
	return nil
}
