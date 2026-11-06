package sftp

import (
	"context"
	"errors"
	"io"
	"math"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/protocols"
	"github.com/nostalume/mountweave/protocols/internal/fileview"
	sshfx "github.com/pkg/sftp/v2/encoding/ssh/filexfer"
)

const fsyncExtension = "fsync@openssh.com"

const (
	readFlag uint32 = 1 << iota
	writeFlag
	appendFlag
	createFlag
	truncateFlag
	exclusiveFlag
)

func (x *session) dispatch(p sshfx.Packet, failure *protocols.Failure) (reply sshfx.PacketMarshaller, err error, fatal bool) {
	defer func() { x.mu.Lock(); x.current = nil; x.mu.Unlock() }()
	unsupported := core.Fail(core.UnsupportedOperation, nil)
	switch p := p.(type) {
	case *sshfx.OpenPacket:
		return x.open(p, failure)
	case *sshfx.OpenDirPacket:
		if len(x.handles) >= x.options.Limits.Handles {
			return nil, core.Fail(core.CapacityExceeded, nil), false
		}
		a, _, err := x.view.Resolve(x.options.Home, p.Path)
		if err != nil {
			return nil, err, false
		}
		h := &handle{address: a}
		failure.Phase = "list"
		if err, fatal := x.page(h); err != nil {
			return nil, err, fatal
		}
		return x.adopt(h), nil, false
	case *sshfx.ClosePacket:
		h := x.handles[p.Handle]
		if h == nil {
			return nil, core.Fail(core.InvalidRequest, nil), false
		}
		failure.Phase = "cleanup"
		err = x.closeHandle(h)
		if err == nil {
			delete(x.handles, p.Handle)
		}
		return okStatus, err, err != nil
	case *sshfx.ReadPacket:
		f := x.file(p.Handle)
		if f == nil || p.Length > x.options.Limits.ReadBytes || !validOffset(p.Offset, int(p.Length)) {
			return nil, core.Fail(core.InvalidRequest, nil), false
		}
		if cap(x.readBuffer) < int(p.Length) {
			x.readBuffer = make([]byte, int(p.Length))
		}
		data := x.readBuffer[:int(p.Length)]
		failure.Phase = "read"
		n, err := f.ReadAt(x.ctx, data, int64(p.Offset))
		if n > 0 && (err == nil || err == io.EOF) {
			return &sshfx.DataPacket{Data: data[:n]}, nil, false
		}
		if err == nil {
			return &sshfx.DataPacket{}, nil, false
		}
		return nil, err, false
	case *sshfx.WritePacket:
		failure.HasChange, failure.Change.Effect = true, core.Unchanged
		f := x.file(p.Handle)
		if f == nil || !validOffset(p.Offset, len(p.Data)) {
			return nil, core.Fail(core.InvalidRequest, nil), false
		}
		var n int
		var change core.Change
		failure.Phase = "write"
		if x.handles[p.Handle].appendOnly {
			n, change, err = f.Append(x.ctx, p.Data)
		} else {
			n, change, err = f.WriteAt(x.ctx, p.Data, int64(p.Offset))
		}
		if err == nil && n != len(p.Data) {
			err = io.ErrShortWrite
		}
		failure.Change = change
		return okStatus, err, change.Effect == core.Unknown
	case *sshfx.FStatPacket:
		f := x.file(p.Handle)
		if f == nil {
			return nil, unsupported, false
		}
		failure.Phase = "stat"
		e, err := f.Stat(x.ctx)
		return &sshfx.AttrsPacket{Attrs: x.options.ModeView.attrs(e)}, err, false
	case *sshfx.FSetStatPacket:
		failure.HasChange, failure.Change.Effect = true, core.Unchanged
		if p.Attrs.Flags != sshfx.AttrSize || p.Attrs.Size > math.MaxInt64 {
			return nil, unsupported, false
		}
		f := x.file(p.Handle)
		if f == nil {
			return nil, core.Fail(core.InvalidRequest, nil), false
		}
		failure.Phase = "truncate"
		change, err := f.Truncate(x.ctx, int64(p.Attrs.Size))
		failure.Change = change
		return okStatus, err, change.Effect == core.Unknown
	case *sshfx.ExtendedPacket:
		if p.ExtendedRequest != fsyncExtension {
			return nil, unsupported, false
		}
		failure.Phase = "sync"
		payload := p.Data.(*sshfx.Buffer)
		id := payload.ConsumeString()
		if payload.Err != nil || payload.Len() != 0 {
			return nil, core.Fail(core.InvalidRequest, nil), false
		}
		f := x.file(id)
		if f == nil {
			return nil, core.Fail(core.InvalidRequest, nil), false
		}
		return okStatus, f.Sync(x.ctx), false
	case *sshfx.ReadDirPacket:
		h := x.handles[p.Handle]
		if h == nil || h.file != nil {
			return nil, core.Fail(core.InvalidRequest, nil), false
		}
		failure.Phase = "list"
		if h.failure != nil {
			return nil, h.failure, false
		}
		if len(h.page) == 0 && h.cursor != "" {
			if err, fatal := x.page(h); err != nil {
				return nil, err, fatal
			}
		}
		if len(h.page) == 0 {
			return nil, io.EOF, false
		}
		r := &sshfx.NamePacket{}
		size := 13
		for len(h.page) > 0 {
			e := h.page[0]
			item := &sshfx.NameEntry{Filename: e.Name, Attrs: x.options.ModeView.attrs(e)}
			entrySize := item.MarshalSize()
			if uint64(size)+uint64(entrySize) > uint64(x.options.Limits.PacketBytes) {
				if len(r.Entries) == 0 {
					return nil, core.Fail(core.ResponseBudgetExceeded, nil), false
				}
				break
			}
			size += entrySize
			r.Entries = append(r.Entries, item)
			h.page[0] = core.Entry{}
			h.page = h.page[1:]
		}
		return r, nil, false
	case *sshfx.RealPathPacket:
		_, name, err := x.view.Resolve(x.options.Home, p.Path)
		return &sshfx.PathPseudoPacket{Path: name}, err, false
	case *sshfx.StatPacket:
		a, _, err := x.view.Resolve(x.options.Home, p.Path)
		if err != nil {
			return nil, err, false
		}
		failure.Phase = "stat"
		e, err := x.files.Stat(x.ctx, x.actor, a)
		return &sshfx.AttrsPacket{Attrs: x.options.ModeView.attrs(e)}, err, false
	case *sshfx.LStatPacket:
		a, visible, err := x.view.Resolve(x.options.Home, p.Path)
		if err != nil {
			return nil, err, false
		}
		failure.Phase = "lstat"
		e, err := x.files.Lstat(x.ctx, x.actor, a)
		if err == nil && visible == "/" && e.Kind != core.Directory {
			err = core.Fail(core.NotDirectory, nil)
		}
		return &sshfx.AttrsPacket{Attrs: x.options.ModeView.attrs(e)}, err, false
	case *sshfx.MkdirPacket:
		failure.HasChange, failure.Change.Effect = true, core.Unchanged
		if err := x.options.ModeView.checkCreate(p.Attrs, core.Directory); err != nil {
			return nil, err, false
		}
		a, _, err := x.view.Resolve(x.options.Home, p.Path)
		if err != nil {
			return nil, err, false
		}
		failure.Phase = "mkdir"
		change, err := x.files.Mkdir(x.ctx, x.actor, a)
		failure.Change = change
		return okStatus, err, change.Effect == core.Unknown
	case *sshfx.RenamePacket:
		failure.HasChange, failure.Change.Effect = true, core.Unchanged
		a, err := x.view.ResolveChange(x.options.Home, p.OldPath)
		if err != nil {
			return nil, err, false
		}
		b, err := x.view.ResolveChange(x.options.Home, p.NewPath)
		if err != nil {
			return nil, err, false
		}
		failure.Phase = "rename"
		result, err := x.files.Move(x.ctx, x.actor, a, b)
		failure.Change, failure.Source = result.Target, &result.Source
		return okStatus, err, result.Source.Effect == core.Unknown || result.Target.Effect == core.Unknown
	case *sshfx.RemovePacket:
		return x.remove(p.Path, core.RemoveNonDirectory, failure)
	case *sshfx.RmdirPacket:
		return x.remove(p.Path, core.RemoveEmptyDirectory, failure)
	default:
		return nil, unsupported, false
	}
}

func (x *session) remove(name string, target core.RemoveTarget, failure *protocols.Failure) (sshfx.PacketMarshaller, error, bool) {
	failure.HasChange, failure.Change.Effect = true, core.Unchanged
	address, err := x.view.ResolveChange(x.options.Home, name)
	if err != nil {
		return nil, err, false
	}
	failure.Phase = "remove"
	failure.Change, err = x.files.Remove(x.ctx, x.actor, address, core.RemoveOptions{Target: target})
	return okStatus, err, failure.Change.Effect == core.Unknown
}

func (x *session) file(id string) *core.BoundFile {
	h := x.handles[id]
	if h == nil {
		return nil
	}
	x.mu.Lock()
	x.current = h.file
	x.mu.Unlock()
	return h.file
}

func (x *session) open(p *sshfx.OpenPacket, failure *protocols.Failure) (sshfx.PacketMarshaller, error, bool) {
	f := p.PFlags
	failure.HasChange, failure.Change.Effect = f&writeFlag != 0, core.Unchanged
	if (p.Attrs.Flags != 0 && f&createFlag == 0) || f & ^uint32(63) != 0 || f&(readFlag|writeFlag) == 0 ||
		f&(appendFlag|createFlag|truncateFlag) != 0 && f&writeFlag == 0 || f&exclusiveFlag != 0 && f&createFlag == 0 {
		return nil, core.Fail(core.UnsupportedOperation, nil), false
	}
	if err := x.options.ModeView.checkCreate(p.Attrs, core.File); err != nil {
		return nil, err, false
	}
	if len(x.handles) >= x.options.Limits.Handles {
		return nil, core.Fail(core.CapacityExceeded, nil), false
	}
	a, _, err := x.view.Resolve(x.options.Home, p.Filename)
	if err != nil {
		return nil, err, false
	}
	o := core.OpenOptions{Mode: core.BoundRead, NoFollow: true}
	failure.Phase = "open"
	if f&writeFlag != 0 {
		failure.Phase = "edit-open"
		o = core.OpenOptions{Mode: core.LiveEdit, NoFollow: true, Read: f&readFlag != 0, Truncate: f&truncateFlag != 0, AppendOnly: f&appendFlag != 0}
		if f&createFlag != 0 {
			o.Creation = core.OpenOrCreate
		}
		if f&exclusiveFlag != 0 {
			o.Creation = core.OpenNew
		}
	}
	content, err := x.files.Open(x.ctx, x.actor, a, o)
	if failure.HasChange {
		failure.Change = content.Change
	}
	if err != nil {
		return nil, err, o.Mode == core.LiveEdit && content.Change.Effect == core.Unknown
	}
	return x.adopt(&handle{file: content.File, appendOnly: o.AppendOnly}), nil, o.Mode == core.LiveEdit && content.Change.Effect == core.Unknown
}

func (x *session) page(h *handle) (err error, fatal bool) {
	defer func() {
		if err != nil {
			h.failure = err
		}
	}()
	p, err := x.files.List(x.ctx, x.actor, core.ListRequest{Query: core.Children(h.address), Cursor: h.cursor, Limit: x.options.Limits.PageEntries, CursorTTL: x.options.CursorTTL})
	if err != nil {
		return err, false
	}
	if p.NextCursor != nil {
		h.cursor = *p.NextCursor
	} else if h.cursor != "" {
		// The final page remains replayable through its input token.
		cleanup := x.files.ReleaseCursor(context.WithoutCancel(x.ctx), x.actor, h.cursor)
		h.cursor = ""
		if cleanup != nil && core.CodeOf(cleanup) != core.CursorUnavailable {
			return cleanup, true
		}
	}
	remaining := x.options.Limits.PageNameBytes
	for _, e := range p.Items {
		n := fileview.NameBytes(e)
		if n > remaining {
			cleanup := x.closeHandle(h)
			h.cursor = ""
			return errors.Join(core.Fail(core.ResponseBudgetExceeded, nil), cleanup), cleanup != nil
		}
		remaining -= n
	}
	h.page = p.Items
	return nil, false
}
