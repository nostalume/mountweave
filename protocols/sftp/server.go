// Package sftp serves a bounded SFTP v3 profile on an authenticated SSH channel.
// The host owns SSH, authentication and Core. ModeView can expose logical modes.
package sftp

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strconv"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/protocols"
	"github.com/nostalume/mountweave/protocols/internal/fileview"
	sshfx "github.com/pkg/sftp/v2/encoding/ssh/filexfer"
)

type Limits struct {
	PacketBytes   uint32
	ReadBytes     uint32
	Handles       int
	PageEntries   int
	PageNameBytes int
}

type Options struct {
	Root      core.Address
	Home      string
	CursorTTL time.Duration
	Limits    Limits
	ModeView  ModeView
	// OnFailure observes failed file requests, including failed replies.
	// It runs synchronously and may be called by concurrent connections.
	OnFailure func(protocols.Failure)
}

type Server struct {
	files   *core.Core
	options Options
	view    fileview.View
}

// New validates configuration without probing or borrowing native resources.
func New(files *core.Core, o Options) (*Server, error) {
	v, err := fileview.New(o.Root, o.Home)
	if err != nil {
		return nil, err
	}
	o.Home = v.Home
	o.Limits.PageEntries, err = fileview.PageEntries(o.Limits.PageEntries)
	if o.Limits.PacketBytes == 0 {
		o.Limits.PacketBytes = 34000
	}
	if o.Limits.ReadBytes == 0 {
		o.Limits.ReadBytes = 32768
	}
	if files == nil || err != nil || o.ModeView > ReadWriteView || o.CursorTTL <= 0 || o.Limits.Handles <= 0 || o.Limits.PageNameBytes <= 0 ||
		uint64(o.Limits.ReadBytes)+13 > uint64(o.Limits.PacketBytes) || uint64(o.Limits.PacketBytes) > uint64(int(^uint(0)>>1)) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	return &Server{files, o, v}, nil
}

type handle struct {
	file       *core.BoundFile
	appendOnly bool
	address    core.Address
	page       []core.Entry
	cursor     string
	failure    error
}

type session struct {
	*Server
	ctx         context.Context
	actor       core.Principal
	conn        io.ReadWriteCloser
	handles     map[string]*handle
	next        uint64
	mu          sync.Mutex
	current     *core.BoundFile
	closeOnce   sync.Once
	closeErr    error
	readBuffer  []byte
	replyBuffer []byte
}

// ServeConn takes ownership of channel, including on early failure. Close must
// be safe concurrently and wake blocked I/O. Calls on a channel execute in wire
// order; independent channels can run concurrently. A File EOF keeps its handle.
func (s *Server) ServeConn(ctx context.Context, actor core.Principal, channel io.ReadWriteCloser) (err error) {
	if channel == nil {
		return core.Fail(core.InvalidRequest, nil)
	}
	life, cancel := context.WithCancel(ctx)
	x := &session{Server: s, ctx: life, actor: actor, conn: channel, handles: make(map[string]*handle)}
	hookDone := make(chan struct{})
	var hookErr error
	stop := context.AfterFunc(life, func() {
		x.closeTransport()
		x.mu.Lock()
		f := x.current
		x.mu.Unlock()
		if f != nil {
			hookErr = f.Close()
		}
		close(hookDone)
	})
	var readDone chan struct{}
	var readErr error
	fatalRequest := false
	defer func() {
		x.closeTransport()
		cancel()
		if readDone != nil {
			<-readDone
			if readErr == io.EOF && !fatalRequest {
				err = nil
			} else if readErr != io.EOF {
				err = errors.Join(err, readErr)
			}
		}
		if !stop() {
			<-hookDone
		}
		for _, h := range x.handles {
			err = errors.Join(err, x.closeHandle(h))
		}
		err = errors.Join(err, hookErr, x.closeErr, ctx.Err())
	}()
	if actor == "" {
		return core.Fail(core.Unauthenticated, nil)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	buf := make([]byte, int(s.options.Limits.PacketBytes)-4)
	var init sshfx.InitPacket
	if err = init.ReadFrom(channel, buf, s.options.Limits.PacketBytes-4); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	if init.Version < 3 {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	hello := sshfx.VersionPacket{Version: 3, Extensions: []*sshfx.ExtensionPair{{Name: fsyncExtension, Data: "1"}}}
	if uint64(hello.MarshalSize()) > uint64(s.options.Limits.PacketBytes) {
		hello.Extensions = nil
	}
	version, _ := hello.MarshalBinary()
	if err = writeAll(channel, version); err != nil {
		return err
	}
	// One executing frame and at most one prefetched frame. The reader can
	// observe EOF during a blocked operation without introducing parallel effects.
	free := make(chan []byte, 2)
	free <- buf
	free <- make([]byte, len(buf))
	frames := make(chan []byte)
	readDone = make(chan struct{})
	go func() {
		defer close(readDone)
		defer close(frames)
		readErr = x.receive(frames, free)
		cancel()
	}()
	for incoming := range frames {
		var req sshfx.RequestPacket
		b := sshfx.NewBuffer(incoming)
		if sshfx.PacketType(incoming[0]) == sshfx.PacketTypeExtended {
			// Keep extension decoding local instead of using the codec's global registry.
			b.ConsumeUint8()
			req.RequestID = b.ConsumeUint32()
			req.Request = &sshfx.ExtendedPacket{ExtendedRequest: b.ConsumeString(), Data: b}
			err = b.Err
		} else {
			err = req.UnmarshalFrom(b)
			if err == nil && b.Len() != 0 {
				err = core.Fail(core.InvalidRequest, nil)
			}
		}
		if err != nil {
			sshfx.PoolReturn(req.Request)
			fatalRequest = true
			return err
		}
		failure := protocols.Failure{Phase: "request"}
		packet, requestErr, fatal := x.dispatch(req.Request, &failure)
		if fatal && requestErr == nil {
			requestErr = core.Fail(core.InvalidUpstreamResponse, nil)
		}
		eof := requestErr == io.EOF && !failure.HasChange && (failure.Phase == "read" || failure.Phase == "list")
		if requestErr != nil {
			failure.Err = requestErr
			if eof {
				packet = &sshfx.StatusPacket{StatusCode: sshfx.StatusEOF, ErrorMessage: sshfx.StatusEOF.String()}
			} else {
				x.report(req, failure)
				packet = status(failure)
			}
		}
		err = x.send(req.RequestID, packet)
		if err != nil && (requestErr == nil || eof) {
			failure.Phase, failure.Err = "reply", err
			x.report(req, failure)
		}
		if err != nil && core.CodeOf(err) == core.ResponseBudgetExceeded {
			fatalRequest = true
		}
		sshfx.PoolReturn(req.Request)
		free <- incoming[:cap(incoming)]
		if fatal {
			fatalRequest = true
			return errors.Join(requestErr, err)
		}
		if err != nil {
			return err
		}
	}
	<-readDone
	if readErr == io.EOF {
		return nil
	}
	return readErr
}

func (x *session) receive(out chan<- []byte, free <-chan []byte) error {
	for {
		var buf []byte
		select {
		case buf = <-free:
		case <-x.ctx.Done():
			return x.ctx.Err()
		}
		var header [4]byte
		if _, err := io.ReadFull(x.conn, header[:]); err != nil {
			return err
		}
		n := binary.BigEndian.Uint32(header[:])
		if n < 5 || n > uint32(len(buf)) {
			return core.Fail(core.InvalidRequest, nil)
		}
		if _, err := io.ReadFull(x.conn, buf[:n]); err != nil {
			return err
		}
		select {
		case out <- buf[:n]:
		case <-x.ctx.Done():
			return x.ctx.Err()
		}
	}
}

func (x *session) closeTransport() { x.closeOnce.Do(func() { x.closeErr = x.conn.Close() }) }

func (x *session) send(id uint32, p sshfx.PacketMarshaller) error {
	if uint64(p.MarshalSize()) > uint64(x.options.Limits.PacketBytes) {
		return core.Fail(core.ResponseBudgetExceeded, nil)
	}
	a, b, err := p.MarshalPacket(id, x.replyBuffer[:0])
	if err != nil {
		return err
	}
	if uint64(len(a))+uint64(len(b)) > uint64(x.options.Limits.PacketBytes) {
		return core.Fail(core.ResponseBudgetExceeded, nil)
	}
	if err = writeAll(x.conn, a); err == nil {
		err = writeAll(x.conn, b)
	}
	x.replyBuffer = a[:0]
	return err
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

func (x *session) closeHandle(h *handle) error {
	var err error
	if h.file != nil {
		err = h.file.Close()
	}
	if h.cursor != "" {
		err = errors.Join(err, x.files.ReleaseCursor(context.WithoutCancel(x.ctx), x.actor, h.cursor))
	}
	return err
}

func (x *session) adopt(h *handle) sshfx.PacketMarshaller {
	x.next++
	id := strconv.FormatUint(x.next, 10)
	x.handles[id] = h
	return &sshfx.HandlePacket{Handle: id}
}

func (x *session) report(req sshfx.RequestPacket, failure protocols.Failure) {
	if observe := x.options.OnFailure; observe != nil {
		failure.Operation = req.Type().String()
		failure.RequestID = req.RequestID
		observe(failure)
	}
}

var okStatus = &sshfx.StatusPacket{StatusCode: sshfx.StatusOK, ErrorMessage: sshfx.StatusOK.String()}

func status(failure protocols.Failure) *sshfx.StatusPacket {
	err := failure.Err
	s := sshfx.StatusFailure
	switch core.CodeOf(err) {
	case core.NotFound:
		s = sshfx.StatusNoSuchFile
	case core.PermissionDenied, core.Unauthenticated, core.UpstreamAccessDenied:
		s = sshfx.StatusPermissionDenied
	case core.UnsupportedOperation, protocols.CreateModeConflict:
		s = sshfx.StatusOpUnsupported
	case core.InvalidRequest, core.InvalidRange:
		s = sshfx.StatusBadMessage
	}
	return &sshfx.StatusPacket{StatusCode: s, ErrorMessage: failure.Message()}
}

func validOffset(offset uint64, length int) bool {
	return offset <= math.MaxInt64 && uint64(length) <= math.MaxInt64-offset
}
