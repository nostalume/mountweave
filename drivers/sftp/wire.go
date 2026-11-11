package sftp

import (
	"errors"
	"fmt"
	"io"

	core "github.com/nostalume/mountweave"
	sshfx "github.com/pkg/sftp/v2/encoding/ssh/filexfer"
)

const packetBytes = sshfx.DefaultMaxDataLength

// wire belongs to one leased activity. Only its bounded exchange sends and
// receives concurrently; metadata calls and exchanges must not overlap.
type wire struct {
	stream io.ReadWriteCloser
	next   uint32
	buffer []byte
	broken bool
}

func newWire(stream io.ReadWriteCloser) (*wire, error) {
	w := &wire{stream: stream, buffer: make([]byte, sshfx.DefaultMaxPacketLength)}
	data, _ := (&sshfx.InitPacket{Version: 3}).MarshalBinary()
	if err := writePacket(stream, data); err != nil {
		return nil, err
	}
	var version sshfx.VersionPacket
	if err := version.ReadFrom(stream, w.buffer, sshfx.DefaultMaxPacketLength); err != nil {
		return nil, err
	}
	if version.Version != 3 {
		return nil, core.Fail(core.UnsupportedOperation, fmt.Errorf("SFTP version %d", version.Version))
	}
	return w, nil
}

func writePacket(w io.Writer, data []byte) error {
	for len(data) != 0 {
		n, err := w.Write(data)
		if n < 0 || n > len(data) {
			return core.Fail(core.InvalidUpstreamResponse, errors.New("invalid transport Write count"))
		}
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

func (w *wire) send(id uint32, req sshfx.PacketMarshaller) error {
	header, data, err := req.MarshalPacket(id, nil)
	if err != nil {
		return err
	}
	if len(header)+len(data)-4 > sshfx.DefaultMaxPacketLength {
		return core.Fail(core.ResponseBudgetExceeded, errors.New("SFTP request exceeds packet limit"))
	}
	if err = writePacket(w.stream, header); err == nil {
		err = writePacket(w.stream, data)
	}
	return err
}

func (w *wire) receive() (*sshfx.RawPacket, error) {
	var raw sshfx.RawPacket
	if err := raw.ReadFrom(w.stream, w.buffer, sshfx.DefaultMaxPacketLength); err != nil {
		return nil, err
	}
	return &raw, nil
}

func (w *wire) abort(err error) error {
	w.broken = true
	if core.CodeOf(err) == core.InternalError {
		err = core.Fail(core.UpstreamFailure, err)
	}
	return errors.Join(err, w.stream.Close())
}

func invalidPacket(err error) error {
	return core.Fail(core.InvalidUpstreamResponse, err)
}

func decode(raw *sshfx.RawPacket, packet sshfx.Packet) error {
	if raw.Type() != packet.Type() {
		return invalidPacket(fmt.Errorf("unexpected SFTP packet %s", raw.Type()))
	}
	if err := packet.UnmarshalPacketBody(&raw.Data); err != nil {
		return invalidPacket(err)
	}
	if raw.Data.Len() != 0 {
		return invalidPacket(errors.New("trailing SFTP packet data"))
	}
	return nil
}

func nativeStatus(raw *sshfx.RawPacket, allowEOF bool) error {
	var status sshfx.StatusPacket
	if err := decode(raw, &status); err != nil {
		return err
	}
	code := core.UpstreamFailure
	switch status.StatusCode {
	case sshfx.StatusOK:
		return nil
	case sshfx.StatusEOF:
		if allowEOF {
			return io.EOF
		}
		return invalidPacket(&status)
	case sshfx.StatusNoSuchFile:
		code = core.NotFound
	case sshfx.StatusPermissionDenied:
		code = core.UpstreamAccessDenied
	case sshfx.StatusOpUnsupported:
		code = core.UnsupportedOperation
	}
	return core.Fail(code, &status)
}

// call's RawPacket borrows the receive buffer until the next call.
func (w *wire) call(req sshfx.PacketMarshaller) (*sshfx.RawPacket, error) {
	w.next++
	if err := w.send(w.next, req); err != nil {
		return nil, w.abort(err)
	}
	raw, err := w.receive()
	if err != nil {
		return nil, w.abort(err)
	}
	if raw.RequestID != w.next {
		return nil, w.abort(invalidPacket(errors.New("unexpected SFTP response ID")))
	}
	if raw.Type() == sshfx.PacketTypeStatus {
		// Keep an untouched packet for a successful status caller.
		copy := *raw
		packet, ok := req.(sshfx.Packet)
		allowEOF := ok && (packet.Type() == sshfx.PacketTypeRead || packet.Type() == sshfx.PacketTypeReadDir)
		if err := nativeStatus(&copy, allowEOF); err != nil {
			if core.CodeOf(err) == core.InvalidUpstreamResponse {
				err = w.abort(err)
			}
			return nil, err
		}
	}
	return raw, nil
}

func (w *wire) status(req sshfx.PacketMarshaller) error {
	raw, err := w.call(req)
	if err != nil {
		return err
	}
	if raw.Type() != sshfx.PacketTypeStatus {
		return w.abort(invalidPacket(errors.New("expected SFTP STATUS")))
	}
	// call already decoded and checked the status without consuming this copy.
	return nil
}

func (w *wire) handle(req sshfx.PacketMarshaller) (string, error) {
	raw, err := w.call(req)
	var packet sshfx.HandlePacket
	if err == nil {
		if err = decode(raw, &packet); err != nil {
			err = w.abort(err)
		}
	}
	return packet.Handle, err
}

func (w *wire) attrs(req sshfx.PacketMarshaller) (sshfx.Attributes, error) {
	raw, err := w.call(req)
	if err != nil {
		return sshfx.Attributes{}, err
	}
	if raw.Type() != sshfx.PacketTypeAttrs {
		return sshfx.Attributes{}, w.abort(invalidPacket(errors.New("expected SFTP ATTRS")))
	}
	attrs, err := consumeAttrs(&raw.Data)
	if err != nil || raw.Data.Len() != 0 {
		return attrs, w.abort(invalidPacket(errors.Join(err, raw.Data.Err, errors.New("invalid SFTP attributes"))))
	}
	return attrs, nil
}

func (w *wire) names(req sshfx.PacketMarshaller) ([]*sshfx.NameEntry, error) {
	raw, err := w.call(req)
	if err != nil {
		return nil, err
	}
	if raw.Type() != sshfx.PacketTypeName {
		return nil, w.abort(invalidPacket(errors.New("expected SFTP NAME")))
	}
	count := raw.Data.ConsumeUint32()
	// Every name needs two string lengths and an attribute flags word. Bound
	// allocation by actual frame bytes, not by the peer's untrusted count.
	if raw.Data.Err != nil || count == 0 || uint64(count) > uint64(raw.Data.Len()/12) {
		return nil, w.abort(invalidPacket(errors.New("invalid SFTP name count")))
	}
	entries := make([]*sshfx.NameEntry, 0, int(count))
	for range count {
		entry := &sshfx.NameEntry{Filename: raw.Data.ConsumeString(), Longname: raw.Data.ConsumeString()}
		entry.Attrs, err = consumeAttrs(&raw.Data)
		if err != nil {
			return nil, w.abort(invalidPacket(err))
		}
		entries = append(entries, entry)
	}
	if raw.Data.Len() != 0 {
		return nil, w.abort(invalidPacket(errors.New("trailing SFTP names")))
	}
	return entries, nil
}

// consumeAttrs keeps native flags while bounding the codec's variable-length
// extension allocation. It decodes each value once from the existing Buffer.
func consumeAttrs(buf *sshfx.Buffer) (a sshfx.Attributes, err error) {
	a.Flags = buf.ConsumeUint32()
	if a.Flags & ^uint32(sshfx.AttrSize|sshfx.AttrUIDGID|sshfx.AttrPermissions|sshfx.AttrACModTime|sshfx.AttrExtended) != 0 {
		return a, errors.New("unknown SFTP attribute flags")
	}
	if a.HasSize() {
		a.Size = buf.ConsumeUint64()
	}
	if a.HasUserGroup() {
		a.UID, a.GID = buf.ConsumeUint32(), buf.ConsumeUint32()
	}
	if a.HasPermissions() {
		a.Permissions = sshfx.FileMode(buf.ConsumeUint32())
	}
	if a.HasACModTime() {
		a.ATime, a.MTime = buf.ConsumeUint32(), buf.ConsumeUint32()
	}
	if a.HasExtended() {
		count := buf.ConsumeUint32()
		if buf.Err != nil || uint64(count) > uint64(buf.Len()/8) {
			return a, errors.New("invalid SFTP extended attribute count")
		}
		a.Extended = make(sshfx.ExtendedAttributes, int(count))
		for i := range a.Extended {
			a.Extended[i] = sshfx.ExtendedAttribute{Type: buf.ConsumeString(), Data: buf.ConsumeString()}
		}
	}
	return a, buf.Err
}

// exchange uses one sender and the calling receiver. Request data remains
// borrowed until the sender has joined, including after a transport failure.
func (w *wire) exchange(reqs []sshfx.PacketMarshaller, consume func(int, *sshfx.RawPacket) error) error {
	first := w.next + 1
	w.next += uint32(len(reqs))
	seen := make([]bool, len(reqs))
	sent := make(chan error, 1)
	go func() {
		for i, req := range reqs {
			if err := w.send(first+uint32(i), req); err != nil {
				sent <- errors.Join(err, w.stream.Close())
				return
			}
		}
		sent <- nil
	}()
	var result error
	for range reqs {
		raw, err := w.receive()
		if err != nil {
			result = w.abort(errors.Join(result, err))
			break
		}
		i := uint32(raw.RequestID - first)
		if uint64(i) >= uint64(len(reqs)) || seen[i] {
			result = w.abort(errors.Join(result, invalidPacket(errors.New("unknown or repeated SFTP response ID"))))
			break
		}
		seen[i] = true
		if err = consume(int(i), raw); err != nil {
			var status *sshfx.StatusPacket
			if !errors.As(err, &status) && !errors.Is(err, io.EOF) {
				result = w.abort(errors.Join(result, err))
				break
			}
			result = errors.Join(result, err)
		}
	}
	if err := <-sent; err != nil {
		w.broken = true
		result = errors.Join(result, err)
	}
	return result
}

func (w *wire) readAt(handle string, p []byte, offset int64, window int) (total int, err error) {
	if offset < 0 || window < 1 {
		return 0, core.Fail(core.InvalidRequest, nil)
	}
	for total < len(p) {
		end := total + min(len(p)-total, window*packetBytes)
		buf := p[total:end]
		type part struct {
			start, size, got int
			eof              bool
		}
		parts := make([]part, (len(buf)+packetBytes-1)/packetBytes)
		for i := range parts {
			parts[i] = part{start: i * packetBytes, size: min(packetBytes, len(buf)-i*packetBytes)}
		}
		last := len(parts)
		for {
			var reqs []sshfx.PacketMarshaller
			var indexes []int
			for i := 0; i < last; i++ {
				s := &parts[i]
				if s.got < s.size && !s.eof {
					reqs = append(reqs, &sshfx.ReadPacket{Handle: handle, Offset: uint64(offset) + uint64(total+s.start+s.got), Length: uint32(s.size - s.got)})
					indexes = append(indexes, i)
				}
			}
			if len(reqs) == 0 {
				break
			}
			err = w.exchange(reqs, func(index int, raw *sshfx.RawPacket) error {
				i := indexes[index]
				s := &parts[i]
				if raw.Type() == sshfx.PacketTypeStatus {
					failure := nativeStatus(raw, true)
					if failure == io.EOF {
						s.eof, last = true, min(last, i+1)
						return nil
					}
					if failure == nil {
						failure = invalidPacket(errors.New("READ returned OK without data"))
					}
					return failure
				}
				data := raw.Data.ConsumeBytes()
				if raw.Type() != sshfx.PacketTypeData || raw.Data.Err != nil || raw.Data.Len() != 0 || len(data) == 0 || len(data) > s.size-s.got {
					return invalidPacket(errors.New("invalid SFTP DATA length or type"))
				}
				copy(buf[s.start+s.got:], data)
				s.got += len(data)
				return nil
			})
			if err != nil {
				break
			}
		}
		n := 0
		for _, s := range parts {
			n += s.got
			if s.got != s.size {
				break
			}
		}
		total += n
		if err != nil {
			return total, err
		}
		if n < len(buf) {
			return total, io.EOF
		}
	}
	return total, nil
}

func (w *wire) writeAt(handle string, p []byte, offset int64, window int) (total int, err error) {
	if offset < 0 || window < 1 {
		return 0, core.Fail(core.InvalidRequest, nil)
	}
	chunk := min(packetBytes, sshfx.DefaultMaxPacketLength-21-len(handle))
	if chunk < 1 {
		return 0, core.Fail(core.ResponseBudgetExceeded, nil)
	}
	for total < len(p) {
		end := total + min(len(p)-total, window*chunk)
		buf := p[total:end]
		count := (len(buf) + chunk - 1) / chunk
		reqs := make([]sshfx.PacketMarshaller, count)
		acked := make([]bool, count)
		for i := range reqs {
			start := i * chunk
			reqs[i] = &sshfx.WritePacket{Handle: handle, Offset: uint64(offset) + uint64(total+start), Data: buf[start:min(start+chunk, len(buf))]}
		}
		err = w.exchange(reqs, func(i int, raw *sshfx.RawPacket) error {
			failure := nativeStatus(raw, false)
			acked[i] = failure == nil
			return failure
		})
		for i, ok := range acked {
			if !ok {
				break
			}
			total += min(chunk, len(buf)-i*chunk)
		}
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
