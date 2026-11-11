package sftp

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	sshfx "github.com/pkg/sftp/v2/encoding/ssh/filexfer"
)

type packetStream struct {
	*bytes.Reader
	written bytes.Buffer
	closed  bool
}

func (s *packetStream) Write(p []byte) (int, error) { return s.written.Write(p) }
func (s *packetStream) Close() error                { s.closed = true; return nil }

func replyWire(t *testing.T, typ sshfx.PacketType, data []byte) (*wire, *packetStream) {
	t.Helper()
	frame, err := (&sshfx.RawPacket{PacketType: typ, RequestID: 1, Data: *sshfx.NewBuffer(data)}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	stream := &packetStream{Reader: bytes.NewReader(frame)}
	return &wire{stream: stream, buffer: make([]byte, sshfx.DefaultMaxPacketLength)}, stream
}

func TestWireBoundsAndCloseReceipts(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   sshfx.PacketType
		build func(*sshfx.Buffer)
	}{
		{"names", sshfx.PacketTypeName, func(b *sshfx.Buffer) { b.AppendUint32(^uint32(0)) }},
		{"empty-names", sshfx.PacketTypeName, func(b *sshfx.Buffer) { b.AppendUint32(0) }},
		{"attributes", sshfx.PacketTypeAttrs, func(b *sshfx.Buffer) {
			b.AppendUint32(sshfx.AttrExtended)
			b.AppendUint32(^uint32(0))
		}},
		{"nested-attributes", sshfx.PacketTypeName, func(b *sshfx.Buffer) {
			b.AppendUint32(1)
			b.AppendString("entry")
			b.AppendString("")
			b.AppendUint32(sshfx.AttrExtended)
			b.AppendUint32(^uint32(0))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := sshfx.NewBuffer(nil)
			tc.build(data)
			w, stream := replyWire(t, tc.typ, data.Bytes())
			var err error
			if tc.typ == sshfx.PacketTypeName {
				_, err = w.names(&sshfx.ReadDirPacket{Handle: "directory"})
			} else {
				_, err = w.attrs(&sshfx.StatPacket{Path: "/file"})
			}
			if core.CodeOf(err) != core.InvalidUpstreamResponse || !w.broken || !stream.closed {
				t.Fatalf("malformed count was admitted: broken=%v closed=%v error=%v", w.broken, stream.closed, err)
			}
		})
	}
	t.Run("opaque-empty-handle", func(t *testing.T) {
		status := &sshfx.StatusPacket{StatusCode: sshfx.StatusOK}
		frame, _ := sshfx.ComposePacket(status.MarshalPacket(1, nil))
		stream := &packetStream{Reader: bytes.NewReader(frame)}
		w := &wire{stream: stream, buffer: make([]byte, sshfx.DefaultMaxPacketLength)}
		c := &connection{wire: w}
		native := ""
		handle := &native
		ack, err := closeHandle(c, &handle, time.Second)
		var request sshfx.RawPacket
		readErr := request.ReadFrom(bytes.NewReader(stream.written.Bytes()), nil, sshfx.DefaultMaxPacketLength)
		var closed sshfx.ClosePacket
		if ack != true || err != nil || handle != nil || readErr != nil || decode(&request, &closed) != nil || closed.Handle != "" {
			t.Fatalf("empty handle was not consumed by CLOSE: ack=%v error=%v frame=%v", ack, err, readErr)
		}
	})
	t.Run("eof-is-operation-specific", func(t *testing.T) {
		data := sshfx.NewBuffer(nil)
		data.AppendUint32(uint32(sshfx.StatusEOF))
		data.AppendString("end")
		data.AppendString("")
		w, stream := replyWire(t, sshfx.PacketTypeStatus, data.Bytes())
		if err := w.status(&sshfx.ClosePacket{Handle: "file"}); core.CodeOf(err) != core.InvalidUpstreamResponse || !stream.closed {
			t.Fatalf("CLOSE accepted EOF: %v", err)
		}
		w, stream = replyWire(t, sshfx.PacketTypeStatus, data.Bytes())
		if _, err := w.names(&sshfx.ReadDirPacket{Handle: "directory"}); !errors.Is(err, io.EOF) || stream.closed || w.broken {
			t.Fatalf("READDIR lost normal EOF: %v", err)
		}
	})
}
