package smb

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	"github.com/hirochachacha/go-smb2/v2/x/protocol"
	"github.com/hirochachacha/go-smb2/v2/x/wire"
	core "github.com/nostalume/mountweave"
)

// These NTSTATUS values are the native operation's outcomes, not inferred
// classifications from path probes or error-message text.
const (
	statusNoMoreFiles  = 0x80000006
	statusSymlink      = 0x8000002d
	statusNoSuchFile   = 0xc000000f
	statusEOF          = 0xc0000011
	statusDenied       = 0xc0000022
	statusNameInvalid  = 0xc0000033
	statusNameMissing  = 0xc0000034
	statusCollision    = 0xc0000035
	statusPathInvalid  = 0xc0000039
	statusPathMissing  = 0xc000003a
	statusNotSupported = 0xc00000bb
	statusIsDirectory  = 0xc00000ba
	statusNotEmpty     = 0xc0000101
	statusNotDirectory = 0xc0000103
	statusDFS          = 0xc0000257
)

func nativeCode(err error) uint32 {
	var reply *protocol.ResponseError
	if errors.As(err, &reply) {
		return reply.Code
	}
	return 0
}

func nativeError(err error) error {
	if err == nil || core.CodeOf(err) != core.InternalError {
		return err
	}
	code := core.UpstreamFailure
	var invalid *protocol.InvalidResponseError
	if errors.As(err, &invalid) {
		code = core.InvalidUpstreamResponse
	} else {
		switch nativeCode(err) {
		case statusNoSuchFile, statusNameMissing, statusPathMissing:
			code = core.NotFound
		case statusCollision:
			code = core.AlreadyExists
		case statusDenied:
			code = core.UpstreamAccessDenied
		case statusNotDirectory, statusPathInvalid:
			code = core.NotDirectory
		case statusIsDirectory:
			code = core.NotFile
		case statusNotEmpty:
			code = core.DirectoryNotEmpty
		case statusSymlink, statusDFS, statusNotSupported:
			code = core.UnsupportedOperation
		case statusNameInvalid:
			code = core.UnrepresentableName
		}
	}
	return core.Fail(code, err)
}

// A known refusal is distinct from a missing response to a mutating CREATE.
func createRefused(err error) bool {
	switch nativeCode(err) {
	case statusCollision, statusDenied, statusNameInvalid, statusNameMissing,
		statusPathInvalid, statusPathMissing, statusNotDirectory, statusIsDirectory,
		statusSymlink, statusDFS, statusNotSupported:
		return true
	}
	return false
}

func metadata(name string, attrs uint32, size int64, modified wire.FiletimeDecoder) core.Metadata {
	m := core.Metadata{Name: name, Kind: core.File}
	if attrs&wire.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		m.Kind = core.SymbolicLink
	} else if attrs&wire.FILE_ATTRIBUTE_DIRECTORY != 0 {
		m.Kind = core.Directory
	} else {
		m.Size = &size
	}
	if modified.LowDateTime() != 0 || modified.HighDateTime() != 0 {
		value := modified.Time()
		m.ModifiedAt = &value
	}
	return m
}

// handle owns one native open. Its FileId never becomes a path-reopening
// adapter or a durable object version. The activity serializes its operations.
type handle struct {
	conn  *connection
	fd    wire.FileId
	owned atomic.Bool
}

func (c *connection) open(ctx context.Context, name string, directory, exclusive bool) (*handle, core.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, core.Metadata{}, err
	}
	access := uint32(wire.FILE_READ_ATTRIBUTES | wire.FILE_READ_DATA)
	options := uint32(wire.FILE_OPEN_REPARSE_POINT | wire.FILE_NON_DIRECTORY_FILE)
	if directory {
		options = wire.FILE_DIRECTORY_FILE | wire.FILE_OPEN_REPARSE_POINT
	}
	disposition := uint32(wire.FILE_OPEN)
	if exclusive {
		disposition = wire.FILE_CREATE
		if !directory {
			access = wire.FILE_READ_ATTRIBUTES | wire.FILE_WRITE_DATA
		}
	}
	req := c.share.Request().WithFollowSymlinks(false).Create(name, access, disposition, options, wire.FILE_ATTRIBUTE_NORMAL)
	// Ordinary reads need not prevent another native client from renaming the
	// opened object. FileId, rather than the pathname, continues the operation.
	req.Get(0).(*wire.CreateRequest).ShareAccess |= wire.FILE_SHARE_DELETE
	response, err := req.Do(context.WithoutCancel(ctx))
	if response != nil {
		defer response.Close()
	}
	if err != nil {
		err = nativeError(err)
		if nativeCode(err) == 0 {
			err = errors.Join(err, c.Close())
		}
		return nil, core.Metadata{}, err
	}
	reply, err := response.Create(0)
	if err != nil {
		return nil, core.Metadata{}, errors.Join(core.Fail(core.InvalidUpstreamResponse, err), c.Close())
	}
	h := &handle{conn: c, fd: reply.FileId().Decode()}
	h.owned.Store(true)
	m := metadata("", reply.FileAttributes(), reply.EndofFile(), reply.LastWriteTime())
	wantedAction := uint32(wire.FILE_OPENED)
	if exclusive {
		wantedAction = wire.FILE_CREATED
	}
	if reply.CreateAction() != wantedAction {
		return h, m, core.Fail(core.InvalidUpstreamResponse, errors.New("SMB CREATE returned an unexpected action"))
	}
	if m.Kind == core.SymbolicLink {
		return h, m, core.Fail(core.UnsupportedOperation, errors.New("SMB open returned a reparse point"))
	}
	if directory && m.Kind != core.Directory {
		return h, m, core.Fail(core.NotDirectory, nil)
	}
	if !directory && m.Kind != core.File {
		return h, m, core.Fail(core.NotFile, nil)
	}
	return h, m, nil
}

func (h *handle) readAt(ctx context.Context, target []byte, offset int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, core.Fail(core.InvalidRange, nil)
	}
	if len(target) == 0 {
		return 0, nil
	}
	request := &protocol.DirectReadRequest{ReadRequest: &wire.ReadRequest{
		Length: uint32(len(target)), Offset: uint64(offset), FileId: h.fd,
	}, Buffer: target}
	response, err := h.conn.share.Request().Append(request).Do(context.WithoutCancel(ctx))
	if response != nil {
		defer response.Close()
	}
	if err != nil {
		if nativeCode(err) == statusEOF {
			return 0, io.EOF
		}
		return 0, nativeError(err)
	}
	data := response.DirectData(0)
	if data == nil {
		reply, failure := response.Read(0)
		if failure != nil {
			return 0, errors.Join(core.Fail(core.InvalidUpstreamResponse, failure), h.conn.Close())
		}
		data = reply.Data()
	}
	if len(data) > len(target) {
		return 0, errors.Join(core.Fail(core.InvalidUpstreamResponse, errors.New("SMB READ exceeds requested length")), h.conn.Close())
	}
	if len(data) == 0 {
		return 0, errors.Join(core.Fail(core.InvalidUpstreamResponse, io.ErrNoProgress), h.conn.Close())
	}
	return copy(target, data), nil
}

func (h *handle) writeAt(ctx context.Context, data []byte, offset int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, core.Fail(core.InvalidRange, nil)
	}
	if len(data) == 0 {
		return 0, nil
	}
	response, err := h.conn.share.Request().WithFileID(h.fd).Write(data, uint64(offset)).Do(context.WithoutCancel(ctx))
	if response != nil {
		defer response.Close()
	}
	if err != nil {
		return 0, nativeError(err)
	}
	reply, err := response.Write(0)
	if err != nil {
		return 0, errors.Join(core.Fail(core.InvalidUpstreamResponse, err), h.conn.Close())
	}
	if uint64(reply.Count()) > uint64(len(data)) {
		return 0, errors.Join(core.Fail(core.InvalidUpstreamResponse, errors.New("SMB WRITE exceeds requested length")), h.conn.Close())
	}
	return int(reply.Count()), nil
}

func (h *handle) page(ctx context.Context, pattern string, maxBytes uint32) ([]core.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	response, err := h.conn.share.Request().WithFileID(h.fd).QueryDir(wire.FileIdBothDirectoryInformation, pattern, maxBytes).Do(context.WithoutCancel(ctx))
	if response != nil {
		defer response.Close()
	}
	if err != nil {
		if code := nativeCode(err); code == statusNoMoreFiles || code == statusNoSuchFile {
			return nil, io.EOF
		}
		return nil, nativeError(err)
	}
	query, err := response.QueryDir(0)
	if err != nil {
		return nil, errors.Join(core.Fail(core.InvalidUpstreamResponse, err), h.conn.Close())
	}
	if uint64(len(query.Output())) > uint64(maxBytes) {
		return nil, errors.Join(core.Fail(core.ResponseBudgetExceeded, errors.New("SMB directory response exceeds requested budget")), h.conn.Close())
	}
	entries, err := query.FileIdBothDirectoryInformation()
	if err != nil {
		return nil, errors.Join(core.Fail(core.InvalidUpstreamResponse, err), h.conn.Close())
	}
	items := make([]core.Metadata, 0, len(entries))
	for _, entry := range entries {
		name := entry.FileName()
		if name == "." || name == ".." {
			continue
		}
		items = append(items, metadata(name, entry.FileAttributes(), entry.EndOfFile(), entry.LastWriteTime()))
	}
	return items, nil
}

// close consumes the open once. A retired session is local cleanup, not a
// CLOSE acknowledgment; missing replies must never trigger a repeated CLOSE.
func (h *handle) close(ctx context.Context) (bool, error) {
	if !h.owned.CompareAndSwap(true, false) {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, errors.Join(err, h.conn.Close())
	}
	if h.conn.closed.Load() {
		return false, h.conn.Close()
	}
	response, err := h.conn.share.Request().WithFileID(h.fd).Close().Do(context.WithoutCancel(ctx))
	if response != nil {
		defer response.Close()
	}
	if err == nil {
		var header wire.PacketCodec
		header, err = response.Header(0)
		if err == nil && (header.Command() != wire.SMB2_CLOSE || wire.CloseResponseDecoder(header.Body()).IsInvalid()) {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("invalid SMB CLOSE confirmation"))
		}
	}
	if err != nil {
		return false, errors.Join(nativeError(err), h.conn.Close())
	}
	return true, nil
}
