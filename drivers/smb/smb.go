// Package smb connects a rooted SMB file share to Mountweave.
package smb

import (
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	smb2 "github.com/hirochachacha/go-smb2/v2"
	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
)

// Options selects one server, share and subtree. Dialer configuration is
// snapshotted; referenced credentials and transport factories remain borrowed
// and must be safe for concurrent use. The Driver owns the resulting sessions.
type Options struct {
	Server           string        `json:"server"`
	Share            string        `json:"share"`
	RootPath         string        `json:"root_path,omitempty"`
	Dialer           *smb2.Dialer  `json:"-"`
	MaxSessions      int           `json:"max_sessions,omitempty"`
	BlockBytes       int           `json:"block_bytes,omitempty"`
	PageBytes        int           `json:"page_bytes,omitempty"`
	MaxBufferedBytes int64         `json:"max_buffered_bytes,omitempty"`
	CleanupTimeout   time.Duration `json:"cleanup_timeout,omitempty"`
}

// Validate is optional static preflight; Open always checks the same rules.
func (o Options) Validate() error {
	_, err := o.settings()
	return err
}

func (o Options) settings() (Options, error) {
	if o.Server == "" || !utf8.ValidString(o.Server) || strings.ContainsAny(o.Server, "\\/\x00\r\n") ||
		!core.ValidName(o.Share) || o.Dialer == nil || o.Dialer.Credentials == nil || o.Dialer.IOPipelineDepth > 65535 ||
		o.MaxSessions < 0 || o.BlockBytes < 0 || o.PageBytes < 0 || o.MaxBufferedBytes < 0 || o.CleanupTimeout < 0 {
		return o, core.Fail(core.InvalidRequest, errors.New("an SMB server, share, dialer and valid resource settings are required"))
	}
	for _, dialect := range o.Dialer.SpecifiedDialects {
		if !slices.Contains([]smb2.Dialect{smb2.SMB202, smb2.SMB210, smb2.SMB300, smb2.SMB302, smb2.SMB311}, dialect) {
			return o, core.Fail(core.InvalidRequest, errors.New("unsupported SMB dialect"))
		}
	}
	for _, cipher := range o.Dialer.Ciphers {
		if !slices.Contains([]smb2.Cipher{smb2.AES128CCM, smb2.AES128GCM, smb2.AES256CCM, smb2.AES256GCM}, cipher) {
			return o, core.Fail(core.InvalidRequest, errors.New("unsupported SMB cipher"))
		}
	}
	switch o.Dialer.TransportDialer.(type) {
	case smb2.QUICDialer, *smb2.QUICDialer:
		if len(o.Dialer.SpecifiedDialects) != 0 && !slices.Contains(o.Dialer.SpecifiedDialects, smb2.SMB311) {
			return o, core.Fail(core.InvalidRequest, errors.New("SMB over QUIC requires dialect 3.1.1"))
		}
	}
	if o.RootPath == "" {
		o.RootPath = "/"
	}
	if err := validatePath(o.RootPath); err != nil {
		return o, err
	}
	if o.MaxSessions == 0 {
		o.MaxSessions = 4
	}
	if o.BlockBytes == 0 {
		o.BlockBytes = 64 << 10
	}
	if o.PageBytes == 0 {
		o.PageBytes = 64 << 10
	}
	// This baseline uses one-credit I/O, valid without SDK-private negotiated
	// limits. These protocol-profile ceilings are not machine-derived budgets.
	if o.BlockBytes > 64<<10 || o.PageBytes > 64<<10 {
		return o, core.Fail(core.InvalidRequest, errors.New("SMB baseline requests must fit a 64 KiB window"))
	}
	if o.MaxBufferedBytes == 0 {
		o.MaxBufferedBytes = 4 << 20
	}
	if o.MaxBufferedBytes < int64(o.BlockBytes) {
		return o, core.Fail(core.InvalidRequest, errors.New("SMB payload budget must fit one I/O block"))
	}
	if o.CleanupTimeout == 0 {
		o.CleanupTimeout = 5 * time.Second
	}
	return o, nil
}

// validatePath preserves the requested components. ADS and Win32 spelling
// normalization are different addresses, not alternate names for a Core file.
func validatePath(p string) error {
	if err := core.ValidatePath(p); err != nil {
		return err
	}
	if p != "/" {
		for _, name := range strings.Split(p[1:], "/") {
			units := 0
			for _, r := range name {
				units++
				if r > 0xffff {
					units++
				}
			}
			if units > 255 || strings.ContainsAny(name, ":*?\"<>|") || strings.HasSuffix(name, ".") || strings.HasSuffix(name, " ") {
				return core.Fail(core.UnrepresentableName, errors.New("SMB stream or normalized spelling is not a tree filename"))
			}
		}
	}
	return nil
}

type Driver struct {
	options  Options
	root     string
	buffer   *upload.Budget
	life     context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	closed   bool
	count    int
	idle     []*connection
	all      map[*connection]struct{}
	creating sync.WaitGroup
}

// Open constructs a rooted instance and verifies directory behavior without
// changing it. A non-nil failed Driver retains cleanup and must be closed.
func Open(ctx context.Context, options Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options, err := options.settings()
	if err != nil {
		return nil, err
	}
	dialer := *options.Dialer
	dialer.SpecifiedDialects = slices.Clone(dialer.SpecifiedDialects)
	dialer.Ciphers = slices.Clone(dialer.Ciphers)
	// Native requests use standard attributes, not SDK security-descriptor
	// extensions that would additionally open the share root during Mount.
	dialer.DisableAAPLExtension = true
	options.Dialer = &dialer
	life, cancel := context.WithCancel(context.Background())
	d := &Driver{options: options, root: strings.ReplaceAll(strings.TrimPrefix(options.RootPath, "/"), "/", "\\"),
		life: life, cancel: cancel, all: make(map[*connection]struct{})}
	d.buffer, _ = upload.New(options.MaxBufferedBytes, 1)
	c, err := d.borrow(ctx)
	if err != nil {
		return d, err
	}
	end := c.scope(ctx)
	h, meta, err := c.open(ctx, d.root, true, false)
	if err == nil && meta.Kind != core.Directory {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err == nil {
		_, failure := h.page(ctx, "*", uint32(options.PageBytes))
		if failure != io.EOF {
			err = failure
		}
	}
	if h != nil {
		cleanup, stop := context.WithTimeout(context.Background(), options.CleanupTimeout)
		join := c.scope(cleanup)
		_, closeErr := h.close(cleanup)
		err = errors.Join(err, closeErr, join(), cleanup.Err())
		stop()
	}
	err = errors.Join(err, end(), ctx.Err())
	if err != nil {
		err = errors.Join(err, c.Close())
	}
	return d, errors.Join(err, d.put(c))
}

func (d *Driver) admit(p core.NativeAddress) error {
	if p.Namespace() != core.TreeNamespace {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	if err := validatePath(p.Value()); err != nil {
		return err
	}
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		return core.Fail(core.MountUnavailable, nil)
	}
	return nil
}

// Each activity exclusively owns this session and share. Aborting it cannot
// invalidate another activity's files or directory enumeration.
type connection struct {
	session   atomic.Pointer[smb2.Session]
	share     *smb2.Share
	closed    atomic.Bool
	closeMu   sync.Mutex
	transport smb2.Transport
}

// Close retires a session by transport shutdown, without a LOGOFF wait. Normal
// operations close handles before returning healthy sessions to the idle set.
func (c *connection) Close() error {
	c.closed.Store(true)
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	var err error
	if session := c.session.Load(); session != nil {
		err = session.Abort()
	} else if c.transport != nil {
		err = c.transport.Close()
	}
	if err != nil && onlyClosed(err) {
		return nil
	}
	return err
}

// An already closed transport is retired. A joined real cleanup failure must
// survive even when another branch also contains net.ErrClosed.
func onlyClosed(err error) bool {
	switch value := err.(type) {
	case interface{ Unwrap() []error }:
		causes := value.Unwrap()
		for _, cause := range causes {
			if !onlyClosed(cause) {
				return false
			}
		}
		return len(causes) != 0
	case interface{ Unwrap() error }:
		return onlyClosed(value.Unwrap())
	default:
		return err != nil && errors.Is(err, net.ErrClosed)
	}
}

// Capture ownership when the native factory returns, before negotiation can
// fail without publishing a Session. No SMB transport implementation is added.
type transportDialer struct {
	base smb2.TransportDialer
	conn *connection
}

func (d transportDialer) Dial(ctx context.Context, server string) (smb2.Transport, error) {
	transport, err := d.base.Dial(ctx, server)
	if transport == nil {
		return nil, err
	}
	d.conn.closeMu.Lock()
	d.conn.transport = transport
	closed := d.conn.closed.Load()
	d.conn.closeMu.Unlock()
	if err != nil || closed {
		if closed {
			err = errors.Join(err, core.Fail(core.MountUnavailable, nil), ctx.Err())
		}
		return nil, errors.Join(err, d.conn.Close())
	}
	return transport, nil
}

// scope reports actual cleanup failures only; the operation owns ctx.Err.
// Stop and join before returning a reusable session to a different activity.
func (c *connection) scope(ctx context.Context) func() error {
	done := make(chan struct{})
	var closeErr error
	stop := context.AfterFunc(ctx, func() { closeErr = c.Close(); close(done) })
	return func() error {
		if !stop() {
			<-done
		}
		return closeErr
	}
}

func (d *Driver) dial(ctx context.Context) (c *connection, err error) {
	call, cancel := context.WithCancel(ctx)
	stopDriver := context.AfterFunc(d.life, cancel)
	defer func() { stopDriver(); cancel() }()
	c = &connection{}
	end := c.scope(call)
	defer func() { err = errors.Join(nativeError(err), end(), call.Err()) }()
	dialer := *d.options.Dialer
	base := dialer.TransportDialer
	if base == nil {
		base = smb2.TCPDialer{}
	}
	dialer.TransportDialer = transportDialer{base: base, conn: c}
	session, err := dialer.Dial(call, d.options.Server)
	if session != nil {
		c.session.Store(session)
		// A callback may have retired the not-yet-published connection. The
		// late session still transfers cleanup, even if Close previously saw nil.
		if c.closed.Load() {
			err = errors.Join(err, c.Close(), call.Err())
		}
	}
	if err != nil {
		return c, err
	}
	c.share, err = session.Mount(call, d.options.Share)
	return c, err
}

func (d *Driver) borrow(ctx context.Context) (*connection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	if n := len(d.idle); n != 0 {
		c := d.idle[n-1]
		d.idle = d.idle[:n-1]
		d.mu.Unlock()
		return c, nil
	}
	if d.count == d.options.MaxSessions {
		d.mu.Unlock()
		return nil, core.Fail(core.CapacityExceeded, errors.New("SMB sessions are occupied"))
	}
	d.count++
	d.creating.Add(1)
	d.mu.Unlock()
	defer d.creating.Done()
	c, err := d.dial(ctx)
	d.mu.Lock()
	closed := d.closed
	if c != nil {
		d.all[c] = struct{}{}
	} else {
		d.count--
	}
	d.mu.Unlock()
	if closed {
		err = errors.Join(err, core.Fail(core.MountUnavailable, nil))
	}
	if err != nil {
		if c != nil {
			err = errors.Join(err, c.Close(), d.put(c))
		}
		return nil, err
	}
	return c, nil
}

func (d *Driver) put(c *connection) error {
	d.mu.Lock()
	if !d.closed && !c.closed.Load() {
		d.idle = append(d.idle, c)
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()
	err := c.Close()
	if err == nil {
		d.mu.Lock()
		if _, owned := d.all[c]; owned {
			delete(d.all, c)
			d.count--
		}
		d.mu.Unlock()
	}
	return err
}

// Close retires owned sessions and joins construction. An SDK shutdown failure
// remains owned; repeated Close does not claim that a cached failure succeeded.
func (d *Driver) Close() error {
	d.mu.Lock()
	d.closed = true
	d.cancel()
	d.idle = nil
	connections := make([]*connection, 0, len(d.all))
	for c := range d.all {
		connections = append(connections, c)
	}
	d.mu.Unlock()
	var err error
	for _, c := range connections {
		err = errors.Join(err, c.Close())
	}
	d.creating.Wait()
	d.mu.Lock()
	connections = connections[:0]
	for c := range d.all {
		connections = append(connections, c)
	}
	d.mu.Unlock()
	for _, c := range connections {
		if failure := c.Close(); failure != nil {
			err = errors.Join(err, failure)
			continue
		}
		d.mu.Lock()
		if _, owned := d.all[c]; owned {
			delete(d.all, c)
			d.count--
		}
		d.mu.Unlock()
	}
	return err
}
