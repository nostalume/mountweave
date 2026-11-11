// Package sftp connects an SSH file-transfer service to Mountweave.
package sftp

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/upload"
	sshfx "github.com/pkg/sftp/v2/encoding/ssh/filexfer"
	"golang.org/x/crypto/ssh"
)

// Options selects a native SSH service and remote root. SSHConfig supplies the
// authentication methods and explicit host-key policy; it is not an ACL model.
type Options struct {
	Address          string            `json:"address"`
	RootPath         string            `json:"root_path"`
	SSHConfig        *ssh.ClientConfig `json:"-"`
	MaxConnections   int               `json:"max_connections,omitempty"`
	MaxRequests      int               `json:"max_requests,omitempty"`
	MaxBufferedBytes int64             `json:"max_buffered_bytes,omitempty"`
	CleanupTimeout   time.Duration     `json:"cleanup_timeout,omitempty"`
}

func (o Options) Validate() error {
	_, err := o.settings()
	return err
}

func (o Options) settings() (Options, error) {
	if _, _, err := net.SplitHostPort(o.Address); err != nil {
		return o, core.Fail(core.InvalidRequest, err)
	}
	if o.RootPath == "" || strings.ContainsAny(o.RootPath, "\x00\r\n") || o.SSHConfig == nil || o.SSHConfig.User == "" || o.SSHConfig.HostKeyCallback == nil ||
		o.MaxConnections < 0 || o.MaxRequests < 0 || o.MaxRequests > math.MaxInt/32768 || o.MaxBufferedBytes < 0 || o.CleanupTimeout < 0 {
		return o, core.Fail(core.InvalidRequest, errors.New("an SSH address, root, authentication and host-key policy are required"))
	}
	if o.MaxConnections == 0 {
		o.MaxConnections = 4
	}
	if o.MaxRequests == 0 {
		o.MaxRequests = 16
	}
	if o.MaxBufferedBytes == 0 {
		o.MaxBufferedBytes = 4 << 20
	}
	if o.CleanupTimeout == 0 {
		o.CleanupTimeout = 5 * time.Second
	}
	if o.MaxBufferedBytes < int64(o.MaxRequests)*32768 {
		return o, core.Fail(core.InvalidRequest, errors.New("buffer budget is smaller than one packet window"))
	}
	return o, nil
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

// Open constructs and verifies an independently owned SSH/SFTP instance.
// A non-nil failed Driver must be closed; it retains any failed local cleanup.
func Open(ctx context.Context, options Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var err error
	options, err = options.settings()
	if err != nil {
		return nil, err
	}
	config := *options.SSHConfig
	config.Auth = append([]ssh.AuthMethod(nil), config.Auth...)
	config.HostKeyAlgorithms = append([]string(nil), config.HostKeyAlgorithms...)
	config.Ciphers = append([]string(nil), config.Ciphers...)
	config.KeyExchanges = append([]string(nil), config.KeyExchanges...)
	config.MACs = append([]string(nil), config.MACs...)
	options.SSHConfig = &config
	life, cancel := context.WithCancel(context.Background())
	d := &Driver{options: options, life: life, cancel: cancel, all: make(map[*connection]struct{})}
	d.buffer, _ = upload.New(options.MaxBufferedBytes, 1)
	c, err := d.borrow(ctx)
	if err != nil {
		return d, err
	}
	end := c.scope(ctx)
	names, err := c.wire.names(&sshfx.RealPathPacket{Path: options.RootPath})
	if err == nil {
		if len(names) != 1 || names[0].Filename == "" || strings.ContainsAny(names[0].Filename, "\x00\r\n") {
			err = core.Fail(core.InvalidUpstreamResponse, errors.New("invalid canonical SFTP root"))
		} else {
			d.root = names[0].Filename
			var attrs sshfx.Attributes
			attrs, err = c.wire.attrs(&sshfx.StatPacket{Path: d.root})
			if err == nil {
				var meta core.Metadata
				meta, err = metadata("", attrs)
				if err == nil && meta.Kind != core.Directory {
					err = core.Fail(core.NotDirectory, nil)
				}
			}
		}
	}
	err = errors.Join(err, end(), d.put(c))
	return d, err
}

func (d *Driver) admit(p core.NativeAddress) error {
	if p.Namespace() != core.TreeNamespace {
		return core.Fail(core.UnsupportedOperation, nil)
	}
	if err := core.ValidatePath(p.Value()); err != nil {
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

type connection struct {
	net     net.Conn
	client  atomic.Pointer[ssh.Client]
	channel ssh.Channel
	wire    *wire
	closed  atomic.Bool
	closeMu sync.Mutex
	retired bool
}

func (c *connection) Read(p []byte) (int, error)  { return c.channel.Read(p) }
func (c *connection) Write(p []byte) (int, error) { return c.channel.Write(p) }

// Close owns the whole exclusive connection, including during channel setup.
// Closing the socket first wakes blocked SSH negotiation and native I/O.
func (c *connection) Close() error {
	c.closed.Store(true)
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.retired {
		return nil
	}
	err := c.net.Close()
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	if client := c.client.Load(); client != nil {
		_ = client.Wait()
	}
	c.retired = true
	return nil
}

// scope belongs to one call/stream. Stop and join before returning a connection
// to the idle set, so a former caller cannot cancel a subsequent borrower.
func (c *connection) scope(ctx context.Context) func() error {
	done := make(chan struct{})
	var closeErr error
	stop := context.AfterFunc(ctx, func() { closeErr = c.Close(); close(done) })
	return func() error {
		if !stop() {
			<-done
		}
		return errors.Join(ctx.Err(), closeErr)
	}
}

func (d *Driver) dial(ctx context.Context) (c *connection, err error) {
	call, cancel := context.WithCancel(ctx)
	stopDriver := context.AfterFunc(d.life, cancel)
	defer func() { stopDriver(); cancel() }()
	dialer := net.Dialer{Timeout: d.options.SSHConfig.Timeout}
	socket, err := dialer.DialContext(call, "tcp", d.options.Address)
	if err != nil {
		return nil, err
	}
	c = &connection{net: socket}
	end := c.scope(call)
	defer func() {
		err = errors.Join(err, end())
		if err != nil {
			closeErr := c.Close()
			err = errors.Join(err, closeErr)
			if closeErr == nil {
				if client := c.client.Load(); client != nil {
					_ = client.Wait()
				}
			}
		}
	}()
	transport, channels, requests, err := ssh.NewClientConn(socket, d.options.Address, d.options.SSHConfig)
	if err != nil {
		return c, err
	}
	client := ssh.NewClient(transport, channels, requests)
	c.client.Store(client)
	channel, incoming, err := client.OpenChannel("session", nil)
	if err != nil {
		return c, err
	}
	c.channel = channel
	go ssh.DiscardRequests(incoming)
	ok, err := channel.SendRequest("subsystem", true, ssh.Marshal(struct{ Name string }{"sftp"}))
	if err != nil || !ok {
		return c, errors.Join(err, core.Fail(core.UnsupportedOperation, errors.New("SFTP subsystem was not accepted")))
	}
	c.wire, err = newWire(c)
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
	if d.count == d.options.MaxConnections {
		d.mu.Unlock()
		return nil, core.Fail(core.CapacityExceeded, errors.New("SFTP connections are occupied"))
	}
	d.count++
	d.creating.Add(1)
	d.mu.Unlock()
	defer d.creating.Done()
	c, err := d.dial(ctx)
	if err != nil && core.CodeOf(err) == core.InternalError {
		err = core.Fail(core.UpstreamFailure, err)
	}
	d.mu.Lock()
	closed := d.closed
	if err == nil && !closed {
		d.all[c] = struct{}{}
	}
	d.mu.Unlock()
	if err != nil || closed {
		if closed {
			err = errors.Join(err, core.Fail(core.MountUnavailable, nil))
		}
		var closeErr error
		if c != nil {
			closeErr = c.Close()
		}
		d.mu.Lock()
		if closeErr != nil {
			d.all[c] = struct{}{} // Retain failed local cleanup, not a reusable client.
		} else {
			d.count--
		}
		d.mu.Unlock()
		err = errors.Join(err, closeErr)
		return nil, err
	}
	return c, nil
}

func (d *Driver) put(c *connection) error {
	d.mu.Lock()
	if !d.closed && !c.closed.Load() && !c.wire.broken {
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

func (d *Driver) Close() error {
	d.mu.Lock()
	d.closed = true
	d.cancel()
	connections := make([]*connection, 0, len(d.all))
	for c := range d.all {
		connections = append(connections, c)
	}
	d.idle = nil
	d.mu.Unlock()
	var err error
	for _, c := range connections {
		err = errors.Join(err, c.Close())
	}
	d.creating.Wait()
	// A canceled constructor may have retained failed cleanup while Close was
	// waiting. Snapshot again after constructors have joined.
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

var _ core.Driver = (*Driver)(nil)
var _ io.ReadWriteCloser = (*connection)(nil)
