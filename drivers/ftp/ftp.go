// Package ftp connects a rooted FTP service to Mountweave.
package ftp

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/nostalume/mountweave"
)

// Security selects the control and data protection policy without downgrade.
type Security uint8

const (
	Plain Security = iota
	ExplicitTLS
	ImplicitTLS
)

// Options describes one native FTP account and root. TLSConfig is cloned; the
// Driver owns its connections, while credential and certificate sources remain
// owned by the caller.
type Options struct {
	Address          string        `json:"address"`
	RootPath         string        `json:"root_path"`
	Username         string        `json:"username"`
	Password         string        `json:"password"`
	Security         Security      `json:"security,omitempty"`
	TLSConfig        *tls.Config   `json:"-"`
	MaxConnections   int           `json:"max_connections,omitempty"`
	MaxResponseBytes int           `json:"max_response_bytes,omitempty"`
	MaxLineBytes     int           `json:"max_line_bytes,omitempty"`
	PageEntries      int           `json:"page_entries,omitempty"`
	CleanupTimeout   time.Duration `json:"cleanup_timeout,omitempty"`
}

// Validate is optional static preflight; Open always checks the same rules.
func (o Options) Validate() error {
	_, err := o.settings()
	return err
}

func (o Options) settings() (Options, error) {
	host, port, err := net.SplitHostPort(o.Address)
	number, portErr := strconv.Atoi(port)
	if err != nil || host == "" || portErr != nil || number < 1 || number > 65535 ||
		o.RootPath == "" || o.Username == "" || strings.ContainsAny(o.RootPath+o.Username+o.Password, "\x00\r\n") ||
		o.Security > ImplicitTLS || o.Security == Plain && o.TLSConfig != nil ||
		o.MaxConnections < 0 || o.MaxResponseBytes < 0 || o.MaxLineBytes < 0 || o.PageEntries < 0 || o.CleanupTimeout < 0 {
		return o, core.Fail(core.InvalidRequest, errors.New("a valid FTP address, root, account and resource settings are required"))
	}
	if o.MaxConnections == 0 {
		o.MaxConnections = 4
	}
	if o.MaxResponseBytes == 0 {
		o.MaxResponseBytes = 1 << 20
	}
	if o.MaxLineBytes == 0 {
		o.MaxLineBytes = 64 << 10
	}
	if o.PageEntries == 0 {
		o.PageEntries = core.MaxPage
	}
	if o.CleanupTimeout == 0 {
		o.CleanupTimeout = 5 * time.Second
	}
	if o.MaxLineBytes < 4 || o.MaxResponseBytes < o.MaxLineBytes {
		return o, core.Fail(core.InvalidRequest, errors.New("FTP reply budget must fit one bounded line"))
	}
	return o, nil
}

type Driver struct {
	options  Options
	root     string
	life     context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	closed   bool
	count    int
	idle     []*connection
	all      map[*connection]struct{}
	creating sync.WaitGroup
}

// Open logs in and resolves the native root. A non-nil failed Driver retains
// any failed local cleanup and must be closed.
func Open(ctx context.Context, options Options) (*Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options, err := options.settings()
	if err != nil {
		return nil, err
	}
	if options.Security != Plain {
		config := options.TLSConfig
		if config == nil {
			config = &tls.Config{}
		}
		options.TLSConfig = config.Clone()
		if options.TLSConfig.ServerName == "" {
			options.TLSConfig.ServerName, _, _ = net.SplitHostPort(options.Address)
		}
		if options.TLSConfig.ClientSessionCache == nil {
			options.TLSConfig.ClientSessionCache = tls.NewLRUClientSessionCache(0)
		}
	}
	life, cancel := context.WithCancel(context.Background())
	d := &Driver{options: options, life: life, cancel: cancel, all: make(map[*connection]struct{})}
	c, err := d.borrow(ctx)
	if err != nil {
		return d, err
	}
	end := c.scope(ctx)
	code, message, err := c.command("CWD", options.RootPath)
	if err == nil {
		err = expect(code, message, 250)
	}
	if err == nil {
		code, message, err = c.command("PWD", "")
		if err == nil {
			err = expect(code, message, 257)
		}
		if err == nil {
			d.root, err = nativeRoot(message)
		}
	}
	err = errors.Join(err, end(), ctx.Err())
	if err != nil {
		err = errors.Join(err, c.Close())
	}
	err = errors.Join(err, d.put(c))
	return d, err
}

// PWD quotes a pathname, escaping an embedded quote by doubling it. The native
// syntax is opaque: neither the host OS nor slash normalization rewrites it.
func nativeRoot(message string) (string, error) {
	message = strings.TrimSpace(message)
	if len(message) > 1 && message[0] == '"' {
		var root strings.Builder
		for i := 1; i < len(message); i++ {
			if message[i] != '"' {
				root.WriteByte(message[i])
				continue
			}
			if i+1 < len(message) && message[i+1] == '"' {
				root.WriteByte('"')
				i++
				continue
			}
			if root.Len() != 0 && !strings.ContainsAny(root.String(), "\x00\r\n") {
				return root.String(), nil
			}
			break
		}
	}
	return "", core.Fail(core.InvalidUpstreamResponse, errors.New("FTP PWD did not return a quoted root"))
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

// A connection is exclusive to one activity. Only raw socket ownership crosses
// cancellation; control and its reader belong to the current command owner.
type connection struct {
	socket  net.Conn
	control net.Conn
	reader  *bufio.Reader
	options Options
	mlsd    bool
	closed  atomic.Bool
	dataMu  sync.Mutex
	data    net.Conn
	closeMu sync.Mutex
	retired bool
}

func (c *connection) adoptData(data net.Conn) error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	c.dataMu.Lock()
	if c.data != nil {
		c.dataMu.Unlock()
		return errors.Join(core.Fail(core.InvalidUpstreamResponse, errors.New("FTP data connection is already owned")), data.Close())
	}
	c.data = data
	c.retired = false // A late dial still transfers cleanup after cancellation.
	closed := c.closed.Load()
	c.dataMu.Unlock()
	if closed {
		return errors.Join(net.ErrClosed, c.closeData())
	}
	return nil
}

func (c *connection) replaceData(old, data net.Conn) error {
	c.dataMu.Lock()
	if c.data == old && !c.closed.Load() {
		c.data = data
		c.dataMu.Unlock()
		return nil
	}
	c.dataMu.Unlock()
	return errors.Join(net.ErrClosed, data.Close())
}

func (c *connection) closeData() error {
	c.dataMu.Lock()
	defer c.dataMu.Unlock()
	if c.data == nil {
		return nil
	}
	deadlineErr := c.data.SetDeadline(time.Now().Add(c.options.CleanupTimeout))
	if errors.Is(deadlineErr, net.ErrClosed) {
		deadlineErr = nil
	}
	err := c.data.Close()
	if err == nil || errors.Is(err, net.ErrClosed) {
		c.data = nil
		err = nil
	}
	return errors.Join(deadlineErr, err)
}

func (c *connection) Close() error {
	c.closed.Store(true)
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.retired {
		return nil
	}
	err := c.socket.Close()
	if errors.Is(err, net.ErrClosed) {
		err = nil
	}
	err = errors.Join(err, c.closeData())
	c.retired = err == nil
	return err
}

func (c *connection) scope(ctx context.Context) func() error {
	done := make(chan struct{})
	var closeErr error
	stop := context.AfterFunc(ctx, func() { closeErr = c.Close(); close(done) })
	return func() error {
		if !stop() {
			<-done
		}
		// The operation owns ctx.Err; cleanup retains only actual close failures.
		return closeErr
	}
}

func (d *Driver) dial(ctx context.Context) (c *connection, err error) {
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(d.life, cancel)
	defer func() { stop(); cancel() }()
	socket, err := (&net.Dialer{}).DialContext(call, "tcp", d.options.Address)
	if err != nil {
		return nil, err
	}
	c = &connection{socket: socket, control: socket, options: d.options, mlsd: true}
	end := c.scope(call)
	defer func() { err = errors.Join(err, end(), call.Err()) }()
	secure := func() error {
		secured := tls.Client(socket, d.options.TLSConfig)
		if failure := secured.HandshakeContext(call); failure != nil {
			return failure
		}
		c.control = secured
		c.reader = bufio.NewReader(secured)
		return nil
	}
	if d.options.Security == ImplicitTLS {
		if err = secure(); err != nil {
			return c, err
		}
	} else {
		c.reader = bufio.NewReader(socket)
	}
	code, message, err := c.reply()
	if err == nil {
		err = expect(code, message, 220)
	}
	if err == nil && d.options.Security == ExplicitTLS {
		code, message, err = c.command("AUTH", "TLS")
		if err == nil {
			err = expect(code, message, 234)
		}
		if err == nil {
			err = secure()
		}
	}
	if err == nil && d.options.Security != Plain {
		for _, cmd := range [][2]string{{"PBSZ", "0"}, {"PROT", "P"}} {
			code, message, err = c.command(cmd[0], cmd[1])
			if err == nil {
				err = expect(code, message, 200)
			}
			if err != nil {
				break
			}
		}
	}
	if err == nil {
		code, message, err = c.command("USER", d.options.Username)
		if err == nil && code == 331 {
			code, message, err = c.command("PASS", d.options.Password)
		}
		if err == nil {
			err = expect(code, message, 230)
		}
	}
	if err == nil {
		code, message, err = c.command("TYPE", "I")
		if err == nil {
			err = expect(code, message, 200)
		}
	}
	if err == nil {
		code, message, err = c.command("FEAT", "")
		if err == nil && code == 211 {
			c.mlsd = false
			for _, line := range strings.Split(message, "\n") {
				fields := strings.Fields(line)
				if len(fields) > 0 && strings.EqualFold(fields[0], "MLST") {
					c.mlsd = true
				}
			}
		} else if err == nil && code != 500 && code != 502 {
			err = expect(code, message, 211)
		}
	}
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
	var c *connection
	if n := len(d.idle); n != 0 {
		c = d.idle[n-1]
		d.idle = d.idle[:n-1]
	} else if d.count == d.options.MaxConnections {
		d.mu.Unlock()
		return nil, core.Fail(core.CapacityExceeded, errors.New("FTP connections are occupied"))
	} else {
		d.count++
		d.creating.Add(1)
	}
	d.mu.Unlock()
	if c == nil {
		defer d.creating.Done()
		var err error
		c, err = d.dial(ctx)
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
			if core.CodeOf(err) == core.InternalError {
				err = core.Fail(core.UpstreamFailure, err)
			}
			if c != nil {
				err = errors.Join(err, c.Close(), d.put(c))
			}
			return nil, err
		}
	}
	if d.root != "" {
		end := c.scope(ctx)
		code, message, err := c.command("CWD", d.root)
		if err == nil {
			err = expect(code, message, 250)
		}
		err = errors.Join(err, end(), ctx.Err())
		if err != nil {
			return nil, errors.Join(err, c.Close(), d.put(c))
		}
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

// Close retires connections and joins pending construction. Failed local socket
// cleanup remains owned for a later Close; no network command is replayed.
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

var _ core.Driver = (*Driver)(nil)
