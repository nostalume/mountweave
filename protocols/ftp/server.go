// Package ftp serves an explicit FTP profile through Core. The host supplies
// authentication, listeners, active dialing and optional TLS certificates.
// No listener, Core, credential store or background task queue is created here.
package ftp

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/protocols"
	"github.com/nostalume/mountweave/protocols/internal/fileview"
)

type StorePolicy uint8

const (
	CompletePublish StorePolicy = iota
	LiveEdit
)

type Login struct {
	Username   string
	Password   string
	RemoteAddr net.Addr
	TLS        *tls.ConnectionState
}

type Limits struct {
	CommandBytes        int
	TransferBufferBytes int
	PageEntries         int
	PageNameBytes       int
}

type Options struct {
	Root               core.Address
	Home               string
	Authenticate       func(context.Context, Login) (core.Principal, error)
	CursorTTL          time.Duration
	StorePolicy        StorePolicy
	ListenPassive      func(context.Context, net.Addr, net.Addr) (net.Listener, netip.AddrPort, error)
	DialActive         func(context.Context, net.Addr, netip.AddrPort) (net.Conn, error)
	TLSConfig          *tls.Config
	DataConnectTimeout time.Duration
	Limits             Limits
	// OnFailure observes failed file requests and transfers. It runs
	// synchronously and may be called by concurrent sessions.
	OnFailure func(protocols.Failure)
}

type Server struct {
	files   *core.Core
	options Options
	view    fileview.View
}

// New snapshots configuration without authentication, network or native I/O.
func New(files *core.Core, o Options) (*Server, error) {
	v, err := fileview.New(o.Root, o.Home)
	if err != nil {
		return nil, err
	}
	o.Home = v.Home
	o.Limits.PageEntries, err = fileview.PageEntries(o.Limits.PageEntries)
	if files == nil || err != nil || o.Authenticate == nil || o.CursorTTL <= 0 || o.DataConnectTimeout <= 0 ||
		o.StorePolicy > LiveEdit || o.ListenPassive == nil && o.DialActive == nil ||
		o.Limits.CommandBytes < 2 || o.Limits.TransferBufferBytes <= 0 || o.Limits.PageNameBytes <= 0 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if o.TLSConfig != nil {
		o.TLSConfig = o.TLSConfig.Clone()
	}
	return &Server{files, o, v}, nil
}

type session struct {
	*Server
	ctx                   context.Context
	base, conn            net.Conn
	reader                *bufio.Reader
	writeMu               sync.Mutex
	closeOnce             sync.Once
	closeErr              error
	actor                 core.Principal
	username, cwd, rename string
	binary, private, pbsz bool
	restart               int64
	passive               net.Listener
	active                netip.AddrPort
	transfer              *transfer
}

// ServeConn takes ownership of a non-nil control connection even on early
// failure. It serves one transfer at a time while continuing to accept ABOR.
// The host retains Core, network factories and its listening sockets.
func (s *Server) ServeConn(ctx context.Context, control net.Conn) (err error) {
	if control == nil {
		return core.Fail(core.InvalidRequest, nil)
	}
	x := &session{Server: s, ctx: ctx, base: control, conn: control, cwd: s.options.Home, binary: false}
	x.reader = bufio.NewReader(control)
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { x.closeTransport(); close(done) })
	defer func() {
		x.closeTransport()
		if !stop() {
			<-done
		}
		if x.transfer != nil {
			x.transfer.cancel()
			<-x.transfer.done
			err = errors.Join(err, x.transfer.fatal)
		}
		if x.passive != nil {
			err = errors.Join(err, x.passive.Close())
		}
		err = errors.Join(err, x.closeErr, ctx.Err())
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = x.reply(220, "File service ready"); err != nil {
		return err
	}
	for {
		line, readErr := x.readLine()
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
		command, arg, _ := strings.Cut(line, " ")
		command = strings.ToUpper(command)
		if command != "RNFR" && command != "RNTO" {
			x.rename = ""
		}
		if x.transfer != nil {
			x.transfer.mu.Lock()
			completing := x.transfer.completing
			x.transfer.mu.Unlock()
			if completing {
				<-x.transfer.done
			}
			select {
			case <-x.transfer.done:
				if x.transfer.fatal != nil {
					return x.transfer.fatal
				}
				x.transfer = nil
			default:
			}
		}
		if x.transfer != nil && command != "ABOR" && command != "QUIT" && command != "NOOP" {
			if err = x.reply(450, "Transfer in progress"); err != nil {
				return err
			}
			continue
		}
		quit, err := x.command(command, arg)
		if err != nil {
			return err
		}
		if quit {
			return nil
		}
	}
}

func (x *session) closeTransport() { x.closeOnce.Do(func() { x.closeErr = x.base.Close() }) }

func (x *session) reply(code int, text string) error {
	x.writeMu.Lock()
	defer x.writeMu.Unlock()
	separator := " "
	if strings.Contains(text, "\r\n") {
		separator = "-"
	}
	_, err := fmt.Fprintf(x.conn, "%03d%s%s\r\n", code, separator, text)
	return err
}

func (x *session) readLine() (string, error) {
	line := make([]byte, 0, min(256, x.options.Limits.CommandBytes))
	for len(line) < x.options.Limits.CommandBytes {
		b, err := x.reader.ReadByte()
		if err != nil {
			if err == io.EOF && len(line) > 0 {
				err = io.ErrUnexpectedEOF
			}
			return "", err
		}
		line = append(line, b)
		if b == '\n' {
			if len(line) < 2 || line[len(line)-2] != '\r' {
				return "", core.Fail(core.InvalidRequest, nil)
			}
			for _, b := range line[:len(line)-2] {
				if b < 32 || b == 127 {
					return "", core.Fail(core.InvalidRequest, nil)
				}
			}
			return string(line[:len(line)-2]), nil
		}
	}
	return "", core.Fail(core.CapacityExceeded, nil)
}

func (x *session) reportFailure(failure protocols.Failure) {
	if observe := x.options.OnFailure; observe != nil {
		observe(failure)
	}
}

func (x *session) failure(command, phase string, err error) error {
	failure := protocols.Failure{Operation: command, Phase: phase, Err: err}
	switch command {
	case "MKD", "XMKD", "RNTO", "STOR", "APPE", "DELE", "RMD":
		failure.HasChange, failure.Change.Effect = true, core.Unchanged
	}
	return x.fileFailure(failure)
}

func (x *session) fileFailure(failure protocols.Failure) error {
	effect, mutating := failure.Effect()
	uncertain := mutating && effect == core.Unknown
	if failure.Err == nil {
		failure.Err = core.Fail(core.InvalidUpstreamResponse, nil)
	}
	x.reportFailure(failure)
	if uncertain {
		return failure.Err
	}
	code := 550
	if core.CodeOf(failure.Err) == core.UnsupportedOperation {
		code = 502
	}
	return x.reply(code, failure.Message())
}

func (x *session) mutationReply(failure protocols.Failure, code int, message string) error {
	effect, _ := failure.Effect()
	if failure.Err != nil || effect == core.Unknown {
		return x.fileFailure(failure)
	}
	if err := x.reply(code, message); err != nil {
		failure.Phase, failure.Err = "reply", err
		x.reportFailure(failure)
		return err
	}
	return nil
}

func (x *session) command(cmd, arg string) (bool, error) {
	switch cmd {
	case "QUIT":
		if x.transfer != nil {
			x.transfer.cancel()
			<-x.transfer.done
			if x.transfer.fatal != nil {
				return false, x.transfer.fatal
			}
			x.transfer = nil
		}
		return true, x.reply(221, "Goodbye")
	case "NOOP":
		return false, x.reply(200, "OK")
	case "SYST":
		return false, x.reply(215, "UNKNOWN")
	case "FEAT":
		features := "Features\r\n UTF8\r\n MLST type*;size*;modify*;\r\n SIZE\r\n MDTM\r\n REST STREAM"
		if x.options.ListenPassive != nil {
			features += "\r\n EPSV\r\n PASV"
		}
		if x.options.DialActive != nil {
			features += "\r\n EPRT"
		}
		if x.options.TLSConfig != nil {
			features += "\r\n AUTH TLS\r\n PBSZ\r\n PROT"
		}
		return false, x.reply(211, features+"\r\n211 End")
	case "AUTH":
		return false, x.authTLS(arg)
	case "PBSZ":
		if x.tlsState() == nil || arg != "0" {
			return false, x.reply(503, "TLS and PBSZ 0 required")
		}
		x.pbsz = true
		return false, x.reply(200, "OK")
	case "PROT":
		if !x.pbsz || x.options.TLSConfig == nil || arg != "C" && arg != "P" {
			return false, x.reply(536, "Protection unavailable")
		}
		x.private = arg == "P"
		return false, x.reply(200, "OK")
	case "USER":
		if err := x.resetData(); err != nil {
			return false, err
		}
		x.actor = ""
		x.username = arg
		x.rename = ""
		x.restart = 0
		x.cwd = x.options.Home
		return false, x.reply(331, "Password required")
	case "PASS":
		if x.username == "" {
			return false, x.reply(503, "USER required")
		}
		x.actor = ""
		x.rename = ""
		x.restart = 0
		if err := x.resetData(); err != nil {
			return false, err
		}
		actor, err := x.options.Authenticate(x.ctx, Login{x.username, arg, x.conn.RemoteAddr(), x.tlsState()})
		if err != nil || actor == "" {
			x.actor = ""
			return false, x.reply(530, "Login failed")
		}
		x.actor = actor
		return false, x.reply(230, "Logged in")
	}
	if x.actor == "" {
		return false, x.reply(530, "Login required")
	}
	switch cmd {
	case "OPTS":
		if strings.ToUpper(arg) == "UTF8 ON" {
			return false, x.reply(200, "OK")
		}
		return false, x.reply(504, "Option unavailable")
	case "TYPE":
		switch strings.ToUpper(arg) {
		case "I":
			x.binary = true
		case "A", "A N":
			x.binary = false
		default:
			return false, x.reply(504, "Representation unavailable")
		}
		x.restart = 0
		return false, x.reply(200, "OK")
	case "MODE", "STRU":
		if cmd == "MODE" && arg == "S" || cmd == "STRU" && arg == "F" {
			return false, x.reply(200, "OK")
		}
		return false, x.reply(504, "Mode unavailable")
	case "PWD", "XPWD":
		return false, x.reply(257, "\""+strings.ReplaceAll(x.cwd, "\"", "\"\"")+"\"")
	case "CWD", "CDUP":
		if cmd == "CDUP" {
			arg = ".."
		}
		a, name, err := x.view.Resolve(x.cwd, arg)
		if err != nil {
			return false, x.failure(cmd, "request", err)
		}
		e, err := x.files.Stat(x.ctx, x.actor, a)
		if err != nil {
			return false, x.failure(cmd, "stat", err)
		}
		if e.Kind != core.Directory {
			return false, x.failure(cmd, "stat", core.Fail(core.NotDirectory, nil))
		}
		x.cwd = name
		return false, x.reply(250, "Directory changed")
	case "SIZE", "MDTM", "MLST":
		a, name, err := x.view.Resolve(x.cwd, arg)
		if err != nil {
			return false, x.failure(cmd, "request", err)
		}
		e, err := x.files.Stat(x.ctx, x.actor, a)
		if err != nil {
			return false, x.failure(cmd, "stat", err)
		}
		if cmd == "MLST" {
			return false, x.reply(250, "Entry\r\n "+facts(e)+" "+name+"\r\n250 End")
		}
		if cmd == "SIZE" && x.binary && e.Kind == core.File && e.Size != nil {
			return false, x.reply(213, strconv.FormatInt(*e.Size, 10))
		}
		if cmd == "MDTM" && e.ModifiedAt != nil {
			return false, x.reply(213, e.ModifiedAt.UTC().Format("20060102150405"))
		}
		return false, x.reply(550, "Representation unavailable")
	case "REST":
		n, err := strconv.ParseInt(arg, 10, 64)
		if !x.binary || err != nil || n < 0 {
			return false, x.reply(501, "Binary offset required")
		}
		x.restart = n
		return false, x.reply(350, "Restart accepted")
	case "PASV", "EPSV":
		return false, x.passiveMode(cmd, arg)
	case "PORT", "EPRT":
		return false, x.activeMode(cmd, arg)
	case "RETR", "STOR", "APPE", "NLST", "MLSD", "LIST":
		return false, x.startTransfer(cmd, arg)
	case "ABOR":
		if x.transfer == nil {
			return false, x.reply(225, "No transfer")
		}
		x.transfer.cancel()
		<-x.transfer.done
		if x.transfer.fatal != nil {
			return false, x.transfer.fatal
		}
		x.transfer = nil
		return false, x.reply(226, "Abort complete")
	case "MKD", "XMKD":
		a, name, err := x.view.Resolve(x.cwd, arg)
		if err != nil {
			return false, x.failure(cmd, "request", err)
		}
		change, err := x.files.Mkdir(x.ctx, x.actor, a)
		return false, x.mutationReply(protocols.Failure{Operation: cmd, Phase: "mkdir", Err: err, Change: change, HasChange: true},
			257, "\""+strings.ReplaceAll(name, "\"", "\"\"")+"\" created")
	case "DELE", "RMD":
		address, err := x.view.ResolveChange(x.cwd, arg)
		if err != nil {
			return false, x.failure(cmd, "request", err)
		}
		target := core.RemoveNonDirectory
		if cmd == "RMD" {
			target = core.RemoveEmptyDirectory
		}
		change, err := x.files.Remove(x.ctx, x.actor, address, core.RemoveOptions{Target: target})
		return false, x.mutationReply(protocols.Failure{Operation: cmd, Phase: "remove", Err: err, Change: change, HasChange: true},
			250, "Removed")
	case "RNFR":
		_, name, err := x.view.Resolve(x.cwd, arg)
		x.rename = ""
		if err != nil {
			return false, x.failure(cmd, "request", err)
		}
		x.rename = name
		return false, x.reply(350, "Name accepted; send RNTO")
	case "RNTO":
		from := x.rename
		x.rename = ""
		if from == "" {
			return false, x.reply(503, "RNFR required")
		}
		a, err := x.view.ResolveChange("/", from)
		if err != nil {
			return false, x.failure(cmd, "request", err)
		}
		b, err := x.view.ResolveChange(x.cwd, arg)
		if err != nil {
			return false, x.failure(cmd, "request", err)
		}
		r, err := x.files.Move(x.ctx, x.actor, a, b)
		return false, x.mutationReply(protocols.Failure{Operation: cmd, Phase: "rename", Err: err, Change: r.Target, HasChange: true, Source: &r.Source},
			250, "Moved")
	default:
		return false, x.reply(502, "Command unavailable")
	}
}

func (x *session) tlsState() *tls.ConnectionState {
	if c, ok := x.conn.(interface{ ConnectionState() tls.ConnectionState }); ok {
		state := c.ConnectionState()
		if state.HandshakeComplete {
			return &state
		}
	}
	return nil
}

func (x *session) authTLS(arg string) error {
	if x.options.TLSConfig == nil || strings.ToUpper(arg) != "TLS" {
		return x.reply(504, "TLS unavailable")
	}
	if x.tlsState() != nil || x.actor != "" {
		return x.reply(503, "AUTH unavailable in current state")
	}
	if x.reader.Buffered() != 0 {
		return core.Fail(core.InvalidRequest, nil)
	}
	if err := x.reply(234, "Start TLS"); err != nil {
		return err
	}
	c := tls.Server(x.conn, x.options.TLSConfig)
	ctx, cancel := context.WithTimeout(x.ctx, x.options.DataConnectTimeout)
	defer cancel()
	if err := c.HandshakeContext(ctx); err != nil {
		return err
	}
	x.conn = c
	x.reader = bufio.NewReader(c)
	x.username = ""
	x.rename = ""
	x.restart = 0
	return nil
}

func facts(e core.Entry) string {
	var b strings.Builder
	if e.Kind == core.File {
		b.WriteString("type=file;")
	} else if e.Kind == core.Directory {
		b.WriteString("type=dir;")
	}
	if e.Size != nil {
		fmt.Fprintf(&b, "size=%d;", *e.Size)
	}
	if e.ModifiedAt != nil {
		fmt.Fprintf(&b, "modify=%s;", e.ModifiedAt.UTC().Format("20060102150405"))
	}
	return b.String()
}
