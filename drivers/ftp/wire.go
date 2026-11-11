package ftp

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
)

// expect preserves the native reply. A 550 can mean a missing name or denied
// access; guessing NotFound would discard that distinction.
func expect(code int, message string, allowed ...int) error {
	for _, want := range allowed {
		if code == want {
			return nil
		}
	}
	kind := core.UpstreamFailure
	switch code {
	case 500, 502, 504:
		kind = core.UnsupportedOperation
	case 530, 532:
		kind = core.UpstreamAccessDenied
	}
	return core.Fail(kind, &textproto.Error{Code: code, Msg: message})
}

func (c *connection) fail(err error) error {
	if core.CodeOf(err) == core.InternalError {
		err = core.Fail(core.UpstreamFailure, err)
	}
	return errors.Join(err, c.Close())
}

// readLine bounds allocation while consuming a line, before any complete-line
// allocation. The reader's own buffer is separate. Filename spaces are retained.
func readLine(reader *bufio.Reader, limit int) (string, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		if len(part) > limit-len(line) {
			return "", core.Fail(core.ResponseBudgetExceeded, errors.New("FTP line exceeds its budget"))
		}
		line = append(line, part...)
		if err == nil {
			if len(line) < 2 || line[len(line)-2] != '\r' {
				return "", core.Fail(core.InvalidUpstreamResponse, errors.New("FTP line is not CRLF terminated"))
			}
			return string(line[:len(line)-2]), nil
		}
		if err != bufio.ErrBufferFull {
			if err == io.EOF && len(line) != 0 {
				err = io.ErrUnexpectedEOF
			}
			return "", err
		}
	}
}

func replyCode(line string) (int, bool) {
	if len(line) < 4 || line[0] < '1' || line[0] > '5' || line[1] < '0' || line[1] > '9' ||
		line[2] < '0' || line[2] > '9' || line[3] != ' ' && line[3] != '-' {
		return 0, false
	}
	code, _ := strconv.Atoi(line[:3])
	return code, true
}

func (c *connection) reply() (int, string, error) {
	line, err := readLine(c.reader, c.options.MaxLineBytes)
	if err != nil {
		return 0, "", c.fail(err)
	}
	code, valid := replyCode(line)
	if !valid {
		return 0, "", c.fail(core.Fail(core.InvalidUpstreamResponse, errors.New("invalid FTP reply")))
	}
	used := len(line) + 2
	if used > c.options.MaxResponseBytes {
		return 0, "", c.fail(core.Fail(core.ResponseBudgetExceeded, nil))
	}
	message := line[4:]
	if line[3] == '-' {
		end := line[:3] + " "
		var text strings.Builder
		text.WriteString(message)
		for {
			line, err = readLine(c.reader, c.options.MaxLineBytes)
			if err != nil {
				return 0, "", c.fail(err)
			}
			if len(line)+2 > c.options.MaxResponseBytes-used {
				return 0, "", c.fail(core.Fail(core.ResponseBudgetExceeded, nil))
			}
			used += len(line) + 2
			text.WriteByte('\n')
			if strings.HasPrefix(line, end) {
				text.WriteString(line[4:])
				break
			}
			text.WriteString(line)
		}
		message = text.String()
	}
	if code == 421 {
		return code, message, c.fail(expect(code, message))
	}
	return code, message, nil
}

func (c *connection) command(command, argument string) (int, string, error) {
	if strings.ContainsAny(command+argument, "\x00\r\n") {
		return 0, "", core.Fail(core.InvalidRequest, errors.New("invalid FTP command argument"))
	}
	line := command
	if argument != "" {
		line += " " + argument
	}
	if len(line) > c.options.MaxLineBytes-2 {
		return 0, "", core.Fail(core.InvalidRequest, errors.New("FTP command exceeds line budget"))
	}
	line += "\r\n"
	n, err := io.WriteString(c.control, line)
	if err == nil && n != len(line) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return 0, "", c.fail(err)
	}
	return c.reply()
}

func passivePort(message string, extended bool) (uint16, error) {
	start, end := strings.IndexByte(message, '('), strings.LastIndexByte(message, ')')
	if start < 0 || end <= start {
		return 0, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid FTP passive endpoint"))
	}
	value := message[start+1 : end]
	var port uint64
	if extended {
		if len(value) < 5 || value[0] < 33 || value[0] > 126 {
			return 0, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		parts := strings.Split(value, value[:1])
		if len(parts) != 5 || parts[0] != "" || parts[1] != "" || parts[2] != "" || parts[4] != "" {
			return 0, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		var err error
		port, err = strconv.ParseUint(parts[3], 10, 16)
		if err != nil {
			return 0, core.Fail(core.InvalidUpstreamResponse, err)
		}
	} else {
		parts := strings.Split(value, ",")
		if len(parts) != 6 {
			return 0, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		for i, part := range parts {
			n, err := strconv.ParseUint(part, 10, 8)
			if err != nil {
				return 0, core.Fail(core.InvalidUpstreamResponse, err)
			}
			if i == 4 {
				port = n * 256
			} else if i == 5 {
				port += n
			}
		}
	}
	if port == 0 {
		return 0, core.Fail(core.InvalidUpstreamResponse, errors.New("zero FTP passive port"))
	}
	return uint16(port), nil
}

// start owns one passive data endpoint. Both PASV and EPSV use the authenticated
// control peer, never an arbitrary address from a server reply.
func (c *connection) start(ctx context.Context, command, argument string) (data net.Conn, err error) {
	code, message, err := c.command("EPSV", "")
	if err != nil {
		return nil, err
	}
	extended := true
	if code == 500 || code == 502 || code == 504 {
		extended = false
		code, message, err = c.command("PASV", "")
		if err != nil {
			return nil, err
		}
	}
	want := 227
	if extended {
		want = 229
	}
	if err = expect(code, message, want); err != nil {
		return nil, err
	}
	port, err := passivePort(message, extended)
	if err != nil {
		return nil, c.fail(err)
	}
	host, _, err := net.SplitHostPort(c.control.RemoteAddr().String())
	if err != nil {
		return nil, c.fail(err)
	}
	data, err = (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(int(port))))
	if err != nil {
		return nil, c.fail(err)
	}
	if err = c.adoptData(data); err != nil {
		return nil, c.fail(err)
	}
	code, message, err = c.command(command, argument)
	if err == nil {
		err = expect(code, message, 125, 150)
		if err != nil && code >= 400 {
			// A complete refusal has no pending transfer reply. This permits a
			// known unsupported MLSD to fall back to LIST on the same session.
			if failure := c.closeData(); failure != nil {
				return nil, c.fail(errors.Join(err, failure))
			}
			return nil, err
		}
	}
	if err != nil {
		return nil, c.fail(err)
	}
	if c.options.Security != Plain {
		secured := tls.Client(data, c.options.TLSConfig)
		if err = c.replaceData(data, secured); err != nil {
			return nil, c.fail(err)
		}
		if err = secured.HandshakeContext(ctx); err != nil {
			return nil, c.fail(err)
		}
		data = secured
	}
	return data, nil
}

// finish is called only after normal data EOF. Missing/failed final confirmation
// retires the exclusive session so its reply cannot reach a later borrower.
func (c *connection) finish() error {
	if err := c.control.SetDeadline(time.Now().Add(c.options.CleanupTimeout)); err != nil {
		return c.fail(err)
	}
	closeErr := c.closeData()
	code, message, err := c.reply()
	if err == nil {
		err = expect(code, message, 226, 250)
	}
	if err == nil {
		err = c.control.SetDeadline(time.Time{})
	}
	if err = errors.Join(closeErr, err); err != nil {
		return c.fail(err)
	}
	return nil
}
