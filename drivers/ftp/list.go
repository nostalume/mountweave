package ftp

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	core "github.com/nostalume/mountweave"
)

func badListing(cause error) error { return core.Fail(core.InvalidUpstreamResponse, cause) }

// parseEntry interprets exported names, not the server's hidden filesystem.
// Unsupported records terminate enumeration instead of disappearing from it.
func parseEntry(line string, machine bool) (m core.Metadata, skip bool, err error) {
	var size string
	if machine {
		facts, name, ok := strings.Cut(line, " ")
		if !ok || !strings.HasSuffix(facts, ";") {
			return m, false, badListing(errors.New("invalid FTP facts record"))
		}
		m.Name = name
		seen := make(map[string]bool)
		for _, fact := range strings.Split(strings.TrimSuffix(facts, ";"), ";") {
			key, value, ok := strings.Cut(fact, "=")
			key = strings.ToLower(key)
			if !ok || key == "" || seen[key] {
				return m, false, badListing(errors.New("invalid or repeated FTP fact"))
			}
			seen[key] = true
			switch key {
			case "type":
				switch strings.ToLower(value) {
				case "file":
					m.Kind = core.File
				case "dir":
					m.Kind = core.Directory
				case "cdir", "pdir":
					return m, true, nil
				default:
					return m, false, core.Fail(core.UnsupportedOperation, errors.New("unsupported FTP entry type"))
				}
			case "size":
				if value == "" {
					return m, false, badListing(errors.New("empty FTP size fact"))
				}
				size = value
			case "modify":
				modified, failure := time.Parse("20060102150405.999999999", value)
				if failure != nil {
					return m, false, badListing(failure)
				}
				m.ModifiedAt = &modified
			}
		}
	} else if strings.HasPrefix(line, "+") {
		facts, name, ok := strings.Cut(line[1:], "\t")
		if !ok {
			return m, false, badListing(errors.New("invalid EPLF record"))
		}
		m.Name = name
		for _, fact := range strings.Split(facts, ",") {
			switch {
			case fact == "/":
				if m.Kind != "" {
					return m, false, badListing(errors.New("conflicting EPLF types"))
				}
				m.Kind = core.Directory
			case fact == "r":
				if m.Kind != "" {
					return m, false, badListing(errors.New("conflicting EPLF types"))
				}
				m.Kind = core.File
			case strings.HasPrefix(fact, "s"):
				size = fact[1:]
			}
		}
	} else {
		// Common Unix LIST has eight fields followed by one name separator.
		// Do not trim the remainder into a different, apparently valid name.
		if strings.HasPrefix(line, "total ") {
			_, failure := strconv.ParseUint(strings.TrimSpace(line[6:]), 10, 64)
			if failure == nil {
				return m, true, nil
			}
		}
		rest := line
		fields := make([]string, 0, 8)
		for range 8 {
			word, tail, found := strings.Cut(rest, " ")
			if !found || word == "" {
				return m, false, core.Fail(core.UnsupportedOperation, errors.New("unsupported FTP LIST format"))
			}
			fields = append(fields, word)
			rest = tail
			if len(fields) < 8 {
				rest = strings.TrimLeft(rest, " ")
			}
		}
		if len(fields[0]) < 10 || rest == "" || rest[0] == ' ' {
			return m, false, core.Fail(core.UnrepresentableName, errors.New("ambiguous FTP LIST name"))
		}
		m.Name = rest
		switch fields[0][0] {
		case '-':
			m.Kind = core.File
		case 'd':
			m.Kind = core.Directory
		default:
			m.Name = "" // Legacy special-entry text may append a link target.
			return m, false, core.Fail(core.UnsupportedOperation, errors.New("unsupported FTP LIST entry type"))
		}
		m.Name, size = rest, fields[4]
	}
	if m.Name == "." || m.Name == ".." {
		return m, true, nil
	}
	if !core.ValidName(m.Name) {
		return m, false, core.Fail(core.UnrepresentableName, nil)
	}
	if m.Kind != core.File && m.Kind != core.Directory {
		return m, false, core.Fail(core.UnsupportedOperation, errors.New("FTP did not supply the entry type"))
	}
	if size != "" && m.Kind == core.File {
		value, failure := strconv.ParseInt(size, 10, 64)
		if failure != nil || value < 0 {
			return m, false, badListing(failure)
		}
		m.Size = &value
	}
	return m, false, nil
}

func (d *Driver) listing(ctx context.Context, c *connection) (net.Conn, bool, error) {
	code, message, err := c.command("TYPE", "A")
	if err == nil {
		err = expect(code, message, 200)
	}
	if err != nil {
		return nil, false, err
	}
	command := "LIST"
	if c.mlsd {
		command = "MLSD"
	}
	data, err := c.start(ctx, command, "")
	if core.CodeOf(err) == core.UnsupportedOperation && command == "MLSD" && !c.closed.Load() {
		c.mlsd = false
		data, err = c.start(ctx, "LIST", "")
	}
	if err != nil {
		return nil, false, err
	}
	return data, c.mlsd, nil
}

// locate leaves the session in the leaf's parent. Each parent enumeration is
// completed, including its final reply, before another command uses the session.
func (d *Driver) locate(ctx context.Context, c *connection, p string, parent bool) (core.Metadata, error) {
	if p == "/" {
		return core.Metadata{Kind: core.Directory}, nil
	}
	parts := strings.Split(p[1:], "/")
	for index, name := range parts {
		if parent && index == len(parts)-1 {
			return core.Metadata{Name: name}, nil
		}
		data, machine, err := d.listing(ctx, c)
		if err != nil {
			return core.Metadata{}, err
		}
		var found *core.Metadata
		reader := bufio.NewReader(data)
		for {
			line, failure := readLine(reader, d.options.MaxLineBytes)
			if failure == io.EOF {
				err = c.finish()
				break
			}
			if failure != nil {
				return core.Metadata{}, c.fail(failure)
			}
			item, skip, failure := parseEntry(line, machine)
			if failure != nil {
				// An unrelated, named special entry need not prevent locating a
				// regular sibling. Full enumeration still reports its limitation.
				if core.CodeOf(failure) == core.UnsupportedOperation && core.ValidName(item.Name) && item.Name != name {
					continue
				}
				return core.Metadata{}, c.fail(failure)
			}
			if !skip && item.Name == name {
				if found != nil {
					return core.Metadata{}, c.fail(badListing(errors.New("duplicate FTP entry name")))
				}
				found = &item
			}
		}
		if err != nil {
			return core.Metadata{}, err
		}
		if found == nil {
			return core.Metadata{}, core.Fail(core.NotFound, nil)
		}
		if index == len(parts)-1 {
			return *found, nil
		}
		if found.Kind != core.Directory {
			return core.Metadata{}, core.Fail(core.NotDirectory, nil)
		}
		code, message, failure := c.command("CWD", name)
		if failure == nil {
			failure = expect(code, message, 250)
		}
		if failure != nil {
			return core.Metadata{}, failure
		}
	}
	panic("unreachable FTP path")
}

func readSelection(selections []core.ReadSelection) (core.ReadSelection, error) {
	if len(selections) > 1 {
		return core.ReadSelection{}, core.Fail(core.InvalidRequest, nil)
	}
	var selection core.ReadSelection
	if len(selections) == 1 {
		selection = selections[0]
	}
	if err := selection.When.Validate(); err != nil {
		return selection, err
	}
	if selection.Version != "" || !selection.When.Empty() && !selection.When.IsPresent() && !selection.When.IsAbsent() {
		return selection, core.Fail(core.UnsupportedOperation, nil)
	}
	return selection, nil
}

func (d *Driver) Stat(ctx context.Context, p core.NativeAddress, selections ...core.ReadSelection) (meta core.Metadata, err error) {
	selection, err := readSelection(selections)
	if err != nil {
		return meta, err
	}
	if err = d.admit(p); err != nil {
		return meta, err
	}
	c, err := d.borrow(ctx)
	if err != nil {
		return meta, err
	}
	end := c.scope(ctx)
	defer func() { err = errors.Join(err, end(), ctx.Err(), d.put(c)) }()
	meta, err = d.locate(ctx, c, p.Value(), false)
	if err == nil {
		err = selection.Check(meta.Evidence)
	}
	return meta, err
}

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (result core.Iterator, err error) {
	if q.Delimiter() != "" || q.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	if err = d.admit(q.Address()); err != nil {
		return nil, err
	}
	c, err := d.borrow(ctx)
	if err != nil {
		return nil, err
	}
	end := c.scope(ctx)
	meta, err := d.locate(ctx, c, q.Address().Value(), false)
	if err == nil && meta.Kind != core.Directory {
		err = core.Fail(core.NotDirectory, nil)
	}
	if err == nil && meta.Name != "" {
		code, message, failure := c.command("CWD", meta.Name)
		err = failure
		if err == nil {
			err = expect(code, message, 250)
		}
	}
	var listing *iterator
	if err == nil {
		var data net.Conn
		var machine bool
		data, machine, err = d.listing(ctx, c)
		if err == nil {
			listing = &iterator{stream: &stream{driver: d, conn: c, data: data}, reader: bufio.NewReader(data), machine: machine}
		}
	}
	err = errors.Join(err, end(), ctx.Err())
	if err != nil {
		return nil, errors.Join(err, c.Close(), d.put(c))
	}
	return listing, nil
}

type iterator struct {
	*stream
	reader  *bufio.Reader
	machine bool
}

func (i *iterator) Next(ctx context.Context, n int) (items []core.Metadata, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if i.closing.Load() {
		return nil, io.ErrClosedPipe
	}
	if i.done {
		return nil, i.terminal
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	stop := i.conn.scope(ctx)
	end := func() error { return errors.Join(stop(), ctx.Err()) }
	for len(items) < min(n, i.driver.options.PageEntries) {
		line, failure := readLine(i.reader, i.driver.options.MaxLineBytes)
		if failure != nil {
			return items, i.finish(failure, end)
		}
		item, skip, failure := parseEntry(line, i.machine)
		if failure != nil {
			return items, i.finish(failure, end)
		}
		if !skip {
			items = append(items, item)
		}
	}
	if err = end(); err != nil {
		err = i.finish(err, nil)
	}
	return items, err
}
