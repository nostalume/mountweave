package webdav

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"

	core "github.com/nostalume/mountweave"
)

const properties = `<propfind xmlns="DAV:"><prop><resourcetype/><getcontentlength/><getlastmodified/><getetag/></prop></propfind>`

type propertySet struct {
	Type *struct {
		Members []struct{ XMLName xml.Name } `xml:",any"`
	} `xml:"DAV: resourcetype"`
	Size     *string `xml:"DAV: getcontentlength"`
	Modified *string `xml:"DAV: getlastmodified"`
	ETag     *string `xml:"DAV: getetag"`
}

type responseItem struct {
	Hrefs  []string `xml:"DAV: href"`
	Status string   `xml:"DAV: status"`
	Props  []struct {
		Prop   propertySet `xml:"DAV: prop"`
		Status string      `xml:"DAV: status"`
	} `xml:"DAV: propstat"`
}

func responseStatus(text string) int {
	fields := strings.Fields(text)
	if len(fields) < 2 || !strings.HasPrefix(fields[0], "HTTP/") {
		return 0
	}
	code, _ := strconv.Atoi(fields[1])
	return code
}

func (r responseItem) metadata() (core.Metadata, error) {
	m := core.Metadata{Kind: core.File}
	if r.Status != "" && responseStatus(r.Status) != http.StatusOK {
		return m, statusError("PROPFIND", responseStatus(r.Status))
	}
	typed := false
	for _, p := range r.Props {
		code := responseStatus(p.Status)
		if code != http.StatusOK {
			if p.Prop.Type == nil {
				switch code {
				case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
					continue
				}
			} else if code == http.StatusNotFound {
				return m, core.Fail(core.UnsupportedOperation, errors.New("WebDAV resource type property is missing"))
			}
			return m, statusError("PROPFIND property", code)
		}
		if p.Prop.Type != nil {
			if typed {
				return m, core.Fail(core.InvalidUpstreamResponse, errors.New("duplicate resource type"))
			}
			typed = true
			for _, member := range p.Prop.Type.Members {
				if member.XMLName != (xml.Name{Space: "DAV:", Local: "collection"}) {
					return m, core.Fail(core.UnsupportedOperation, errors.New("unsupported WebDAV resource type"))
				}
				m.Kind = core.Directory
			}
		}
		if p.Prop.Size != nil {
			size, err := strconv.ParseInt(strings.TrimSpace(*p.Prop.Size), 10, 64)
			if err != nil || size < 0 {
				return m, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid WebDAV content length"))
			}
			m.Size = &size
		}
		if p.Prop.Modified != nil {
			modified, err := http.ParseTime(*p.Prop.Modified)
			if err != nil {
				return m, core.Fail(core.InvalidUpstreamResponse, err)
			}
			m.ModifiedAt = &modified
		}
		if p.Prop.ETag != nil {
			// Some servers return a raw object ID in this optional property.
			tag := core.Validator{Tag: *p.Prop.ETag}
			if tag.Valid() {
				m.Evidence.Validator = tag
			}
		}
	}
	if !typed {
		return m, core.Fail(core.UnsupportedOperation, errors.New("WebDAV resource type was not supplied"))
	}
	if m.Kind == core.Directory {
		m.Size = nil
	}
	return m, nil
}

// A response is kept only by its iterator. Neither the opening context nor a
// successful Next's context becomes the lifetime of a partially consumed page.
type iterator struct {
	driver                *Driver
	target, next, current *url.URL
	depth                 string
	life                  context.Context
	cancel                context.CancelFunc
	mu                    sync.Mutex
	response              *http.Response
	reader                *io.LimitedReader
	decoder               *xml.Decoder
	self                  bool
	done, closed          bool
	failed                error
}

func (d *Driver) enumeration(ctx context.Context, target *url.URL, depth string) *iterator {
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &iterator{driver: d, target: target, next: target, depth: depth, life: life, cancel: cancel}
}

func (i *iterator) closeResponse() error {
	if i.response == nil {
		return nil
	}
	if err := i.response.Body.Close(); err != nil {
		return unexpectedEOF(err)
	}
	i.response, i.reader, i.decoder = nil, nil, nil
	i.current = nil
	return nil
}

func (i *iterator) token(allowEOF bool) (xml.Token, error) {
	token, err := i.decoder.Token()
	if i.reader.N == 0 {
		return nil, core.Fail(core.ResponseBudgetExceeded, nil)
	}
	if err != nil && (!allowEOF || err != io.EOF) {
		return nil, core.Fail(core.InvalidUpstreamResponse, unexpectedEOF(err))
	}
	return token, err
}

func (i *iterator) Close() error {
	i.cancel() // Interrupt an active read before waiting for its lock.
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closed = true
	i.next = nil
	return i.closeResponse()
}

func (i *iterator) fetch() error {
	response, err := i.driver.request(i.life, "PROPFIND", i.next, strings.NewReader(properties), http.Header{
		"Depth": {i.depth}, "Content-Type": {"application/xml; charset=utf-8"},
	})
	if err != nil {
		return err
	}
	i.response = response
	i.current = response.Request.URL
	if response.StatusCode != http.StatusMultiStatus {
		return statusError("PROPFIND", response.StatusCode)
	}
	next, err := nextLink(response.Header)
	if err != nil {
		return err
	}
	i.next = nil
	if next != "" {
		if i.depth == "0" {
			return core.Fail(core.InvalidUpstreamResponse, nil)
		}
		nextURL, to, err := i.driver.href(i.current, next, true)
		if err != nil {
			return err
		}
		at, _ := uriPath(i.target)
		if at != to || nextURL.RawQuery == i.current.RawQuery && nextURL.ForceQuery == i.current.ForceQuery {
			return core.Fail(core.InvalidUpstreamResponse, errors.New("continuation changes the collection or stalls"))
		}
		// A continuation advances the query of the collection already reached.
		target := *i.current
		target.RawQuery, target.ForceQuery = nextURL.RawQuery, nextURL.ForceQuery
		i.next = &target
	}
	i.reader = &io.LimitedReader{R: response.Body, N: i.driver.options.MaxResponseBytes + 1}
	i.decoder = xml.NewDecoder(i.reader)
	for {
		token, err := i.token(false)
		if err != nil {
			return err
		}
		if start, ok := token.(xml.StartElement); ok {
			if start.Name != (xml.Name{Space: "DAV:", Local: "multistatus"}) {
				return core.Fail(core.InvalidUpstreamResponse, nil)
			}
			return nil
		}
		if text, ok := token.(xml.CharData); ok && strings.TrimSpace(string(text)) != "" {
			return core.Fail(core.InvalidUpstreamResponse, nil)
		}
	}
}

func (i *iterator) Next(ctx context.Context, n int) (items []core.Metadata, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if i.failed != nil {
		return nil, i.failed
	}
	if i.done {
		return nil, io.EOF
	}
	joined := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { i.cancel(); close(joined) })
	var closeErr error
	defer func() {
		if !stop() {
			<-joined
		}
		if cause := ctx.Err(); cause != nil {
			if err == io.EOF {
				err = nil
			}
			err = errors.Join(err, cause)
		}
		if err != nil && err != io.EOF {
			if closeErr == nil {
				err = errors.Join(err, i.closeResponse())
			}
			i.failed = err
		}
	}()
	if i.decoder == nil {
		if err = i.fetch(); err != nil {
			return nil, err
		}
	}
	parent, _ := uriPath(i.target)
	described := false
	for len(items) < n {
		token, failure := i.token(false)
		if failure != nil {
			return nil, failure
		}
		switch t := token.(type) {
		case xml.StartElement:
			if !described && t.Name == (xml.Name{Space: "DAV:", Local: "responsedescription"}) {
				described = true
				for {
					text, err := i.token(false)
					if err != nil {
						return nil, err
					}
					if _, end := text.(xml.EndElement); end {
						break
					}
					switch text.(type) {
					case xml.CharData, xml.Comment, xml.ProcInst:
					default:
						return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("invalid WebDAV response description"))
					}
				}
				continue
			}
			if described || t.Name != (xml.Name{Space: "DAV:", Local: "response"}) {
				return nil, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			var item responseItem
			failure = i.decoder.DecodeElement(&item, &t)
			if i.reader.N == 0 {
				return nil, core.Fail(core.ResponseBudgetExceeded, nil)
			}
			if failure != nil {
				return nil, core.Fail(core.InvalidUpstreamResponse, unexpectedEOF(failure))
			}
			if len(item.Hrefs) != 1 {
				return nil, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			_, at, failure := i.driver.href(i.current, item.Hrefs[0], false)
			if failure != nil {
				return nil, failure
			}
			meta, failure := item.metadata()
			if failure != nil {
				return nil, failure
			}
			if at == parent {
				i.self = true
				if i.depth == "0" {
					meta.Name = path.Base(at)
					if at == strings.TrimSuffix(i.driver.root.Path, "/") || i.driver.root.Path == "/" && at == "/" {
						meta.Name = ""
					}
					items = append(items, meta)
				} else if meta.Kind != core.Directory {
					return nil, core.Fail(core.NotDirectory, nil)
				}
				continue
			}
			if i.depth != "1" || path.Dir(at) != parent || !core.ValidName(path.Base(at)) {
				return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("response is not a direct child"))
			}
			meta.Name = path.Base(at)
			items = append(items, meta)
		case xml.EndElement:
			if t.Name != (xml.Name{Space: "DAV:", Local: "multistatus"}) {
				return nil, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			// Require normal document EOF, not just a closing tag followed by junk.
			for {
				trailing, e := i.token(true)
				if e == io.EOF {
					break
				}
				if e != nil {
					return nil, e
				}
				switch t := trailing.(type) {
				case xml.Comment, xml.ProcInst:
					continue
				case xml.CharData:
					if strings.TrimSpace(string(t)) == "" {
						continue
					}
				}
				return nil, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			if closeErr = i.closeResponse(); closeErr != nil {
				return nil, closeErr
			}
			if i.next == nil {
				if !i.self {
					return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("collection response is missing"))
				}
				i.done = true
				return items, io.EOF
			}
			return items, nil
		case xml.CharData:
			if strings.TrimSpace(string(t)) != "" {
				return nil, core.Fail(core.InvalidUpstreamResponse, nil)
			}
		}
	}
	return items, nil
}

func nextLink(header http.Header) (string, error) {
	var next string
	for _, value := range header.Values("Link") {
		for {
			part, bracket := strings.CutPrefix(strings.TrimLeft(value, " \t"), "<")
			target, rest, end := strings.Cut(part, ">")
			if !bracket || !end {
				return "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid WebDAV Link"))
			}
			var relation string
			var hasRelation, anchored bool
			value = strings.TrimLeft(rest, " \t")
			for strings.HasPrefix(value, ";") {
				name, param, rest, ok := linkParameter(value[1:])
				if !ok {
					return "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid WebDAV Link parameter"))
				}
				switch {
				case !hasRelation && strings.EqualFold(name, "rel"):
					relation, hasRelation = param, true
				case strings.EqualFold(name, "anchor"):
					anchored = true
				}
				value = rest
			}
			if !anchored {
				for _, rel := range strings.Fields(relation) {
					if !strings.EqualFold(rel, "next") {
						continue
					}
					if target == "" {
						return "", core.Fail(core.InvalidUpstreamResponse, errors.New("empty WebDAV continuation"))
					}
					if next != "" {
						return "", core.Fail(core.InvalidUpstreamResponse, errors.New("ambiguous WebDAV continuation"))
					}
					next = target
					break
				}
			}
			if value == "" {
				break
			}
			if value[0] != ',' {
				return "", core.Fail(core.InvalidUpstreamResponse, errors.New("invalid WebDAV Link separator"))
			}
			value = value[1:]
		}
	}
	return next, nil
}

// linkParameter consumes the token and quoted-string syntax of RFC 8288.
func linkParameter(text string) (name, value, rest string, ok bool) {
	name, rest = linkToken(strings.TrimLeft(text, " \t"))
	if name == "" {
		return "", "", "", false
	}
	rest = strings.TrimLeft(rest, " \t")
	if !strings.HasPrefix(rest, "=") {
		return name, "", rest, true
	}
	rest = strings.TrimLeft(rest[1:], " \t")
	if !strings.HasPrefix(rest, "\"") {
		value, rest = linkToken(rest)
		return name, value, strings.TrimLeft(rest, " \t"), value != ""
	}
	var unquoted strings.Builder
	for i := 1; i < len(rest); i++ {
		c := rest[i]
		if c == '"' {
			return name, unquoted.String(), strings.TrimLeft(rest[i+1:], " \t"), true
		}
		if c == '\\' {
			i++
			if i == len(rest) {
				break
			}
			c = rest[i]
		}
		if (c < ' ' && c != '\t') || c == 0x7f {
			break
		}
		unquoted.WriteByte(c)
	}
	return "", "", "", false
}

func linkToken(text string) (token, rest string) {
	rest = strings.TrimLeftFunc(text, func(c rune) bool {
		return c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || strings.ContainsRune("!#$%&'*+-.^_`|~", c)
	})
	return text[:len(text)-len(rest)], rest
}

func (d *Driver) Enumerate(ctx context.Context, query core.NativeQuery) (core.Iterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query.Delimiter() != "" || query.StartAfter() != "" {
		return nil, core.Fail(core.UnsupportedOperation, nil)
	}
	target, err := d.target(query.Address())
	if err != nil {
		return nil, err
	}
	return d.enumeration(ctx, target, "1"), nil
}
