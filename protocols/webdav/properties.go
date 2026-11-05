package webdav

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"

	"strconv"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/protocols/internal/httpwire"
)

type propertyQuery struct {
	names          []xml.Name
	all, namesOnly bool
}

var properties = []xml.Name{
	{Space: "DAV:", Local: "displayname"},
	{Space: "DAV:", Local: "resourcetype"},
	{Space: "DAV:", Local: "getcontentlength"},
	{Space: "DAV:", Local: "getlastmodified"},
	{Space: "DAV:", Local: "getcontenttype"},
	{Space: "DAV:", Local: "getetag"},
}

func propertyRequest(reader io.Reader) (query propertyQuery, err error) {
	decoder := xml.NewDecoder(reader)
	root := false
	selected := false
	for {
		token, e := decoder.Token()
		if e == io.EOF {
			if !root {
				query.all = true
				return query, nil
			}
			if !selected {
				return query, core.Fail(core.InvalidRequest, nil)
			}
			return query, nil
		}
		if e != nil {
			return query, errors.Join(core.Fail(core.InvalidRequest, nil), e)
		}
		switch t := token.(type) {
		case xml.StartElement:
			if !root {
				if t.Name != (xml.Name{Space: "DAV:", Local: "propfind"}) {
					return query, core.Fail(core.InvalidRequest, nil)
				}
				root = true
				continue
			}
			if selected || t.Name.Space != "DAV:" {
				return query, core.Fail(core.InvalidRequest, nil)
			}
			selected = true
			switch t.Name.Local {
			case "allprop", "propname":
				query.all = true
				query.namesOnly = t.Name.Local == "propname"
				if e = decoder.Skip(); e != nil {
					return query, errors.Join(core.Fail(core.InvalidRequest, nil), e)
				}
			case "prop":
				for {
					token, e = decoder.Token()
					if e != nil {
						return query, errors.Join(core.Fail(core.InvalidRequest, nil), e)
					}
					if _, end := token.(xml.EndElement); end {
						break
					}
					if name, start := token.(xml.StartElement); start {
						query.names = append(query.names, name.Name)
						if e = decoder.Skip(); e != nil {
							return query, errors.Join(core.Fail(core.InvalidRequest, nil), e)
						}
					}
				}
			default:
				return query, core.Fail(core.InvalidRequest, nil)
			}
		case xml.CharData:
			if strings.TrimSpace(string(t)) != "" {
				return query, core.Fail(core.InvalidRequest, nil)
			}
		}
	}
}

func (h *Handler) propfind(w http.ResponseWriter, r *http.Request, p core.Principal, a core.Address) {
	conditions, parseErr := httpwire.ParseConditions(r.Header)
	if parseErr != nil {
		h.failure(w, r, parseErr)
		return
	}
	depth := r.Header.Values("Depth")
	if len(depth) > 1 {
		fail(w, 400, "invalid-depth")
		return
	}
	value := "infinity"
	if len(depth) == 1 {
		value = strings.TrimSpace(depth[0])
	}
	if value == "infinity" {
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `<error xmlns="DAV:"><propfind-finite-depth/></error>`)
		return
	}
	if value != "0" && value != "1" {
		fail(w, 400, "invalid-depth")
		return
	}
	body := r.Body
	if body == nil {
		body = http.NoBody
	}
	query, err := propertyRequest(http.MaxBytesReader(w, body, h.options.Limits.XMLBytes))
	if err != nil {
		h.failure(w, r, err)
		return
	}
	ctx := r.Context()
	if value == "1" {
		selection, captureErr := h.files.Capture(ctx, core.AddressAccess{Address: a})
		if captureErr != nil {
			h.failure(w, r, captureErr)
			return
		}
		ctx = selection.Context(ctx)
	}
	entry, err := h.files.Stat(ctx, p, a, core.ReadSelection{When: conditions.When})
	if err != nil {
		h.failure(w, r, err)
		return
	}
	entries, err := h.entries(ctx, p, entry, value == "1")
	if err != nil {
		h.failure(w, r, err)
		return
	}
	if r.Context().Err() != nil {
		return
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	encoder := xml.NewEncoder(w)
	start := xml.StartElement{Name: xml.Name{Space: "DAV:", Local: "multistatus"}}
	if err = encoder.EncodeToken(start); err == nil {
		for _, e := range entries {
			if err = h.propertyResponse(encoder, e, query); err != nil {
				break
			}
		}
	}
	if err == nil {
		err = encoder.EncodeToken(start.End())
	}
	if err == nil {
		err = encoder.Flush()
	}
	if err != nil {
		panic(http.ErrAbortHandler)
	}
}

func propertyAvailable(e core.Entry, name xml.Name) bool {
	if name.Space != "DAV:" {
		return false
	}
	switch name.Local {
	case "displayname", "resourcetype":
		return true
	case "getcontentlength":
		return e.Kind == core.File && e.Size != nil
	case "getlastmodified":
		return e.ModifiedAt != nil
	case "getcontenttype":
		return e.Kind == core.File
	case "getetag":
		return e.Evidence.Validator.Valid()
	}
	return false
}

func (h *Handler) propertyResponse(encoder *xml.Encoder, entry core.Entry, query propertyQuery) error {
	response := xml.StartElement{Name: xml.Name{Space: "DAV:", Local: "response"}}
	if err := encoder.EncodeToken(response); err != nil {
		return err
	}
	if err := encoder.EncodeElement(h.href(entry.Address, "/", entry.Kind == core.Directory), xml.StartElement{Name: xml.Name{Space: "DAV:", Local: "href"}}); err != nil {
		return err
	}
	names := query.names
	if query.all {
		names = properties
	}
	for _, available := range []bool{true, false} {
		if !available && query.all {
			continue
		}
		var group []xml.Name
		for _, name := range names {
			if propertyAvailable(entry, name) == available {
				group = append(group, name)
			}
		}
		if len(group) == 0 {
			continue
		}
		stat := xml.StartElement{Name: xml.Name{Space: "DAV:", Local: "propstat"}}
		prop := xml.StartElement{Name: xml.Name{Space: "DAV:", Local: "prop"}}
		if err := encoder.EncodeToken(stat); err != nil {
			return err
		}
		if err := encoder.EncodeToken(prop); err != nil {
			return err
		}
		for _, name := range group {
			start := xml.StartElement{Name: name}
			if err := encoder.EncodeToken(start); err != nil {
				return err
			}
			if available && !query.namesOnly {
				var text string
				switch name.Local {
				case "displayname":
					text = entry.Name
				case "getcontentlength":
					text = strconv.FormatInt(*entry.Size, 10)
				case "getcontenttype":
					text = "application/octet-stream"
				case "getetag":
					text = httpwire.WireValidator(entry.Evidence.Validator)
				case "getlastmodified":
					text = entry.ModifiedAt.UTC().Format(http.TimeFormat)
				case "resourcetype":
					if entry.Kind == core.Directory {
						collection := xml.StartElement{Name: xml.Name{Space: "DAV:", Local: "collection"}}
						if err := encoder.EncodeToken(collection); err != nil {
							return err
						}
						if err := encoder.EncodeToken(collection.End()); err != nil {
							return err
						}
					}
				}
				if text != "" {
					if err := encoder.EncodeToken(xml.CharData(text)); err != nil {
						return err
					}
				}
			}
			if err := encoder.EncodeToken(start.End()); err != nil {
				return err
			}
		}
		if err := encoder.EncodeToken(prop.End()); err != nil {
			return err
		}
		status := "HTTP/1.1 200 OK"
		if !available {
			status = "HTTP/1.1 404 Not Found"
		}
		if err := encoder.EncodeElement(status, xml.StartElement{Name: xml.Name{Space: "DAV:", Local: "status"}}); err != nil {
			return err
		}
		if err := encoder.EncodeToken(stat.End()); err != nil {
			return err
		}
	}
	return encoder.EncodeToken(response.End())
}
