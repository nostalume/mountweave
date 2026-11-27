package pan115check

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"iter"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
)

const (
	maxReceipts        = 64
	max115ReceiptBytes = 512 << 10
)

type fileObservation115 struct {
	mu       sync.Mutex
	wire     map[network.RequestID]fileExchange115
	finished recentEvents[network.EventLoadingFinished]
	statuses recentEvents[fileStatus115]
	writes   recentEvents[fileExchange115]
	ready    chan fileExchange115
	receipts recentEvents[fileReceipt115]
}

type fileStatus115 struct {
	id   network.RequestID
	code int64
}

type fileExchange115 struct {
	id           network.RequestID
	loader       cdp.LoaderID
	operation    string
	responseOp   string
	parent       string
	started      cdp.MonotonicTime
	acknowledged cdp.MonotonicTime
	completed    cdp.MonotonicTime
	invalid      bool
	complete     bool // The request asks for an unfiltered first page.
}

type fileReceipt115 struct {
	fileExchange115
	file      string
	directory string // Move destination or delete source.
	name      [sha256.Size]byte
	files     map[string][sha256.Size]byte
	rejected  bool
}

// Observe starts passive observation on the 115 web portal. Call the returned
// function to stop it and obtain the anonymous results. Callers own the tab
// and enable the Network domain before starting observation.
func Observe(ctx context.Context, entry *url.URL) func() []string {
	const portal = "https://f.115.com:443"
	if requestOrigin(entry) != portal {
		return nil
	}
	return observe(ctx, portal, "https://webapi.115.com:443")
}

func observe(ctx context.Context, portal, service string) func() []string {
	tree, err := chromedp.Call(ctx, page.GetFrameTree, cdp.Empty{})
	if err != nil || tree.FrameTree == nil || tree.FrameTree.Frame == nil {
		return nil
	}
	frame := tree.FrameTree.Frame.ID
	listen, stop := context.WithCancel(ctx)
	var listeners sync.WaitGroup
	o := &fileObservation115{wire: make(map[network.RequestID]fileExchange115), ready: make(chan fileExchange115, 16)}
	endpoint := func(raw string) (*url.URL, string) {
		u, _ := url.Parse(raw)
		if requestOrigin(u) == service {
			switch u.EscapedPath() {
			case "/files":
				return u, "list"
			case "/files/batch_rename":
				return u, "rename"
			case "/files/move":
				return u, "move"
			case "/rb/delete":
				return u, "delete"
			}
		}
		return nil, ""
	}
	watch(&listeners, chromedp.Events(listen, network.RequestWillBeSent), func(ev network.EventRequestWillBeSent) {
		if ev.FrameID != frame || ev.LoaderID == "" || ev.Request == nil {
			return
		}
		document, _ := url.Parse(ev.DocumentURL)
		u, operation := endpoint(ev.Request.URL)
		if requestOrigin(document) != portal || u == nil {
			return
		}
		parent := ""
		complete := false
		if operation == "list" {
			query, err := url.ParseQuery(u.RawQuery)
			if ev.Request.Method != http.MethodGet || err != nil || len(query["aid"]) != 1 || query.Get("aid") != "1" ||
				len(query["cid"]) != 1 || !valid115ID(query.Get("cid"), true) {
				return
			}
			parent = query.Get("cid")
			complete = fullList115(query)
		} else if ev.Request.Method != http.MethodPost {
			return
		}
		o.update(ev.RequestID, func(w *fileExchange115) {
			w.loader, w.operation, w.parent, w.started = ev.LoaderID, operation, parent, ev.Timestamp
			w.invalid = w.invalid || ev.RedirectResponse != nil
			w.complete = complete
			if operation != "list" && ev.RedirectResponse == nil {
				o.writes.add(*w)
			}
		})
	})
	watch(&listeners, chromedp.Events(listen, network.ResponseReceived), func(ev network.EventResponseReceived) {
		resp := ev.Response
		if ev.FrameID != frame || resp == nil {
			return
		}
		_, operation := endpoint(resp.URL)
		if operation == "" {
			return
		}
		o.update(ev.RequestID, func(w *fileExchange115) {
			w.acknowledged, w.responseOp = ev.Timestamp, operation
			w.invalid = w.invalid || !ev.HasExtraInfo || resp.Status != http.StatusOK || resp.FromDiskCache || resp.FromServiceWorker || resp.FromPrefetchCache
		})
	})
	watch(&listeners, chromedp.Events(listen, network.LoadingFinished), func(ev network.EventLoadingFinished) {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.finished.add(ev)
		o.enqueue(o.wire[ev.RequestID])
	})
	watch(&listeners, chromedp.Events(listen, network.ResponseReceivedExtraInfo), func(ev network.EventResponseReceivedExtraInfo) {
		o.mu.Lock()
		defer o.mu.Unlock()
		// The wire status distinguishes an upstream 200 from a cached 200 backed by a 304.
		o.statuses.add(fileStatus115{ev.RequestID, ev.StatusCode})
		o.enqueue(o.wire[ev.RequestID])
	})
	listeners.Go(func() {
		for {
			select {
			case <-listen.Done():
				return
			case w := <-o.ready:
				ctx, cancel := context.WithTimeout(listen, 2*time.Second)
				if receipt, ok := read115Receipt(ctx, w); ok {
					o.receipts.add(receipt)
				}
				cancel()
			}
		}
	})
	return sync.OnceValue(func() []string {
		stop()
		listeners.Wait()
		return o.result()
	})
}

func (o *fileObservation115) update(id network.RequestID, apply func(*fileExchange115)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	w, exists := o.wire[id]
	if !exists && len(o.wire) >= maxReceipts {
		clear(o.wire)
	}
	w.id = id
	apply(&w)
	o.wire[id] = w
	o.enqueue(w)
}

func (o *fileObservation115) enqueue(w fileExchange115) {
	if w.invalid {
		delete(o.wire, w.id)
		return
	}
	if w.started == 0 || w.acknowledged <= w.started || w.operation != w.responseOp {
		return
	}
	var status int64
	for _, reply := range o.statuses.ordered() {
		if reply.id == w.id {
			status = reply.code
			if status != http.StatusOK {
				break
			}
		}
	}
	if status == 0 {
		return
	}
	for _, event := range o.finished.ordered() {
		if event.RequestID != w.id || event.Timestamp < w.started {
			continue
		}
		delete(o.wire, w.id)
		if status == http.StatusOK && event.EncodedDataLength <= max115ReceiptBytes {
			// Chromium can timestamp a short transfer's completion before its response event.
			w.completed = max(event.Timestamp, w.acknowledged)
			select {
			case o.ready <- w:
			default:
			}
		}
		return
	}
}

func read115Receipt(ctx context.Context, w fileExchange115) (fileReceipt115, bool) {
	body, err := chromedp.Call(ctx, network.GetResponseBody, network.GetResponseBodyParams{RequestID: w.id})
	if err != nil {
		return fileReceipt115{}, false
	}
	var form string
	if w.operation != "list" {
		post, err := chromedp.Call(ctx, network.GetRequestPostData, network.GetRequestPostDataParams{RequestID: w.id})
		if err != nil {
			return fileReceipt115{}, false
		}
		form = string(post.PostData)
	}
	return parse115Receipt(w, body.Body, form)
}

func parse115Receipt(w fileExchange115, body []byte, form string) (fileReceipt115, bool) {
	var response struct {
		State  *bool           `json:"state"`
		CID    json.Number     `json:"cid"`
		Count  json.Number     `json:"count"`
		Offset json.Number     `json:"offset"`
		Data   json.RawMessage `json:"data"`
	}
	if len(body) > max115ReceiptBytes || json.Unmarshal(body, &response) != nil || response.State == nil {
		return fileReceipt115{}, false
	}
	receipt := fileReceipt115{fileExchange115: w, rejected: !*response.State}
	if w.operation != "list" {
		if len(form) > 8192 {
			return fileReceipt115{}, false
		}
		fields, err := url.ParseQuery(form)
		if err != nil || len(fields["format"]) != 1 || fields.Get("format") != "json" {
			return fileReceipt115{}, false
		}
		return parse115Change(receipt, fields)
	}
	var files []struct {
		FID  json.Number `json:"fid"`
		CID  json.Number `json:"cid"`
		PID  json.Number `json:"pid"`
		Name string      `json:"n"`
	}
	if receipt.rejected || response.CID.String() != w.parent || json.Unmarshal(response.Data, &files) != nil || files == nil {
		return fileReceipt115{}, false
	}
	receipt.files = make(map[string][sha256.Size]byte)
	receipt.complete = w.complete && response.Offset.String() == "0" && response.Count.String() == strconv.Itoa(len(files))
	for _, file := range files {
		id := file.FID.String()
		if id == "" {
			receipt.complete = receipt.complete && valid115ID(file.CID.String(), false) && file.PID.String() == w.parent
			continue
		}
		if _, exists := receipt.files[id]; !valid115ID(id, false) || exists || file.CID.String() != w.parent || file.Name == "" {
			return fileReceipt115{}, false
		}
		receipt.files[id] = sha256.Sum256([]byte(file.Name))
	}
	return receipt, true
}

func parse115Change(receipt fileReceipt115, fields url.Values) (fileReceipt115, bool) {
	if receipt.operation == "rename" {
		if len(fields) == 2 {
			for key, values := range fields {
				if id, ok := strings.CutPrefix(key, "files_new_name["); ok && strings.HasSuffix(id, "]") &&
					valid115ID(strings.TrimSuffix(id, "]"), false) && len(values) == 1 && values[0] != "" {
					receipt.file, receipt.name = strings.TrimSuffix(id, "]"), sha256.Sum256([]byte(values[0]))
					return receipt, true
				}
			}
		}
		return fileReceipt115{}, false
	}
	for key, values := range fields {
		if len(values) != 1 {
			return fileReceipt115{}, false
		}
		switch key {
		case "format", "fid[0]", "pid":
		case "ignore_warn":
			if receipt.operation != "delete" || values[0] != "0" && values[0] != "1" {
				return fileReceipt115{}, false
			}
		default:
			return fileReceipt115{}, false
		}
	}
	receipt.file, receipt.directory = fields.Get("fid[0]"), fields.Get("pid")
	return receipt, valid115ID(receipt.file, false) && valid115ID(receipt.directory, true)
}

func fullList115(query url.Values) bool {
	if limit, err := strconv.Atoi(query.Get("limit")); err != nil || limit <= 0 || query.Get("offset") != "0" ||
		query.Get("type") != "0" || query.Get("show_dir") != "1" || query.Get("count_folders") != "1" {
		return false
	}
	for key, values := range query {
		if len(values) != 1 {
			return false
		}
		switch key {
		case "aid", "cid", "offset", "limit", "o", "asc", "natsort", "format", "count_folders", "custom_order", "fc_mix", "show_dir":
		case "type", "nf", "fc":
			if values[0] != "0" {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func valid115ID(id string, root bool) bool {
	value, err := strconv.ParseUint(id, 10, 64)
	return err == nil && (value != 0 || root) && strconv.FormatUint(value, 10) == id
}

func (o *fileObservation115) result() []string {
	if o == nil {
		return nil
	}
	receipts := o.receipts.ordered()
	writes := o.writes.ordered()
	finished := make(map[network.RequestID]cdp.MonotonicTime)
	for _, event := range o.finished.ordered() {
		finished[event.RequestID] = event.Timestamp
	}
	responses := make(map[network.RequestID]fileReceipt115)
	for _, receipt := range receipts {
		if receipt.file != "" {
			responses[receipt.id] = receipt
		}
	}
	confirmed, unconfirmed := map[string]bool{}, map[string]bool{}
	for _, change := range receipts {
		if change.file == "" || change.rejected {
			continue
		}
		var before fileReceipt115
		for _, list := range receipts {
			if _, ok := list.files[change.file]; ok && list.loader == change.loader &&
				list.completed < change.started && list.completed > before.completed {
				before = list
			}
		}
		var after cdp.MonotonicTime
		if before.parent != "" {
			for _, list := range receipts {
				if list.loader == change.loader && list.started > change.completed && (after == 0 || list.completed < after) &&
					change.matches(before, list) {
					after = list.completed
				}
			}
		}
		if o.writes.truncated && before.completed < writes[0].started {
			after = 0
		}
		if after != 0 {
			for _, other := range writes {
				if other.id == change.id || other.loader != change.loader || other.started > after {
					continue
				}
				if receipt, ok := responses[other.id]; ok {
					if receipt.rejected || receipt.file != change.file || receipt.completed < before.completed {
						continue
					}
				} else if end := finished[other.id]; end >= other.started && end < before.completed {
					continue
				}
				after = 0
				break
			}
		}
		confirmed[change.operation] = confirmed[change.operation] || after != 0
		unconfirmed[change.operation] = unconfirmed[change.operation] || after == 0
	}
	var result []string
	for _, operation := range []string{"rename", "move", "delete"} {
		if confirmed[operation] {
			label := operation + "_confirmed"
			if operation == "delete" {
				label = "delete_source_confirmed"
			}
			result = append(result, label)
		}
		if unconfirmed[operation] {
			result = append(result, operation+"_unconfirmed")
		}
	}
	return result
}

func (change fileReceipt115) matches(before, after fileReceipt115) bool {
	name, found := after.files[change.file]
	switch change.operation {
	case "rename":
		return found && after.parent == before.parent && before.files[change.file] != change.name && name == change.name
	case "move":
		return found && before.parent != change.directory && after.parent == change.directory
	case "delete":
		return !found && after.complete && before.parent == change.directory && after.parent == change.directory
	}
	return false
}

type recentEvents[T any] struct {
	items     [maxReceipts]T
	next      int
	count     int
	truncated bool
}

func (r *recentEvents[T]) add(event T) {
	r.items[r.next] = event
	r.next = (r.next + 1) % len(r.items)
	r.truncated = r.truncated || r.count == len(r.items)
	r.count = min(r.count+1, len(r.items))
}

func (r *recentEvents[T]) ordered() []T {
	if !r.truncated {
		return r.items[:r.count]
	}
	return slices.Concat(r.items[r.next:], r.items[:r.next])
}

func requestOrigin(u *url.URL) string {
	if u == nil || u.User != nil || u.Hostname() == "" || u.Opaque != "" || u.Scheme != "http" && u.Scheme != "https" {
		return ""
	}
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return u.Scheme + "://" + net.JoinHostPort(strings.ToLower(u.Hostname()), port)
}

func watch[E any](listeners *sync.WaitGroup, events iter.Seq2[E, error], observe func(E)) {
	listeners.Go(func() {
		for event, err := range events {
			if err != nil {
				return
			}
			observe(event)
		}
	})
}
