package uc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	browserio "github.com/chromedp/cdproto/io"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
	transport "github.com/nostalume/mountweave/browser"
)

func openBrowserDownload(tab context.Context, call WebsiteCall, account, origin, endpoint string) ucOpenBody {
	slot := make(chan struct{}, 1)
	broken := false // read and written only while holding the slot
	return func(ctx context.Context, file ucFile, opts core.OpenOptions) (_ core.Opened, err error) {
		var wanted *core.ContentRange
		if opts.Range != nil {
			actual, err := opts.Range.Resolve(*file.meta.Size)
			if err != nil {
				return core.Opened{}, err
			}
			wanted = &actual
		} else if *file.meta.Size == 0 {
			return core.Opened{}, core.Fail(core.UnsupportedOperation, errors.New("UC completes empty downloads locally in its website"))
		}
		if file.route == "" {
			return core.Opened{}, core.Fail(core.UnsupportedOperation, errors.New("UC could not address the containing folder in its website"))
		}
		select {
		case slot <- struct{}{}:
		case <-ctx.Done():
			return core.Opened{}, ctx.Err()
		case <-tab.Done():
			return core.Opened{}, core.Fail(core.MountUnavailable, nil)
		}
		if err := ctx.Err(); err != nil {
			<-slot
			return core.Opened{}, err
		}
		if broken {
			<-slot
			return core.Opened{}, core.Fail(core.MountUnavailable, errors.New("UC browser download session needs to be reopened"))
		}
		life, cancel := context.WithCancel(tab)
		stop := context.AfterFunc(ctx, cancel)
		setup, stopSetup := context.WithTimeout(life, 25*time.Second)
		defer stopSetup()

		var handle browserio.StreamHandle
		var grantRequest, bodyRequest, paused fetch.RequestID
		var address string
		enabled := false
		closeStream := sync.OnceValue(func() error {
			stop()
			cancel()
			defer func() { <-slot }()
			if !enabled || tab.Err() != nil {
				return nil
			}
			cleanup, finish := context.WithTimeout(tab, 2*time.Second)
			defer finish()
			var closeErr error
			if handle != "" {
				if _, err := chromedp.Call(cleanup, browserio.Close, browserio.CloseParams{Handle: handle}); err != nil {
					closeErr = core.Fail(core.UpstreamFailure, errors.New("UC browser attachment stream did not close"))
				}
			}
			if paused != "" {
				_ = abortUCRequest(cleanup, paused)
			}
			if _, err := chromedp.Call(cleanup, fetch.Disable, cdp.Empty{}); err != nil {
				broken = true
				return errors.Join(closeErr, core.Fail(core.UpstreamFailure, errors.New("UC browser download observation did not close")))
			}
			if address != "" {
				raw, _ := json.Marshal(address)
				_, _ = call(cleanup, `async address => {
					for (const frame of document.querySelectorAll("iframe"))
						if (frame.src === address) frame.remove();
					return "{}";
				}`, raw)
			}
			return closeErr
		})
		transferred := false
		defer func() {
			if !transferred {
				err = errors.Join(err, closeStream())
			}
		}()

		if _, err := chromedp.CallBrowser(setup, browser.SetDownloadBehavior, browser.SetDownloadBehaviorParams{
			Behavior: browser.SetDownloadBehaviorBehaviorDeny,
		}); err != nil {
			return core.Opened{}, transport.TransferFailure(ctx, setup, "UC browser could not disable automatic saving")
		}
		if _, err := chromedp.Call(setup, network.Enable, network.EnableParams{}); err != nil {
			return core.Opened{}, transport.TransferFailure(ctx, setup, "UC browser could not observe the download request")
		}
		events := chromedp.Events(setup, fetch.RequestPaused)
		patterns := []*fetch.RequestPattern{
			{URLPattern: endpoint + "*", RequestStage: fetch.RequestStageRequest},
			{ResourceType: network.ResourceTypeDocument, RequestStage: fetch.RequestStageRequest},
		}
		enabled = true // also release observation when the Enable receipt is lost
		if _, err := chromedp.Call(setup, fetch.Enable, fetch.EnableParams{Patterns: patterns}); err != nil {
			return core.Opened{}, transport.TransferFailure(ctx, setup, "UC browser could not observe the download response")
		}

		action := make(chan error, 1)
		go func() {
			err := clickUCMountedDownload(setup, call, origin, file)
			action <- err
			if err != nil {
				stopSetup()
			}
		}()
		defer func() {
			stopSetup()
			if actionErr := <-action; actionErr != nil && life.Err() == nil {
				err = errors.Join(err, actionErr)
			}
		}()

		for event, eventErr := range events {
			if eventErr != nil {
				break
			}
			paused = event.RequestID
			if event.Request == nil {
				return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, nil)
			}
			request := event.Request
			grant := request.URL == endpoint || strings.HasPrefix(request.URL, endpoint+"?")
			// Chromium can report an OPTIONS preflight with the caller's resource type.
			// Keep its CORS exchange intact, then validate the actual POST independently.
			if grant && request.Method == http.MethodOptions && event.ResponseStatusCode == 0 && event.ResponseErrorReason == "" {
				fromWebsite, requestsPOST := false, false
				for name, value := range request.Headers {
					fromWebsite = fromWebsite || strings.EqualFold(name, "Origin") && value == origin
					requestsPOST = requestsPOST || strings.EqualFold(name, "Access-Control-Request-Method") && value == http.MethodPost
				}
				grant = !fromWebsite || !requestsPOST
			}
			body := address != "" && request.URL == address
			if event.RedirectedRequestID != "" ||
				event.ResourceType == network.ResourceTypeDocument && !body && !strings.HasPrefix(request.URL, origin+"/") {
				if err := abortUCRequest(setup, event.RequestID); err != nil {
					return core.Opened{}, transport.TransferFailure(ctx, setup, "UC browser could not stop an unexpected request")
				}
				paused = ""
				if event.RedirectedRequestID != "" && (event.RedirectedRequestID == grantRequest || event.RedirectedRequestID == bodyRequest) {
					return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("UC redirected the selected download"))
				}
				continue
			}

			switch {
			case grant:
				if request.Method != http.MethodPost || event.ResponseErrorReason != "" {
					return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, nil)
				}
				if event.ResponseStatusCode == 0 {
					if grantRequest != "" || !ucSelectedDownload(setup, &event, file.id) {
						return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("UC download did not match the selected file"))
					}
					grantRequest = event.RequestID
					if _, err := readUCMetadata(setup, call, ucMetadataRequest{Operation: "account", Account: account}); err != nil {
						return core.Opened{}, err
					}
					if err := continueUCRequest(setup, &event, true, nil); err != nil {
						return core.Opened{}, transport.TransferFailure(ctx, setup, "UC browser could not request the download")
					}
					paused = ""
					continue
				}
				if event.RequestID != grantRequest || address != "" {
					return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, nil)
				}
				address, err = ucDownloadAddress(setup, &event, origin, file)
				if err != nil {
					return core.Opened{}, err
				}
				// Correlate the signed URL before releasing the website's grant response.
				if _, err := chromedp.Call(setup, fetch.ContinueResponse, fetch.ContinueResponseParams{RequestID: grantRequest}); err != nil {
					return core.Opened{}, transport.TransferFailure(ctx, setup, "UC browser could not release the download receipt")
				}
			case body:
				if request.Method != http.MethodGet || event.ResourceType != network.ResourceTypeDocument ||
					bodyRequest != "" && event.RequestID != bodyRequest || event.ResponseErrorReason != "" {
					return core.Opened{}, core.Fail(core.InvalidUpstreamResponse, errors.New("UC did not load the selected file as its original attachment"))
				}
				bodyRequest = event.RequestID
				if event.ResponseStatusCode == 0 {
					if err := continueUCRequest(setup, &event, true, wanted); err != nil {
						return core.Opened{}, transport.TransferFailure(ctx, setup, "UC browser could not open the attachment")
					}
					paused = ""
					continue
				}
				headers := transport.ResponseHeaders(event.ResponseHeaders)
				opened, err := ucDownloadMetadata(event.ResponseStatusCode, headers, file, wanted)
				if err != nil {
					return core.Opened{}, err
				}
				stream, err := chromedp.Call(setup, fetch.TakeResponseBodyAsStream, fetch.TakeResponseBodyAsStreamParams{RequestID: bodyRequest})
				if err != nil || stream.Stream == "" {
					return core.Opened{}, transport.TransferFailure(ctx, setup, "UC browser did not expose the attachment stream")
				}
				handle = stream.Stream
				remaining := *file.meta.Size
				if opened.Length != nil {
					remaining = *opened.Length
				}
				opened.Body = transport.NewBody(life, handle, closeStream,
					errors.New("UC browser attachment could not be read"), remaining, wanted != nil && headers.Get("Content-Length") == "")
				transferred = true
				return opened, nil
			default:
				if err := continueUCRequest(setup, &event, false, nil); err != nil {
					return core.Opened{}, transport.TransferFailure(ctx, setup, "UC browser could not continue a website request")
				}
			}
			paused = ""
		}
		return core.Opened{}, transport.TransferFailure(ctx, setup, "UC browser did not receive the selected attachment")
	}
}

func continueUCRequest(ctx context.Context, event *fetch.EventRequestPaused, observe bool, wanted *core.ContentRange) error {
	params := fetch.ContinueRequestParams{RequestID: event.RequestID}
	if observe {
		params.InterceptResponse = &observe
	}
	if wanted != nil {
		params.Headers = transport.RangeHeaders(event.Request.Headers, fmt.Sprintf("bytes=%d-%d", wanted.Start, wanted.End))
	}
	_, err := chromedp.Call(ctx, fetch.ContinueRequest, params)
	return err
}

func abortUCRequest(ctx context.Context, request fetch.RequestID) error {
	_, err := chromedp.Call(ctx, fetch.FailRequest, fetch.FailRequestParams{
		RequestID: request, ErrorReason: network.ErrorReasonBlockedByClient,
	})
	return err
}

func ucSelectedDownload(ctx context.Context, event *fetch.EventRequestPaused, fid string) bool {
	var raw []byte
	if entries := event.Request.PostDataEntries; len(entries) == 1 && entries[0] != nil {
		raw = entries[0].Bytes
	}
	if len(raw) == 0 && event.NetworkID != "" {
		data, err := chromedp.Call(ctx, network.GetRequestPostData, network.GetRequestPostDataParams{RequestID: event.NetworkID})
		if err != nil {
			return false
		}
		raw = data.PostData
	}
	var object map[string]json.RawMessage
	if len(raw) > 4096 || json.Unmarshal(raw, &object) != nil {
		return false
	}
	for key := range object {
		if key != "fids" && key != "_t_group" {
			return false
		}
	}
	var fids []string
	return json.Unmarshal(object["fids"], &fids) == nil && len(fids) == 1 && fids[0] == fid
}

func ucDownloadAddress(ctx context.Context, event *fetch.EventRequestPaused, origin string, file ucFile) (string, error) {
	if err := ucDownloadStatus(event.ResponseStatusCode); err != nil {
		return "", err
	}
	bad := core.Fail(core.InvalidUpstreamResponse, errors.New("UC did not return a matching download receipt"))
	if event.ResponseStatusCode != http.StatusOK {
		return "", bad
	}
	for _, header := range event.ResponseHeaders {
		if strings.EqualFold(header.Name, "Content-Length") {
			size, err := strconv.ParseInt(header.Value, 10, 64)
			if err != nil || size < 0 || size > 32<<10 {
				return "", bad
			}
		}
	}
	body, err := chromedp.Call(ctx, fetch.GetResponseBody, fetch.GetResponseBodyParams{RequestID: event.RequestID})
	var receipt struct {
		Code, Status json.RawMessage
		Data         []struct {
			FID  string `json:"fid"`
			Size *int64 `json:"size"`
			URL  string `json:"download_url"`
		}
	}
	if err != nil || len(body.Body) > 32<<10 || json.Unmarshal(body.Body, &receipt) != nil || len(receipt.Data) != 1 {
		return "", bad
	}
	code := string(receipt.Code)
	if code != "0" && code != `"OK"` && (code != "" || string(receipt.Status) != `"OK"`) {
		return "", bad
	}
	v := receipt.Data[0]
	if len(v.URL) > 16384 || v.FID != "" && v.FID != file.id || v.Size != nil && *v.Size != *file.meta.Size {
		return "", bad
	}
	target, err := url.Parse(v.URL)
	if err != nil || target.Scheme != "https" || target.User != nil || target.Fragment != "" {
		return "", bad
	}
	host := target.Hostname()
	website, _ := url.Parse(origin)
	isFixture := host == website.Hostname() && net.ParseIP(host).IsLoopback()
	if !isFixture && (!strings.Contains(host, ".") || net.ParseIP(host) != nil || target.Port() != "") {
		return "", bad
	}
	return target.String(), nil
}

func ucDownloadMetadata(status int64, headers http.Header, file ucFile, wanted *core.ContentRange) (core.Opened, error) {
	var opened core.Opened
	if err := ucDownloadStatus(status); err != nil {
		return opened, err
	}
	length := *file.meta.Size
	contentRange := headers.Get("Content-Range")
	encoding := headers.Get("Content-Encoding")
	bad := core.Fail(core.InvalidUpstreamResponse, errors.New("UC attachment metadata did not match the selected file"))
	for _, name := range []string{"Content-Length", "Content-Range", "Content-Encoding", "Content-Disposition"} {
		if len(headers.Values(name)) > 1 {
			return opened, bad
		}
	}
	if encoding != "" && !strings.EqualFold(encoding, "identity") {
		return opened, bad
	}
	if wanted == nil {
		kind, _, _ := mime.ParseMediaType(headers.Get("Content-Disposition"))
		if status != http.StatusOK || kind != "attachment" || contentRange != "" {
			return opened, bad
		}
	} else {
		if status != http.StatusPartialContent || contentRange != fmt.Sprintf("bytes %d-%d/%d", wanted.Start, wanted.End, wanted.Total) {
			return opened, bad
		}
		length = wanted.End - wanted.Start + 1
	}
	if raw := headers.Get("Content-Length"); raw != "" {
		size, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || size != length {
			return opened, bad
		}
		opened.Length = &length
	}
	opened.Metadata = file.meta
	if wanted != nil {
		opened.Length, opened.Range = &length, wanted
	}
	return opened, nil
}

func ucDownloadStatus(status int64) error {
	switch status {
	case http.StatusOK, http.StatusPartialContent:
		return nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return core.Fail(core.UpstreamAccessDenied, errors.New("UC refused the download"))
	case http.StatusTooManyRequests:
		return core.Fail(core.UpstreamThrottled, nil)
	case http.StatusRequestedRangeNotSatisfiable:
		return core.Fail(core.RangeNotSatisfiable, nil)
	default:
		return core.Fail(core.UpstreamFailure, fmt.Errorf("UC download HTTP %d", status))
	}
}

const ucMountedActionPoint = `(origin, route, fid, button, previous) => {
	if (window !== window.top || location.origin !== origin) return false;
	const expected = new URL(location.href);
	expected.hash = route;
	const lists = document.querySelectorAll(".file-list.table-all-list");
	if (location.hash !== expected.hash || lists.length !== 1) return false;
	const list = lists[0];
	const item = ':is(.ant-table-row, .grid-item)[data-row-key]';
	const rows = list.querySelectorAll(item + '[data-row-key="' + CSS.escape(fid) + '"]');
	if (rows.length > 1) return false;
	let target = rows[0];
	let pageKey = "";
	if (button) {
		if (!target) return false;
		const actions = target.querySelectorAll('.hover-oper-item.hoitem-down[title="下载"]');
		if (actions.length !== 1) return false;
		target = actions[0];
	} else {
		if (!target) {
			pageKey = list.querySelector(item + ':not([data-row-key="drag-bottom"])')?.dataset.rowKey;
			if (!pageKey || pageKey === previous) return false;
			const pager = ':is(.ant-table-pagination, .file-list-pagination)';
			const controls = list.querySelectorAll(pager + ' li.ant-pagination-item-1:not(.ant-pagination-item-active)');
			const next = previous === "" && controls.length > 0 ? controls :
				list.querySelectorAll(pager + ' li.ant-pagination-next[aria-disabled="false"]');
			if (next.length !== 1) return false;
			target = next[0];
		}
		target.scrollIntoView({block: "center", inline: "nearest"});
	}
	const bounds = target.getBoundingClientRect();
	const x = (Math.max(0, bounds.left) + Math.min(window.innerWidth, bounds.right)) / 2;
	const y = (Math.max(0, bounds.top) + Math.min(window.innerHeight, bounds.bottom)) / 2;
	return target.contains(document.elementFromPoint(x, y)) ? {x, y, pageKey} : false;
}`

func clickUCMountedDownload(ctx context.Context, call WebsiteCall, origin string, file ucFile) error {
	raw, _ := json.Marshal(file.route)
	if _, err := call(ctx, `async route => { location.hash = route; return "{}"; }`, raw); err != nil {
		return err
	}
	type point struct {
		X, Y    float64
		PageKey string
	}
	find := func(button bool, previous string) (point, error) {
		return chromedp.Run(ctx, chromedp.PollFunction[point](ucMountedActionPoint,
			chromedp.WithPollingArgs(origin, file.route, file.id, button, previous),
			chromedp.WithPollingTimeout(8*time.Second),
			chromedp.WithPollingInterval(125*time.Millisecond)))
	}
	var previous string
	for {
		row, err := find(false, previous)
		if err != nil {
			return core.Fail(core.UpstreamFailure, errors.New("UC website did not show the selected file"))
		}
		if row.PageKey == "" {
			if _, err := chromedp.Run(ctx, chromedp.MouseEvent(chromedp.MouseMoved, row.X, row.Y)); err != nil {
				return core.Fail(core.UpstreamFailure, errors.New("UC website file could not be selected"))
			}
			break
		}
		if _, err := chromedp.Run(ctx, chromedp.MouseClickXY(row.X, row.Y)); err != nil {
			return core.Fail(core.UpstreamFailure, errors.New("UC website page could not be turned"))
		}
		previous = row.PageKey
	}
	button, err := find(true, "")
	if err != nil {
		return core.Fail(core.UnsupportedOperation, errors.New("UC website did not offer a Download action for the selected file"))
	}
	if _, err := chromedp.Run(ctx, chromedp.MouseClickXY(button.X, button.Y)); err != nil {
		return core.Fail(core.UpstreamFailure, errors.New("UC website Download action could not be selected"))
	}
	return nil
}
