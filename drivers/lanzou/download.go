package lanzou

import (
	"context"
	"encoding/json"
	"errors"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/internal/httpcontent"
)

// Download opens the validated share target and returns its native response.
type Download = func(context.Context, *url.URL, *core.Range) (core.Opened, error)

const lanZouShareInfo = `async fid => {
	if (!/^[1-9][0-9]{0,18}$/.test(fid)) return "{}";
	const frame=Array.from(window.frames).find(w=>{
		try { return w.location.origin===location.origin && typeof w.folder==="function"; } catch { return false; }
	});
	if (!frame) return "{}";
	try {
		const res=await frame.fetch("/doupload.php",{method:"POST",body:new URLSearchParams({task:"22",file_id:fid}),
			credentials:"same-origin",headers:{"Accept":"application/json","X-Requested-With":"XMLHttpRequest"},
			redirect:"error",signal:AbortSignal.timeout(6500)});
		if (!res.ok) return JSON.stringify({code:String(res.status)});
		const body=await res.json();
		const info=body?.info;
		return JSON.stringify({code:String(body?.zt??""), origin:info?.is_newd,id:info?.f_id,
			password:String(info?.onof)==="1"?info?.pwd:""});
	} catch { return "{}"; }
}`

var (
	lanZouShareHost         = regexp.MustCompile(`^(?:[a-z0-9-]+\.)?lanzou[a-z]?\.com$`)
	lanZouShareID           = regexp.MustCompile(`^[A-Za-z0-9_-]{2,80}$`)
	lanZouDownloadAction    = regexp.MustCompile(`https://apifile\.woozooo\.com/ajaxfile\.php\?file=([1-9][0-9]{0,18})["']`)
	lanZouDownloadSignature = regexp.MustCompile(`var\s+isngis\s*=\s*'([A-Za-z0-9_+=%/-]{18,800})'`)
)

func (d *Driver) Open(ctx context.Context, address core.NativeAddress, options core.OpenOptions) (core.Opened, error) {
	if err := options.Validate(); err != nil {
		return core.Opened{}, err
	}
	if options.Mode != core.StreamRead || options.NoFollow || options.Selection.Version != "" {
		return core.Opened{}, core.Fail(core.UnsupportedOperation, nil)
	}
	if err := options.Selection.When.ForDomain(""); err != nil {
		return core.Opened{}, err
	}
	entry, err := d.resolve(ctx, address)
	if err != nil {
		return core.Opened{}, err
	}
	if entry.meta.Kind != core.File {
		return core.Opened{}, core.Fail(core.NotFile, nil)
	}
	if err := options.Selection.Check(core.Evidence{}); err != nil {
		return core.Opened{}, err
	}
	target, err := d.downloadTarget(ctx, entry)
	if err != nil {
		return core.Opened{}, err
	}
	opened, err := d.download(ctx, target, options.Range)
	opened.Metadata.Name = entry.meta.Name
	return opened, err
}

func (d *Driver) downloadTarget(ctx context.Context, file lanZouEntry) (*url.URL, error) {
	args, _ := json.Marshal(file.id)
	raw, err := d.call(ctx, lanZouShareInfo, args)
	if err != nil {
		return nil, err
	}
	var share struct {
		Code, Origin, ID, Password string
	}
	if len(raw) > 64<<10 || json.Unmarshal(raw, &share) != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	switch share.Code {
	case "9", "401", "403":
		return nil, core.Fail(core.UpstreamAccessDenied, nil)
	case "4", "429":
		return nil, core.Fail(core.UpstreamThrottled, nil)
	case "1":
	default:
		return nil, core.Fail(core.UpstreamFailure, errors.New("LanZou did not return an existing file share"))
	}
	page, err := url.Parse(share.Origin)
	if err != nil || !lanZouHTTPS(page) || !lanZouShareHost.MatchString(page.Hostname()) || page.RawQuery != "" || (page.Path != "" && page.Path != "/") ||
		!lanZouShareID.MatchString(share.ID) || len(share.Password) > 128 {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	origin := page.Scheme + "://" + page.Host
	page.Path = "/" + share.ID
	data, err := d.downloadRequest(ctx, http.MethodGet, page.String(), "", nil)
	if err != nil {
		return nil, err
	}
	script := string(data)
	if i := strings.Index(script, "function down_p("); i >= 0 && share.Password != "" {
		script = script[i:]
	} else {
		return nil, core.Fail(core.UnsupportedOperation, errors.New("LanZou returned an unsupported download page layout"))
	}
	action := lanZouDownloadAction.FindStringSubmatch(script)
	sign := lanZouDownloadSignature.FindStringSubmatch(script)
	if len(action) != 2 || action[1] != file.id || len(sign) != 2 {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("LanZou download page did not bind the selected file"))
	}
	endpoint := "https://apifile.woozooo.com/ajaxfile.php?file=" + file.id
	form := url.Values{"action": {"downprocess"}, "sign": {sign[1]}, "kd": {"1"}, "p": {share.Password}}
	data, err = d.downloadRequest(ctx, http.MethodPost, endpoint, origin, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	var reply struct {
		Code          json.Number `json:"zt"`
		Dom, URL, Inf string
	}
	if json.Unmarshal(data, &reply) != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	if reply.Code != "1" {
		return nil, core.Fail(core.UpstreamAccessDenied, errors.New("LanZou did not authorize download"))
	}
	if html.UnescapeString(reply.Inf) != file.meta.Name {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("LanZou download receipt changed the file name"))
	}
	cdn, err := url.Parse(reply.Dom)
	if err != nil || !lanZouHTTPS(cdn) || !strings.HasSuffix(cdn.Hostname(), ".lanrar.com") || cdn.RawQuery != "" ||
		(cdn.Path != "" && cdn.Path != "/") || len(reply.URL) == 0 || len(reply.URL) > 4096 {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	target, err := cdn.Parse("/file/" + reply.URL)
	if err != nil || !lanZouHTTPS(target) || target.Host != cdn.Host || !strings.HasPrefix(target.Path, "/file/") {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	return target, nil
}

func lanZouHTTPS(u *url.URL) bool {
	return httpcontent.IsHTTPS(u) && u.Port() == ""
}

func denyRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

func (d *Driver) downloadRequest(ctx context.Context, method, target, origin string, form io.Reader) (_ []byte, err error) {
	req, err := http.NewRequestWithContext(ctx, method, target, form)
	if err != nil {
		return nil, core.Fail(core.InvalidUpstreamResponse, nil)
	}
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Origin", origin)
		req.Header.Set("Referer", origin+"/")
	}
	res, err := d.control.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, core.Fail(core.UpstreamFailure, errors.New("LanZou download request failed"))
	}
	defer func() { err = errors.Join(err, res.Body.Close()) }()
	switch res.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, core.Fail(core.NotFound, nil)
	case http.StatusUnauthorized, http.StatusForbidden:
		return nil, core.Fail(core.UpstreamAccessDenied, nil)
	case http.StatusTooManyRequests:
		return nil, core.Fail(core.UpstreamThrottled, nil)
	default:
		return nil, core.Fail(core.UpstreamFailure, errors.New("unexpected LanZou download response"))
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, (1<<20)+1))
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, core.Fail(core.UpstreamFailure, errors.New("LanZou download response could not be read"))
	}
	if len(data) > 1<<20 {
		return nil, core.Fail(core.ResponseBudgetExceeded, nil)
	}
	return data, nil
}
