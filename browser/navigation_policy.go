package browser

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
	core "github.com/nostalume/mountweave"
)

// PrepareHTTPSOnlyProfile sets the navigation limits of a dedicated Chromium
// profile. The caller must hold exclusive ownership and close its browser first.
func PrepareHTTPSOnlyProfile(profile string) error {
	if !filepath.IsAbs(profile) {
		return errors.New("browser profile directory must be absolute")
	}
	dir := filepath.Join(profile, "Default")
	path := filepath.Join(dir, "Preferences")
	prefs := make(map[string]json.RawMessage)
	if data, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(data, &prefs); err != nil || prefs == nil {
			return errors.New("browser preferences are not a JSON object")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	policy := make(map[string]json.RawMessage)
	if raw, exists := prefs["policy"]; exists {
		if err := json.Unmarshal(raw, &policy); err != nil || policy == nil {
			return errors.New("browser navigation preferences are not a JSON object")
		}
	}
	const blocked, allowed = `["*"]`, `["https:*"]`
	if string(policy["url_blocklist"]) == blocked && string(policy["url_allowlist"]) == allowed {
		return nil
	}
	policy["url_blocklist"] = json.RawMessage(blocked)
	policy["url_allowlist"] = json.RawMessage(allowed)
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	prefs["policy"] = policyJSON
	data, err := json.Marshal(prefs)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".mweave-preferences-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, writeErr := tmp.Write(data)
	if err := errors.Join(writeErr, tmp.Close()); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// CheckHTTPSOnlyNavigation verifies native navigation denial in an initialized
// Chromium tab before it opens the provider's website.
func CheckHTTPSOnlyNavigation(ctx context.Context) error {
	scheme := "mweave-policy-fixture-" + strings.ToLower(rand.Text())
	probe, err := chromedp.Call(ctx, page.Navigate, page.NavigateParams{URL: scheme + "://guard.invalid/"})
	if err != nil {
		return fmt.Errorf("could not verify browser navigation protection: %w", err)
	}
	if probe.ErrorText != "net::ERR_BLOCKED_BY_ADMINISTRATOR" {
		return errors.New("browser did not enforce navigation protection")
	}
	return nil
}

// ReadPublicDocument evaluates an expression on a public page in an isolated
// browser context without the mounted account's cookies.
func ReadPublicDocument(ctx, browser context.Context, entryURL, pathPrefix, expression string) (string, error) {
	entry, err := url.Parse(entryURL)
	if err != nil || entry.Scheme != "https" || entry.Host == "" || entry.User != nil ||
		entry.Opaque != "" || !strings.HasPrefix(pathPrefix, "/") || !strings.HasSuffix(pathPrefix, "/") {
		return "", core.Fail(core.InvalidRequest, nil)
	}
	tab, closeTab := chromedp.NewContext(browser, chromedp.WithNewBrowserContext())
	defer closeTab()
	action, cancel := context.WithTimeout(tab, 15*time.Second)
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	response, err := chromedp.RunResponse(action, chromedp.Navigate(entry.String()))
	switch {
	case ctx.Err() != nil:
		return "", ctx.Err()
	case errors.Is(action.Err(), context.DeadlineExceeded):
		return "", core.Fail(core.UpstreamTimeout, nil)
	case err != nil || response == nil:
		return "", core.Fail(core.UpstreamFailure, errors.New("public page navigation failed"))
	case response.Status == 401 || response.Status == 403:
		return "", core.Fail(core.UpstreamAccessDenied, errors.New("public page refused access"))
	case response.Status != 200:
		return "", core.Fail(core.UpstreamFailure, errors.New("public page did not load"))
	}
	origin := strconv.Quote(entry.Scheme + "://" + entry.Host)
	exact := strconv.Quote(strings.TrimSuffix(pathPrefix, "/"))
	prefix := strconv.Quote(pathPrefix)
	guarded := "(async()=>window===window.top&&location.origin===" + origin +
		"&&(location.pathname===" + exact + "||location.pathname.startsWith(" + prefix + "))?await (" + expression + ")():null)()"
	value, err := chromedp.Run(action, chromedp.Evaluate[*string](guarded, chromedp.EvalAwaitPromise))
	switch {
	case ctx.Err() != nil:
		return "", ctx.Err()
	case errors.Is(action.Err(), context.DeadlineExceeded):
		return "", core.Fail(core.UpstreamTimeout, nil)
	case err != nil || value == nil:
		return "", core.Fail(core.InvalidUpstreamResponse, errors.New("public page left the selected document"))
	}
	return *value, nil
}
