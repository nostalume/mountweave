package browser

import (
	"net/url"
	"strings"
)

// KnownMutationReceiptRoute identifies provider endpoints whose structured
// file mutation receipts may be labelled as plain text. The caller decides
// whether to capture them and interprets their actual result.
func KnownMutationReceiptRoute(endpoint *url.URL) bool {
	if endpoint == nil || endpoint.Scheme != "https" || endpoint.User != nil ||
		endpoint.Opaque != "" || endpoint.Port() != "" && endpoint.Port() != "443" {
		return false
	}
	switch strings.ToLower(endpoint.Hostname()) {
	case "pan.baidu.com":
		return endpoint.EscapedPath() == "/api/filemanager"
	case "up.woozooo.com":
		return endpoint.EscapedPath() == "/doupload.php"
	default:
		return false
	}
}
