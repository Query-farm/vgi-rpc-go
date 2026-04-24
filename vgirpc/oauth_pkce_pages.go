// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"fmt"
	"html"
	"log/slog"
	"net/http"
)

// ---------------------------------------------------------------------------
// Error HTML page
// ---------------------------------------------------------------------------

var oauthErrorHTMLTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Authentication Error &mdash; vgi-rpc</title>
` + fontImports + `
` + errorPageStyle + `
</head>
<body>
<div class="logo">
  <img src="` + logoURL + `" alt="vgi-rpc logo">
</div>
<h1>Authentication Error</h1>
<p>%s</p>
%s
<p><a href="%s">Try again</a></p>
<footer>
  Powered by <a href="https://vgi-rpc.query.farm"><code>vgi-rpc</code></a>
</footer>
</body>
</html>`

// oauthErrorPage renders a user-friendly OAuth error page.
func oauthErrorPage(message, detail, retryURL string) []byte {
	detailHTML := ""
	if detail != "" {
		detailHTML = fmt.Sprintf(`<div class="detail">%s</div>`, html.EscapeString(detail))
	}
	return []byte(fmt.Sprintf(oauthErrorHTMLTemplate,
		html.EscapeString(message),
		detailHTML,
		html.EscapeString(retryURL),
	))
}

// ---------------------------------------------------------------------------
// User-info JS snippet for landing/describe pages
// ---------------------------------------------------------------------------

const userInfoStyle = `#vgi-user-info {
  position: fixed; top: 12px; right: 16px; z-index: 1000;
  font-family: 'Inter', system-ui, sans-serif; font-size: 0.85em;
  display: flex; align-items: center; gap: 8px;
  background: #fff; border: 1px solid #e0ddd0; border-radius: 20px;
  padding: 4px 14px 4px 6px; box-shadow: 0 2px 8px rgba(0,0,0,0.06);
}
#vgi-user-info img {
  width: 26px; height: 26px; border-radius: 50%;
}
#vgi-user-info .email { color: #2c2c1e; font-weight: 500; }
#vgi-user-info a {
  color: #6b6b5a; text-decoration: none; margin-left: 4px;
  font-size: 0.9em;
}
#vgi-user-info a:hover { color: #8b0000; }`

// userInfoScriptTemplate uses %%s placeholders that get filled by fmt.Sprintf.
// The JavaScript uses single-quoted strings and raw escapes.
const userInfoScriptTemplate = `(function() {
  var c = document.cookie.match('(^|;)\\s*%s=([^;]+)');
  if (!c) return;
  try {
    var parts = c[2].split('.');
    var payload = JSON.parse(atob(parts[1].replace(/-/g,'+').replace(/_/g,'/')));
    var el = document.getElementById('vgi-user-info');
    if (!el) return;
    var html = '';
    if (payload.picture) html += '<img src="' + payload.picture + '" alt="">';
    html += '<span class="email">' + (payload.email || payload.sub || '') + '</span>';
    html += '<a href="%s">Sign out</a>';
    el.innerHTML = html;
  } catch(e) {}
})();`

// buildUserInfoHTML returns the HTML snippet (style + div + script) for user info display.
func buildUserInfoHTML(prefix string) []byte {
	logoutURL := prefix + "/_oauth/logout"
	script := fmt.Sprintf(userInfoScriptTemplate, authCookieName, logoutURL)
	return []byte(fmt.Sprintf(
		"<style>%s</style>\n<div id=\"vgi-user-info\"></div>\n<script>%s</script>",
		userInfoStyle, script,
	))
}

// if the client disconnected mid-response.
func writePkcePage(w http.ResponseWriter, body []byte) {
	if _, err := w.Write(body); err != nil {
		slog.Debug("oauth: response write failed", "err", err)
	}
}
