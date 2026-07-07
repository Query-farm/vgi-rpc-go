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

// if the client disconnected mid-response.
func writePkcePage(w http.ResponseWriter, body []byte) {
	if _, err := w.Write(body); err != nil {
		slog.Debug("oauth: response write failed", "err", err)
	}
}
