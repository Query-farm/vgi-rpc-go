// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"sort"
	"strings"
)

// --- HTML templates ---

const fontImports = `<link rel="preconnect" href="https://fonts.googleapis.com">` +
	`<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>` +
	`<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">`

const logoURL = "https://vgi-rpc-python.query.farm/assets/logo-hero.png"

const notFoundHTMLTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>404 &mdash; vgi-rpc endpoint</title>
<style>
  body { font-family: system-ui, -apple-system, sans-serif; max-width: 600px;
         margin: 60px auto; padding: 0 20px; color: #333; text-align: center; }
  .logo { margin-bottom: 24px; }
  .logo img { width: 120px; height: 120px; }
  h1 { color: #555; }
  code { background: #f4f4f4; padding: 2px 6px; border-radius: 3px; font-size: 0.95em; }
  a { color: #0066cc; }
  p { line-height: 1.6; }
</style>
</head>
<body>
<div class="logo">
  <img src="%s" alt="vgi-rpc logo">
</div>
<h1>404 &mdash; Not Found</h1>
<p>This is a <code>vgi-rpc</code> service endpoint%s.</p>
<p>RPC methods are available under <code>%s/&lt;method&gt;</code>.</p>
<p>Learn more at <a href="https://vgi-rpc.query.farm">vgi-rpc.query.farm</a>.</p>
</body>
</html>`

const landingHTMLTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>%s &mdash; vgi-rpc</title>
%s
<style>
  body { font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 600px;
         margin: 0 auto; padding: 60px 20px 0; color: #2c2c1e; text-align: center;
         background: #faf8f0; }
  .logo { margin-bottom: 24px; }
  .logo img { width: 140px; height: 140px; border-radius: 50%%;
               box-shadow: 0 4px 24px rgba(0,0,0,0.12); }
  h1 { color: #2d5016; margin-bottom: 8px; font-weight: 700; }
  code { font-family: 'JetBrains Mono', monospace; background: #f0ece0;
          padding: 2px 6px; border-radius: 3px; font-size: 0.9em; color: #2c2c1e; }
  a { color: #2d5016; text-decoration: none; }
  a:hover { color: #4a7c23; }
  p { line-height: 1.7; color: #6b6b5a; }
  .meta { font-size: 0.9em; color: #6b6b5a; }
  .links { margin-top: 28px; display: flex; flex-wrap: wrap; justify-content: center; gap: 8px; }
  .links a { display: inline-block; padding: 8px 18px; border-radius: 6px;
              border: 1px solid #4a7c23; color: #2d5016; font-weight: 600;
              font-size: 0.9em; transition: all 0.2s ease; }
  .links a:hover { background: #4a7c23; color: #fff; }
  .links a.primary { background: #2d5016; color: #fff; border-color: #2d5016; }
  .links a.primary:hover { background: #4a7c23; border-color: #4a7c23; }
  footer { margin-top: 48px; padding: 20px 0; border-top: 1px solid #f0ece0;
            color: #6b6b5a; font-size: 0.85em; }
  footer a { color: #2d5016; font-weight: 600; }
  footer a:hover { color: #4a7c23; }
</style>
</head>
<body>
<div class="logo">
  <img src="%s" alt="vgi-rpc logo">
</div>
<h1>%s</h1>
<p class="meta">Powered by <code>vgi-rpc</code> (Go) &middot; server <code>%s</code></p>
<p>This is a <code>vgi-rpc</code> service endpoint.</p>
<div class="links">
%s
<a href="https://vgi-rpc.query.farm">Learn more about <code>vgi-rpc</code></a>
</div>
<footer>
  &copy; 2026 &#x1F69C; <a href="https://query.farm">Query.Farm LLC</a>
</footer>
</body>
</html>`

const describeHTMLTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>%s API Reference &mdash; vgi-rpc</title>
%s
<style>
  body { font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 900px;
         margin: 0 auto; padding: 40px 20px 0; color: #2c2c1e; background: #faf8f0; }
  .header { text-align: center; margin-bottom: 40px; }
  .header .logo img { width: 80px; height: 80px; border-radius: 50%%;
                       box-shadow: 0 3px 16px rgba(0,0,0,0.10); }
  .header h1 { margin-bottom: 4px; color: #2d5016; font-weight: 700; }
  .header .subtitle { color: #6b6b5a; font-size: 1.1em; margin-top: 0; }
  .header .meta { color: #6b6b5a; font-size: 0.9em; }
  .header .meta a { color: #2d5016; font-weight: 600; }
  .header .meta a:hover { color: #4a7c23; }
  code { font-family: 'JetBrains Mono', monospace; background: #f0ece0;
          padding: 2px 6px; border-radius: 3px; font-size: 0.85em; color: #2c2c1e; }
  a { color: #2d5016; text-decoration: none; }
  a:hover { color: #4a7c23; }
  .card { border: 1px solid #f0ece0; border-radius: 8px; padding: 20px;
           margin-bottom: 16px; background: #fff; }
  .card:hover { border-color: #c8a43a; }
  .card-header { display: flex; align-items: center; gap: 10px; margin-bottom: 12px; }
  .method-name { font-family: 'JetBrains Mono', monospace; font-size: 1.1em; font-weight: 600;
                  color: #2d5016; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px;
            font-size: 0.75em; font-weight: 600; text-transform: uppercase;
            letter-spacing: 0.03em; }
  .badge-unary { background: #e8f5e0; color: #2d5016; }
  .badge-stream { background: #e0ecf5; color: #1a4a6b; }
  .badge-exchange { background: #f5e6f0; color: #6b234a; }
  .badge-producer { background: #e0f0f5; color: #1a5a6b; }
  .badge-header { background: #f5eee0; color: #6b4423; }
  table { width: 100%%; border-collapse: collapse; font-size: 0.9em; }
  th { text-align: left; padding: 8px 10px; background: #f0ece0; color: #2c2c1e;
        font-weight: 600; border-bottom: 2px solid #e0dcd0; }
  td { padding: 8px 10px; border-bottom: 1px solid #f0ece0; }
  td code { font-size: 0.85em; }
  .no-params { color: #6b6b5a; font-style: italic; font-size: 0.9em; }
  .section-label { font-size: 0.8em; font-weight: 600; text-transform: uppercase;
                    letter-spacing: 0.05em; color: #6b6b5a; margin-top: 14px;
                    margin-bottom: 6px; }
  footer { text-align: center; margin-top: 48px; padding: 20px 0;
            border-top: 1px solid #f0ece0; color: #6b6b5a; font-size: 0.85em; }
  footer a { color: #2d5016; font-weight: 600; }
  footer a:hover { color: #4a7c23; }
</style>
</head>
<body>
<div class="header">
  <div class="logo">
    <img src="%s" alt="vgi-rpc logo">
  </div>
  <h1>%s</h1>
  <p class="subtitle">API Reference</p>
  <p class="meta">Powered by <code>vgi-rpc</code> (Go) &middot; server <code>%s</code>
%s</p>
</div>
%s
<footer>
  <a href="https://vgi-rpc.query.farm">Learn more about <code>vgi-rpc</code></a>
  &middot;
  &copy; 2026 &#x1F69C; <a href="https://query.farm">Query.Farm LLC</a>
</footer>
</body>
</html>`

// --- Page builders ---

func buildNotFoundHTML(prefix, protocolName string) []byte {
	var fragment string
	if protocolName != "" {
		fragment = " serving <strong>" + html.EscapeString(protocolName) + "</strong>"
	}
	return []byte(fmt.Sprintf(notFoundHTMLTemplate,
		logoURL,
		fragment,
		html.EscapeString(prefix),
	))
}

func buildLandingHTML(prefix, protocolName, serverID, describePath, repoURL string) []byte {
	var links strings.Builder
	if describePath != "" {
		fmt.Fprintf(&links, `<a class="primary" href="%s">View service API</a>`+"\n",
			html.EscapeString(describePath))
	}
	if repoURL != "" {
		fmt.Fprintf(&links, `<a href="%s">Source repository</a>`+"\n",
			html.EscapeString(repoURL))
	}
	return []byte(fmt.Sprintf(landingHTMLTemplate,
		html.EscapeString(protocolName), // <title>
		fontImports,
		logoURL,
		html.EscapeString(protocolName), // <h1>
		html.EscapeString(serverID),
		links.String(),
	))
}

func buildDescribeHTML(s *Server, prefix, protocolName, repoURL string) []byte {
	serverID := s.serverID

	var repoLink string
	if repoURL != "" {
		repoLink = fmt.Sprintf(`&middot; <a href="%s">Source repository</a>`,
			html.EscapeString(repoURL))
	}

	names := s.availableMethods()
	sort.Strings(names)

	var cards strings.Builder
	for _, name := range names {
		if name == "__describe__" {
			continue
		}
		info := s.methods[name]
		buildMethodCard(&cards, info)
	}

	return []byte(fmt.Sprintf(describeHTMLTemplate,
		html.EscapeString(protocolName), // <title>
		fontImports,
		logoURL,
		html.EscapeString(protocolName), // <h1>
		html.EscapeString(serverID),
		repoLink,
		cards.String(),
	))
}

func buildMethodCard(w *strings.Builder, info *methodInfo) {
	badgeClass := "badge-unary"
	badgeLabel := "UNARY"
	if info.Type != MethodUnary {
		badgeClass = "badge-stream"
		badgeLabel = "STREAM"
	}

	w.WriteString(`<div class="card">`)
	w.WriteString(`<div class="card-header">`)
	fmt.Fprintf(w, `<span class="method-name">%s</span>`, html.EscapeString(info.Name))
	fmt.Fprintf(w, `<span class="badge %s">%s</span>`, badgeClass, badgeLabel)

	if info.Type == MethodExchange {
		w.WriteString(`<span class="badge badge-exchange">exchange</span>`)
	} else if info.Type == MethodProducer {
		w.WriteString(`<span class="badge badge-producer">producer</span>`)
	}
	if info.HasHeader {
		w.WriteString(`<span class="badge badge-header">header</span>`)
	}
	w.WriteString(`</div>`) // card-header

	// Parameters table
	if info.ParamsSchema.NumFields() > 0 {
		// Build param types map
		paramTypes := make(map[string]string, info.ParamsSchema.NumFields())
		for i := range info.ParamsSchema.NumFields() {
			f := info.ParamsSchema.Field(i)
			paramTypes[f.Name] = arrowTypeToString(f.Type)
		}

		// Parse defaults
		var defaults map[string]any
		if len(info.ParamDefaults) > 0 {
			defaults = make(map[string]any, len(info.ParamDefaults))
			for k, v := range info.ParamDefaults {
				defaults[k] = coerceDefaultValue(v, info.ParamsSchema, k)
			}
		}

		w.WriteString(`<div class="section-label">Parameters</div>`)
		w.WriteString(`<table><tr><th>Name</th><th>Type</th><th>Default</th></tr>`)
		for i := range info.ParamsSchema.NumFields() {
			f := info.ParamsSchema.Field(i)
			defaultStr := "&mdash;"
			if defaults != nil {
				if dv, ok := defaults[f.Name]; ok {
					b, _ := json.Marshal(dv)
					defaultStr = html.EscapeString(string(b))
				}
			}
			fmt.Fprintf(w, `<tr><td><code>%s</code></td><td><code>%s</code></td><td>%s</td></tr>`,
				html.EscapeString(f.Name),
				html.EscapeString(paramTypes[f.Name]),
				defaultStr,
			)
		}
		w.WriteString(`</table>`)
	} else {
		w.WriteString(`<p class="no-params">No parameters</p>`)
	}

	// Returns table for unary methods
	if info.Type == MethodUnary && info.ResultType != nil && info.ResultSchema.NumFields() > 0 {
		w.WriteString(`<div class="section-label">Returns</div>`)
		w.WriteString(`<table><tr><th>Name</th><th>Type</th></tr>`)
		for i := range info.ResultSchema.NumFields() {
			f := info.ResultSchema.Field(i)
			fmt.Fprintf(w, `<tr><td><code>%s</code></td><td><code>%s</code></td></tr>`,
				html.EscapeString(f.Name),
				html.EscapeString(arrowTypeToString(f.Type)),
			)
		}
		w.WriteString(`</table>`)
	}

	// Header schema for stream methods
	if info.HasHeader && info.HeaderSchema != nil && info.HeaderSchema.NumFields() > 0 {
		w.WriteString(`<div class="section-label">Stream Header</div>`)
		w.WriteString(`<table><tr><th>Name</th><th>Type</th></tr>`)
		for i := range info.HeaderSchema.NumFields() {
			f := info.HeaderSchema.Field(i)
			fmt.Fprintf(w, `<tr><td><code>%s</code></td><td><code>%s</code></td></tr>`,
				html.EscapeString(f.Name),
				html.EscapeString(arrowTypeToString(f.Type)),
			)
		}
		w.WriteString(`</table>`)
	}

	w.WriteString(`</div>`) // card
	w.WriteString("\n")
}

// --- HTTP handlers ---

func (h *HttpServer) handleLandingPage(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(h.landingHTML)
}

func (h *HttpServer) handleDescribePage(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(h.describeHTML)
}

func (h *HttpServer) handleNotFound(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write(h.notFoundHTML)
}
