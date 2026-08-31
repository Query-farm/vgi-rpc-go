// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Command docverify checks that the documentation matches the code.
//
// It exists because every Go example in the docs once carried the import
// path github.com/Query-farm/vgi-rpc/vgirpc — the Python reference repo,
// which is not a Go module at all. Every documented `go get` and every
// example failed, and nothing caught it.
//
// Four checks run over README.md and docs/*.md:
//
//  1. modules  — every github.com/Query-farm/... path referenced anywhere
//     in the docs resolves to a real package directory in this repo.
//  2. compile  — every fenced go block that is a complete program
//     (`package main`) builds against the local source, not the published
//     module, so API drift fails immediately.
//  3. symbols  — every alias.Symbol reference in a go block (vgirpc.Unary,
//     vgiotel.InstrumentServer, …) is an exported symbol of the package
//     that alias resolves to. This is what covers the ~50 fragment blocks
//     that are not complete programs and so cannot be compiled.
//  4. links    — every relative link and image path resolves on disk.
//
// Fragments are not compiled: most blocks are deliberately partial. The
// symbol check is the substitute.
//
// A line preceded by an HTML comment containing "docverify:ignore" is
// skipped by the modules check, for prose that must cite a wrong path
// deliberately.
//
// Usage:
//
//	go run ./tools/docverify            # from the repo root
//	go run ./tools/docverify -skip-compile   # offline / no module cache
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const modulePrefix = "github.com/Query-farm/vgi-rpc-go"

// docFiles returns every markdown file to check: the top-level docs plus the
// whole docs/ tree. Walking rather than globbing matters — docs/guide/ holds
// two thirds of the documentation, and a docs/*.md glob silently skipped it.
func docFiles(root string) ([]string, error) {
	var files []string
	for _, top := range []string{"README.md", "CLAUDE.md"} {
		p := filepath.Join(root, top)
		if _, err := os.Stat(p); err == nil {
			files = append(files, p)
		}
	}
	err := filepath.WalkDir(filepath.Join(root, "docs"), func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, ".md") {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

// --- problem reporting ------------------------------------------------------

type problem struct {
	File  string
	Line  int
	Check string
	Msg   string
}

type reporter struct {
	root     string
	problems []problem
}

func (r *reporter) add(file string, line int, check, msg string) {
	rel, err := filepath.Rel(r.root, file)
	if err != nil {
		rel = file
	}
	r.problems = append(r.problems, problem{File: rel, Line: line, Check: check, Msg: msg})
}

// --- code block extraction --------------------------------------------------

type codeBlock struct {
	Lang string
	Body string
	Line int // 1-indexed line of the opening fence
}

var fenceRE = regexp.MustCompile("^```([a-zA-Z0-9_+-]*)\\s*$")

// extractBlocks pulls fenced code blocks out of markdown.
func extractBlocks(src string) []codeBlock {
	var blocks []codeBlock
	lines := strings.Split(src, "\n")
	for i := 0; i < len(lines); i++ {
		m := fenceRE.FindStringSubmatch(lines[i])
		if m == nil {
			continue
		}
		lang := m[1]
		start := i
		var body []string
		i++
		for i < len(lines) && !strings.HasPrefix(lines[i], "```") {
			body = append(body, lines[i])
			i++
		}
		blocks = append(blocks, codeBlock{Lang: lang, Body: strings.Join(body, "\n"), Line: start + 1})
	}
	return blocks
}

// --- check 1: module paths --------------------------------------------------

// modulePathRE matches any Query-farm module-ish path in prose, code, or URLs.
var modulePathRE = regexp.MustCompile(`github\.com/Query-farm/[A-Za-z0-9._/-]+`)

// checkModules verifies every referenced Query-farm path resolves to a real
// package directory in this repo. Bare repo links (the Python reference at
// github.com/Query-farm/vgi-rpc, or a plain link to this repo) are allowed.
func checkModules(rep *reporter, file, src string) {
	lines := strings.Split(src, "\n")
	for lineNo, line := range lines {
		// Docs sometimes need to name a wrong path on purpose (this tool's
		// own rationale does). An HTML comment on the preceding line opts
		// that line out.
		if lineNo > 0 && strings.Contains(lines[lineNo-1], "docverify:ignore") {
			continue
		}
		for _, raw := range modulePathRE.FindAllString(line, -1) {
			path := strings.TrimRight(raw, ").,`\"'/")

			// Plain repository links, not import paths.
			if path == "github.com/Query-farm/vgi-rpc" || path == modulePrefix {
				continue
			}
			// Any other repo under the org is out of scope.
			if !strings.HasPrefix(path, "github.com/Query-farm/vgi-rpc") {
				continue
			}

			// A path under the Python repo can never be a Go package. This is
			// the exact class of error that motivated this tool.
			if strings.HasPrefix(path, "github.com/Query-farm/vgi-rpc/") {
				rep.add(file, lineNo+1, "modules", fmt.Sprintf(
					"%q is not a Go module — github.com/Query-farm/vgi-rpc is the Python reference repo; did you mean %s/%s ?",
					path, modulePrefix, strings.TrimPrefix(path, "github.com/Query-farm/vgi-rpc/")))
				continue
			}

			if !strings.HasPrefix(path, modulePrefix+"/") {
				continue
			}
			sub := strings.TrimPrefix(path, modulePrefix+"/")
			// GitHub repository UI routes are links, not Go import paths.
			if strings.HasPrefix(sub, "actions/") {
				continue
			}
			// pkg.go.dev badge URLs append .svg to the real package path.
			sub = strings.TrimSuffix(sub, ".svg")
			dir := filepath.Join(rep.root, filepath.FromSlash(sub))
			if !dirHasGo(dir) {
				rep.add(file, lineNo+1, "modules", fmt.Sprintf(
					"%q does not resolve to a package — %s has no .go files", path, filepath.ToSlash(sub)))
			}
		}
	}
}

func dirHasGo(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") {
			return true
		}
	}
	return false
}

// --- check 2: compile complete programs -------------------------------------

// checkCompile builds every `package main` go block against the local source.
func checkCompile(rep *reporter, file string, blocks []codeBlock, goVersion string) {
	for _, b := range blocks {
		if b.Lang != "go" || !strings.Contains(b.Body, "package main") {
			continue
		}
		if err := buildSnippet(rep.root, b.Body, goVersion); err != nil {
			rep.add(file, b.Line, "compile", fmt.Sprintf("example does not build:\n%s", indent(err.Error())))
		}
	}
}

func buildSnippet(root, body, goVersion string) error {
	tmp, err := os.MkdirTemp("", "docverify-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)

	if err := os.WriteFile(filepath.Join(tmp, "main.go"), []byte(body), 0o644); err != nil {
		return err
	}

	// Point the module at the local checkout so examples are verified
	// against working-tree code rather than the last published release.
	gomod := fmt.Sprintf("module docverify\n\ngo %s\n\nrequire %s v0.0.0\n\nreplace %s => %s\n",
		goVersion, modulePrefix, modulePrefix, root)
	if err := os.WriteFile(filepath.Join(tmp, "go.mod"), []byte(gomod), 0o644); err != nil {
		return err
	}

	for _, args := range [][]string{
		{"mod", "tidy"},
		{"build", "./..."},
	} {
		cmd := exec.Command("go", args...)
		cmd.Dir = tmp
		cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod")
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("go %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	return nil
}

func indent(s string) string {
	var b strings.Builder
	for _, l := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		b.WriteString("      " + l + "\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

// --- check 3: symbol references ---------------------------------------------

var (
	importLineRE = regexp.MustCompile(`^\s*(?:([a-zA-Z_][a-zA-Z0-9_]*)\s+)?"(github\.com/Query-farm/[^"]+)"`)
	anyImportRE  = regexp.MustCompile(`^\s*(?:([a-zA-Z_][a-zA-Z0-9_]*)\s+)?"([a-z0-9][^"]*\.[^"]*/[^"]+)"`)
	selectorRE   = regexp.MustCompile(`\b([a-z][a-zA-Z0-9_]*)\.([A-Z][A-Za-z0-9_]*)`)
)

// checkSymbols verifies alias.Symbol references resolve to exported symbols.
// This covers the fragment blocks that cannot be compiled.
func checkSymbols(rep *reporter, file string, blocks []codeBlock, exports map[string]map[string]bool) {
	for _, b := range blocks {
		if b.Lang != "go" {
			continue
		}

		// Names bound by third-party imports in this block. A default alias
		// for one of our packages must not shadow them — CLAUDE.md's Sentry
		// example imports getsentry/sentry-go as `sentry`, which is not our
		// vgirpc/sentry package.
		foreign := map[string]bool{}
		for _, line := range strings.Split(b.Body, "\n") {
			m := anyImportRE.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			name, path := m[1], m[2]
			if strings.HasPrefix(path, "github.com/Query-farm/") {
				continue
			}
			if name == "" {
				// Package name usually matches the last path element, modulo
				// the common -go suffix and a /vN major-version element.
				name = strings.TrimSuffix(lastElem(stripMajor(path)), "-go")
			}
			foreign[name] = true
		}

		// Resolve aliases declared in this block; fall back to the default
		// alias (last path element) for our own packages.
		alias := map[string]string{}
		for pkgPath := range exports {
			if n := lastElem(pkgPath); !foreign[n] {
				alias[n] = pkgPath
			}
		}
		for _, line := range strings.Split(b.Body, "\n") {
			if m := importLineRE.FindStringSubmatch(line); m != nil {
				name, path := m[1], m[2]
				if name == "" {
					name = lastElem(path)
				}
				alias[name] = path
			}
		}

		seen := map[string]bool{}
		for _, line := range strings.Split(b.Body, "\n") {
			code := stripComment(line)
			for _, m := range selectorRE.FindAllStringSubmatch(code, -1) {
				a, sym := m[1], m[2]
				pkgPath, ok := alias[a]
				if !ok {
					continue // not one of our packages
				}
				exp, ok := exports[pkgPath]
				if !ok {
					continue
				}
				key := a + "." + sym
				if seen[key] || exp[sym] {
					continue
				}
				seen[key] = true
				rep.add(file, b.Line, "symbols", fmt.Sprintf(
					"%s.%s does not exist in %s", a, sym, pkgPath))
			}
		}
	}
}

// stripMajor drops a trailing /vN major-version path element.
func stripMajor(p string) string {
	i := strings.LastIndex(p, "/")
	if i < 0 {
		return p
	}
	last := p[i+1:]
	if len(last) > 1 && last[0] == 'v' && strings.IndexFunc(last[1:], func(r rune) bool { return r < '0' || r > '9' }) < 0 {
		return p[:i]
	}
	return p
}

func lastElem(p string) string {
	i := strings.LastIndex(p, "/")
	if i < 0 {
		return p
	}
	return p[i+1:]
}

// stripComment removes // comments and whole-line block comments so prose
// inside examples is not mistaken for code.
func stripComment(line string) string {
	if i := strings.Index(line, "//"); i >= 0 {
		line = line[:i]
	}
	trimmed := strings.TrimSpace(line)
	if strings.HasPrefix(trimmed, "*") || strings.HasPrefix(trimmed, "/*") {
		return ""
	}
	return line
}

// loadExports parses a package directory and returns its exported top-level
// names (including struct fields' methods are not needed — selectors in docs
// are package-level).
func loadExports(dir string) (map[string]bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	out := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		// Every file is parsed regardless of build tags: a symbol that only
		// exists on one platform still exists as far as the docs go.
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			return nil, err
		}
		{
			for _, d := range f.Decls {
				switch decl := d.(type) {
				case *ast.FuncDecl:
					if decl.Recv == nil && decl.Name.IsExported() {
						out[decl.Name.Name] = true
					}
				case *ast.GenDecl:
					for _, spec := range decl.Specs {
						switch s := spec.(type) {
						case *ast.TypeSpec:
							if s.Name.IsExported() {
								out[s.Name.Name] = true
							}
						case *ast.ValueSpec:
							for _, n := range s.Names {
								if n.IsExported() {
									out[n.Name] = true
								}
							}
						}
					}
				}
			}
		}
	}
	return out, nil
}

// --- check 4: relative links and assets -------------------------------------

var (
	mdLinkRE  = regexp.MustCompile(`!?\[[^\]]*\]\(([^)]+)\)`)
	htmlSrcRE = regexp.MustCompile(`(?:src|href)="([^"]+)"`)
)

// maskCode blanks the contents of fenced code blocks and inline code spans,
// preserving line numbering. Without this, Go generic syntax such as
// `Unary[P, R](server, name, handler)` parses as a markdown link whose
// target is "server, name, handler".
func maskCode(src string) string {
	lines := strings.Split(src, "\n")
	inFence := false
	for i, line := range lines {
		if fenceRE.MatchString(line) || strings.HasPrefix(line, "```") {
			inFence = !inFence
			lines[i] = ""
			continue
		}
		if inFence {
			lines[i] = ""
			continue
		}
		lines[i] = stripInlineCode(line)
	}
	return strings.Join(lines, "\n")
}

// stripInlineCode blanks `...` spans, keeping the line length stable.
func stripInlineCode(line string) string {
	var b strings.Builder
	inCode := false
	for _, r := range line {
		if r == '`' {
			inCode = !inCode
			b.WriteRune(' ')
			continue
		}
		if inCode {
			b.WriteRune(' ')
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func checkLinks(rep *reporter, file, src string) {
	dir := filepath.Dir(file)
	for lineNo, line := range strings.Split(maskCode(src), "\n") {
		var targets []string
		for _, m := range mdLinkRE.FindAllStringSubmatch(line, -1) {
			targets = append(targets, m[1])
		}
		for _, m := range htmlSrcRE.FindAllStringSubmatch(line, -1) {
			targets = append(targets, m[1])
		}
		for _, t := range targets {
			t = strings.TrimSpace(t)
			// External, anchors, templating, and mkdocs attribute lists.
			if t == "" || strings.HasPrefix(t, "http://") || strings.HasPrefix(t, "https://") ||
				strings.HasPrefix(t, "#") || strings.HasPrefix(t, "mailto:") ||
				strings.Contains(t, "{{") || strings.HasPrefix(t, "<") {
				continue
			}
			if i := strings.IndexAny(t, "#?"); i >= 0 {
				t = t[:i]
			}
			if t == "" {
				continue
			}
			if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(t))); err != nil {
				rep.add(file, lineNo+1, "links", fmt.Sprintf("%q does not resolve on disk", t))
			}
		}
	}
}

// --- main -------------------------------------------------------------------

func main() {
	var (
		root        = flag.String("root", ".", "repository root")
		skipCompile = flag.Bool("skip-compile", false, "skip building complete examples (offline use)")
	)
	flag.Parse()

	absRoot, err := filepath.Abs(*root)
	if err != nil {
		fatal(err)
	}

	goVersion, err := moduleGoVersion(filepath.Join(absRoot, "go.mod"))
	if err != nil {
		fatal(err)
	}

	// Exported symbols for each of our packages referenced from docs.
	exports := map[string]map[string]bool{}
	for _, pkg := range []string{"vgirpc", "vgirpc/otel", "vgirpc/sentry", "vgirpc/jwtauth", "vgirpc/s3", "vgirpc/gcs"} {
		dir := filepath.Join(absRoot, filepath.FromSlash(pkg))
		if !dirHasGo(dir) {
			continue
		}
		syms, err := loadExports(dir)
		if err != nil {
			fatal(fmt.Errorf("parsing %s: %w", pkg, err))
		}
		exports[modulePrefix+"/"+pkg] = syms
	}

	files, err := docFiles(absRoot)
	if err != nil {
		fatal(err)
	}

	rep := &reporter{root: absRoot}
	checked := 0
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			fatal(err)
		}
		src := string(raw)
		blocks := extractBlocks(src)

		checkModules(rep, f, src)
		checkLinks(rep, f, src)
		checkSymbols(rep, f, blocks, exports)
		if !*skipCompile {
			checkCompile(rep, f, blocks, goVersion)
		}
		checked++
	}

	if len(rep.problems) == 0 {
		fmt.Printf("docverify: %d files OK\n", checked)
		return
	}

	sort.Slice(rep.problems, func(i, j int) bool {
		if rep.problems[i].File != rep.problems[j].File {
			return rep.problems[i].File < rep.problems[j].File
		}
		return rep.problems[i].Line < rep.problems[j].Line
	})
	for _, p := range rep.problems {
		fmt.Printf("%s:%d: [%s] %s\n", p.File, p.Line, p.Check, p.Msg)
	}
	fmt.Fprintf(os.Stderr, "\ndocverify: %d problem(s) in %d files\n", len(rep.problems), checked)
	os.Exit(1)
}

// moduleGoVersion reads the `go` directive so generated snippet modules match.
func moduleGoVersion(gomod string) (string, error) {
	raw, err := os.ReadFile(gomod)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if f := strings.Fields(line); len(f) == 2 && f[0] == "go" {
			return f[1], nil
		}
	}
	return "", fmt.Errorf("no go directive in %s", gomod)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "docverify:", err)
	os.Exit(2)
}
