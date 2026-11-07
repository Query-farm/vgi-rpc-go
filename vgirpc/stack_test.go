package vgirpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func decodeExtra(t *testing.T, err error, trace traceOpts) map[string]json.RawMessage {
	t.Helper()
	var extra map[string]json.RawMessage
	if jerr := json.Unmarshal([]byte(buildErrorExtra(err, errorModelOf(err), trace)), &extra); jerr != nil {
		t.Fatal(jerr)
	}
	return extra
}

func handlerSiteError() error { return Errorf("lookup %q failed", "k") }

// An error that recorded its creation stack reports that stack: the handler
// frame is present, the dispatcher's buildErrorExtra is not.
func TestTracebackUsesTheErrorSiteStack(t *testing.T) {
	err := handlerSiteError()
	var st StackTracer
	if !errors.As(err, &st) {
		t.Fatal("Errorf must return a StackTracer")
	}
	extra := decodeExtra(t, err, traceOpts{include: true, protocol: "demo.v1", method: "get"})
	var tb string
	_ = json.Unmarshal(extra["traceback"], &tb)
	if !strings.Contains(tb, "handlerSiteError") {
		t.Fatalf("traceback lacks the error site:\n%s", tb)
	}
	if strings.Contains(tb, "buildErrorExtra") || strings.Contains(tb, "formatTraceback") {
		t.Fatalf("traceback carries dispatcher frames:\n%s", tb)
	}
	if !strings.HasPrefix(tb, `*errors.errorString: lookup "k" failed`) || !strings.Contains(tb, "in demo.v1/get") {
		t.Fatalf("traceback header = %q", tb)
	}
	var frames []stackFrame
	_ = json.Unmarshal(extra["frames"], &frames)
	if len(frames) == 0 || !strings.HasSuffix(frames[0].Function, "handlerSiteError") {
		t.Fatalf("frames = %+v", frames)
	}
	var typ string
	_ = json.Unmarshal(extra["exception_type"], &typ)
	if typ != "*errors.errorString" {
		t.Fatalf("exception_type = %q; the stack carrier must be invisible", typ)
	}
}

// An error with no stack gets a synthesized, non-empty trace naming the type,
// message, protocol/method and causes -- and no dispatcher frames.
func TestTracebackSynthesizedWithoutStack(t *testing.T) {
	err := fmt.Errorf("write failed: %w", &RpcError{Type: "IOError", Message: "disk full"})
	extra := decodeExtra(t, err, traceOpts{include: true, protocol: "demo.v1", method: "put"})
	var tb string
	_ = json.Unmarshal(extra["traceback"], &tb)
	want := "IOError: write failed: IOError: disk full\n  in demo.v1/put\ncaused by *vgirpc.RpcError: IOError: disk full"
	if !strings.HasPrefix(tb, want) {
		t.Fatalf("traceback = %q, want prefix %q", tb, want)
	}
	if strings.Contains(tb, "vgirpc.formatTraceback") || strings.Contains(tb, ".go:") || strings.Contains(tb, "goroutine") {
		t.Fatalf("synthesized traceback carries Go frames:\n%s", tb)
	}
	if string(extra["frames"]) != "[]" {
		t.Fatalf("frames = %s, want []", extra["frames"])
	}
	if off := decodeExtra(t, err, traceOpts{}); off["traceback"] != nil || off["frames"] != nil {
		t.Fatalf("tracebacks off still sent: %v", off)
	}
}

func TestWithStackKeepsAnExistingStack(t *testing.T) {
	if WithStack(nil) != nil {
		t.Fatal("WithStack(nil) != nil")
	}
	inner := handlerSiteError()
	if WithStack(inner) != inner {
		t.Fatal("WithStack re-captured an existing stack")
	}
	plain := errors.New("x")
	if w := WithStack(plain); !errors.Is(w, plain) || w.Error() != "x" {
		t.Fatal("WithStack must wrap transparently")
	}
}

// An error converted to an RpcError keeps the original's stack.
func TestRpcErrorCarriesAConvertedStack(t *testing.T) {
	orig := handlerSiteError()
	rpc := &RpcError{Type: "RuntimeError", Message: orig.Error()}
	if StackTraceOf(rpc) != nil || WithStack(rpc) == error(rpc) {
		t.Fatal("a bare RpcError must carry no stack")
	}
	rpc.SetStackTrace(StackTraceOf(orig))
	extra := decodeExtra(t, rpc, traceOpts{include: true, method: "m"})
	var tb string
	_ = json.Unmarshal(extra["traceback"], &tb)
	if !strings.HasPrefix(tb, "RuntimeError: ") || !strings.Contains(tb, "handlerSiteError") {
		t.Fatalf("traceback = %s", tb)
	}
}
