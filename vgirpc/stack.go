package vgirpc

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
)

// traceOpts is the traceback setting an error response is written with.
type traceOpts struct {
	// include is the operator's setting (Server.SetIncludeTracebacks).
	include bool
	// protocol and method name the call that raised the error; they label the
	// synthesized trace of an error that carries no stack.
	protocol, method string
}

// StackTracer is implemented by an error that recorded the call stack where it
// was created. When a handler's error (or any error it wraps) implements it,
// the EXCEPTION batch's traceback and frames are that stack -- the error site
// -- rather than a synthesized one.
//
// The returned program counters are in the form runtime.Callers reports.
// [Errorf] and [WithStack] return errors that implement it.
type StackTracer interface {
	StackTrace() []uintptr
}

// stackError attaches the creation-site stack to an error.
type stackError struct {
	err error
	pcs []uintptr
}

func (e *stackError) Error() string         { return e.err.Error() }
func (e *stackError) Unwrap() error         { return e.err }
func (e *stackError) StackTrace() []uintptr { return e.pcs }

const maxStackDepth = 32

func callers(skip int) []uintptr {
	pcs := make([]uintptr, maxStackDepth)
	n := runtime.Callers(skip+2, pcs)
	return pcs[:n]
}

// Errorf is fmt.Errorf that also records the caller's stack, so a handler
// returning it sends the error site as the remote traceback. %w wrapping works
// as with fmt.Errorf; a wrapped error's own stack, if any, takes precedence.
func Errorf(format string, args ...any) error {
	return &stackError{err: fmt.Errorf(format, args...), pcs: callers(1)}
}

// WithStack records the caller's stack on err. It returns nil for nil, and err
// unchanged when err already carries a stack.
func WithStack(err error) error {
	if err == nil {
		return nil
	}
	var st StackTracer
	if errors.As(err, &st) {
		return err
	}
	return &stackError{err: err, pcs: callers(1)}
}

const maxTracebackChars = 16000

// formatTraceback builds log_extra.traceback and frames for err.
//
// An error carrying a stack (StackTracer, the innermost one in the chain, which
// is closest to the fault) reports that stack. Otherwise the trace is
// synthesized (WIRE_PROTOCOL.md §8): "<ErrorType>: <message>", the
// "<protocol>/<method>" that raised it, and the wrapped causes -- never the
// dispatcher's own frames, which say nothing about where the error came from.
func formatTraceback(err error, errType string, trace traceOpts) (string, *[]stackFrame) {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s\n", errType, err.Error())
	if trace.method != "" {
		if trace.protocol != "" && !strings.Contains(trace.method, "/") {
			fmt.Fprintf(&b, "  in %s/%s\n", trace.protocol, trace.method)
		} else {
			fmt.Fprintf(&b, "  in %s\n", trace.method)
		}
	}
	frames := []stackFrame{}
	var pcs []uintptr
	for e := err; e != nil; e = errors.Unwrap(e) {
		if st, ok := e.(StackTracer); ok && len(st.StackTrace()) > 0 {
			pcs = st.StackTrace()
		}
	}
	if len(pcs) > 0 {
		cf := runtime.CallersFrames(pcs)
		for {
			f, more := cf.Next()
			if f.Function != "" {
				fmt.Fprintf(&b, "%s\n\t%s:%d\n", f.Function, f.File, f.Line)
				frames = append(frames, stackFrame{File: f.File, Line: f.Line, Function: f.Function})
			}
			if !more {
				break
			}
		}
	}
	prev := err.Error()
	for c := errors.Unwrap(err); c != nil; c = errors.Unwrap(c) {
		if _, isStack := c.(*stackError); isStack {
			continue
		}
		if msg := c.Error(); msg != prev {
			fmt.Fprintf(&b, "caused by %T: %s\n", c, msg)
			prev = msg
		}
	}
	out := strings.TrimRight(b.String(), "\n")
	if len(out) > maxTracebackChars {
		out = out[:maxTracebackChars] + "\n… <traceback truncated>"
	}
	return out, &frames
}
