package vgirpc

import (
	"encoding/json"
	"fmt"
	"runtime"
)

// ErrRpc is a sentinel for use with errors.Is to check whether any error in a
// chain is an *RpcError.
var ErrRpc = &RpcError{}

// RpcError represents an error in the vgi_rpc protocol.
type RpcError struct {
	Type      string // e.g. "ValueError", "RuntimeError"
	Message   string
	Traceback string
	RequestID string
}

func (e *RpcError) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

// Is supports errors.Is by matching any *RpcError target.
func (e *RpcError) Is(target error) bool {
	_, ok := target.(*RpcError)
	return ok
}

// stackFrame represents a single frame in a Go stack trace,
// matching the Python wire format for error batch log_extra.
type stackFrame struct {
	File     string `json:"file"`
	Line     int    `json:"line"`
	Function string `json:"function"`
}

// errorExtra is the JSON structure written to vgi_rpc.log_extra
// for EXCEPTION-level log batches.
type errorExtra struct {
	ExceptionType    string       `json:"exception_type"`
	ExceptionMessage string       `json:"exception_message"`
	Traceback        string       `json:"traceback"`
	Frames           []stackFrame `json:"frames"`
}

// buildErrorExtra creates the JSON string for vgi_rpc.log_extra from an error.
func buildErrorExtra(err error) string {
	errType := fmt.Sprintf("%T", err)

	// Try to get a more specific type name
	if rpcErr, ok := err.(*RpcError); ok {
		errType = rpcErr.Type
	}

	// Capture Go stack trace
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	traceback := string(buf[:n])

	// Extract frames from runtime callers
	var frames []stackFrame
	pcs := make([]uintptr, 10)
	n = runtime.Callers(2, pcs)
	if n > 0 {
		callersFrames := runtime.CallersFrames(pcs[:n])
		count := 0
		for {
			frame, more := callersFrames.Next()
			if count >= 5 {
				break
			}
			frames = append(frames, stackFrame{
				File:     frame.File,
				Line:     frame.Line,
				Function: frame.Function,
			})
			count++
			if !more {
				break
			}
		}
	}

	extra := errorExtra{
		ExceptionType:    errType,
		ExceptionMessage: err.Error(),
		Traceback:        traceback,
		Frames:           frames,
	}

	data, _ := json.Marshal(extra)
	return string(data)
}
