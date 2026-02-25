// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

// LogLevel represents the severity of a log message in the vgi_rpc protocol.
type LogLevel string

const (
	// LogException is the most severe level, used for unrecoverable errors
	// that terminate request processing.
	LogException LogLevel = "EXCEPTION"
	// LogError indicates a recoverable error condition.
	LogError LogLevel = "ERROR"
	// LogWarn indicates a warning that may require attention.
	LogWarn LogLevel = "WARN"
	// LogInfo indicates a normal informational message.
	LogInfo LogLevel = "INFO"
	// LogDebug indicates a verbose diagnostic message.
	LogDebug LogLevel = "DEBUG"
	// LogTrace is the least severe level, used for fine-grained tracing.
	LogTrace LogLevel = "TRACE"
)

// logLevelPriority returns a numeric priority for log levels (lower = more severe).
func logLevelPriority(level LogLevel) int {
	switch level {
	case LogException:
		return 0
	case LogError:
		return 1
	case LogWarn:
		return 2
	case LogInfo:
		return 3
	case LogDebug:
		return 4
	case LogTrace:
		return 5
	default:
		return 6
	}
}

// KV is a key-value pair for structured log extras.
type KV struct {
	Key   string
	Value string
}

// LogMessage represents a client-directed log message.
type LogMessage struct {
	Level   LogLevel
	Message string
	Extras  map[string]string
}
