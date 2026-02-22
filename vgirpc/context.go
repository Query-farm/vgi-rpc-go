// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import "context"

// CallContext provides request-scoped information and logging to method handlers.
type CallContext struct {
	Ctx       context.Context // request-scoped context
	RequestID string
	ServerID  string
	Method    string
	LogLevel  LogLevel // client-requested minimum log level
	logs      []LogMessage
}

// ClientLog records a log message that will be sent to the client.
// The message is only recorded if its level is at or above the client-requested log level.
func (ctx *CallContext) ClientLog(level LogLevel, msg string, extras ...KV) {
	if logLevelPriority(level) > logLevelPriority(ctx.LogLevel) {
		return
	}
	logMsg := LogMessage{
		Level:   level,
		Message: msg,
	}
	if len(extras) > 0 {
		logMsg.Extras = make(map[string]string, len(extras))
		for _, kv := range extras {
			logMsg.Extras[kv.Key] = kv.Value
		}
	}
	ctx.logs = append(ctx.logs, logMsg)
}

// drainLogs returns and clears all accumulated log messages.
func (ctx *CallContext) drainLogs() []LogMessage {
	logs := ctx.logs
	ctx.logs = nil
	return logs
}
