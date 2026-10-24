// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"log/slog"
)

// MultiDispatchHook combines hooks into one [DispatchHook], so a server can
// carry an access log, Sentry and OpenTelemetry at once despite holding a
// single hook.
//
// OnDispatchStart runs the hooks in order, each receiving the context the
// previous one returned, so a span opened by an earlier hook is current for a
// later one. OnDispatchEnd runs them in reverse — first entered, last exited —
// handing each hook its own token and the call's final context, the one every
// hook contributed to. A hook that reads the context at the end (the access
// log stamps trace_id/span_id there) therefore sees a span opened by a hook
// added after it, and the order hooks are added in does not matter.
//
// A hook that panics is isolated: the panic is logged, the remaining hooks
// still run, and a hook whose OnDispatchStart panicked does not get an
// OnDispatchEnd, again matching the server's handling of a lone hook.
//
// Nil hooks are dropped and nested MultiDispatchHooks are flattened. With no
// hooks left the result is nil, so installing it costs a server nothing; with
// one, that hook is returned unwrapped.
func MultiDispatchHook(hooks ...DispatchHook) DispatchHook {
	var flat []DispatchHook
	for _, h := range hooks {
		switch h := h.(type) {
		case nil:
		case *multiDispatchHook:
			flat = append(flat, h.hooks...)
		default:
			flat = append(flat, h)
		}
	}
	switch len(flat) {
	case 0:
		return nil
	case 1:
		return flat[0]
	}
	return &multiDispatchHook{hooks: flat}
}

type multiDispatchHook struct {
	hooks []DispatchHook
}

// multiHookToken records, per child hook, the token its OnDispatchStart
// returned and whether it returned at all.
type multiHookToken struct {
	tokens []HookToken
	active []bool
}

func (m *multiDispatchHook) OnDispatchStart(ctx context.Context, info DispatchInfo) (context.Context, HookToken) {
	tok := &multiHookToken{
		tokens: make([]HookToken, len(m.hooks)),
		active: make([]bool, len(m.hooks)),
	}
	for i, h := range m.hooks {
		func() {
			defer func() {
				if rv := recover(); rv != nil {
					slog.Error("dispatch hook start panic", "hook", i, "err", rv)
				}
			}()
			hookCtx, hookToken := h.OnDispatchStart(ctx, info)
			if hookCtx != nil {
				ctx = hookCtx
			}
			tok.tokens[i] = hookToken
			tok.active[i] = true
		}()
	}
	return ctx, tok
}

func (m *multiDispatchHook) OnDispatchEnd(ctx context.Context, token HookToken, info DispatchInfo, stats *CallStatistics, err error) {
	tok, ok := token.(*multiHookToken)
	if !ok {
		return
	}
	for i := len(m.hooks) - 1; i >= 0; i-- {
		if !tok.active[i] {
			continue
		}
		func() {
			defer func() {
				if rv := recover(); rv != nil {
					slog.Error("dispatch hook end panic", "hook", i, "err", rv)
				}
			}()
			m.hooks[i].OnDispatchEnd(ctx, tok.tokens[i], info, stats, err)
		}()
	}
}
