package vgirpc

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// NewTestOutputCollector returns an OutputCollector for unit-testing a
// producer's or exchange's Produce/Exchange/Process code without a server.
//
// The collector behaves as the one dispatch hands a producer: at most one data
// batch per call, Finish allowed. Inspect what the code emitted with
// [OutputCollector.Batches], [OutputCollector.Logs] and
// [OutputCollector.Finished]; [OutputCollector.Validate] reports what dispatch
// would reject (a second data batch, or an unfinished turn with no data).
// Call [OutputCollector.Reset] between turns to reuse it, which releases the
// batches it holds.
//
//	out := vgirpc.NewTestOutputCollector(schema)
//	if err := state.Produce(ctx, out, callCtx); err != nil { ... }
//	rows := out.Batches()[0].NumRows()
func NewTestOutputCollector(schema *arrow.Schema) *OutputCollector {
	return newOutputCollector(schema, "", true)
}

// Batches returns the data batches emitted since the collector was created or
// last Reset (log batches excluded), in order. The collector keeps ownership:
// retain a batch to use it after Reset.
func (o *OutputCollector) Batches() []arrow.RecordBatch {
	var out []arrow.RecordBatch
	for _, ab := range o.batches {
		if ab.meta != nil {
			if _, isLog := ab.meta.GetValue(MetaLogLevel); isLog {
				continue
			}
		}
		out = append(out, ab.batch)
	}
	return out
}

// TestLog is a client-directed log message captured by an OutputCollector.
type TestLog struct {
	Level   LogLevel
	Message string
}

// Logs returns the ClientLog messages emitted since the collector was created
// or last Reset, in order.
func (o *OutputCollector) Logs() []TestLog {
	var out []TestLog
	for _, ab := range o.batches {
		if ab.meta == nil {
			continue
		}
		level, isLog := ab.meta.GetValue(MetaLogLevel)
		if !isLog {
			continue
		}
		msg, _ := ab.meta.GetValue(MetaLogMessage)
		out = append(out, TestLog{Level: LogLevel(level), Message: msg})
	}
	return out
}

// Validate reports the error dispatch would raise for this turn: a protocol
// violation (more than one data batch), or no data batch on a turn that did
// not Finish.
func (o *OutputCollector) Validate() error { return o.validate() }

// Reset releases the collected batches and clears the per-turn state, keeping
// Finished so a test loop can stop on it.
func (o *OutputCollector) Reset() {
	o.releaseBatches()
}
