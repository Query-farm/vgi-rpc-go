// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// serializeSchema serializes an Arrow schema to IPC format bytes.
//
// All that survives describe.go, which owned the retired __describe__ payload
// and the byte-based digest taken over it. Reflection carries schemas as IPC
// too -- a client's whole purpose in asking is to get a schema it can hand to
// its own Arrow implementation -- so the serializer outlived the format it was
// written for. The digest did not: the canonical hash (protocolhash.go) is
// taken over what Arrow decodes to, precisely so these bytes need not match
// across ports.
func serializeSchema(schema *arrow.Schema) []byte {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	w.Close()
	return buf.Bytes()
}
