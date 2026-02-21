package vgirpc

// Well-known metadata keys used in the vgi_rpc wire protocol.
// These appear as custom_metadata on Arrow IPC RecordBatch messages.
const (
	MetaMethod         = "vgi_rpc.method"
	MetaRequestVersion = "vgi_rpc.request_version"
	MetaRequestID      = "vgi_rpc.request_id"
	MetaLogLevel       = "vgi_rpc.log_level"
	MetaLogMessage     = "vgi_rpc.log_message"
	MetaLogExtra       = "vgi_rpc.log_extra"
	MetaServerID       = "vgi_rpc.server_id"
	MetaStreamState    = "vgi_rpc.stream_state"
	MetaShmOffset      = "vgi_rpc.shm_offset"
	MetaShmLength      = "vgi_rpc.shm_length"
	MetaLocation       = "vgi_rpc.location"

	ProtocolVersion = "1"
)
