package wal

const (
	// pageSize defines the size of a single page in the wal
	pageSize = 4096

	// maxPayloadSize defines the max size a payload can have to still fit in a single page
	maxPayloadSize = pageSize - 64
)

const (
	pageStatusInvalid  = 0
	pageStatusOther    = 1
	pageStatusWritten  = 2
	pageStatusComitted = 3
	pageStatusApplied  = 4
)
