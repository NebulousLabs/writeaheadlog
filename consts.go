package wal

const (
	// pageSize defines the size of a single page in the wal
	pageSize = 4096

	// maxPayloadSize defines the max size a payload can have to still fit in a single page
	maxPayloadSize = pageSize - 64
)

const (
	pageStatusOther    = 0
	pageStatusWritten  = 1
	pageStatusComitted = 2
	pageStatusApplied  = 3
)
