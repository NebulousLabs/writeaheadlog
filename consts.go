package wal

const (
	pageSize       = 4096
	pageMetaSize   = 64 // 32-byte checksum + 4 uint64s
	maxPayloadSize = pageSize - pageMetaSize

	checksumSize = 32
)

const (
	pageStatusInvalid = iota
	pageStatusOther
	pageStatusWritten
	pageStatusComitted
	pageStatusApplied
)

const (
	recoveryStateInvalid = iota
	recoveryStateClean
	recoveryStateUnclean
	recoveryStateWipe
)

const (
	metadataHeader  = "WAL"
	metadataVersion = "1.0"
)

// A checksum is a 256-bit blake2b hash.
type checksum [checksumSize]byte
