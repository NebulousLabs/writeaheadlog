package wal

const (
	checksumSize   = 16
	pageSize       = 4096
	pageMetaSize   = checksumSize + 4*8 // checksum + 4 uint64s
	maxPayloadSize = pageSize - pageMetaSize
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

// A checksum is a 128-bit blake2b hash.
type checksum [checksumSize]byte
