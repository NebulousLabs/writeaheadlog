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
	metadataHeader  = "github.com/NebulousLabs/writeaheadlog\n"
	metadataVersion = "v1.0.0  \n" // Extra room to allow for versions like v3.14.15

	// 48 bytes total, intentionally divisible by 16.
	metadataHeaderSize = 38
	metadataVersionSize = 9
	metadataStatusSize = 1
)

// A checksum is a 128-bit blake2b hash.
type checksum [checksumSize]byte
