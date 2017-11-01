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

var (
	metadataHeader          = [4]byte{'w', 'a', 'l', '\n'}
	metadataVersion         = [10]byte{'v', '1', '.', '0', '.', '0', '\n'}
	metadataShutdownClean   = [2]byte{'c', '\n'}
	metadataShutdownUnclean = [2]byte{'u', '\n'}
	metadataShutdownWipe    = [2]byte{'s', '\n'}
)

// A checksum is a 256-bit blake2b hash.
type checksum [checksumSize]byte
