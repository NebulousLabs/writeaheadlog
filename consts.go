package writeaheadlog

const (
	checksumSize = 16
	pageSize     = 4096
	pageMetaSize = checksumSize + 4*8 // checksum + 4 uint64s

	// MaxPayloadSize is the number of bytes that can fit into a single
	// page. For best performance, the number of pages written should be
	// minimized, so clients should try to keep the length of an Update's
	// Instructions field slightly below a multiple of MaxPayloadSize.
	MaxPayloadSize = pageSize - pageMetaSize
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
