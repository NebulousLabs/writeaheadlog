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

var (
	// "github.com/NebulousLabs/writeaheadlog\n"
	metadataHeader = [38]byte{'g', 'i', 't', 'h', 'u', 'b', '.', 'c', 'o', 'm', '/',
		'N', 'e', 'b', 'u', 'l', 'o', 'u', 's', 'L', 'a', 'b', 's', '/',
		'w', 'r', 'i', 't', 'e', 'a', 'h', 'e', 'a', 'd', 'l', 'o', 'g', '\n'}

	// "v1.0.0   \n" - 3 spaces left to leave room for vXX.XX.XX\n
	metadataVersion = [10]byte{'v', '1', '.', '0', '.', '0', ' ', ' ', ' ', '\n'}

	// First byte is the actual value, second byte is the newline.
	metadataStatusSize = 2
)

// A checksum is a 128-bit blake2b hash.
type checksum [checksumSize]byte
