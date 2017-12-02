package writeaheadlog

const (
	checksumSize = 16
	pageSize     = 4096
	pageMetaSize = 8 // nextPage offset

	// MaxPayloadSize is the number of bytes that can fit into a single
	// page. For best performance, the number of pages written should be
	// minimized, so clients should try to keep the length of an Update's
	// Instructions field slightly below a multiple of MaxPayloadSize.
	MaxPayloadSize = pageSize - pageMetaSize

	// firstPageMetaSize is the size of the marshalled non-payload data of a
	// transaction's firstPage. It includes the txnStatus, sequence number,
	// checksum, and nextPage offset.
	firstPageMetaSize = 8 + 8 + checksumSize + 8

	// maxFirstPayloadSize is the number of bytes that can fit into the first
	// page of a transaction. The first page holds more metadata than the
	// subsequent pages, so its maximum payload is smaller.
	maxFirstPayloadSize = pageSize - firstPageMetaSize
)

const (
	// txnStatusInvalid indicates an incorrectly initialized page.
	txnStatusInvalid = iota

	// txnStatusWritten indicates that the transaction has been written, but
	// not fully committed, meaning it should be ignored upon recovery.
	txnStatusWritten

	// txnStatusCommitted indicates that the transaction has been committed,
	// but not completed. During recovery, transactions with this status
	// should be loaded and their updates should be provided to the user.
	txnStatusComitted

	// txnStatusApplied indicates that the transaction has been committed and
	// applied. Transactions with this status can be ignored during recovery,
	// and their associated pages can be reclaimed.
	txnStatusApplied
)

const (
	recoveryStateInvalid = iota
	recoveryStateClean
	recoveryStateUnclean
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
