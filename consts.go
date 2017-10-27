package wal

import "github.com/NebulousLabs/Sia/crypto"

const (
	pageSize       = 4096
	pageMetaSize   = 64 // 32-byte checksum + 4 uint64s
	maxPayloadSize = pageSize - pageMetaSize
)

const (
	pageStatusInvalid = iota
	pageStatusOther
	pageStatusWritten
	pageStatusComitted
	pageStatusApplied
)

var (
	// metadata defines the WAL v1.0.0 metadata
	metadata = Metadata{
		Header:  "WAL",
		Version: "1.0.0",
	}
)

// Metadata contains the header and version of the data being stored.
type Metadata struct {
	Header, Version string
}

type checksum [crypto.HashSize]byte
