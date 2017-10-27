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

const (
	metadataHeader  = "WAL"
	metadataVersion = "1.0"
)

// metadata contains the header and version of the WAL.
type metadata struct {
	Header, Version string
}

type checksum [crypto.HashSize]byte
