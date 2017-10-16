package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/errors"
)

// page is an on-disk page in the logFile which contains information about
// an update.
type page struct {
	// offset is the offset in the file that this page has.
	offset uint64 // Is NOT marshalled to disk.
	// pageStatus should never be set to '0' since this is the default value
	// and would indicate an incorrectly initialized page
	//
	// pageStatus is set to '1' if the page is not the first page.
	//
	// pageStatus is set to '2' if the transaction has been written, but not
	// fully comitted, meaning it should be ignored upon load, and that the
	// associated pages can be reclaimed.
	//
	// pageStatus is set to '3' if the transaction has been committed, but not
	// completed. If upon load, a page is found with status '2', it should be
	// unmarshalled and passed to the caller of 'New' as an update, sorted
	// according to the
	//
	// pageStatus is set to '4' if the transaction has been committed and
	// applied, meaning that the transaction can be ignored upon load, and the
	// associated pages can be reclaimed.
	pageStatus uint64 // Gets marshalled to disk.

	// nextPage points to the logical next page in the logFile for this
	// transaction. The page may not appear in the file in-order. This value
	// is set to math.MaxUint64 if there is no next page (indicating this
	// page is the last). The first page may be the only page, in which
	// case it is also the last page.
	nextPage *page // Gets marshalled as nextPage.offset.

	// transactionNumber is only saved for the last page, which can be
	// determined by nextPage being nil, or when marshalled to disk if
	// it is marshalled to math.MaxUint64
	transactionNumber uint64 // Gets marshalled to disk.

	// transactionChecksum is the hash of all the pages and data in the
	// committed transaction, including page status, nextPage, the payloads,
	// and the transaction number. The checksum is only used internal to the
	// WAL.
	transactionChecksum [crypto.HashSize]byte // Gets marshalled to disk for the first page only.

	// payload contains the marshalled update, which may be spread over multiple
	// pages if it is large. If spread over multiple pages, the full payload
	// can be assembled by appending the separate payloads together. To keep the
	// full size of the page at under pageSize bytes, the payload should not
	// be greater than (pageSize - 64 bytes).
	payload []byte // Gets marshalled to disk.
}

// Marshal marshals a page.
func (p page) Marshal() ([]byte, error) {
	var nextPage uint64
	if p.nextPage != nil {
		nextPage = p.nextPage.offset
	} else {
		nextPage = math.MaxUint64
	}

	buffer := new(bytes.Buffer)

	// write checksum, pageStatus, transactionNumber and nextPage
	_, err1 := buffer.Write(p.transactionChecksum[:])
	err2 := binary.Write(buffer, binary.LittleEndian, p.pageStatus)
	if p.pageStatus == pageStatusInvalid {
		panic(errors.New("Sanity check failed. Page was marshalled with invalid PageStatus"))
	}

	err3 := binary.Write(buffer, binary.LittleEndian, p.transactionNumber)
	err4 := binary.Write(buffer, binary.LittleEndian, nextPage)

	// write payloadSize and payload
	err5 := binary.Write(buffer, binary.LittleEndian, uint64(len(p.payload)))
	_, err6 := buffer.Write(p.payload)

	// check for errors
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil || err6 != nil {
		return nil, errors.Compose(errors.New("Failed to marshal wal page"), err1, err2, err3, err4, err5, err6)
	}

	// sanity check: page should be smaller or equal to pageSize
	if buffer.Len() > pageSize {
		panic(fmt.Errorf("sanity check failed: marshalled page is %d bytes too large",
			pageSize-buffer.Len()))
	}

	return buffer.Bytes(), nil
}

// Write writes the page to disk
func (p page) Write(f file) error {
	data, err := p.Marshal()
	if err != nil {
		return build.ExtendErr("Marshalling the page failed", err)
	}
	// Write the page to the file
	_, err = f.WriteAt(data, int64(p.offset))
	if err != nil {
		return build.ExtendErr("Writing the page to disk failed", err)
	}

	return nil
}
