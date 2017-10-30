package wal

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/NebulousLabs/errors"
)

// page is an on-disk page in the logFile which contains information about
// an update.
type page struct {
	// offset is the offset in the file that this page has.
	offset uint64 // NOT marshalled to disk.

	// transactionChecksum is the hash of all the pages and data in the
	// committed transaction, including page status, nextPage, the payloads,
	// and the transaction number. The checksum is only used internal to the
	// WAL.
	transactionChecksum checksum

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
	pageStatus uint64 // marshalled to disk.

	// nextPage points to the logical next page in the logFile for this
	// transaction. The page may not appear in the file in-order. This value
	// is set to math.MaxUint64 if there is no next page (indicating this
	// page is the last). The first page may be the only page, in which
	// case it is also the last page.
	nextPage *page // marshalled as nextPage.offset.

	// transactionNumber is only saved for the last page, which can be
	// determined by nextPage being nil, or when marshalled to disk if
	// it is marshalled to math.MaxUint64
	transactionNumber uint64 // marshalled to disk.

	// payload contains the marshalled update, which may be spread over multiple
	// pages if it is large. If spread over multiple pages, the full payload
	// can be assembled by appending the separate payloads together. To keep the
	// full size of the page at under pageSize bytes, the payload should not
	// be greater than (pageSize - 64 bytes).
	payload []byte // marshalled to disk.
}

func (p page) size() int { return pageMetaSize + len(p.payload) }

// appendTo appends the marshalled bytes of p to buf, returning the new slice.
func (p *page) appendTo(buf []byte) []byte {
	// sanity checks
	if p.pageStatus == pageStatusInvalid {
		panic(errors.New("Sanity check failed. Page was marshalled with invalid PageStatus"))
	} else if p.size() > pageSize {
		panic(fmt.Sprintf("sanity check failed: page is %d bytes too large", p.size()-pageSize))
	}

	var nextPagePosition uint64
	if p.nextPage != nil {
		nextPagePosition = p.nextPage.offset
	} else {
		nextPagePosition = math.MaxUint64
	}

	// ensure buf is large enough to hold p
	if cap(buf) < p.size() {
		buf = make([]byte, 0, p.size())
	}
	buf = buf[:p.size()]

	// write page contents
	n := copy(buf[:], p.transactionChecksum[:])
	binary.LittleEndian.PutUint64(buf[n:], p.pageStatus)
	binary.LittleEndian.PutUint64(buf[n+8:], p.transactionNumber)
	binary.LittleEndian.PutUint64(buf[n+16:], nextPagePosition)
	binary.LittleEndian.PutUint64(buf[n+24:], uint64(len(p.payload)))
	copy(buf[n+32:], p.payload)

	return buf
}
