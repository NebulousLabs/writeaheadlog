package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/NebulousLabs/Sia/build"
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

func (p *page) writeToNoChecksum(w io.Writer) error {
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

	// write pageStatus, transactionNumber, nextPage, and payload size
	buf := make([]byte, 8+8+8+8)
	binary.LittleEndian.PutUint64(buf[0:8], p.pageStatus)
	binary.LittleEndian.PutUint64(buf[8:16], p.transactionNumber)
	binary.LittleEndian.PutUint64(buf[16:24], nextPagePosition)
	binary.LittleEndian.PutUint64(buf[24:32], uint64(len(p.payload)))
	if _, err := w.Write(buf); err != nil {
		return err
	}

	// write payload
	if _, err := w.Write(p.payload); err != nil {
		return err
	}

	return nil
}

func (p *page) writeTo(w io.Writer) error {
	// write checksum
	if _, err := w.Write(p.transactionChecksum[:]); err != nil {
		return err
	}
	// write the rest
	return p.writeToNoChecksum(w)
}

// WriteToFile writes the page to disk
func (p page) writeToFile(f file) error {
	buf := new(bytes.Buffer)
	p.writeTo(buf)
	if _, err := f.WriteAt(buf.Bytes(), int64(p.offset)); err != nil {
		return build.ExtendErr("Writing the page to disk failed", err)
	}

	return nil
}
