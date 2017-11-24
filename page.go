package writeaheadlog

import (
	"encoding/binary"
	"fmt"
	"math"
)

// page is linked list of on-disk pages that comprise a set of Updates.
type page struct {
	// nextPage points to the logical next page in the logFile for this
	// transaction. The page may not appear in the file in-order. When
	// marshalled, this value is encoded as nextPage.offset. If nextPage is
	// nil, it is encoded as math.MaxUint64.
	nextPage *page

	// offset is the offset in the file that this page has. It is not
	// marshalled to disk.
	offset uint64

	// payload contains the marshalled Updates, which may be spread over
	// multiple pages. If spread over multiple pages, the full payload can be
	// assembled via concatenation.
	payload []byte
}

func (p page) size() int { return pageMetaSize + len(p.payload) }

func (p page) nextOffset() uint64 {
	if p.nextPage == nil {
		return math.MaxUint64
	}
	return p.nextPage.offset
}

// appendTo appends the marshalled bytes of p to buf, returning the new slice.
func (p *page) appendTo(buf []byte) []byte {
	if p.size() > pageSize {
		panic(fmt.Sprintf("sanity check failed: page is %d bytes too large", p.size()-pageSize))
	}

	// if buf has enough capacity to hold p, use it; otherwise allocate
	var b []byte
	if rest := buf[len(buf):]; cap(rest) >= p.size() {
		b = rest[:p.size()]
	} else {
		b = make([]byte, p.size())
	}

	// write page contents
	binary.LittleEndian.PutUint64(b[0:], p.nextOffset())
	copy(b[8:], p.payload)

	return append(buf, b...)
}
