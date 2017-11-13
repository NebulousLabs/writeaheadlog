package writeaheadlog

import (
	"bytes"
	"testing"

	"github.com/NebulousLabs/fastrand"
)

// TestPageMarshalling checks that pages can be marshalled and unmarshalled correctly
func TestPageMarshalling(t *testing.T) {
	nextPage := page{
		offset: 12345,
	}
	currentPage := page{
		nextPage:            &nextPage,
		offset:              4096,
		transactionNumber:   42,
		payload:             []byte{1, 1, 2, 3, 5, 8, 13, 21, 1, 1, 2, 3, 5, 8, 13, 21},
		pageStatus:          pageStatusComitted,
		transactionChecksum: checksum{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}

	// Marshal and unmarshal data
	b := currentPage.appendTo(nil)

	var pageRestored page
	unmarshalPage(&pageRestored, b)

	// Check if the fields are the same
	if pageRestored.transactionNumber != currentPage.transactionNumber {
		t.Errorf("transaction number was %v but should be %v",
			pageRestored.transactionNumber, currentPage.transactionNumber)
	}
	if bytes.Compare(pageRestored.payload, currentPage.payload) != 0 {
		t.Errorf("payload was %v but should be %v",
			pageRestored.payload, currentPage.payload)
	}
	if pageRestored.pageStatus != currentPage.pageStatus {
		t.Errorf("pageStatus was %v but should be %v",
			pageRestored.pageStatus, currentPage.pageStatus)
	}
	if pageRestored.transactionChecksum != currentPage.transactionChecksum {
		t.Errorf("transactionChecksum was %v but should be %v",
			pageRestored.transactionChecksum, currentPage.transactionChecksum)
	}
}

// BenchmarkPageAppendTo benchmarks the appendTo method of page.
func BenchmarkPageAppendTo(b *testing.B) {
	p := page{
		offset:            4096,
		transactionNumber: 42,
		payload:           fastrand.Bytes(MaxPayloadSize), // ensure marshalled size is 4096 bytes
		pageStatus:        pageStatusComitted,
		nextPage: &page{
			offset: 12345,
		},
	}
	fastrand.Read(p.transactionChecksum[:])
	buf := make([]byte, pageSize)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		p.appendTo(buf[:0])
	}
}
