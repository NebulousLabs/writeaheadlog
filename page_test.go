package writeaheadlog

import (
	"bytes"
	"testing"
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
		transactionChecksum: [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
	}

	// Marshal and unmarshal data
	marshalledBytes, err := currentPage.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal the page %v", err)
	}

	// Check if marshalled data has the correct length
	if len(marshalledBytes) != pageSize {
		t.Errorf("Marshalled data length should be %v but was %v", pageSize, len(marshalledBytes))
	}

	pageRestored := page{}
	pageRestored.Unmarshal(marshalledBytes)

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
