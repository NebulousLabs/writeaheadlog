package wal

import (
	"bytes"
	"testing"
	"runtime"

	"github.com/NebulousLabs/fastrand"
	"github.com/NebulousLabs/Sia/crypto"
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
		transactionChecksum: [crypto.HashSize]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
	}

	// Marshal and unmarshal data
	marshalledBytes, err := currentPage.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal the page %v", err)
	}

	pageRestored := page{}
	unmarshalPage(&pageRestored, marshalledBytes)

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

// BenchmarkPageMarshalling benchmarks the marshalling of pages in a worst case scenario where all
// pages are processed in the same thread
func BenchmarkPageMarshalling(b *testing.B) {
	nextPage := page{
		offset: 12345,
	}
	currentPage := page{
		nextPage:            &nextPage,
		offset:              4096,
		transactionNumber:   42,
		payload:             fastrand.Bytes(4096 - 64), // ensure marshalled size is 4096 bytes
		pageStatus:          pageStatusComitted,
		transactionChecksum: [crypto.HashSize]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
	}

	// Create around 400mb of pages
	pages := 100000

	// Start timer after the test was set up
	b.ResetTimer()

	// Marshal and unmarshal pagesPerThread pages
	for i:= 0; i<pages; i++ {
		marshalledBytes, err := currentPage.Marshal()
		if err != nil {
			b.Fatalf("Failed to marshal the page %v", err)
		}

		pageRestored := page{}
		_, err = unmarshalPage(&pageRestored, marshalledBytes)
		if err != nil {
			b.Fatalf("Failed to unmarshal the page %v", err)
		}
	}
}

// BenchmarkPageMarshallingParallel benchmarks the marshalling of pages in a best case scenario where all
// pages are evenly distributed across cpu cores
func BenchmarkPageMarshallingParallel(b *testing.B) {
	nextPage := page{
		offset: 12345,
	}
	currentPage := page{
		nextPage:            &nextPage,
		offset:              4096,
		transactionNumber:   42,
		payload:             fastrand.Bytes(4096 - 64), // ensure marshalled size is 4096 bytes
		pageStatus:          pageStatusComitted,
		transactionChecksum: [crypto.HashSize]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
	}

	// Start one thread per cpu core
	numThreads := runtime.NumCPU()

	// Create around 400mb of pages
	pagesPerThread := 100000 / numThreads

	wait := make(chan struct{})
	f := func () {
		// Marshal and unmarshal pagesPerThread pages
		for i:= 0; i<pagesPerThread; i++ {
			marshalledBytes, err := currentPage.Marshal()
			if err != nil {
				b.Fatalf("Failed to marshal the page %v", err)
			}

			pageRestored := page{}
			_, err = unmarshalPage(&pageRestored, marshalledBytes)
			if err != nil {
				b.Fatalf("Failed to unmarshal the page %v", err)
			}
		}
		// signal the channel when finished
		wait<- struct{}{}
	}

	// Start timer after the test was set up
	b.ResetTimer()

	// Start the threads and wait for them to finish
	for i:=0; i<numThreads;i++ {
		go f()
	}
	for i:=0; i<numThreads;i++ {
		<-wait
	}
}
