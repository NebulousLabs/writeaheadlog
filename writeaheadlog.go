// Package wal defines and implements a general purpose, high performance
// write-ahead-log for performing ACID transactions to disk without sacrificing
// speed or latency more than fundamentally required.
package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"os"
	"sort"
	"sync"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/errors"
)

// WAL is a general purpose, high performance write-ahead-log for performing
// ACID transactions to disk without sacrificing speed or latency more than
// fundamentally required.
type WAL struct {
	// availablePages lists the offset of file pages which currently have completed or
	// voided updates in them. The pages are in no particular order.
	availablePages []uint64

	// filePageCount indicates the number of pages total in the file. If the
	// number of availablePages ever drops below the number of pages required
	// for a new transaction, then the file is extended, new pages are added,
	// and the availablePages array is updated to include the extended pages.
	filePageCount int

	// atomicNextTxnNum is used to give every transaction a unique transaction
	// number. The transaction will then wait until atomicTransactionCounter allows
	// the transaction to be committed. This ensures that transactions are committed
	// in the correct order.
	atomicNextTxnNum uint64

	// logFile contains all of the persistent data associated with the log.
	logFile file

	// path is the path to the underlying logFile
	path string

	// mu is used to lock the availablePages field of the wal
	mu sync.Mutex

	// syncCond is used to schedule the calls to fsync
	syncCond *sync.Cond

	// syncMu is the lock contained in syncCond and must be held before
	// changing the state of the syncCond
	syncMu sync.Mutex

	// syncCount is a counter that indicates how many transactions are
	// currently waiting for a fsync
	syncCount uint64

	// stopChan is a channel that is used to signal a shutdown
	stopChan chan struct{}

	// syncing indicates if the syncing thread is currently being executed
	syncing bool

	// recoveryComplete indicates if the caller signalled that the recovery is complete
	recoveryComplete bool

	// dependencies are used to inject special behaviour into the wal by providing
	// custom dependencies when the wal is created and calling deps.disrupt(setting).
	// The following settings are currently available
	deps dependencies
}

type (
	// SortPage is a helper struct for sorting, that contains a page and the offset
	// of the next page
	SortPage struct {
		p        *page
		nextPage uint64
	}
	// ByTxnNumber is a type that implements the sorting interface for the SortPage struct
	ByTxnNumber []SortPage
)

// Len returns the length of the slice
func (p ByTxnNumber) Len() int {
	return len(p)
}

// Swap swaps two elements of the slice
func (p ByTxnNumber) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// Less determines if one element of the slice is less than the other
func (p ByTxnNumber) Less(i, j int) bool {
	return p[i].p.transactionNumber < p[j].p.transactionNumber
}

// allocatePages creates new pages and adds them to the available pages of the wal
func (w *WAL) allocatePages(numPages int) {
	// Starting at index 1 because the first page is reserved for metadata
	start := w.filePageCount + 1
	for i := start; i < start+numPages; i++ {
		w.availablePages = append(w.availablePages, uint64(i)*pageSize)
	}
	w.filePageCount += numPages
}

// newWal initializes and returns a wal.
func newWal(path string, deps dependencies) (u []Update, w *WAL, err error) {
	// Create a new WAL
	newWal := WAL{
		deps:     deps,
		stopChan: make(chan struct{}),
		path:     path,
	}

	// Create a condition for the wal
	newWal.syncCond = sync.NewCond(&newWal.syncMu)
	// Try opening the WAL file.
	data, err := deps.readFile(path)
	if err == nil {
		// Recover WAL and return updates
		updates, err := newWal.recoverWal(data)
		if err != nil {
			return nil, nil, err
		}

		// Reuse the existing wal
		newWal.logFile, err = deps.openFile(path, os.O_RDWR, 0600)

		return updates, &newWal, err

	} else if !os.IsNotExist(err) {
		// the file exists but couldn't be opened
		return nil, nil, build.ExtendErr("walFile was not opened successfully", err)
	}

	// Create new empty WAL
	newWal.logFile, err = deps.create(path)
	if err != nil {
		return nil, nil, build.ExtendErr("walFile could not be created", err)
	}

	// Write the metadata to the WAL
	if err = writeWALMetadata(newWal.logFile); err != nil {
		return nil, nil, build.ExtendErr("Failed to write metadata to file", err)
	}

	// When we create a new wal we don't need the caller to signal recoveryComplete
	newWal.recoveryComplete = true
	return nil, &newWal, nil
}

// readWALMetadata reads WAL metadata from the input file, returning an error
// if the result is unexpected.
func readWALMetadata(data []byte) error {
	var md metadata
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&md); err != nil {
		return build.ExtendErr("error reading WAL metadata", err)
	} else if md.Header != metadataHeader {
		return errors.New("WAL metadata header does not match header found in WAL file")
	} else if md.Version != metadataVersion {
		return errors.New("WAL metadata version does not match version found in WAL file")
	}
	return nil
}

// recover recovers a WAL and returns comitted but not finished updates
func (w *WAL) recoverWal(data []byte) ([]Update, error) {
	// Get all the first pages to sort them by txn number
	var firstPages ByTxnNumber

	// Validate metadata
	if err := readWALMetadata(data[0:]); err != nil {
		return nil, err
	}
	// Starting at index 1 because the first page is reserved for metadata
	for i := 1; int64(i)*pageSize < int64(len(data)); i++ {
		// read the page data
		offset := int64(i) * pageSize

		// unmarshall the page
		var p page
		var nextPage uint64
		var err error
		p.offset = uint64(offset)

		// Check if the
		if offset+pageSize > int64(len(data)) {
			nextPage, err = unmarshalPage(&p, data[offset:])
		} else {
			nextPage, err = unmarshalPage(&p, data[offset:offset+pageSize])
		}
		if err != nil {
			continue
		}

		// If the page is the first page of a transaction remember it
		if p.pageStatus == pageStatusComitted {
			firstPages = append(firstPages, SortPage{p: &p, nextPage: nextPage})
		}
	}

	// Sort the first pages by transaction number
	sort.Sort(firstPages)
	// Recover the transactions in order and get their updates
	updates := []Update{}
	for _, sp := range firstPages {
		var txn Transaction
		err := unmarshalTransaction(&txn, sp.p, sp.nextPage, data)
		if err != nil {
			continue
		}
		updates = append(updates, txn.Updates...)
	}

	// If there were no updates we can savely set the recovery to complete
	if len(updates) == 0 {
		w.recoveryComplete = true
	}

	return updates, nil
}

// RecoveryComplete is called after a wal is recovered to signal that it is
// save to reset the wal
func (w *WAL) RecoveryComplete() error {
	// Marshal the pageStatusApplied
	pageAppliedBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(pageAppliedBytes, pageStatusApplied)

	// Get the length of the file.
	length, err := w.logFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// Set all pages to applied.
	for offset := int64(crypto.HashSize) + pageSize; offset < length; offset += pageSize {
		if _, err := w.logFile.WriteAt(pageAppliedBytes, offset); err != nil {
			return err
		}
	}
	// Sync to lock down the obliteration.
	w.logFile.Sync()

	w.recoveryComplete = true
	return nil
}

// managedReservePages reserves pages for a given payload. If it needs to
// allocate new pages it will do so
func (w *WAL) managedReservePages(data []byte) []page {
	// Find out how many pages are needed for the payload
	numPages := len(data) / maxPayloadSize
	if len(data)%maxPayloadSize != 0 {
		numPages++
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	// allocate more pages if necessary
	if pagesNeeded := numPages - len(w.availablePages); pagesNeeded > 0 {
		w.allocatePages(pagesNeeded)

		// sanity check: the number of available pages should now equal the number of required ones
		if len(w.availablePages) != numPages {
			panic(errors.New("sanity check failed: num of available pages != num of required pages"))
		}
	}

	// Reserve some pages and remove them from the available ones
	reservedPages := w.availablePages[len(w.availablePages)-numPages:]
	w.availablePages = w.availablePages[:len(w.availablePages)-numPages]

	// Set the fields of each page
	pages := make([]page, numPages)
	for i := range pages {
		// Set offset according to the index in reservedPages
		pages[i].offset = reservedPages[i]

		// Set nextPage if the current page isn't the last one
		// otherwise let it be nil
		if i+1 < numPages {
			pages[i].nextPage = &pages[i+1]
		}

		// Set pageStatus of the first page to pageStatusWritten
		if i == 0 {
			pages[i].pageStatus = pageStatusWritten
		} else {
			pages[i].pageStatus = pageStatusOther
		}

		// Copy part of the update into the payload
		payloadsize := maxPayloadSize
		if len(data[i*maxPayloadSize:]) < payloadsize {
			payloadsize = len(data[i*maxPayloadSize:])
		}
		pages[i].payload = make([]byte, payloadsize)
		copy(pages[i].payload, data[i*maxPayloadSize:])
	}

	return pages
}

// UnmarshalPage is a helper function that unmarshals the page and returns the offset of the next one
// Note: setting offset and validating the checksum needs to be handled by the caller
func unmarshalPage(p *page, b []byte) (nextPage uint64, err error) {
	buffer := bytes.NewBuffer(b)

	// Read checksum, pageStatus, transactionNumber and nextPage
	_, err1 := buffer.Read(p.transactionChecksum[:])
	err2 := binary.Read(buffer, binary.LittleEndian, &p.pageStatus)
	err3 := binary.Read(buffer, binary.LittleEndian, &p.transactionNumber)
	err4 := binary.Read(buffer, binary.LittleEndian, &nextPage)

	// Read payloadSize
	var payloadSize uint64
	err5 := binary.Read(buffer, binary.LittleEndian, &payloadSize)

	// Read payload
	p.payload = make([]byte, payloadSize)
	_, err6 := buffer.Read(p.payload[:])

	// Check for errors
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil || err6 != nil {
		err = errors.Compose(errors.New("Failed to unmarshal wal page"), err1, err2, err3, err4, err5, err6)
		return
	}

	return
}

// unmarshalTransaction unmarshals a transaction starting at the first page
func unmarshalTransaction(txn *Transaction, firstPage *page, nextPageOffset uint64, logData []byte) error {
	// Set the first page of the txn
	txn.firstPage = firstPage

	p := firstPage
	npo := nextPageOffset
	var err error
	var txnPayload []byte
	// Unmarshal the pages in the order of the transaction
	for {
		// Get the payload of each page
		txnPayload = append(txnPayload, p.payload...)

		// Determine if the last page was reached and set the final page of the txn accordingly
		if npo == math.MaxUint64 {
			p.nextPage = nil
			p = nil
			break
		}

		// Set the offset for the next page before npo is overwritten
		var nextPage page
		nextPage.offset = npo

		// Unmarshal the page. The last page might not have pageSize bytes.
		if npo+pageSize > uint64(len(logData)) {
			npo, err = unmarshalPage(&nextPage, logData[npo:])
		} else {
			npo, err = unmarshalPage(&nextPage, logData[npo:npo+pageSize])
		}
		if err != nil {
			return err
		}

		// Move on to the next page
		p.nextPage = &nextPage
		p = &nextPage
	}

	// Verify the checksum before unmarshalling the updates. Otherwise we might
	// get errors later
	if err := txn.validateChecksum(); err != nil {
		return err
	}

	// Restore updates from payload
	if txn.Updates, err = unmarshalUpdates(txnPayload); err != nil {
		return build.ExtendErr("Unable to unmarshal updates", err)
	}

	// Set flags accordingly
	txn.setupComplete = true
	txn.commitComplete = true

	return nil
}

// writeWALMetadata writes WAL metadata to the input file.
func writeWALMetadata(f file) error {
	md := metadata{
		Header:  metadataHeader,
		Version: metadataVersion,
	}
	changeBytes, err := json.MarshalIndent(md, "", "\t")
	if err != nil {
		return build.ExtendErr("could not marshal WAL metadata", err)
	}

	// Sanity check. Metadata must not be greater than pageSize
	if len(changeBytes) > pageSize {
		panic("WAL metadata is greater than pageSize")
	}

	_, err = f.WriteAt(changeBytes, 0)
	if err != nil {
		return build.ExtendErr("unable to write WAL metadata", err)
	}
	return nil
}

// Close closes the wal and frees used resources
func (w *WAL) Close() error {
	close(w.stopChan)
	return w.logFile.Close()
}

// New will open a WAL. If the previous run did not shut down cleanly, a set of
// updates will be returned which got committed successfully to the WAL, but
// were never signaled as fully completed.
//
// If no WAL exists, a new one will be created.
//
// If in debugging mode, the WAL may return a series of updates multiple times,
// simulating multiple consecutive unclean shutdowns. If the updates are
// properly idempotent, there should be no functional difference between the
// multiple appearances and them just being loaded a single time correctly.
func New(path string) (u []Update, w *WAL, err error) {
	// Create a wal with production dependencies
	return newWal(path, prodDependencies{})
}
