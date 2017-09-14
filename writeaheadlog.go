// Package wal defines and implements a general purpose, high performance
// write-ahead-log for performing ACID transactions to disk without sacrificing
// speed or latency more than fundamentally required.
package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"sync"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/persist"
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
	filePageCount uint64

	// atomicNextTransaction is used to give every transaction a unique transaction
	// number. The transaction will then wait until atomicTransactionCounter allows
	// the transaction to be committed. This ensures that transactions are committed
	// in the correct order.
	atomicNextTransaction uint64

	// atomicTransactionCounter tracks what transaction is next to be committed. Each
	// transaction is given a unique, ordered transaction number upon being
	// committed, ensuring that updates can be returned to the user in the
	// correct order in the event of unclean shutdown.
	atomicTransactionCounter uint64

	// logFile contains all of the persistent data associated with the log.
	logFile *os.File

	// mu is used to lock the availablePages field of the wal
	mu sync.Mutex

	// log is used to log errors and warnings
	log *persist.Logger

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
func (w *WAL) allocatePages(numPages uint64) {
	// Starting at index 1 because the first page is reserved for metadata
	for i := w.filePageCount + 1; i < w.filePageCount+numPages+1; i++ {
		w.availablePages = append(w.availablePages, i*pageSize)
	}
	w.filePageCount += numPages
}

// newWal initializes and returns a wal.
func newWal(path string, logger *persist.Logger, deps dependencies) (u []Update, w *WAL, err error) {
	// Create a new WAL
	newWal := WAL{
		deps:     deps,
		log:      logger,
		stopChan: make(chan struct{}),
	}

	// Create a condition for the wal
	newWal.syncCond = sync.NewCond(&newWal.syncMu)

	// Try opening the WAL file.
	data, err := ioutil.ReadFile(path)
	if err == nil {
		// err == nil indicates that there is a WAL file which can be recovered to determine
		// if the shutdown was clean or not
		newWal.log.Println("WARN: WAL file detected, performing recovery.")

		// Recover WAL and return updates
		updates, err := newWal.recover(data)
		if err != nil {
			return nil, nil, err
		}

		// Reuse the existing wal
		newWal.logFile, err = os.OpenFile(path, os.O_RDWR, 0600)

		// Invalidate data in wal
		newWal.logFile.Write(make([]byte, len(data)))

		return updates, &newWal, err

	} else if !os.IsNotExist(err) {
		// the file exists but couldn't be opened
		return nil, nil, build.ExtendErr("walFile was not opened successfully", err)
	}

	// Create new empty WAL
	newWal.logFile, err = os.Create(path)
	if err != nil {
		return nil, nil, build.ExtendErr("walFile could not be created", err)
	}

	// Write the metadata to the WAL
	if err = writeWALMetadata(newWal.logFile); err != nil {
		return nil, nil, build.ExtendErr("Failed to write metadata to file", err)
	}

	// Start the sync loop
	go threadedWalSync(&newWal)

	return nil, &newWal, nil
}

// readWALMetadata reads WAL metadata from the input file, returning an error
// if the result is unexpected.
func readWALMetadata(data []byte) error {
	var md persist.Metadata
	decoder := json.NewDecoder(bytes.NewBuffer(data))
	err := decoder.Decode(&md)
	if err != nil {
		return build.ExtendErr("error reading WAL metadata", err)
	}
	if md.Header != metadata.Header {
		return errors.New("WAL metadata header does not match header found in WAL file")
	}
	if md.Version != metadata.Version {
		return errors.New("WAL metadata version does not match version found in WAL file")
	}
	return nil
}

// recover recovers a WAL and returns comitted but not finished updates
func (w *WAL) recover(data []byte) ([]Update, error) {
	// Get all the first pages to sort them by txn number
	var firstPages ByTxnNumber
	var err error

	// check if the data is long enough to contain the metadata
	if len(data) < pageSize {
		return nil, errors.New("existing WAL with incomplete metadata found")
	}

	// Validate metadata
	if err := readWALMetadata(data[0:pageSize]); err != nil {
		return nil, err
	}

	// Starting at index 1 because the first page is reserved for metadata
	for i := 1; int64(i+1)*pageSize-1 < int64(len(data)); i++ {
		// read the page data
		offset := int64(i) * pageSize

		// increase the number of available pages
		w.availablePages = append(w.availablePages, uint64(offset))

		// unmarshall the page
		var p page
		p.offset = uint64(offset)
		nextPage, err := unmarshalPage(&p, data[offset:offset+pageSize])
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
		if err = unmarshalTransaction(&txn, sp.p, sp.nextPage, data); err != nil {
			// Stop recovering transactions
			break
		}
		updates = append(updates, txn.Updates...)
	}
	w.filePageCount = uint64(len(w.availablePages))

	return updates, nil
}

// reservePages reserves a number of pages by removing them from the available
// pages. If it needs to allocate new pages it will do so
func (w *WAL) reservePages(numPages uint64) []uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	// Check if enough pages are available
	if uint64(len(w.availablePages)) < numPages {
		// Add enough pages for the new transaction
		numNewPages := numPages - uint64(len(w.availablePages))
		w.allocatePages(numNewPages)

		// sanity check: the number of available pages should equal the number of required ones
		if uint64(len(w.availablePages)) != numPages {
			panic(errors.New("sanity check failed: num of available pages != num of required pages"))
		}
	}

	// Reserve some pages and remove them from the available ones
	reservedPages := make([]uint64, numPages)
	copy(reservedPages[:], w.availablePages[uint64(len(w.availablePages))-numPages:])
	w.availablePages = w.availablePages[:uint64(len(w.availablePages))-numPages]
	return reservedPages
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
	var nextPage page
	var err error
	var txnPayload []byte
	// Unmarshal the pages in the order of the transaction
	for {
		// Get the payload of each page
		txnPayload = append(txnPayload, p.payload...)

		// Determine if the last page was reached and set the final page of the txn accordingly
		if npo == math.MaxUint64 {
			txn.finalPage = p
			p.nextPage = nil
			p = nil
			break
		}

		// Set the offset for the next page before npo is overwritten
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

	// Sanity check: firstPage must lead to lastPage
	p = txn.firstPage
	for p.nextPage != nil {
		p = p.nextPage
	}
	if p.offset != txn.finalPage.offset {
		panic("sanity check failed. firstPage doesn't lead to finalPage")
	}

	// Restore updates from payload
	if txn.Updates, err = unmarshalUpdates(txnPayload); err != nil {
		return build.ExtendErr("Unable to unmarshal updates", err)
	}

	// Set flags accordingly
	txn.setupComplete = true
	txn.commitComplete = true

	// Verify the checksum
	return txn.validateChecksum()
}

// writeWALMetadata writes WAL metadata to the input file.
func writeWALMetadata(f *os.File) error {
	changeBytes, err := json.MarshalIndent(metadata, "", "\t")
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
func (w *WAL) Close() {
	close(w.stopChan)
	w.logFile.Close()
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
func New(path string, logger *persist.Logger) (u []Update, w *WAL, err error) {
	// Create a wal with production dependencies
	return newWal(path, logger, prodDependencies{})
}
