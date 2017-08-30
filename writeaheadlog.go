// Package wal defines and implements a general purpose, high performance
// write-ahead-log for performing ACID transactions to disk without sacrificing
// speed or latency more than fundamentally required.
package wal

import (
	"encoding/json"
	"encoding/binary"
	"io"
	"math"
	"os"
	"sync"
	"bytes"

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
	// number of availablePages ever drops below the number of pages
	// required, then the file is extended, new pages are added, and the
	// availablePages array is updated to include the extended pages.
	filePageCount uint64

	// transactionCounter tracks what transaction is next to be committed. Each
	// transaction is given a unique, ordered transaction number upon being
	// committed, ensuring that transactions can be returned to the user in the
	// correct order in the event of unclean shutdown.
	transactionCounter uint64

	// logFile contains all of the persistent data associated with the log.
	logFile *os.File

	// Utilities
	settings map[string]bool
	log      *persist.Logger
	mu       sync.RWMutex
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
func New(path string, logger *persist.Logger, cancel <-chan struct{}, walStopped chan struct{}, settings map[string]bool) (u []Update, w *WAL, err error) {
	// Create a new WAL
	newWal := WAL{
		availablePages:     []uint64{},
		filePageCount:      0,
		transactionCounter: 0,
		logFile:            nil,
		log:                logger,
		settings:           settings,
	}

	// Try opening the WAL file.
	newWal.logFile, err = os.OpenFile(path, os.O_RDWR, 0600)
	if err == nil {
		// err == nil indicates that there is a WAL file, which means that the
		// previous shutdown was not clean. Restore the WAL and return the updates
		newWal.log.Println("WARN: WAL file detected, performing recovery after unclean shutdown.")

		// Recover WAL and return updates
		updates, err := newWal.recover()
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

	// If there were no errors prepare clean shutdown
	go func() {
		select {
		case <-cancel:
		}
		w.logFile.Close()
		if !w.settings["cleanWALFile"] {
			os.Remove(path)
		}
		close(walStopped)
	}()
	return nil, &newWal, nil
}

// NewTransaction creates a transaction from a set of updates
func (w *WAL) NewTransaction(updates []Update) *Transaction {
	// Create new transaction
	newTransaction := Transaction{
		commitComplete:  false,
		setupComplete:   false,
		releaseComplete: false,
		firstPage:       nil,
		finalPage:       nil,
		Updates:         updates,
		wal:             w,
	}
	return &newTransaction
}

// restoreTransactions restores the transactions contained in a WAL from its pages
// and a dictionary which maps each page to the next one
func (w *WAL) restoreTransactions(pages []page, previousPages map[uint64]page) ([]Transaction, error) {
	firstPages := []page{}
	// Link all pages to their predecessors
	for _, page := range pages {
		previousPage, exists := previousPages[page.offset]
		if page.pageStatus == pageStatusInvalid {
			// Sanity check. Pages shouldn't be saved with pageStatusInvalid
			panic(errors.New("Page was not initialized correctly"))
		}
		// ignore pages that were not yet committed or were already released
		if !exists && page.pageStatus == pageStatusComitted {
			// The page seems to be a firstPage. Remember it.
			firstPages = append(firstPages, page)
		} else if exists {
			// Link the page to it's predecessor
			previousPage.nextPage = &page
		}
	}
	// Create one transaction for each first page
	txns := []Transaction{}
	for _, currentPage := range firstPages {
		// loop over all the pages of the transaction, retrieve the payloads and decode them
		p := &currentPage
		var finalPage *page
		updateBytes := []byte{}
		for p != nil {
			updateBytes = append(updateBytes, p.payload...)
			// remember lastPage
			if p.nextPage == nil {
				finalPage = p
			}
			p = p.nextPage
		}

		// Unmarshal the updates of the current transaction
		updates := []Update{}
		err := json.Unmarshal(updateBytes, &updates)
		if err != nil {
			return nil, build.ExtendErr("Unable to unmarshal updates", err)
		}

		// Create the transaction
		firstPage := currentPage
		txn := Transaction{
			firstPage: &firstPage,
			finalPage: finalPage,
			wal:       w,
			Updates:   updates,
		}

		// sanity check: firstPage must lead to lastPage
		p = txn.firstPage
		for p.nextPage != nil {
			p = p.nextPage
		}
		if p.offset != txn.finalPage.offset {
			panic("sanity check failed. firstPage doesn't lead to finalPage")
		}

		// set the progress accordingly
		txn.commitComplete = true
		txn.setupComplete = true
		txns = append(txns, txn)
	}
	return txns, nil
}

// UnmarshalPage is a helper function that unmarshals the page and returns the offset of the next one
// Note: setting offset and validating the checksum needs to be handled by the caller
func unmarshalPage(p *page, b []byte) (nextPage uint64, err error) {
	buffer := bytes.NewBuffer(b)

	// Read pageStatus, transactionNumber, nextPage and checksum
	err1 := binary.Read(buffer, binary.LittleEndian, &p.pageStatus)
	err2 := binary.Read(buffer, binary.LittleEndian, &p.transactionNumber)
	err3 := binary.Read(buffer, binary.LittleEndian, &nextPage)
	_, err4 := buffer.Read(p.transactionChecksum[:])

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

// recover recovers a WAL and returns comitted but not finished updates
func (w *WAL) recover() ([]Update, error) {
	// Read pages from the WAL one at a time and load them back into memory.
	var pageBytes [pageSize]byte
	var pages []page
	previousPages := make(map[uint64]page)
	var err error
	for i := 0; err == nil; i++ {
		// read the page data
		offset := int64(i) * pageSize
		_, err = w.logFile.ReadAt(pageBytes[:], offset)
		if err != nil {
			continue
		}

		// unmarshall the page
		var currentPage page
		currentPage.offset = uint64(offset)
		nextPage, err := unmarshalPage(&currentPage, pageBytes[:])
		if err != nil {
			continue
		}

		// remember the value of nextPage so we can link pages back together later
		// if the next page equals MaxUint64 there is no next page
		if nextPage != math.MaxUint64 {
			previousPages[nextPage] = currentPage
		}

		pages = append(pages, currentPage)
	}
	if err != io.EOF {
		w.log.Println("ERROR: could not load WAL:", err)
		return nil, build.ExtendErr("error loading WAL json", err)
	}

	// restore transactions
	txns, err := w.restoreTransactions(pages, previousPages)

	// filter out corrupted updates
	updates := []Update{}
	for _, txn := range txns {
		if err := txn.validateChecksum(); err != nil {
			continue
		}
		updates = append(updates, txn.Updates...)
	}
	return updates, nil
}
