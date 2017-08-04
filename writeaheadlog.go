// package writeaheadlog defines and implements a general purpose, high performance
// write-ahead-log for performing ACID transactions to disk without sacrificing
// speed or latency more than fundamentally required.
package writeaheadlog

import (
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/persist"
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

	// uncommittedTransactions contains transactions that are ready to be committed
	uncommittedTransactions []*Transaction

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
func New(path string, logger *persist.Logger, cancel <-chan struct{}, syncLoopStopped chan struct{}, settings map[string]bool) (u []Update, w *WAL, err error) {
	// Create a new WAL
	newWal := WAL{
		availablePages:     []uint64{},
		filePageCount:      0,
		transactionCounter: 0,
		logFile:            nil,
		log:                logger,
		settings:           settings,
	}

	// Start the sync loop if there were no errors
	defer func() {
		if err == nil {
			go newWal.threadedSyncLoop(path, cancel, syncLoopStopped)
		}
	}()

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

// threadedSyncLoop commits all uncommitted transactions
func (w *WAL) threadedSyncLoop(walPath string, cancel <-chan struct{}, syncLoopStopped chan struct{}) {
	var shutdown bool = false
	for {
		select {
		case <-cancel:
			// shutdown after one more iteration
			shutdown = true
		case <-time.After(syncLoopInterval):
		}
		w.mu.Lock()
		// commit all transactions that are ready
		for _, txn := range w.uncommittedTransactions {
			txn.commit()
		}
		w.logFile.Sync()
		w.mu.Unlock()

		// shutdown
		if shutdown {
			w.logFile.Close()
			if !w.settings["cleanWALFile"] {
				os.Remove(walPath)
			}
			close(syncLoopStopped)
			return
		}
	}
}

// restoreTransactions restores the transactions contained in a WAL from its pages
// and a dictionary which maps each page to the next one
func (w *WAL) restoreTransactions(pages []page, previousPages map[uint64]page) ([]Transaction, error) {
	firstPages := []page{}
	// Link all pages to their predecessors
	for _, page := range pages {
		previousPage, exists := previousPages[page.offset]
		if !exists && page.pageStatus != pageStatusOther {
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

		// set the progress according to the pageStatus
		switch currentPage.pageStatus {
		case pageStatusApplied:
			txn.releaseComplete = true
			fallthrough
		case pageStatusComitted:
			txn.commitComplete = true
			fallthrough
		case pageStatusWritten:
			txn.setupComplete = true
		default:
			// TODO shouldn't happen
		}
		txns = append(txns, txn)
	}
	return txns, nil
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
		_, err = w.logFile.ReadAt(pageBytes[:], int64(i)*pageSize)
		if err != nil {
			continue
		}

		// unmarshall the page
		var currentPage page
		nextPage, err := currentPage.Unmarshal(pageBytes[:])
		if err != nil {
			continue
		}

		// remember the value of nextPage so we can link pages back together later
		// if the next page equals MaxUint64 there is no next page
		previousPages[nextPage] = currentPage

		pages = append(pages, currentPage)
	}
	if err != io.EOF {
		w.log.Println("ERROR: could not load WAL:", err)
		return nil, build.ExtendErr("error loading WAL json", err)
	}

	// restore transactions
	txns, err := w.restoreTransactions(pages, previousPages)

	// filter out corrupted, uncommitted and finished updates
	updates := []Update{}
	for _, txn := range txns {
		if err := txn.validateChecksum(); err != nil {
			continue
		}
		if txn.firstPage.pageStatus == pageStatusApplied || txn.firstPage.pageStatus == pageStatusWritten {
			continue
		}
		updates = append(updates, txn.Updates...)
	}
	return updates, nil
}
