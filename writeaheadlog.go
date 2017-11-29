// Package writeaheadlog defines and implements a general purpose, high
// performance write-ahead-log for performing ACID transactions to disk without
// sacrificing speed or latency more than fundamentally required.
package writeaheadlog

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/NebulousLabs/errors"
)

// WAL is a general purpose, high performance write-ahead-log for performing
// ACID transactions to disk without sacrificing speed or latency more than
// fundamentally required.
type WAL struct {
	// atomicNextTxnNum is used to give every transaction a unique transaction
	// number. The transaction will then wait until atomicTransactionCounter allows
	// the transaction to be committed. This ensures that transactions are committed
	// in the correct order.
	atomicNextTxnNum uint64

	// atomicUnfinishedTxns counts how many transactions were created but not
	// released yet. This counter needs to be 0 for the wal to exit cleanly.
	atomicUnfinishedTxns int64

	// Variables to coordinate batched syncs. See sync.go for more information.
	atomicSyncStatus uint32        // 0: no syncing thread, 1: syncing thread, empty queue, 2: syncing thread, non-empty queue.
	syncErr          *error        // coordinates errors around consecutive fsyncs
	syncMu           sync.Mutex    // protects sync variables
	syncRWMu         *sync.RWMutex // coordinates blocking around consecutive fsyncs
	syncStatus       int

	// availablePages lists the offset of file pages which currently have completed or
	// voided updates in them. The pages are in no particular order.
	availablePages []uint64

	// filePageCount indicates the number of pages total in the file. If the
	// number of availablePages ever drops below the number of pages required
	// for a new transaction, then the file is extended, new pages are added,
	// and the availablePages array is updated to include the extended pages.
	filePageCount int

	// recoveryComplete indicates if the caller signalled that the recovery is complete
	recoveryComplete bool

	// dependencies are used to inject special behaviour into the wal by providing
	// custom dependencies when the wal is created and calling deps.disrupt(setting).
	// The following settings are currently available
	deps     dependencies
	logFile  file
	mu       sync.Mutex
	path     string        // path of the underlying logFile
	stopChan chan struct{} // signals shutdown
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
	// Create a new WAL.
	//
	// sync.go expects there to be both a syncErr and a syncRWMu, and it
	// expects the syncRWMu to have a writelock. We must prepare that at
	// startup.
	newWal := &WAL{
		deps:     deps,
		stopChan: make(chan struct{}),
		syncRWMu: new(sync.RWMutex),
		syncErr:  new(error),
		path:     path,
	}
	newWal.syncRWMu.Lock()

	// Create a condition for the wal
	// Try opening the WAL file.
	data, err := deps.readFile(path)
	if err == nil {
		// Reuse the existing wal
		newWal.logFile, err = deps.openFile(path, os.O_RDWR, 0600)
		if err != nil {
			return nil, nil, errors.Extend(errors.New("unable to open wal logFile"), err)
		}

		// Recover WAL and return updates
		updates, err := newWal.recoverWAL(data)
		if err != nil {
			err = errors.Compose(err, newWal.logFile.Close())
			return nil, nil, errors.Extend(err, errors.New("unable to perform wal recovery"))
		}
		if len(updates) == 0 {
			// if there are no updates to apply, set the recovery to complete
			newWal.recoveryComplete = true
		}
		return updates, newWal, nil

	} else if !os.IsNotExist(err) {
		// the file exists but couldn't be opened
		return nil, nil, errors.Extend(err, errors.New("walFile was not opened successfully"))
	}

	// Create new empty WAL
	newWal.logFile, err = deps.create(path)
	if err != nil {
		return nil, nil, errors.Extend(err, errors.New("walFile could not be created"))
	}
	// Write the metadata to the WAL
	if err = writeWALMetadata(newWal.logFile); err != nil {
		return nil, nil, errors.Extend(err, errors.New("Failed to write metadata to file"))
	}
	// No recovery needs to be performed.
	newWal.recoveryComplete = true
	return nil, newWal, nil
}

// readWALMetadata reads WAL metadata from the input file, returning an error
// if the result is unexpected.
func readWALMetadata(data []byte) (uint16, error) {
	// The metadata should at least long enough to contain all the fields.
	if len(data) < len(metadataHeader)+len(metadataVersion)+metadataStatusSize {
		return 0, errors.New("unable to read wal metadata")
	}

	// Check that the header and version match.
	if !bytes.Equal(data[:len(metadataHeader)], metadataHeader[:]) {
		return 0, errors.New("file header is incorrect")
	}
	if !bytes.Equal(data[len(metadataHeader):len(metadataHeader)+len(metadataVersion)], metadataVersion[:]) {
		return 0, errors.New("file version is unrecognized - maybe you need to upgrade")
	}
	// Determine and return the current status of the file.
	fileState := uint16(data[len(metadataHeader)+len(metadataVersion)])
	if fileState <= 0 || fileState > 3 {
		fileState = recoveryStateUnclean
	}
	return fileState, nil
}

// recoverWAL recovers a WAL and returns comitted but not finished updates
func (w *WAL) recoverWAL(data []byte) ([]Update, error) {
	// Validate metadata
	recoveryState, err := readWALMetadata(data[0:])
	if err != nil {
		return nil, errors.Extend(err, errors.New("unable to read wal metadata"))
	}

	if recoveryState == recoveryStateClean {
		if err := w.writeRecoveryState(recoveryStateUnclean); err != nil {
			return nil, errors.Extend(err, errors.New("unable to write WAL recovery state"))
		}
		w.recoveryComplete = true
		return nil, nil
	}

	// If recoveryState is set to wipe we don't need to recover but we have to
	// wipe the wal and change the state
	if recoveryState == recoveryStateWipe {
		if err := w.wipeWAL(); err != nil {
			return nil, errors.Extend(err, errors.New("unable to wipe wal"))
		}
		if err := w.writeRecoveryState(recoveryStateUnclean); err != nil {
			return nil, errors.Extend(err, errors.New("unable to write WAL recovery state"))
		}
		w.recoveryComplete = true
		if err := w.logFile.Sync(); err != nil {
			return nil, errors.Extend(err, errors.New("unable to sync after writing recovery state"))
		}
		return nil, nil
	}

	// load all normal pages
	type diskPage struct {
		page
		nextPageOffset uint64
	}
	pageSet := make(map[uint64]*diskPage) // keyed by offset
	for i := uint64(pageSize); i+pageSize <= uint64(len(data)); i += pageSize {
		nextOffset := binary.LittleEndian.Uint64(data[i:])
		if nextOffset < pageSize {
			// nextOffset is actually a transaction status
			continue
		}
		pageSet[i] = &diskPage{
			page: page{
				offset:  i,
				payload: data[i+pageMetaSize : i+pageSize],
			},
			nextPageOffset: nextOffset,
		}
	}

	// fill in each nextPage pointer
	for _, p := range pageSet {
		if nextDiskPage, ok := pageSet[p.nextPageOffset]; ok {
			p.nextPage = &nextDiskPage.page
		}
	}

	// reconstruct transactions
	var txns []Transaction
nextTxn:
	for i := pageSize; i+pageSize <= len(data); i += pageSize {
		status := binary.LittleEndian.Uint64(data[i:])
		if status != txnStatusComitted {
			continue
		}
		// decode metadata and first page
		seq := binary.LittleEndian.Uint64(data[i+8:])
		var diskChecksum checksum
		n := copy(diskChecksum[:], data[i+16:])
		nextPageOffset := binary.LittleEndian.Uint64(data[i+16+n:])
		firstPage := &page{
			payload: data[i+firstPageMetaSize : i+pageSize],
		}
		if nextDiskPage, ok := pageSet[nextPageOffset]; ok {
			firstPage.nextPage = &nextDiskPage.page
		}

		// Check if the pages of the transaction form a loop
		visited := make(map[uint64]struct{})
		for page := firstPage; page != nil; page = page.nextPage {
			if _, exists := visited[page.offset]; exists {
				// Loop detected
				continue nextTxn
			}
			visited[page.offset] = struct{}{}
		}

		txn := Transaction{
			status:         status,
			sequenceNumber: seq,
			firstPage:      firstPage,
		}

		// validate checksum
		if txn.checksum() != diskChecksum {
			continue
		}

		// decode updates
		var updateBytes []byte
		for page := txn.firstPage; page != nil; page = page.nextPage {
			updateBytes = append(updateBytes, page.payload...)
		}
		updates, err := unmarshalUpdates(updateBytes)
		if err != nil {
			continue
		}
		txn.Updates = updates

		txns = append(txns, txn)
	}

	// sort txns by sequence number
	sort.Slice(txns, func(i, j int) bool {
		return txns[i].sequenceNumber < txns[j].sequenceNumber
	})

	// concatenate the updates of each transaction
	var updates []Update
	for _, txn := range txns {
		updates = append(updates, txn.Updates...)
	}
	return updates, nil
}

// writeRecoveryState is a helper function that changes the recoveryState on disk
func (w *WAL) writeRecoveryState(state uint16) error {
	_, err := w.logFile.WriteAt([]byte{byte(state)}, int64(len(metadataHeader)+len(metadataVersion)))
	if err != nil {
		return err
	}
	return w.logFile.Sync()
}

// RecoveryComplete is called after a wal is recovered to signal that it is
// safe to reset the wal
func (w *WAL) RecoveryComplete() error {
	// Set the metadata to wipe
	if err := w.writeRecoveryState(recoveryStateWipe); err != nil {
		return err
	}

	// Sync before we start wiping
	if err := w.logFile.Sync(); err != nil {
		return err
	}

	// Simulate crash after recovery state was written
	if w.deps.disrupt("RecoveryFail") {
		return nil
	}

	// Wipe the wal
	if err := w.wipeWAL(); err != nil {
		return err
	}

	// Set the metadata to unclean again
	if err := w.writeRecoveryState(recoveryStateUnclean); err != nil {
		return err
	}

	w.recoveryComplete = true
	return nil
}

// managedReservePages reserves pages for a given payload and links them
// together, allocating new pages if necessary. It returns the first page in
// the chain.
func (w *WAL) managedReservePages(data []byte) *page {
	// Find out how many pages are needed for the payload
	numPages := len(data) / MaxPayloadSize
	if len(data)%MaxPayloadSize != 0 {
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
	buf := bytes.NewBuffer(data)
	pages := make([]page, numPages)
	for i := range pages {
		// Set nextPage if the current page isn't the last one
		if i+1 < numPages {
			pages[i].nextPage = &pages[i+1]
		}

		// Set offset according to the index in reservedPages
		pages[i].offset = reservedPages[i]

		// Copy part of the update into the payload
		pages[i].payload = buf.Next(MaxPayloadSize)
	}

	return &pages[0]
}

// wipeWAL sets all the pages of the WAL to applied so that they can be reused.
func (w *WAL) wipeWAL() error {
	// Marshal the txnStatusApplied
	txnAppliedBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(txnAppliedBytes, txnStatusApplied)

	// Get the length of the file.
	stat, err := w.logFile.Stat()
	if err != nil {
		return err
	}
	length := stat.Size()

	// Set all pages to applied.
	for offset := int64(pageSize); offset < length; offset += pageSize {
		if _, err := w.logFile.WriteAt(txnAppliedBytes, offset); err != nil {
			return err
		}
	}

	// Sync the wipe to disk
	return w.logFile.Sync()
}

// writeWALMetadata writes WAL metadata to the input file.
func writeWALMetadata(f file) error {
	// Create the metadata.
	data := make([]byte, 0, len(metadataHeader)+len(metadataVersion)+metadataStatusSize)
	data = append(data, metadataHeader[:]...)
	data = append(data, metadataVersion[:]...)
	// Penultimate byte is the recovery state, and final byte is a newline.
	data = append(data, byte(recoveryStateUnclean))
	data = append(data, byte('\n'))
	_, err := f.WriteAt(data, 0)
	return err
}

// Close closes the wal, frees used resources and checks for active
// transactions.
func (w *WAL) Close() error {
	// Check if there are unfinished transactions
	var err1 error
	if atomic.LoadInt64(&w.atomicUnfinishedTxns) != 0 {
		err1 = errors.New("There are still non-released transactions left")
	}

	// Write the recovery state to indicate clean shutdown if no error occured
	if err1 == nil && !w.deps.disrupt("UncleanShutdown") {
		err1 = w.writeRecoveryState(recoveryStateClean)
	}

	// Close the logFile and stopChan
	err2 := w.logFile.Close()
	close(w.stopChan)

	return errors.Compose(err1, err2)
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
	return newWal(path, &dependencyProduction{})
}
