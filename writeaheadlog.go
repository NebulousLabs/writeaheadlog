// Package writeaheadlog defines and implements a general purpose, high
// performance write-ahead-log for performing ACID transactions to disk without
// sacrificing speed or latency more than fundamentally required.
package writeaheadlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

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
	atomicSyncStatus uint32         // 0: no syncing thread, 1: syncing thread, empty queue, 2: syncing thread, non-empty queue.
	atomicSyncState  unsafe.Pointer // points to a struct containing a RWMutex and an error

	// availablePages lists the offset of file pages which currently have completed or
	// voided updates in them. The pages are in no particular order.
	availablePages []uint64

	// filePageCount indicates the number of pages total in the file. If the
	// number of availablePages ever drops below the number of pages required
	// for a new transaction, then the file is extended, new pages are added,
	// and the availablePages array is updated to include the extended pages.
	filePageCount uint64

	// wg is a WaitGroup that allows us to wait for the syncThread to finish to
	// ensure a clean shutdown
	wg sync.WaitGroup

	// dependencies are used to inject special behaviour into the wal by providing
	// custom dependencies when the wal is created and calling deps.disrupt(setting).
	// The following settings are currently available
	deps    dependencies
	logFile file
	mu      sync.Mutex
	path    string // path of the underlying logFile
}

// allocatePages creates new pages and adds them to the available pages of the wal
func (w *WAL) allocatePages(numPages uint64) {
	// Starting at index 1 because the first page is reserved for metadata
	start := w.filePageCount + 1
	for i := start; i < start+numPages; i++ {
		w.availablePages = append(w.availablePages, uint64(i)*pageSize)
	}
	w.filePageCount += numPages
}

// newWal initializes and returns a wal.
func newWal(path string, deps dependencies) (txns []*Transaction, w *WAL, err error) {
	// Create a new WAL.
	newWal := &WAL{
		deps: deps,
		path: path,
	}
	// sync.go expects the sync state to be initialized with a locked rwMu at
	// startup.
	ss := new(syncState)
	ss.rwMu.Lock()
	atomic.StorePointer(&newWal.atomicSyncState, unsafe.Pointer(ss))

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
		txns, err = newWal.recoverWAL(data)
		if err != nil {
			err = errors.Compose(err, newWal.logFile.Close())
			return nil, nil, errors.Extend(err, errors.New("unable to perform wal recovery"))
		}

		return txns, newWal, nil

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
func (w *WAL) recoverWAL(data []byte) ([]*Transaction, error) {
	// Validate metadata
	recoveryState, err := readWALMetadata(data[0:])
	if err != nil {
		return nil, errors.Extend(err, errors.New("unable to read wal metadata"))
	}

	if recoveryState == recoveryStateClean {
		if err := w.writeRecoveryState(recoveryStateUnclean); err != nil {
			return nil, errors.Extend(err, errors.New("unable to write WAL recovery state"))
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
	var txns []*Transaction
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
			offset:  uint64(i),
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

		txn := &Transaction{
			status:         status,
			setupComplete:  true,
			commitComplete: true,
			sequenceNumber: seq,
			firstPage:      firstPage,
			wal:            w,
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

	// filePageCount is the number of pages minus 1 metadata page
	w.filePageCount = uint64(len(data)) / pageSize
	if len(data)%pageSize != 0 {
		w.filePageCount++
	}
	if w.filePageCount > 0 {
		w.filePageCount--
	}

	// find out which pages are used and add the unused ones to availablePages
	usedPages := make(map[uint64]struct{})
	for _, txn := range txns {
		for page := txn.firstPage; page != nil; page = page.nextPage {
			usedPages[page.offset] = struct{}{}
		}
	}
	for offset := uint64(pageSize); offset < w.filePageCount*pageSize; offset += pageSize {
		if _, exists := usedPages[offset]; !exists {
			w.availablePages = append(w.availablePages, offset)
		}
	}

	// make sure that the unfinished txn counter has the correct value
	w.atomicUnfinishedTxns = int64(len(txns))

	return txns, nil
}

// writeRecoveryState is a helper function that changes the recoveryState on disk
func (w *WAL) writeRecoveryState(state uint16) error {
	_, err := w.logFile.WriteAt([]byte{byte(state)}, int64(len(metadataHeader)+len(metadataVersion)))
	if err != nil {
		return err
	}
	return w.logFile.Sync()
}

// managedReservePages reserves pages for a given payload and links them
// together, allocating new pages if necessary. It returns the first page in
// the chain.
func (w *WAL) managedReservePages(data []byte) *page {
	// Find out how many pages are needed for the payload
	numPages := uint64(len(data) / MaxPayloadSize)
	if len(data)%MaxPayloadSize != 0 {
		numPages++
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	// allocate more pages if necessary
	if pagesNeeded := int64(numPages) - int64(len(w.availablePages)); pagesNeeded > 0 {
		w.allocatePages(uint64(pagesNeeded))

		// sanity check: the number of available pages should now equal the number of required ones
		if int64(len(w.availablePages)) != int64(numPages) {
			panic(fmt.Errorf("sanity check failed: num of available pages (%v) != num of required pages (%v)", len(w.availablePages), numPages))
		}
	}

	// Reserve some pages and remove them from the available ones
	reservedPages := w.availablePages[uint64(len(w.availablePages))-numPages:]
	w.availablePages = w.availablePages[:uint64(len(w.availablePages))-numPages]

	// Set the fields of each page
	buf := bytes.NewBuffer(data)
	pages := make([]page, numPages)
	for i := range pages {
		// Set nextPage if the current page isn't the last one
		if uint64(i+1) < numPages {
			pages[i].nextPage = &pages[i+1]
		}

		// Set offset according to the index in reservedPages
		pages[i].offset = reservedPages[i]

		// Copy part of the update into the payload
		pages[i].payload = buf.Next(MaxPayloadSize)
	}

	return &pages[0]
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

	// Make sure sync thread isn't running
	w.wg.Wait()

	// Close the logFile
	err2 := w.logFile.Close()

	return errors.Compose(err1, err2)
}

// CloseIncomplete closes the WAL and reports the number of transactions that
// are still uncomitted.
func (w *WAL) CloseIncomplete() (int64, error) {
	w.wg.Wait()
	return atomic.LoadInt64(&w.atomicUnfinishedTxns), w.logFile.Close()
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
func New(path string) ([]*Transaction, *WAL, error) {
	// Create a wal with production dependencies
	return newWal(path, &dependencyProduction{})
}
