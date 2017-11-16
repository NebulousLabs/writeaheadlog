package writeaheadlog

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"

	"github.com/NebulousLabs/errors"
	"golang.org/x/crypto/blake2b"
)

// Update defines a single update that can be sent to the WAL and saved
// atomically. Updates are sent to the wal in groups of one or more, and upon
// being signaled, will be saved to disk in a high-integrity, all- or-nothing
// fasion that follows ACID principles.
//
// The name and version are optional, however best-practice code will make use
// of these fields.
//
// When using the Update, it recommended that you typecast the Update type to
// another type which has methods on it for creating and applying the Update +
// instructions, including any special handling based on the version.
type Update struct {
	// The name of the update type. When the WAL is loaded after an unclean
	// shutdown, any un-committed changes will be passed as Updates back to the
	// caller instantiating the WAL. The caller should determine what code to
	// run on the the update based on the name and version.
	Name string

	// The version of the update type. Update types and implementations can be
	// tweaked over time, and a version field allows for easy compatibility
	// between releases, even if an upgrade occurs between an unclean shutdown
	// and a reload.
	Version string

	// The marshalled data directing the update. The data is an opaque set of
	// instructions to follow that implement and idempotent change to a set of
	// persistent files. A series of unclean shutdowns in rapid succession could
	// mean that these instructions get followed multiple times, which means
	// idempotent instructions are required.
	Instructions []byte
}

// Transaction defines a series of updates that are to be performed atomically.
// In the event of an unclean shutdown, either all updates will have been saved
// together at full integrity, or none of the updates in the transaction will
// have been saved at all.
//
// While multiple transactions can be safely open at the same time, multiple
// methods should not be called on the transaction at the same time - the WAL
// is thread-safe, but the transactions are not.
//
// A Transaction is created by calling NewTransaction. Afterwards, the
// transactions SignalSetupComplete has to be called which returns a channel
// that is closed once the transaction is committed. Finally
// SignalUpdatesApplied needs to be called after the transaction was committed
// to signal a successfull transaction and free used pages.
type Transaction struct {
	// setupComplete, commitComplete, and releaseComplete signal the progress of
	// the transaction, and should be set to 'true' in order.
	//
	// When setupComplete is set to true, it means that the creater of the
	// transaction is ready for the transaction to be committed.
	//
	// When commitComplete is set to true, it means that the WAL has
	// successfully and fully committed the transaction.
	//
	// releaseComplete is set to true when the caller has fully applied the
	// transaction, meaning the transaction can be over-written safely in the
	// WAL, and the on-disk pages can be reclaimed for future transactions.
	setupComplete   bool
	commitComplete  bool
	releaseComplete bool

	// firstPage is the first page of the transaction. It is the last page that
	// is written when finalizing a commit and when releasing a transaction.
	firstPage *page

	// Updates defines the set of updates that compose the transaction.
	Updates []Update

	// The wal that was used to create the transaction
	wal *WAL
	// initComplete is used to signal if initializing the transaction is complete
	initComplete chan struct{}
	// initErr stores possible errors that might have occured during
	// initialization
	initErr error
}

// checksum calculates the checksum of a transaction excluding the checksum
// field of each page
func (t Transaction) checksum() (c checksum) {
	h, _ := blake2b.New256(nil)
	buf := make([]byte, pageSize)
	for page := t.firstPage; page != nil; page = page.nextPage {
		b := page.appendTo(buf[:0])
		h.Write(b[checksumSize:]) // exclude checksum
	}
	copy(c[:], h.Sum(buf[:0]))
	return
}

// commit commits a transaction by setting the correct status and checksum
func (t *Transaction) commit(done chan error) {
	// Signal completion of the commit
	defer close(done)

	// Make sure that the initialization of the transaction finished
	<-t.initComplete
	if t.initErr != nil {
		done <- t.initErr
		return
	}

	// set the status of the first page first
	t.firstPage.pageStatus = pageStatusComitted

	// Set the transaction number of the first page and increase the transactionCounter of the wal
	t.firstPage.transactionNumber = atomic.AddUint64(&t.wal.atomicNextTxnNum, 1) - 1

	// calculate the checksum
	t.firstPage.transactionChecksum = t.checksum()

	// Finalize the commit by writing the first page with the updated status if
	// there have been no errors so far.
	if t.wal.deps.disrupt("CommitFail") {
		// Disk failure causes the commit to fail
		done <- errors.New("Write failed on purpose")
		return
	}
	if _, err := t.wal.logFile.WriteAt(t.firstPage.appendTo(nil), int64(t.firstPage.offset)); err != nil {
		done <- errors.Extend(err, errors.New("Writing the first page failed"))
		return
	}

	if err := t.wal.fSync(); err != nil {
		done <- errors.Extend(err, errors.New("Writing the first page failed"))
		return
	}

	t.commitComplete = true
}

// marshalUpdates marshals the updates of a transaction
func marshalUpdates(updates []Update) []byte {
	// preallocate buffer of appropriate size
	var size int
	for _, u := range updates {
		size += 8 + len(u.Name)
		size += 8 + len(u.Version)
		size += 8 + len(u.Instructions)
	}
	buf := make([]byte, size)

	var n int
	for _, u := range updates {
		// u.Name
		binary.LittleEndian.PutUint64(buf[n:], uint64(len(u.Name)))
		n += 8
		n += copy(buf[n:], u.Name)
		// u.Version
		binary.LittleEndian.PutUint64(buf[n:], uint64(len(u.Version)))
		n += 8
		n += copy(buf[n:], u.Version)
		// u.Instructions
		binary.LittleEndian.PutUint64(buf[n:], uint64(len(u.Instructions)))
		n += 8
		n += copy(buf[n:], u.Instructions)
	}
	return buf
}

// unmarshalUpdates unmarshals the updates of a transaction
func unmarshalUpdates(data []byte) ([]Update, error) {
	// helper function for reading length-prefixed data
	buf := bytes.NewBuffer(data)
	nextPrefix := func(buf *bytes.Buffer) ([]byte, bool) {
		if buf.Len() < 8 {
			// missing length prefix
			return nil, false
		}
		l := int(binary.LittleEndian.Uint64(buf.Next(8)))
		if l < 0 || l > buf.Len() {
			// invalid length prefix
			return nil, false
		}
		return buf.Next(l), true
	}

	var updates []Update
	for {
		if buf.Len() == 0 {
			break
		}

		name, ok := nextPrefix(buf)
		if !ok {
			return nil, errors.New("failed to unmarshal name")
		}

		version, ok := nextPrefix(buf)
		if !ok {
			return nil, errors.New("failed to unmarshal version")
		}

		instructions, ok := nextPrefix(buf)
		if !ok {
			return nil, errors.New("failed to unmarshal instructions")
		}

		updates = append(updates, Update{
			Name:         string(name),
			Version:      string(version),
			Instructions: instructions,
		})
	}

	return updates, nil
}

// threadedInitTransaction reserves pages of the wal, marshalls the
// transactions's updates into a payload and splits the payload equally among
// the pages. Once finished those pages are written to disk and the transaction
// is committed.
func initTransaction(t *Transaction) {
	defer close(t.initComplete)

	// Marshal all the updates to get their total length on disk
	data := marshalUpdates(t.Updates)

	// Get the pages from the wal and set the first page's status
	pages := t.wal.managedReservePages(data)
	pages[0].pageStatus = pageStatusWritten

	// Set the first page of the transaction
	t.firstPage = &pages[0]

	// write the pages to disk
	if err := t.writeToFile(); err != nil {
		t.initErr = errors.Extend(err, errors.New("Couldn't write the page to file"))
		return
	}
}

// validateChecksum checks if a transaction has been corrupted by computing a hash
// and comparing it to the one in the firstPage of the transaction
func (t Transaction) validateChecksum() error {
	if t.firstPage == nil {
		return errors.New("firstPage is nil")
	} else if t.checksum() != t.firstPage.transactionChecksum {
		return errors.New("checksum not valid")
	}
	return nil
}

// SignalUpdatesApplied  informs the WAL that it is safe to free the used pages to reuse them in a new transaction
func (t *Transaction) SignalUpdatesApplied() error {
	if !t.setupComplete || !t.commitComplete || t.releaseComplete {
		return errors.New("misuse of transaction - call each of the signaling methods exactly once, in serial, in order")
	}
	t.releaseComplete = true

	// Set the page status to applied
	t.firstPage.pageStatus = pageStatusApplied

	// Write the page to disk
	var err error
	if t.wal.deps.disrupt("ReleaseFail") {
		// Disk failure causes the commit to fail
		err = errors.New("Write failed on purpose")
	} else {
		_, err = t.wal.logFile.WriteAt(t.firstPage.appendTo(nil), int64(t.firstPage.offset))
	}
	if err != nil {
		return errors.Extend(err, errors.New("Couldn't write the page to file"))
	}
	if err := t.wal.fSync(); err != nil {
		return errors.Extend(err, errors.New("Couldn't write the page to file"))
	}

	// Update the wal's available pages
	t.wal.mu.Lock()
	for page := t.firstPage; page != nil; page = page.nextPage {
		// Append the index of the freed page
		t.wal.availablePages = append(t.wal.availablePages, page.offset)
	}
	t.wal.mu.Unlock()

	// Decrease the number of active transactions
	if atomic.AddInt64(&t.wal.atomicUnfinishedTxns, -1) < 0 {
		panic("Sanity check failed. atomicUnfinishedTxns should never be negative")
	}

	return nil
}

// append is a helper function to append updates to a transaction on which
// SignalSetupComplete hasn't been called yet
func (t *Transaction) append(updates []Update, done chan error) {
	defer close(done)

	// If there is nothing to append we are done
	if len(updates) == 0 {
		return
	}

	// Make sure that the initialization finished
	<-t.initComplete
	if t.initErr != nil {
		done <- t.initErr
		return
	}

	// Marshal the data
	data := marshalUpdates(updates)

	// Find last page to which we append and count the pages
	lastPage := t.firstPage
	for lastPage.nextPage != nil {
		lastPage = lastPage.nextPage
	}

	// Write as much data to the last page as possible
	lenDiff := MaxPayloadSize - len(lastPage.payload)
	if len(data) <= lenDiff {
		lastPage.payload = append(lastPage.payload, data...)
		data = nil
	} else {
		lastPage.payload = append(lastPage.payload, data[:lenDiff]...)
		data = data[lenDiff:]
	}

	// If there is no more data to write we are done
	if data == nil {
		return
	}

	// Get enough pages for the remaining data
	pages := t.wal.managedReservePages(data)
	lastPage.nextPage = &pages[0]

	// Write the new pages to disk and sync them
	buf := make([]byte, pageSize)
	for _, page := range pages {
		b := page.appendTo(buf[:0])
		if _, err := t.wal.logFile.WriteAt(b, int64(page.offset)); err != nil {
			done <- errors.Extend(err, errors.New("Writing the page to disk failed"))
			return
		}
	}
	if err := t.wal.fSync(); err != nil {
		done <- errors.Extend(err, errors.New("Writing the new pages to disk failed"))
		return
	}

	// Link the new pages to the last one and sync the last page
	b := lastPage.appendTo(buf[:0])
	if _, err := t.wal.logFile.WriteAt(b, int64(lastPage.offset)); err != nil {
		done <- errors.Extend(err, errors.New("Writing the last page to disk failed"))
		return
	}
	if err := t.wal.fSync(); err != nil {
		done <- errors.Extend(err, errors.New("Writing the last page to disk failed"))
		return
	}

	// Append the updates to the transaction
	t.Updates = append(t.Updates, updates...)
}

// Append appends additional updates to a transaction
func (t *Transaction) Append(updates []Update) <-chan error {
	done := make(chan error, 1)

	if t.setupComplete || t.commitComplete || t.releaseComplete {
		done <- errors.New("misuse of transaction - can't append to transaction once it is committed/released")
		return done
	}

	go t.append(updates, done)
	return done
}

// SignalSetupComplete will signal to the WAL that any required setup has
// completed, and that the WAL can safely commit to the transaction being
// applied atomically.
func (t *Transaction) SignalSetupComplete() <-chan error {
	done := make(chan error, 1)

	if t.setupComplete || t.commitComplete || t.releaseComplete {
		done <- errors.New("misuse of transaction - call each of the signaling methods exactly ones, in serial, in order")
		return done
	}
	t.setupComplete = true

	// Commit the transaction non-blocking
	go t.commit(done)
	return done
}

// NewTransaction creates a transaction from a set of updates
func (w *WAL) NewTransaction(updates []Update) (*Transaction, error) {
	if !w.recoveryComplete {
		return nil, errors.New("can't call NewTransaction before recovery is complete")
	}
	// Check that there are updates for the transaction to process.
	if len(updates) == 0 {
		return nil, errors.New("cannot create a transaction without updates")
	}

	// Create new transaction
	newTransaction := Transaction{
		Updates:      updates,
		wal:          w,
		initComplete: make(chan struct{}),
	}

	// Initialize the transaction by splitting up the payload among free pages
	// and writing them to disk.
	go initTransaction(&newTransaction)

	// Increase the number of active transaction
	atomic.AddInt64(&w.atomicUnfinishedTxns, 1)

	return &newTransaction, nil
}

// writeToFile writes all the pages of the transaction to disk
func (t *Transaction) writeToFile() error {
	buf := make([]byte, pageSize)
	for page := t.firstPage; page != nil; page = page.nextPage {
		b := page.appendTo(buf[:0])
		if _, err := t.wal.logFile.WriteAt(b, int64(page.offset)); err != nil {
			return errors.Extend(err, errors.New("Writing the page to disk failed"))
		}
	}
	return nil
}
