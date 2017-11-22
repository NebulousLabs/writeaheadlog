package writeaheadlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
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
	// run on the the update based on the name and version. The length of the
	// Name is capped to 255 bytes
	Name string

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

	// status indicates the status of the transaction. It is marshalled to
	// disk. See consts.go for an explanation of each status type.
	status uint64

	// sequenceNumber is a unique identifier for the transaction that orders
	// it in relation to other transactions. It is marshalled to disk.
	sequenceNumber uint64

	// firstPage is the first page of the transaction. It is marshalled to
	// disk. Note that because additional transaction metadata (status,
	// sequenceNumber, checksum) is marshalled alongside firstPage, the
	// capacity of firstPage.payload is smaller than subsequent pages.
	//
	// firstPage is never nil for valid transactions.
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

var bufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, pageSize)
	},
}

// verify confirms if an update is valid. Otherwise it will panic
func (u *Update) verify() {
	if len(u.Name) == 0 {
		panic("Name of transaction cannot be empty")
	}
	if len(u.Name) > math.MaxUint8 {
		panic(fmt.Sprintf("Length of update.Name cannot exceed %v characters", math.MaxUint8))
	}
}

// checksum calculates the checksum of a transaction excluding the checksum
// field of each page
func (t Transaction) checksum() (c checksum) {
	h, _ := blake2b.New256(nil)
	buf := bufPool.Get().([]byte)
	defer bufPool.Put(buf)
	// write the transaction metadata
	binary.LittleEndian.PutUint64(buf[:], t.status)
	binary.LittleEndian.PutUint64(buf[8:], t.sequenceNumber)
	h.Write(buf[:16])
	// write pages
	for page := t.firstPage; page != nil; page = page.nextPage {
		for i := range buf {
			buf[i] = 0
		}
		page.appendTo(buf[:0])
		h.Write(buf)
	}
	copy(c[:], h.Sum(buf[:0]))
	return
}

// commit commits a transaction by setting the correct status and checksum
func (t *Transaction) commit() error {
	// Make sure that the initialization of the transaction finished
	<-t.initComplete
	if t.initErr != nil {
		return t.initErr
	}

	// Set the transaction status
	t.status = txnStatusComitted

	// Set the sequence number and increase the WAL's transactionCounter
	t.sequenceNumber = atomic.AddUint64(&t.wal.atomicNextTxnNum, 1) - 1

	// Calculate the checksum
	checksum := t.checksum()

	if t.wal.deps.disrupt("CommitFail") {
		return errors.New("Write failed on purpose")
	}

	// Marshal metadata into buffer
	buf := bufPool.Get().([]byte)
	binary.LittleEndian.PutUint64(buf[:], t.status)
	binary.LittleEndian.PutUint64(buf[8:], t.sequenceNumber)
	copy(buf[16:], checksum[:])

	// Finalize the commit by writing the metadata to disk.
	_, err := t.wal.logFile.WriteAt(buf[:16+checksumSize], int64(t.firstPage.offset))
	bufPool.Put(buf)
	if err != nil {
		return errors.Extend(err, errors.New("Writing the first page failed"))
	}

	if err := t.wal.fSync(); err != nil {
		return errors.Extend(err, errors.New("Writing the first page failed"))
	}

	t.commitComplete = true
	return nil
}

// marshalUpdates marshals the updates of a transaction
func marshalUpdates(updates []Update) []byte {
	// preallocate buffer of appropriate size
	var size int
	for _, u := range updates {
		size += 1 + len(u.Name)
		size += 8 + len(u.Instructions)
	}
	buf := make([]byte, size)

	var n int
	for _, u := range updates {
		// u.Name
		buf[n] = byte(len(u.Name))
		n++
		n += copy(buf[n:], u.Name)
		// u.Instructions
		binary.LittleEndian.PutUint64(buf[n:], uint64(len(u.Instructions)))
		n += 8
		n += copy(buf[n:], u.Instructions)
	}
	return buf
}

// nextPrefix is a helper function that reads the next prefix of prefixLen and
// prefixed data from a buffer and returns the data and a bool to indicate
// success.
func nextPrefix(prefixLen int, buf *bytes.Buffer) ([]byte, bool) {
	if buf.Len() < prefixLen {
		// missing length prefix
		return nil, false
	}
	var l int
	switch prefixLen {
	case 8:
		l = int(binary.LittleEndian.Uint64(buf.Next(prefixLen)))
	case 4:
		l = int(binary.LittleEndian.Uint32(buf.Next(prefixLen)))
	case 2:
		l = int(binary.LittleEndian.Uint16(buf.Next(prefixLen)))
	case 1:
		l = int(buf.Next(prefixLen)[0])
	default:
		return nil, false
	}
	if l < 0 || l > buf.Len() {
		// invalid length prefix
		return nil, false
	}
	return buf.Next(l), true
}

// unmarshalUpdates unmarshals the updates of a transaction
func unmarshalUpdates(data []byte) ([]Update, error) {
	// helper function for reading length-prefixed data
	buf := bytes.NewBuffer(data)

	var updates []Update
	for {
		if buf.Len() == 0 {
			break
		}

		name, ok := nextPrefix(1, buf)
		if !ok {
			return nil, errors.New("failed to unmarshal name")
		} else if len(name) == 0 {
			// end of updates
			break
		}

		instructions, ok := nextPrefix(8, buf)
		if !ok {
			return nil, errors.New("failed to unmarshal instructions")
		}

		updates = append(updates, Update{
			Name:         string(name),
			Instructions: instructions,
		})
	}

	return updates, nil
}

// threadedInitTransaction reserves pages of the wal, marshals the
// transactions's updates into a payload, and splits the payload equally among
// the pages. It then writes the transaction metadata and pages to disk.
func threadedInitTransaction(t *Transaction) {
	defer close(t.initComplete)

	// Marshal all the updates to get their total length on disk
	data := marshalUpdates(t.Updates)

	// Get the pages from the wal and set the status
	//
	// NOTE: managedReservePages only returns pages that contain up to
	// MaxPayloadSize bytes. However, the first page can only contain
	// maxFirstPayloadSize bytes. To rectify this, we reserve pages separately
	// for the first page and the remainder, and join them together.
	if len(data) > maxFirstPayloadSize {
		t.firstPage = t.wal.managedReservePages(data[:maxFirstPayloadSize])
		t.firstPage.nextPage = t.wal.managedReservePages(data[maxFirstPayloadSize:])
	} else {
		t.firstPage = t.wal.managedReservePages(data)
	}
	t.status = txnStatusWritten

	// write the metadata and first page
	buf := bufPool.Get().([]byte)
	defer bufPool.Put(buf)
	for i := range buf {
		buf[i] = 0
	}
	binary.LittleEndian.PutUint64(buf[:], t.status)
	binary.LittleEndian.PutUint64(buf[8:], t.sequenceNumber)
	var c checksum // checksum is left blank until transaction is committed
	n := copy(buf[16:], c[:])
	binary.LittleEndian.PutUint64(buf[n+16:], t.firstPage.nextOffset())
	copy(buf[n+24:], t.firstPage.payload)
	if _, err := t.wal.logFile.WriteAt(buf, int64(t.firstPage.offset)); err != nil {
		t.initErr = errors.Extend(err, errors.New("Writing the first page to disk failed"))
		return
	}
	// write subsequent pages
	for page := t.firstPage.nextPage; page != nil; page = page.nextPage {
		if err := t.writePage(page); err != nil {
			t.initErr = errors.Extend(err, errors.New("Writing the page to disk failed"))
			return
		}
	}
}

// SignalUpdatesApplied informs the WAL that it is safe to reuse t's pages.
func (t *Transaction) SignalUpdatesApplied() error {
	if !t.setupComplete || !t.commitComplete || t.releaseComplete {
		return errors.New("misuse of transaction - call each of the signaling methods exactly once, in serial, in order")
	}
	t.releaseComplete = true

	// Set the status to applied
	t.status = txnStatusApplied

	// Write the status to disk
	if t.wal.deps.disrupt("ReleaseFail") {
		return errors.New("Write failed on purpose")
	}

	buf := bufPool.Get().([]byte)
	binary.LittleEndian.PutUint64(buf, t.status)
	_, err := t.wal.logFile.WriteAt(buf[:8], int64(t.firstPage.offset))
	bufPool.Put(buf)
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
func (t *Transaction) append(updates []Update) (err error) {
	// If there is nothing to append we are done
	if len(updates) == 0 {
		return nil
	}

	// Make sure that the initialization finished
	<-t.initComplete
	if t.initErr != nil {
		return t.initErr
	}

	// Marshal the data
	data := marshalUpdates(updates)

	// Find last page, to which we will append
	lastPage := t.firstPage
	for lastPage.nextPage != nil {
		lastPage = lastPage.nextPage
	}

	// Preserve the original payload of the last page and the original updates
	// of the transaction if an error occurs
	defer func() {
		if err != nil {
			lastPage.payload = lastPage.payload[:len(lastPage.payload)]
			t.Updates = t.Updates[:len(t.Updates)]
			lastPage.nextPage = nil

			// Write last page
			err = t.writePage(lastPage)
			err = errors.Compose(err, errors.Extend(err, errors.New("Writing the last page to disk failed")))
		}
	}()

	// Write as much data to the last page as possible
	var lenDiff int
	if lastPage == t.firstPage {
		// firstPage holds less data than subsequent pages
		lenDiff = maxFirstPayloadSize - len(lastPage.payload)
	} else {
		lenDiff = MaxPayloadSize - len(lastPage.payload)
	}

	if len(data) <= lenDiff {
		lastPage.payload = append(lastPage.payload, data...)
		data = nil
	} else {
		lastPage.payload = append(lastPage.payload, data[:lenDiff]...)
		data = data[lenDiff:]
	}

	// If there is no more data to write, we don't need to allocate any new
	// pages. Write the new last page to disk and append the new updates.
	if len(data) == 0 {
		if err := t.writePage(lastPage); err != nil {
			return errors.Extend(err, errors.New("Writing the last page to disk failed"))
		}
		t.Updates = append(t.Updates, updates...)
		return nil
	}

	// Get enough pages for the remaining data
	lastPage.nextPage = t.wal.managedReservePages(data)

	// Write the new pages, then write the tail page that links to them.
	// Writing in this order ensures that if writing the new pages fails, the
	// old tail page remains valid.
	for page := lastPage.nextPage; page != nil; page = page.nextPage {
		if err := t.writePage(page); err != nil {
			return errors.Extend(err, errors.New("Writing the page to disk failed"))
		}
	}

	// write last page
	if err := t.writePage(lastPage); err != nil {
		return errors.Extend(err, errors.New("Writing the last page to disk failed"))
	}

	// Append the updates to the transaction
	t.Updates = append(t.Updates, updates...)
	return nil
}

// Append appends additional updates to a transaction
func (t *Transaction) Append(updates []Update) <-chan error {
	// Verify the updates
	for _, u := range updates {
		u.verify()
	}
	done := make(chan error, 1)

	if t.setupComplete || t.commitComplete || t.releaseComplete {
		done <- errors.New("misuse of transaction - can't append to transaction once it is committed/released")
		return done
	}

	go func() {
		done <- t.append(updates)
	}()
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
	go func() {
		done <- t.commit()
	}()
	return done
}

// writePage is a helper function that writes a page of a transaction to the
// correct offset.
func (t *Transaction) writePage(page *page) error {
	buf := bufPool.Get().([]byte)
	defer bufPool.Put(buf)

	// Adjust offset if page is the first page of the transaction
	offset := page.offset
	if page == t.firstPage {
		const shift = firstPageMetaSize - pageMetaSize
		offset += shift
		buf = buf[:len(buf)-shift]
	}
	for i := range buf {
		buf[i] = 0
	}
	page.appendTo(buf[:0])
	_, err := t.wal.logFile.WriteAt(buf, int64(offset))
	return err
}

// NewTransaction creates a transaction from a set of updates.
func (w *WAL) NewTransaction(updates []Update) (*Transaction, error) {
	if !w.recoveryComplete {
		return nil, errors.New("can't call NewTransaction before recovery is complete")
	}
	// Check that there are updates for the transaction to process.
	if len(updates) == 0 {
		return nil, errors.New("cannot create a transaction without updates")
	}
	// Verify the updates
	for _, u := range updates {
		u.verify()
	}

	// Create new transaction
	txn := &Transaction{
		Updates:      updates,
		wal:          w,
		initComplete: make(chan struct{}),
	}

	// Initialize the transaction by splitting up the payload among free pages
	// and writing them to disk.
	go threadedInitTransaction(txn)

	// Increase the number of active transaction
	atomic.AddInt64(&w.atomicUnfinishedTxns, 1)

	return txn, nil
}
