package wal

import (
	"bytes"
	"encoding/binary"
	"io"
	"runtime"
	"sync/atomic"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/errors"
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

	// firstPage and finalPage can be the same page. When committing the
	// transaction, the first page is updated to indicate that the commitment
	// is complete, and the final page is updated to have an update number (so
	// that when loaded later, the pages can be returned in the correct
	// order) and the checksum on the final page is added which covers all of
	// the data of the whole transaction.
	//
	// The middle pages do not change after being written.
	firstPage *page
	finalPage *page

	// Updates defines the set of updates that compose the transaction.
	Updates []Update

	// The wal that was used to create the transaction
	wal *WAL

	// An internal channel to signal that the transaction was initialized and
	// can be committed
	initComplete chan error
}

// checksum calculates the checksum of a transaction excluding the checksum
// field of each page
func (t Transaction) checksum() ([crypto.HashSize]byte, error) {
	page := t.firstPage

	// Marshall all the pages and calculate the checksum
	var data []byte
	for page != nil {
		bytes, err := page.Marshal()
		if err != nil {
			return [crypto.HashSize]byte{}, build.ExtendErr("Failed to marshall page", err)
		}
		// Ignore the transactionChecksum field (first 32 bytes)
		data = append(data, bytes[crypto.HashSize:]...)
		page = page.nextPage
	}
	return crypto.HashBytes(data), nil
}

//commit commits a transaction by setting the correct status and checksum
func (t *Transaction) commit(done chan error) {
	// Signal completion of the commit
	defer close(done)

	// Make sure that the initialization of the transaction finished
	select {
	case err := <-t.initComplete:
		if err != nil {
			done <- err
			return
		}
	}

	// set the status of the first page first
	t.firstPage.pageStatus = pageStatusComitted

	// Set the transaction number of the first page and increase the transactionCounter of the wal
	t.firstPage.transactionNumber = atomic.AddUint64(&t.wal.atomicNextTransaction, 1) - 1

	// calculate the checksum and write it to the first page
	checksum, err := t.checksum()
	if err != nil {
		// Don't return here to avoid a deadlock. Do it after the counter was increased
		done <- build.ExtendErr("Unable to create checksum of transaction", err)
	}
	t.firstPage.transactionChecksum = checksum

	// Wait until it is safe to commit the transaction
	for atomic.LoadUint64(&t.wal.atomicTransactionCounter) != t.firstPage.transactionNumber {
		runtime.Gosched()
	}

	// Finalize the commit by writing the first page with the updated status if
	// there have been no errors so far.
	if err == nil {
		err = t.firstPage.Write(t.wal.logFile)
	}

	// Increase the transaction counter
	atomic.AddUint64(&t.wal.atomicTransactionCounter, 1)

	if err != nil {
		done <- build.ExtendErr("Writing the first page failed", err)
		return
	}

	t.commitComplete = true
	t.wal.fSync()
}

// marshalUpdates marshals the updates of a transaction
func marshalUpdates(updates []Update) ([]byte, error) {
	buffer := new(bytes.Buffer)

	for _, update := range updates {
		// Marshal name
		name := []byte(update.Name)
		err1 := binary.Write(buffer, binary.LittleEndian, uint64(len(name)))
		_, err2 := buffer.Write(name)

		// Marshal version
		version := []byte(update.Version)
		err3 := binary.Write(buffer, binary.LittleEndian, uint64(len(version)))
		_, err4 := buffer.Write(version)

		// Append instructions
		err5 := binary.Write(buffer, binary.LittleEndian, uint64(len(update.Instructions)))
		_, err6 := buffer.Write(update.Instructions)

		if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil || err6 != nil {
			return nil, errors.Compose(errors.New("Failed to marshal updates"), err1, err2, err3, err4, err5, err6)
		}
	}

	return buffer.Bytes(), nil
}

// unmarshalUpdates unmarshals the updates of a transaction
func unmarshalUpdates(data []byte) ([]Update, error) {
	buffer := bytes.NewBuffer(data)
	updates := make([]Update, 0)

	for {
		update := Update{}

		// Unmarshal name
		var nameLength uint64
		err1 := binary.Read(buffer, binary.LittleEndian, &nameLength)
		if err1 == io.EOF {
			// End of buffer reached
			break
		}

		name := make([]byte, nameLength)
		_, err2 := buffer.Read(name)

		// Unmarshal version
		var versionLength uint64
		err3 := binary.Read(buffer, binary.LittleEndian, &versionLength)

		version := make([]byte, versionLength)
		_, err4 := buffer.Read(version)

		// Unmarshal instructions
		var instructionsLength uint64
		err5 := binary.Read(buffer, binary.LittleEndian, &instructionsLength)

		instructions := make([]byte, instructionsLength)
		_, err6 := buffer.Read(instructions)

		// Check if any errors occured
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil || err6 != nil {
			return nil, errors.Compose(errors.New("Failed to unmarshal updates"), err1, err2, err3, err4, err5, err6)
		}

		update.Name = string(name)
		update.Version = string(version)
		update.Instructions = instructions

		updates = append(updates, update)
	}

	return updates, nil
}

// threadedInitTransaction reserves pages of the wal, marshalls the
// transactions's updates into a payload and splits the payload equally among
// the pages. Once finished those pages are written to disk and the transaction
// is committed.
func threadedInitTransaction(t *Transaction) {
	defer close(t.initComplete)

	// Marshal all the updates to get their total length on disk
	data, err := marshalUpdates(t.Updates)
	if err != nil {
		t.initComplete <- build.ExtendErr("could not marshal update", err)
		return
	}

	// Find out how many pages are needed for the update
	requiredPages := uint64(float64(len(data))/float64(maxPayloadSize+8) + 1)

	// Get the pages from the wal
	reservedPages := t.wal.reservePages(requiredPages)

	// Set the fields of each page
	pages := make([]page, requiredPages)
	for i := uint64(0); i < requiredPages; i++ {
		// Set offset according to the index in reservedPages
		pages[i].offset = reservedPages[i]

		// Set nextPage if the current page isn't the last one
		// otherwise let it be nil
		if i+1 < requiredPages {
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

	// Set the first and final page of the transaction
	t.firstPage = &pages[0]
	t.finalPage = &pages[len(pages)-1]

	// write the pages to disk and set the pageStatus
	page := t.firstPage
	for page != nil {
		err := page.Write(t.wal.logFile)
		if err != nil {
			t.initComplete <- build.ExtendErr("Couldn't write the page to file", err)
			return
		}
		page = page.nextPage
	}
}

// validateChecksum checks if a transaction has been corrupted by computing a hash
// and comparing it to the one in the finalPage of the transaction
func (t Transaction) validateChecksum() error {
	// Check if finalPage is set
	if t.firstPage == nil {
		return errors.New("Couldn't verify checksum. firstPage is nil")
	}
	checksum, err := t.checksum()
	if err != nil {
		return errors.New("Failed to create checksum for validation")
	}
	if checksum != t.firstPage.transactionChecksum {
		return errors.New("checksum not valid")
	}
	return nil
}

// NewTransaction creates a transaction from a set of updates
func (w *WAL) NewTransaction(updates []Update) *Transaction {
	// Create new transaction
	newTransaction := Transaction{
		Updates:      updates,
		wal:          w,
		initComplete: make(chan error),
	}

	// Initialize the transaction by splitting up the payload among free pages
	// and writing them to disk.
	go threadedInitTransaction(&newTransaction)

	return &newTransaction
}

// SignalUpdatesApplied  informs the WAL that it is safe to free the used pages to reuse them in a new transaction
func (t *Transaction) SignalUpdatesApplied() <-chan error {
	if !t.setupComplete || !t.commitComplete || t.releaseComplete {
		panic("misuse of transaction - call each of the signaling methods exactly once, in serial, in order")
	}
	t.releaseComplete = true
	notifyChannel := make(chan error)

	go func() {
		// Set the page status to applied
		t.firstPage.pageStatus = pageStatusApplied

		// Write the page to disk
		err := t.firstPage.Write(t.wal.logFile)
		if err != nil {
			notifyChannel <- build.ExtendErr("Couldn't write the page to file", err)
			return
		}
		t.wal.fSync()
		// Update the wallets available pages
		page := t.firstPage
		t.wal.mu.Lock()
		for page != nil {
			// Append the index of the freed page
			t.wal.availablePages = append(t.wal.availablePages, page.offset)
			page = page.nextPage
		}
		t.wal.mu.Unlock()
		notifyChannel <- nil
	}()
	return notifyChannel
}

// SignalSetupComplete will signal to the WAL that any required setup has
// completed, and that the WAL can safely commit to the transaction being
// applied atomically.
func (t *Transaction) SignalSetupComplete() <-chan error {
	if t.setupComplete || t.commitComplete || t.releaseComplete {
		panic("misuse of transaction - call each of the signaling methods exactly ones, in serial, in order")
	}
	t.setupComplete = true
	done := make(chan error)

	// Commit the transaction non-blocking
	go t.commit(done)

	return done
}
