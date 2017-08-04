package writeaheadlog

import (
	"encoding/json"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/errors"
)

// Transaction defines a series of updates that are to be performed atomically.
// In the event of an unclean shutdown, either all updates will have been saved
// together at full integrity, or none of the updates in the transaction will
// have been saved at all.
//
// While m multiple transactions can be safely open at the same time, multiple
// methods should not be called on the transaction at the same time - the WAL is
// thread-safe, but the transactions are not.
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
}

// signalSetupComplete will signal to the WAL that any required setup has
// completed, and that the WAL can safely commit to the transaction being
// applied atomically.
func (t *Transaction) SignalSetupComplete() <-chan error {
	if t.setupComplete || t.commitComplete || t.releaseComplete {
		panic("misuse of transaction - call each of the signaling methods exactly ones, in serial, in order")
	}
	t.setupComplete = true
	notifyChannel := make(chan error)

	// Create the transaction non-blocking
	go func() {
		// TODO: maybe not lock the whole scope
		defer close(notifyChannel)
		t.wal.mu.Lock()
		defer t.wal.mu.Unlock()

		// Marshal all the updates to get their total length on disk
		data, err := json.Marshal(t.Updates)
		if err != nil {
			notifyChannel <- build.ExtendErr("could not marshal update", err)
			return
		}

		// Find out how many pages are needed for the update
		requiredPages := uint64(float64(len(data))/float64(maxPayloadSize+8) + 1)

		// Check if enough pages are available
		if uint64(len(t.wal.availablePages)) < requiredPages {
			// add enough pages for the new transaction
			numNewPages := requiredPages - uint64(len(t.wal.availablePages))
			for i := t.wal.filePageCount; i < t.wal.filePageCount+numNewPages; i++ {
				t.wal.availablePages = append(t.wal.availablePages, i*pageSize)
			}
			t.wal.filePageCount += numNewPages

			// sanity check: the number of available pages should equal the number of required ones
			if uint64(len(t.wal.availablePages)) != requiredPages {
				panic(errors.New("sanity check failed: num of available pages != num of required pages"))
			}
		}

		// Reserve some pages and remove them from the available ones
		reservedPages := t.wal.availablePages[uint64(len(t.wal.availablePages))-requiredPages:]
		t.wal.availablePages = t.wal.availablePages[:uint64(len(t.wal.availablePages))-requiredPages]

		pages := make([]page, requiredPages)
		for i := uint64(0); i < requiredPages; i++ {
			// Set offset according to the index in reservedPages
			pages[i].offset = pageSize * reservedPages[i]

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

		// Set the transaction number of the last page and increase the transactionCounter of the wal
		pages[len(pages)-1].transactionNumber = t.wal.transactionCounter
		t.wal.transactionCounter++

		// Set the first and final page of the transaction
		t.firstPage = &pages[0]
		t.finalPage = &pages[len(pages)-1]

		// write the pages to disk and set the pageStatus
		page := t.firstPage
		for page != nil {
			err := page.Write(t.wal.logFile)
			if err != nil {
				notifyChannel <- build.ExtendErr("Couldn't write the page to file", err)
				return
			}
			page = page.nextPage
		}

		// Add transaction to the queue of uncommitted ones
		t.wal.uncommittedTransactions = append(t.wal.uncommittedTransactions, t)

		// Indicate success
		notifyChannel <- nil
	}()
	return notifyChannel
}

// MarshalJSON marshales a transaction.
func (t Transaction) checksum() ([crypto.HashSize]byte, error) {
	page := t.firstPage
	var data []byte
	for page != nil {
		// make sure that the checksum field is just zeros before calculating the checksum
		copy(page.transactionChecksum[:], make([]byte, crypto.HashSize))

		bytes, err := page.Marshal()
		if err != nil {
			return [crypto.HashSize]byte{}, build.ExtendErr("Failed to marshall page", err)
		}
		data = append(data, bytes...)
		page = page.nextPage
	}
	return crypto.HashBytes(data), nil
}

// validateChecksum checks if a transaction has been corrupted by computing a hash
// and comparing it to the one in the finalPage of the transaction
func (t Transaction) validateChecksum() error {
	// Check if finalPage is set
	if t.finalPage == nil {
		return errors.New("Couldn't verify checksum. finalPage is nil")
	}
	checksum, err := t.checksum()
	if err != nil {
		return errors.New("Failed to create checksum for validation")
	}
	if checksum != t.finalPage.transactionChecksum {
		return errors.New("Checksum not valid.")
	}
	return nil
}

//commit commits a transaction by setting the correct status and checksum
func (t Transaction) commit() error {
	// set the status of the first page first
	t.firstPage.pageStatus = pageStatusComitted

	// calculate the checksum and write it to the last page
	checksum, err := t.checksum()
	if err != nil {
		return build.ExtendErr("Unable to create checksum of transaction", err)
	}
	t.finalPage.transactionChecksum = checksum
	err = t.finalPage.Write(t.wal.logFile)
	if err != nil {
		return build.ExtendErr("Writing the final page failed", err)
	}

	// Finalize the commit by writing the first page with the updated status
	err = t.firstPage.Write(t.wal.logFile)
	if err != nil {
		return build.ExtendErr("Writing the first page failed", err)
	}

	return nil
}

// signalReleaseComplete informs the WAL that it is safe to free the used pages to reuse them in a new transaction
func (t *Transaction) SignalApplyComplete() <-chan error {
	if !t.setupComplete || !t.commitComplete || t.releaseComplete {
		panic("misuse of transaction - call each of the signaling methods exactly ones, in serial, in order")
	}
	t.releaseComplete = true
	notifyChannel := make(chan error)

	go func() {
		t.wal.mu.Lock()
		defer t.wal.mu.Unlock()

		// Set the page status to applied
		t.firstPage.pageStatus = pageStatusApplied

		// Write the page to disk
		err := t.firstPage.Write(t.wal.logFile)
		if err != nil {
			notifyChannel <- build.ExtendErr("Couldn't write the page to file", err)
		}

		// Update the wallets available pages
		page := t.firstPage
		for page != nil {
			// Calculate and append the index of the page
			t.wal.availablePages = append(t.wal.availablePages, page.offset)
			page = page.nextPage
		}
	}()
	return notifyChannel
}
