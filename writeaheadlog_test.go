package wal

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/persist"
	"github.com/NebulousLabs/fastrand"
)

// walTester holds a WAL along with some other fields
// useful for testing, and has methods implemented on it that can assist
// testing.
type walTester struct {
	wal *WAL

	updates    []Update
	logpath    string
	walStopped chan struct{}
	cancel     chan struct{}
}

// Close is a helper function for a clean tester shutdown
func (wt *walTester) Close() {
	// Signal the wal to stop
	close(wt.cancel)

	// Wait for the wal to shutdown
	select {
	case <-wt.walStopped:
	}
}

// newContractManagerTester returns a ready-to-rock contract manager tester.
func newWALTester(name string, settings map[string]bool) (*walTester, error) {
	if testing.Short() {
		panic("use of newContractManagerTester during short testing")
	}

	// Create temp dir
	testdir := build.TempDir("wal", name)
	err := os.MkdirAll(testdir, 0700)
	if err != nil {
		return nil, err
	}

	// Create logger
	var buf bytes.Buffer
	log := persist.NewLogger(&buf)

	logpath := filepath.Join(testdir, "log.wal")
	walStopped := make(chan struct{})
	cancel := make(chan struct{})
	updates, wal, err := New(logpath, log, cancel, walStopped, settings)
	if err != nil {
		return nil, err
	}
	cmt := &walTester{
		wal:        wal,
		logpath:    logpath,
		updates:    updates,
		walStopped: walStopped,
		cancel:     cancel,
	}
	return cmt, nil
}

// getTransactionPages is
func transactionPages(txn *Transaction) (pages []page) {
	page := txn.firstPage
	for page != nil {
		pages = append(pages, *page)
		page = page.nextPage
	}
	return
}

// TestTransactionInterrupted checks if an interrupt between committing and releasing a
// transaction is handled correctly upon reboot
func TestTransactionInterrupted(t *testing.T) {
	settings := make(map[string]bool)
	settings["cleanWALFile"] = true
	wt, err := newWALTester(t.Name(), settings)
	if err != nil {
		t.Error(err)
	}
	// Create a transaction with 1 update
	updates := []Update{}
	updates = append(updates, Update{
		Name:         "test",
		Version:      "1.0",
		Instructions: fastrand.Bytes(1234),
	})
	// Create one transaction which will be committed and one that will be applied
	txn := wt.wal.NewTransaction(updates)
	txn2 := wt.wal.NewTransaction(updates)

	// wait for the transactions to be committed
	wait := txn.SignalSetupComplete()
	if err := <-wait; err != nil {
		t.Errorf("SignalSetupComplete for the first transaction failed %v", err)
	}
	wait2 := txn2.SignalSetupComplete()
	if err := <-wait2; err != nil {
		t.Errorf("SignalSetupComplete for the second transaction failed")
	}

	// release the changes of the second transaction
	wait2 = txn2.SignalApplyComplete()
	if err := <-wait2; err != nil {
		t.Errorf("SignalApplyComplete for the second transaction failed")
	}

	// shutdown the wal
	wt.Close()

	// make sure the wal is still there
	if _, err := os.Stat(wt.logpath); os.IsNotExist(err) {
		t.Errorf("wal was deleted at %v", wt.logpath)
	}

	// Restart it and check if exactly 1 unfinished transaction is reported
	cancel2 := make(chan struct{})
	updates2, _, err := New(wt.logpath, wt.wal.log, cancel2, make(chan struct{}), make(map[string]bool))
	if err != nil {
		t.Error(err)
	}

	if len(updates2) != len(updates) {
		t.Errorf("Number of updates after restart didn't match. Expected %v, but was %v",
			len(updates), len(updates2))
	}
}

// TestWalParallel checks if the wal still works without errors under a high load parallel work
// The wal won't be deleted but reloaded instead to check if the amount of returned failed updates
// equals 0
func TestWalParallel(t *testing.T) {
	cancel := make(chan struct{})
	settings := make(map[string]bool)
	settings["cleanWALFile"] = true
	walStopped := make(chan struct{})
	wt, err := newWALTester(t.Name(), settings)
	if err != nil {
		t.Error(err)
	}

	// Prepare a random update
	updates := []Update{}
	updates = append(updates, Update{
		Name:         "test",
		Version:      "1.0",
		Instructions: fastrand.Bytes(1234),
	})

	// Define a function that creates a transaction from this update and applies it
	done := make(chan error)
	f := func() {
		// Create txn
		txn := wt.wal.NewTransaction(updates)
		// Wait for the txn to be committed
		if err := <-txn.SignalSetupComplete(); err != nil {
			done <- err
			return
		}
		if err := <-txn.SignalApplyComplete(); err != nil {
			done <- err
			return
		}
		done <- nil
	}

	// Create numThreads instances of the function and wait for it to complete without error
	numThreads := 10000
	for i := 0; i < numThreads; i++ {
		go f()
	}
	for i := 0; i < numThreads; i++ {
		err := <-done
		if err != nil {
			t.Errorf("Thread %v failed: %v", i, err)
		}
	}

	// The number of available pages should equal the number of created pages
	if wt.wal.filePageCount != uint64(len(wt.wal.availablePages)) {
		t.Errorf("number of available pages doesn't match the number of created ones. Expected %v, but was %v",
			wt.wal.availablePages, wt.wal.filePageCount)
	}

	// shutdown the wal
	close(cancel)
	<-walStopped

	// Get the fileinfo
	fi, err := os.Stat(wt.logpath)
	if os.IsNotExist(err) {
		t.Errorf("wal was deleted but shouldn't have")
	}

	// Log some stats about the file
	t.Logf("filesize: %v mb", float64(fi.Size())/float64(1e+6))
	t.Logf("used pages: %v", wt.wal.filePageCount)

	// Restart it and check that no unfinished transactions are reported
	cancel2 := make(chan struct{})
	updates2, _, err := New(wt.logpath, wt.wal.log, cancel2, make(chan struct{}), make(map[string]bool))
	if err != nil {
		t.Error(err)
	}
	if len(updates2) != 0 {
		t.Errorf("Number of updates after restart didn't match. Expected %v, but was %v",
			0, len(updates2))
	}
}

// TestPageRecycling checks if pages are actually freed and used again after a transaction was applied
func TestPageRecycling(t *testing.T) {
	wt, err := newWALTester(t.Name(), make(map[string]bool))
	if err != nil {
		t.Error(err)
	}

	// Prepare a random update
	updates := []Update{}
	updates = append(updates, Update{
		Name:         "test",
		Version:      "1.0",
		Instructions: fastrand.Bytes(5000),
	})

	// Create txn
	txn := wt.wal.NewTransaction(updates)
	// Wait for the txn to be committed
	if err := <-txn.SignalSetupComplete(); err != nil {
		t.Errorf("SignalSetupComplete failed: %v", err)
	}

	// There should be no available pages before the transaction was applied
	if len(wt.wal.availablePages) != 0 {
		t.Errorf("Number of available pages should be 0 but was %v", len(wt.wal.availablePages))
	}

	if err := <-txn.SignalApplyComplete(); err != nil {
		t.Errorf("SignalApplyComplete failed: %v", err)
	}

	usedPages := wt.wal.filePageCount
	availablePages := len(wt.wal.availablePages)
	// The number of used pages should be greater than 0
	if usedPages == 0 {
		t.Errorf("The number of used pages should be greater than 0")
	}
	// Make sure usedPages equals availablePages and remember the values
	if usedPages != uint64(availablePages) {
		t.Errorf("number of used pages doesn't match number of available pages")
	}

	// Create second txn
	txn2 := wt.wal.NewTransaction(updates)
	// Wait for the txn to be committed
	if err := <-txn2.SignalSetupComplete(); err != nil {
		t.Errorf("SignalSetupComplete failed: %v", err)
	}
	// There should be no available pages before the transaction was applied
	if len(wt.wal.availablePages) != 0 {
		t.Errorf("Number of available pages should be 0 but was %v", len(wt.wal.availablePages))
	}
	if err := <-txn2.SignalApplyComplete(); err != nil {
		t.Errorf("SignalApplyComplete failed: %v", err)
	}

	// The number of used pages shouldn't have increased and still be equal to the number of available ones
	if wt.wal.filePageCount != usedPages || len(wt.wal.availablePages) != availablePages {
		t.Errorf("expected used pages %v, was %v", usedPages, wt.wal.filePageCount)
		t.Errorf("expected available pages %v, was %v", availablePages, len(wt.wal.availablePages))
	}
}

// TestRestoreTransactions checks that restoring transactions from a WAL works correctly
func TestRestoreTransactions(t *testing.T) {
	wt, err := newWALTester(t.Name(), make(map[string]bool))
	if err != nil {
		t.Error(err)
	}

	// Create 10 transactions with 1 update each
	txns := []Transaction{}
	totalPages := []page{}
	totalUpdates := []Update{}
	for i := 0; i < 10; i++ {
		updates := []Update{}
		updates = append(updates, Update{
			Name:         "test",
			Version:      "1.0",
			Instructions: fastrand.Bytes(5000), // ensures that 2 pages will be created
		})
		totalUpdates = append(totalUpdates, updates...)
		// Create a new transaction
		txn := wt.wal.NewTransaction(updates)
		wait := txn.SignalSetupComplete()
		if err := <-wait; err != nil {
			t.Errorf("SignalSetupComplete failed %v", err)
		}

		// Check that 2 pages were created
		pages := transactionPages(txn)
		if len(pages) != 2 {
			t.Errorf("Txn has wrong size. Expected %v but was %v", 2, len(pages))
		}
		totalPages = append(totalPages, pages...)
		txns = append(txns, *txn)
	}

	// create a dictionary that takes a page offset and maps it to the page that points to that offset
	previousPages := make(map[uint64]page)
	for _, page := range totalPages {
		if page.nextPage != nil {
			previousPages[page.nextPage.offset] = page
		}
	}

	// restore the transactions
	recoveredTxns, err := wt.wal.restoreTransactions(totalPages, previousPages)

	// check if the recovered transactions have the same length as before
	if len(recoveredTxns) != len(txns) {
		t.Errorf("Recovered txns don't have same length as before. Expected %v but was %v", len(txns),
			len(recoveredTxns))
	}

	// check that all txns point to valid pages
	for i, txn := range recoveredTxns {
		if txn.firstPage == nil {
			t.Errorf("%v: The firstPage of the txn is nil", i)
		}
		if txn.finalPage == nil {
			t.Errorf("%v: The finalPage of the txn is nil", i)
		}
		if txn.firstPage.pageStatus != txns[i].firstPage.pageStatus {
			t.Errorf("%v: The pageStatus of the txn is %v but should be",
				txn.firstPage.pageStatus, txns[i].firstPage.pageStatus)
		}
		if txn.finalPage.transactionNumber != txns[i].finalPage.transactionNumber {
			t.Errorf("%v: The transactionNumber of the txn is %v but should be",
				txn.finalPage.transactionNumber, txns[i].finalPage.transactionNumber)
		}
		if txn.finalPage.transactionChecksum != txns[i].finalPage.transactionChecksum {
			t.Errorf("%v: The transactionChecksum of the txn is %v but should be",
				txn.finalPage.transactionChecksum, txns[i].finalPage.transactionChecksum)
		}
	}

	// Decode the updates
	recoveredUpdates := []Update{}
	for _, txn := range recoveredTxns {
		// loop over all the pages of the transaction, retrieve the payloads and decode them
		page := txn.firstPage
		var updateBytes []byte
		for page != nil {
			updateBytes = append(updateBytes, page.payload...)
			page = page.nextPage
		}
		// Unmarshal the updates of the current transaction
		var currentUpdates []Update
		err := json.Unmarshal(updateBytes, &currentUpdates)
		if err != nil {
			t.Errorf("Unmarshal of updates failed %v", err)
		}
		recoveredUpdates = append(recoveredUpdates, currentUpdates...)
	}

	// Check if the number of recovered updates matches the total number of original updates
	if len(totalUpdates) != len(recoveredUpdates) {
		t.Errorf("The number of recovered updates doesn't match the number of original updates."+
			" expected %v but was %v", len(totalUpdates), len(recoveredUpdates))
	}

	// Check if the recovered updates match the original updates
	originalData, err1 := json.Marshal(totalUpdates)
	recoveredData, err2 := json.Marshal(recoveredUpdates)
	if err1 != nil || err2 != nil {
		t.Errorf("Failed to marshall data for comparison")
	}
	if bytes.Compare(originalData, recoveredData) != 0 {
		t.Errorf("The recovered data doesn't match the original data")
	}

	// shutdown the wal
	close(wt.cancel)
	time.Sleep(time.Second)

	// make sure the wal is gone
	if _, err := os.Stat(wt.logpath); !os.IsNotExist(err) {
		t.Error("wal was not deleted after clean shutdown")
	}
}
