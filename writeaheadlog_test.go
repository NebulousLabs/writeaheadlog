package writeaheadlog

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NebulousLabs/fastrand"
)

// retry will call 'fn' 'tries' times, waiting 'durationBetweenAttempts'
// between each attempt, returning 'nil' the first time that 'fn' returns nil.
// If 'nil' is never returned, then the final error returned by 'fn' is
// returned.
func retry(tries int, durationBetweenAttempts time.Duration, fn func() error) (err error) {
	for i := 1; i < tries; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		time.Sleep(durationBetweenAttempts)
	}
	return fn()
}

// tempDir joins the provided directories and prefixes them with the testing
// directory.
func tempDir(dirs ...string) string {
	path := filepath.Join(os.TempDir(), "wal", filepath.Join(dirs...))
	os.RemoveAll(path) // remove old test data
	return path
}

// walTester holds a WAL along with some other fields
// useful for testing, and has methods implemented on it that can assist
// testing.
type walTester struct {
	wal *WAL

	updates []Update
	path    string
}

// Close is a helper function for a clean tester shutdown
func (wt *walTester) Close() error {
	// Close wal
	return wt.wal.Close()
}

// newWalTester returns a ready-to-rock walTester.
func newWALTester(name string, deps dependencies) (*walTester, error) {
	// Create temp dir
	testdir := tempDir(name)
	err := os.MkdirAll(testdir, 0700)
	if err != nil {
		return nil, err
	}

	path := filepath.Join(testdir, "test.wal")
	updates, wal, err := newWal(path, deps)
	if err != nil {
		return nil, err
	}

	if err := wal.RecoveryComplete(); err != nil {
		wal.Close()
		return nil, err
	}

	cmt := &walTester{
		wal:     wal,
		updates: updates,
		path:    path,
	}
	return cmt, nil
}

// transactionPages returns all of the pages associated with a transaction.
func transactionPages(txn *Transaction) (pages []page) {
	page := txn.firstPage
	for page != nil {
		pages = append(pages, *page)
		page = page.nextPage
	}
	return
}

// TestCommitFailed checks if a corruption of the first page of the
// transaction during the commit is handled correctly
func TestCommitFailed(t *testing.T) {
	wt, err := newWALTester(t.Name(), &dependencyCommitFail{})
	if err != nil {
		t.Fatal(err)
	}

	// Create a transaction with 1 update
	updates := []Update{}
	updates = append(updates, Update{
		Name:         "test",
		Version:      "1.0",
		Instructions: fastrand.Bytes(1234),
	})

	// Create the transaction
	txn, err := wt.wal.NewTransaction(updates)
	if err != nil {
		t.Fatal(err)
	}

	// Committing the txn should fail on purpose
	wait := txn.SignalSetupComplete()
	if err := <-wait; err == nil {
		t.Error("SignalSetupComplete should have failed but didn't")
	}

	// shutdown the wal
	wt.Close()

	// make sure the wal is still there
	if _, err := os.Stat(wt.path); os.IsNotExist(err) {
		t.Errorf("wal was deleted at %v", wt.path)
	}

	// Restart it. No unfinished updates should be reported since they were
	// never committed
	updates2, w, err := New(wt.path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if len(updates2) != 0 {
		t.Errorf("Number of updates after restart didn't match. Expected %v, but was %v",
			0, len(updates2))
	}
}

// TestMisleadingWrite tests the scenario where Write returns nil, but Sync
// later returns an error.
func TestMisleadingWrite(t *testing.T) {
	t.Skip("not implemented yet")
}

func BenchmarkMarshalUpdates(b *testing.B) {
	updates := make([]Update, 100)
	for i := range updates {
		updates[i] = Update{
			Name:         "test",
			Version:      "1.0",
			Instructions: fastrand.Bytes(1234),
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		marshalUpdates(updates)
	}
}

func BenchmarkUnmarshalUpdates(b *testing.B) {
	updates := make([]Update, 100)
	for i := range updates {
		updates[i] = Update{
			Name:         "test",
			Version:      "1.0",
			Instructions: fastrand.Bytes(1234),
		}
	}
	data := marshalUpdates(updates)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := unmarshalUpdates(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TestReleaseFailed checks if a corruption of the first page of the
// transaction during the commit is handled correctly
func TestReleaseFailed(t *testing.T) {
	wt, err := newWALTester(t.Name(), &dependencyReleaseFail{})
	if err != nil {
		t.Fatal(err)
	}

	// Create a transaction with 1 update
	updates := []Update{}
	updates = append(updates, Update{
		Name:         "test",
		Version:      "1.0",
		Instructions: fastrand.Bytes(1234),
	})

	// Create the transaction
	txn, err := wt.wal.NewTransaction(updates)
	if err != nil {
		t.Fatal(err)
	}

	// Committing the txn should fail on purpose
	wait := txn.SignalSetupComplete()
	if err := <-wait; err != nil {
		t.Errorf("SignalSetupComplete failed %v", err)
	}

	// Committing the txn should fail on purpose
	if err := txn.SignalUpdatesApplied(); err == nil {
		t.Error("SignalUpdatesApplies should have failed but didn't")
	}

	// shutdown the wal
	wt.Close()

	// make sure the wal is still there
	if _, err := os.Stat(wt.path); os.IsNotExist(err) {
		t.Errorf("wal was deleted at %v", wt.path)
	}

	// Restart it. There should be 1 unfinished update since it was committed
	// but never released
	updates2, w, err := New(wt.path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if len(updates2) != 1 {
		t.Errorf("Number of updates after restart didn't match. Expected %v, but was %v",
			1, len(updates2))
	}
}

// TestReleaseNotCalled checks if an interrupt between committing and releasing a
// transaction is handled correctly upon reboot
func TestReleaseNotCalled(t *testing.T) {
	wt, err := newWALTester(t.Name(), &dependencyUncleanShutdown{})
	if err != nil {
		t.Fatal(err)
	}
	// Create a transaction with 1 update
	updates := []Update{}
	updates = append(updates, Update{
		Name:         "test",
		Version:      "1.0",
		Instructions: fastrand.Bytes(1234),
	})
	// Create one transaction which will be committed and one that will be applied
	txn, err := wt.wal.NewTransaction(updates)
	if err != nil {
		t.Fatal(err)
	}
	txn2, err := wt.wal.NewTransaction(updates)
	if err != nil {
		t.Fatal(err)
	}

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
	if err := txn2.SignalUpdatesApplied(); err != nil {
		t.Errorf("SignalApplyComplete for the second transaction failed")
	}

	// shutdown the wal
	wt.Close()

	// make sure the wal is still there
	if _, err := os.Stat(wt.path); os.IsNotExist(err) {
		t.Errorf("wal was deleted at %v", wt.path)
	}

	// Restart it and check if exactly 1 unfinished transaction is reported
	updates2, w, err := New(wt.path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if len(updates2) != len(updates) {
		t.Errorf("Number of updates after restart didn't match. Expected %v, but was %v",
			len(updates), len(updates2))
	}
}

// TestPayloadCorrupted creates 2 update and corrupts the first one. Therefore
// no unfinished transactions should be reported because the second one is
// assumed to be corrupted too
func TestPayloadCorrupted(t *testing.T) {
	wt, err := newWALTester(t.Name(), &dependencyUncleanShutdown{})
	if err != nil {
		t.Fatal(err)
	}

	// Create a transaction with 1 update
	updates := []Update{}
	updates = append(updates, Update{
		Name:         "test",
		Version:      "1.0",
		Instructions: fastrand.Bytes(1234),
	})

	// Create 2 txns
	txn, err := wt.wal.NewTransaction(updates)
	if err != nil {
		t.Fatal(err)
	}
	txn2, err := wt.wal.NewTransaction(updates)
	if err != nil {
		t.Fatal(err)
	}

	// Committing the txns but don't release them
	wait := txn.SignalSetupComplete()
	if err := <-wait; err != nil {
		t.Errorf("SignalSetupComplete failed %v", err)
	}

	wait = txn2.SignalSetupComplete()
	if err := <-wait; err != nil {
		t.Errorf("SignalSetupComplete failed %v", err)
	}

	// Corrupt the payload of the first txn
	txn.firstPage.payload = fastrand.Bytes(2000)
	_, err = txn.wal.logFile.WriteAt(txn.firstPage.appendTo(nil), int64(txn.firstPage.offset))
	if err != nil {
		t.Errorf("Corrupting the page failed %v", err)
	}

	// shutdown the wal
	wt.Close()

	// make sure the wal is still there
	if _, err := os.Stat(wt.path); os.IsNotExist(err) {
		t.Errorf("wal was deleted at %v", wt.path)
	}

	// Restart it. 1 unfinished transaction should be reported
	updates2, w, err := New(wt.path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if len(updates2) != 1 {
		t.Errorf("Number of updates after restart didn't match. Expected %v, but was %v",
			1, len(updates2))
	}
}

// TestPayloadCorrupted2 creates 2 update and corrupts the second one. Therefore
// one unfinished transaction should be reported
func TestPayloadCorrupted2(t *testing.T) {
	wt, err := newWALTester(t.Name(), &dependencyUncleanShutdown{})
	if err != nil {
		t.Fatal(err)
	}

	// Create a transaction with 1 update
	updates := []Update{}
	updates = append(updates, Update{
		Name:         "test",
		Version:      "1.0",
		Instructions: fastrand.Bytes(1234),
	})

	// Create 2 txns
	txn, err := wt.wal.NewTransaction(updates)
	if err != nil {
		t.Fatal(err)
	}
	txn2, err := wt.wal.NewTransaction(updates)
	if err != nil {
		t.Fatal(err)
	}

	// Committing the txns but don't release them
	wait := txn.SignalSetupComplete()
	if err := <-wait; err != nil {
		t.Errorf("SignalSetupComplete failed %v", err)
	}

	wait = txn2.SignalSetupComplete()
	if err := <-wait; err != nil {
		t.Errorf("SignalSetupComplete failed %v", err)
	}

	// Corrupt the payload of the second txn
	txn2.firstPage.payload = fastrand.Bytes(2000)
	_, err = txn2.wal.logFile.WriteAt(txn2.firstPage.appendTo(nil), int64(txn2.firstPage.offset))
	if err != nil {
		t.Errorf("Corrupting the page failed %v", err)
	}

	// shutdown the wal
	wt.Close()

	// make sure the wal is still there
	if _, err := os.Stat(wt.path); os.IsNotExist(err) {
		t.Errorf("wal was deleted at %v", wt.path)
	}

	// Restart it. 1 Unfinished transaction should be reported.
	updates2, w, err := New(wt.path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if len(updates2) != 1 {
		t.Errorf("Number of updates after restart didn't match. Expected %v, but was %v",
			0, len(updates2))
	}
}

// TestWalParallel checks if the wal still works without errors under a high load parallel work
// The wal won't be deleted but reloaded instead to check if the amount of returned failed updates
// equals 0
func TestWalParallel(t *testing.T) {
	wt, err := newWALTester(t.Name(), &prodDependencies{})
	if err != nil {
		t.Fatal(err)
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
		txn, err := wt.wal.NewTransaction(updates)
		if err != nil {
			done <- err
			return
		}
		// Wait for the txn to be committed
		if err := <-txn.SignalSetupComplete(); err != nil {
			done <- err
			return
		}
		if err := txn.SignalUpdatesApplied(); err != nil {
			done <- err
			return
		}
		done <- nil
	}

	// Create numThreads instances of the function and wait for it to complete without error
	numThreads := 1000
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
	if wt.wal.filePageCount != len(wt.wal.availablePages) {
		t.Errorf("number of available pages doesn't match the number of created ones. Expected %v, but was %v",
			wt.wal.availablePages, wt.wal.filePageCount)
	}

	// shutdown the wal
	wt.Close()

	// Get the fileinfo
	fi, err := os.Stat(wt.path)
	if os.IsNotExist(err) {
		t.Fatalf("wal was deleted but shouldn't have")
	}

	// Log some stats about the file
	t.Logf("filesize: %v mb", float64(fi.Size())/float64(1e+6))
	t.Logf("used pages: %v", wt.wal.filePageCount)

	// Restart it and check that no unfinished transactions are reported
	updates2, w, err := New(wt.path)
	if err != nil {
		t.Error(err)
	}
	defer w.Close()

	if len(updates2) != 0 {
		t.Errorf("Number of updates after restart didn't match. Expected %v, but was %v",
			0, len(updates2))
	}
}

// TestPageRecycling checks if pages are actually freed and used again after a transaction was applied
func TestPageRecycling(t *testing.T) {
	wt, err := newWALTester(t.Name(), &prodDependencies{})
	if err != nil {
		t.Error(err)
	}
	defer wt.Close()

	// Prepare a random update
	updates := []Update{}
	updates = append(updates, Update{
		Name:         "test",
		Version:      "1.0",
		Instructions: fastrand.Bytes(5000),
	})

	// Create txn
	txn, err := wt.wal.NewTransaction(updates)
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the txn to be committed
	if err := <-txn.SignalSetupComplete(); err != nil {
		t.Errorf("SignalSetupComplete failed: %v", err)
	}

	// There should be no available pages before the transaction was applied
	if len(wt.wal.availablePages) != 0 {
		t.Errorf("Number of available pages should be 0 but was %v", len(wt.wal.availablePages))
	}

	if err := txn.SignalUpdatesApplied(); err != nil {
		t.Errorf("SignalApplyComplete failed: %v", err)
	}

	usedPages := wt.wal.filePageCount
	availablePages := len(wt.wal.availablePages)
	// The number of used pages should be greater than 0
	if usedPages == 0 {
		t.Errorf("The number of used pages should be greater than 0")
	}
	// Make sure usedPages equals availablePages and remember the values
	if usedPages != availablePages {
		t.Errorf("number of used pages doesn't match number of available pages")
	}

	// Create second txn
	txn2, err := wt.wal.NewTransaction(updates)
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the txn to be committed
	if err := <-txn2.SignalSetupComplete(); err != nil {
		t.Errorf("SignalSetupComplete failed: %v", err)
	}
	// There should be no available pages before the transaction was applied
	if len(wt.wal.availablePages) != 0 {
		t.Errorf("Number of available pages should be 0 but was %v", len(wt.wal.availablePages))
	}
	if err := txn2.SignalUpdatesApplied(); err != nil {
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
	wt, err := newWALTester(t.Name(), &dependencyUncleanShutdown{})
	if err != nil {
		t.Error(err)
	}
	defer wt.Close()

	// Create 10 transactions with 1 update each
	txns := []Transaction{}
	totalPages := []page{}
	totalUpdates := []Update{}
	for i := 0; i < 2; i++ {
		updates := []Update{}
		updates = append(updates, Update{
			Name:         "test",
			Version:      "1.0",
			Instructions: fastrand.Bytes(5000), // ensures that 2 pages will be created
		})
		totalUpdates = append(totalUpdates, updates...)

		// Create a new transaction
		txn, err := wt.wal.NewTransaction(updates)
		if err != nil {
			t.Fatal(err)
		}
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

	// restore the transactions
	recoveredTxns := []Transaction{}
	logData, err := ioutil.ReadFile(wt.path)
	if err != nil {
		t.Fatal(err)
	}

	for _, txn := range txns {
		var restoredTxn Transaction
		err := unmarshalTransaction(&restoredTxn, txn.firstPage, txn.firstPage.nextPage.offset, logData)
		if err != nil {
			t.Error(err)
		}
		recoveredTxns = append(recoveredTxns, restoredTxn)
	}

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
		if txn.firstPage.pageStatus != txns[i].firstPage.pageStatus {
			t.Errorf("%v: The pageStatus of the txn is %v but should be",
				txn.firstPage.pageStatus, txns[i].firstPage.pageStatus)
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
		currentUpdates, err := unmarshalUpdates(updateBytes)
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
}

// TestRecoveryFailed checks if the WAL behave correctly if a crash occurs
// during a call to RecoveryComplete
func TestRecoveryFailed(t *testing.T) {
	wt, err := newWALTester(t.Name(), &dependencyUncleanShutdown{})
	if err != nil {
		t.Error(err)
	}

	// Prepare random updates
	numUpdates := 10
	updates := []Update{}
	for i := 0; i < numUpdates; i++ {
		updates = append(updates, Update{
			Name:         "test",
			Version:      "1.0",
			Instructions: fastrand.Bytes(10000),
		})
	}

	// Create txn
	txn, err := wt.wal.NewTransaction(updates)
	if err != nil {
		return
	}

	// Wait for the txn to be committed
	if err = <-txn.SignalSetupComplete(); err != nil {
		return
	}

	// Close and restart the wal.
	if err := wt.Close(); err == nil {
		t.Error("There should have been an error but there wasn't")
	}

	updates2, w, err := newWal(wt.path, &dependencyRecoveryFail{})
	if err != nil {
		t.Fatal(err)
	}

	// New should return numUpdates updates
	if len(updates2) != numUpdates {
		t.Errorf("There should be %v updates but there were %v", numUpdates, len(updates2))
	}

	// Signal that the recovery is complete
	if err := w.RecoveryComplete(); err != nil {
		t.Errorf("Failed to signal completed recovery: %v", err)
	}

	// Restart the wal again
	if err := w.Close(); err != nil {
		t.Errorf("Failed to close wal: %v", err)
	}
	updates3, w, err := New(wt.path)
	if err != nil {
		t.Fatal(err)
	}

	// There should be 0 updates this time
	if len(updates3) != 0 {
		t.Errorf("There should be %v updates but there were %v", 0, len(updates3))
	}
	// The metadata should say "unclean"
	mdData := make([]byte, pageSize)
	if _, err := w.logFile.ReadAt(mdData, 0); err != nil {
		t.Fatal(err)
	}
	recoveryState, err := readWALMetadata(mdData)
	if err != nil {
		t.Fatal(err)
	}
	if recoveryState != recoveryStateUnclean {
		t.Errorf("recoveryState should be %v but was %v",
			recoveryStateUnclean, recoveryState)
	}

	// Close the wal again and check that the file still exists on disk
	if err := w.Close(); err != nil {
		t.Errorf("Failed to close wal: %v", err)
	}
	_, err = os.Stat(wt.path)
	if os.IsNotExist(err) {
		t.Errorf("wal was deleted but shouldn't have")
	}
}

// TestTransactionAppend tests the functionality of the Transaction's append
// call
func TestTransactionAppend(t *testing.T) {
	wt, err := newWALTester(t.Name(), &prodDependencies{})
	if err != nil {
		t.Fatal(err)
	}

	// Create a transaction with 1 update
	updates := []Update{}
	updates = append(updates, Update{
		Name:         "test",
		Version:      "1.0",
		Instructions: fastrand.Bytes(3000),
	})
	// Create one transaction which will be committed and one that will be applied
	txn, err := wt.wal.NewTransaction(updates)
	if err != nil {
		t.Fatal(err)
	}

	// Append another update
	if err := <-txn.Append(updates); err != nil {
		t.Errorf("Append failed: %v", err)
	}

	// wait for the transactions to be committed
	wait := txn.SignalSetupComplete()
	if err := <-wait; err != nil {
		t.Errorf("SignalSetupComplete for the first transaction failed %v", err)
	}

	// shutdown the wal
	wt.Close()

	// Restart it and check if exactly 1 unfinished transaction is reported
	updates2, w, err := New(wt.path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if len(updates2) != len(updates)*2 {
		t.Errorf("Number of updates after restart didn't match. Expected %v, but was %v",
			len(updates), len(updates2))
	}
}

// benchmarkTransactionSpeed is a helper function to create benchmarks that run
// for 1 min to find out how many transactions can be applied to the wal and
// how large the wal grows during that time using a certain number of threads.
// When appendUpdate is set to 'true', a second update will be appended to the
// transaction before it is committed.
func benchmarkTransactionSpeed(b *testing.B, numThreads int, appendUpdate bool) {
	b.Logf("Running benchmark with %v threads", numThreads)

	wt, err := newWALTester(b.Name(), &prodDependencies{})
	if err != nil {
		b.Error(err)
	}
	defer wt.Close()

	// Prepare a random update
	updates := []Update{}
	updates = append(updates, Update{
		Name:         "test",
		Version:      "1.0",
		Instructions: fastrand.Bytes(4000), // 1 page / txn
	})

	// Define a function that creates a transaction from this update and
	// applies it. It returns the duration it took to commit the transaction.
	f := func() (latency time.Duration, err error) {
		// Get start time
		startTime := time.Now()
		// Create txn
		txn, err := wt.wal.NewTransaction(updates)
		if err != nil {
			return
		}
		// Append second update
		if appendUpdate {
			if err = <-txn.Append(updates); err != nil {
				return
			}
		}
		// Wait for the txn to be committed
		if err = <-txn.SignalSetupComplete(); err != nil {
			return
		}
		// Calculate latency after committing
		latency = time.Since(startTime)
		if err = txn.SignalUpdatesApplied(); err != nil {
			return
		}
		return latency, nil
	}

	// Create a channel to stop threads
	stop := make(chan struct{})

	// Create atomic variables to count transactions and errors
	var atomicNumTxns uint64
	var atomicNumErr uint64

	// Create waitgroup to wait for threads before reading the counters
	var wg sync.WaitGroup

	// Save latencies
	latencies := make([]time.Duration, numThreads, numThreads)

	// Start threads
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			for {
				// Check for stop signal
				select {
				case <-stop:
					return
				default:
				}
				// Execute the function
				latency, err := f()
				if err != nil {
					// Abort thread on error
					atomic.AddUint64(&atomicNumErr, 1)
					return
				}
				atomic.AddUint64(&atomicNumTxns, 1)

				// Remember highest latency
				if latency > latencies[j] {
					latencies[j] = latency
				}
			}
		}(i)
	}

	// Kill threads after 1 minute
	select {
	case <-time.After(time.Minute):
		close(stop)
	}

	// Wait for each thread to finish
	wg.Wait()

	// Check if any errors happened
	if atomicNumErr > 0 {
		b.Fatalf("%v errors happened during execution", atomicNumErr)
	}

	// Get the fileinfo
	fi, err := os.Stat(wt.path)
	if os.IsNotExist(err) {
		b.Errorf("wal was deleted but shouldn't have")
	}

	// Find the maximum latency it took to commit a transaction
	var maxLatency time.Duration
	for i := 0; i < numThreads; i++ {
		if latencies[i] > maxLatency {
			maxLatency = latencies[i]
		}
	}

	// Log results
	b.Logf("filesize: %v mb", float64(fi.Size())/float64(1e+6))
	b.Logf("used pages: %v", wt.wal.filePageCount)
	b.Logf("total transactions: %v", atomicNumTxns)
	b.Logf("txn/s: %v", float64(atomicNumTxns)/60.0)
	b.Logf("maxLatency: %v", maxLatency)

}

// BenchmarkTransactionSpeedAppend runs benchmarkTransactionSpeed with append =
// false
func BenchmarkTransactionSpeed(b *testing.B) {
	numThreads := []int{1, 10, 100, 1000}
	for _, n := range numThreads {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchmarkTransactionSpeed(b, n, false)
		})
	}
}

// BenchmarkTransactionSpeedAppend runs benchmarkTransactionSpeed with append =
// true
func BenchmarkTransactionSpeedAppend(b *testing.B) {
	numThreads := []int{1, 10, 100, 1000}
	for _, n := range numThreads {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchmarkTransactionSpeed(b, n, true)
		})
	}
}

// benchmarkDiskWrites writes numThreads pages of pageSize size and spins up 1
// goroutine for each page that overwrites it numWrites times
func benchmarkDiskWrites(b *testing.B, numWrites int, pageSize int, numThreads int) {
	b.Logf("Starting benchmark with %v writes and %v threads for pages of size %v",
		numWrites, numThreads, pageSize)

	// Get a tmp dir path
	tmpdir := tempDir(b.Name())

	// Create dir
	err := os.MkdirAll(tmpdir, 0700)
	if err != nil {
		b.Fatal(err)
	}

	// Create a tmp file
	f, err := os.Create(filepath.Join(tmpdir, "wal.dat"))
	if err != nil {
		b.Fatal(err)
	}

	// Close it after test
	defer f.Close()

	// Write numThreads pages to file
	_, err = f.Write(fastrand.Bytes(pageSize * numThreads))
	if err != nil {
		b.Fatal(err)
	}

	// Sync it
	if err = f.Sync(); err != nil {
		b.Fatal(err)
	}

	// Define random page data
	data := fastrand.Bytes(pageSize)

	// Declare a waitGroup for later
	var wg sync.WaitGroup

	// Count errors during execution
	var atomicCounter uint64

	// Declare a function that writes a page at the offset i * pageSize 4 times
	write := func(i int) {
		defer wg.Done()
		for j := 0; j < numWrites; j++ {
			if _, err = f.WriteAt(data, int64(i*pageSize)); err != nil {
				atomic.AddUint64(&atomicCounter, 1)
				return
			}
			if err = f.Sync(); err != nil {
				atomic.AddUint64(&atomicCounter, 1)
				return
			}
		}
	}

	// Reset the timer
	b.ResetTimer()

	// Create one thread for each page and make it overwrite the page and call sync
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go write(i)
	}

	// Wait for the threads and check if they were successfull
	wg.Wait()
	if atomicCounter > 0 {
		b.Fatalf("%v errors happened during execution", atomicCounter)
	}
	// Get fileinfo
	info, err := f.Stat()
	if err != nil {
		b.Fatal(err)
	}

	// Print some info
	b.Logf("Number of threads: %v", numThreads)
	b.Logf("PageSize: %v bytes", pageSize)
	b.Logf("Filesize after benchmark %v", info.Size())
}

// BenchmarkDiskWrites1 starts benchmarkDiskWrites with 9990 threads, 4kib
// pages and overwrites those pages once
//
// Results (Model, seconds, date)
//
// ST1000DM003 , 3.5, 09/17/2017
// MZVLW512HMJP, 4.4, 09/17/2017
//
func BenchmarkDiskWrites1(b *testing.B) {
	benchmarkDiskWrites(b, 1, 4096, 9990)
}

// BenchmarkDiskWrites4 starts benchmarkDiskWrites with 9990 threads, 4kib
// pages and overwrites those pages 4 times
//
// Results (Model, seconds, date)
//
// ST1000DM003 , 56.4, 09/17/2017
// MZVLW512HMJP, ??? , 09/17/2017	(results vary between 6 and 30 seconds)
//
func BenchmarkDiskWrites4(b *testing.B) {
	benchmarkDiskWrites(b, 4, 4096, 9990)
}
