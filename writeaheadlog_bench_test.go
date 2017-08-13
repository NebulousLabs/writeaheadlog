package wal

import (
	"os"
	"testing"
	"time"

	"github.com/NebulousLabs/fastrand"
)

// BenchmarkTransactionSpeed runs for 1 min to find out how many transactions
// can be applied to the wal and how large the wal grows during that time.
func BenchmarkTransactionSpeed(b *testing.B) {
	wt, err := newWALTester(b.Name(), make(map[string]bool))
	if err != nil {
		b.Error(err)
	}

	// Prepare a random update
	updates := []Update{}
	updates = append(updates, Update{
		Name:         "test",
		Version:      "1.0",
		Instructions: fastrand.Bytes(1234), // 1 page / txn
	})

	// Define a function that creates a transaction from this update and applies it
	f := func() error {
		// Create txn
		txn := wt.wal.NewTransaction(updates)
		// Wait for the txn to be committed
		if err := <-txn.SignalSetupComplete(); err != nil {
			return err
		}
		if err := <-txn.SignalApplyComplete(); err != nil {
			return err
		}
		return nil
	}

	// Create numThreads instances of the function which repeatedly call f() until 1 minute passed
	numThreads := 1000
	stop := make(chan struct{})
	numTxns := make(chan int)
	for i := 0; i < numThreads; i++ {
		go func() {
			txns := 0
			for {
				select {
				case <-stop:
					numTxns <- txns
					return
				default:
				}
				if err := f(); err != nil {
					b.Error(err)
					close(numTxns)
					return
				}
				txns++
			}
		}()
	}

	// Kill threads after 1 minute
	select {
	case <-time.After(time.Minute):
		close(stop)
	}

	// Wait for each thread to finish and sum up the number of finished txns
	totalTxns := 0
	for i := 0; i < numThreads; i++ {
		totalTxns += <-numTxns
	}

	// Get the fileinfo
	fi, err := os.Stat(wt.logpath)
	if os.IsNotExist(err) {
		b.Errorf("wal was deleted but shouldn't have")
	}

	// Log results
	b.Logf("filesize: %v mb", float64(fi.Size())/float64(1e+6))
	b.Logf("used pages: %v", wt.wal.filePageCount)
	b.Logf("total transactions: %v", totalTxns)
	b.Logf("txn/s: %v", float64(totalTxns)/60.0)
}
