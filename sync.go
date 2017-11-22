package writeaheadlog

import (
	"time"
)

// threadedSync syncs the WAL in regular intervals
func (w *WAL) threadedSync() {
	for {
		// Holding the lock of the condition is not required before calling
		// Signal or Broadcast, but we also want to check the syncCount to
		// avoid unnecessary syncs.
		w.syncCond.L.Lock()
		if w.syncCount == 0 {
			// nothing to sync
			w.syncing = false
			w.syncCond.L.Unlock()
			return
		}

		// Reset syncCount
		w.syncCount = 0

		// Unlock the syncCond.L for other threads to queue up
		w.syncCond.L.Unlock()

		// Sync the file and set the error
		err := w.logFile.Sync()
		w.syncCond.L.Lock()
		w.syncErr = err
		w.syncCond.L.Unlock()

		// Signal waiting threads that they can continue execution
		w.syncCond.Broadcast()

		// Wait a millisecond to allow for more syncs to queue up
		<-time.After(time.Nanosecond)
	}
}

// fSync syncs the WAL's underlying file.
func (w *WAL) fSync() error {
	// We need to hold the lock of the condition before using it
	w.syncCond.L.Lock()
	defer w.syncCond.L.Unlock()

	// Increment the number of syncing threads
	w.syncCount++

	// If we are the only syncing thread, spawn the threadedSync loop
	if !w.syncing {
		w.syncing = true
		go w.threadedSync()
	}

	// Wait for threadedSync to call Sync
	w.syncCond.Wait()

	// Return the Sync error
	return w.syncErr
}
