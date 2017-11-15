package writeaheadlog

import (
	"github.com/NebulousLabs/Sia/build"
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
			w.syncCond.L.Unlock()
			return
		}

		// Reset syncCount
		w.syncCount = 0

		// Unlock the syncCond.L for other threads to queue up
		w.syncCond.L.Unlock()

		// If the sync fails we should abort to avoid more corruption
		if err := w.logFile.Sync(); err != nil {
			if !w.deps.disrupt("FaultyDisk") {
				build.Critical(build.ExtendErr("Failed to sync wal. Aborting to avoid corruption", err))
			}
		}

		// Signal waiting threads that they can continue execution
		w.syncCond.Broadcast()
	}
}

// fSync syncs the WAL's underlying file.
func (w *WAL) fSync() {
	// We need to hold the lock of the condition before using it
	w.syncCond.L.Lock()
	defer w.syncCond.L.Unlock()

	// Increment the number of syncing threads
	w.syncCount++

	// If we are the only syncing thread, spawn the threadedSync loop
	if w.syncCount == 1 {
		go w.threadedSync()
	}

	// Signal the syncing thread that we require syncing the wal
	w.syncCount++

	// Wait for the syncing thread to call fsync
	w.syncCond.Wait()
}
