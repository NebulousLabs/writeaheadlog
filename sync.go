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
		w.syncMu.Lock()
		if w.syncCount == 0 {
			// nothing to sync
			w.syncing = false
			w.syncMu.Unlock()
			return
		}

		// Reset syncCount
		w.syncCount = 0

		// Unlock the syncMu for other threads to queue up
		w.syncMu.Unlock()

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

// fSync syncs the wal in regular intervalls
func (w *WAL) fSync() {
	// We need to hold the lock of the condition before using it
	w.syncMu.Lock()
	defer w.syncMu.Unlock()

	// If there is currently no instance of the syncing thread create one
	if !w.syncing {
		w.syncing = true
		go w.threadedSync()
	}

	// Signal the syncing thread that we require syncing the wal
	w.syncCount++

	// Wait for the syncing thread to call fsync
	w.syncCond.Wait()
}
