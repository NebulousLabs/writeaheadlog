package wal

import (
	"time"

	"github.com/NebulousLabs/Sia/build"
)

// threadedWalSync syncs the wal in regular intervalls
func threadedWalSync(w *WAL) {
	for {
		select {
		case <-w.stopChan:
			// Shutdown
			return
		case <-time.After(time.Millisecond):
			// Sync every few milliseconds
		}

		// Holding the lock of the condition is not required before calling
		// Signal or Broadcast, but we also want to check the syncCount to
		// avoid unnecessary syncs.
		w.syncMu.Lock()
		if w.syncCount == 0 {
			// nothing to sync
			w.syncMu.Unlock()
			continue
		}

		// Reset syncCount
		w.syncCount = 0

		// If the sync fails we should abort to avoid more corruption
		if err := w.logFile.Sync(); err != nil {
			panic(build.ExtendErr("Failed to sync wal. Aborting to avoid corruption", err))
		}

		// Signal waiting threads that they can continue execution
		w.syncCond.Broadcast()
		w.syncMu.Unlock()
	}
}

// fSync syncs the wal in regular intervalls
func (w *WAL) fSync() {
	// We need to hold the lock of the condition before using it
	w.syncMu.Lock()
	defer w.syncMu.Unlock()

	// Signal the syncing thread that we require syncing the wal
	w.syncCount++

	// Wait for the syncing thread to call fsync
	w.syncCond.Wait()
}
