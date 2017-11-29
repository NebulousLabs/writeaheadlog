package writeaheadlog

import (
	"sync"
	"sync/atomic"
)

// threadedSync syncs the WAL in regular intervals, returning if there is no
// more work to do.
func (w *WAL) threadedSync() {
	for {
		// Check if there is a syncing job. If there is no syncing job, the
		// thread can return.
		swapped := atomic.CompareAndSwapUint32(&w.atomicSyncStatus, 1, 0)
		if swapped {
			return
		}
		// Indicate that all existing syncing jobs have been completed.
		atomic.StoreUint32(&w.atomicSyncStatus, 1)

		// Fetch the syncErr and syncMu for this upcoming sync call, and then
		// replace them with a new syncErr and syncMu for the next sync call.
		w.syncMu.Lock()
		syncErr := w.syncErr
		w.syncErr = new(error)
		syncMu := w.syncRWMu
		w.syncRWMu = new(sync.RWMutex)
		w.syncRWMu.Lock()
		w.syncMu.Unlock()

		// Sync, and set the syncErr. Then unlock the syncMu, which will inform
		// all waiting threads that the sync has completed.
		*syncErr = w.logFile.Sync()
		syncMu.Unlock()
	}
}

// fSync will synchronize with the existing threadedSync thread to batch this
// fsync. If no threadedSync thread exists, it will create one.
func (w *WAL) fSync() error {
	// Fetch the lock for the next fsync, and the error for the next fsync.
	// Need to fetch these values before indicating that there is a pending
	// fsync, to guarantee that an fsync will run after fetching these vaules.
	w.syncMu.Lock()
	syncMu := w.syncRWMu
	syncErr := w.syncErr
	w.syncMu.Unlock()

	// Set the sync status to '2' to indicate that there is at least one thread
	// waiting for a sync. When we do this, we guarantee that an fsync is going
	// to run following the update. It is possible, due to asynchrony, that the
	// fsync has already completed and so we are running an unecessary fsync,
	// but that is acceptable.
	oldStatus := atomic.SwapUint32(&w.atomicSyncStatus, 2)
	// If the status was previously '0', there is no syncing thread, and a new
	// one must be spawned.
	if oldStatus == 0 {
		go w.threadedSync()
	}

	// The syncMu will hold a writelock until the fsync is completed. When we
	// are able to grab a readlock, we know that the fsync has completed, and
	// that the error has been written. We can safely read and return the
	// error.
	syncMu.RLock()
	return *syncErr
}
