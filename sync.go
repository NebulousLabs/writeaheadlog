package writeaheadlog

// When the WAL is created, sync.go requires the syncState to exist, and have a
// write-locked rwMu.

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// syncState contains the RWMutex and the error that correspond to a single
// fsync. The syncState is swapped out atomically each fsync cycle.
type syncState struct {
	err  error
	rwMu sync.RWMutex
}

// threadedSync syncs the WAL in regular intervals, exiting if there is no more
// work to do.
func (w *WAL) threadedSync() {
	defer w.wg.Done()
	for {
		// Check if there is a syncing job. If there is no syncing job, the
		// thread can return.
		swapped := atomic.CompareAndSwapUint32(&w.atomicSyncStatus, 1, 0)
		if swapped {
			// A zero will only be swapped in if the value in place was '1',
			// which indicates that no work has been added since the previous
			// fsync. We can return safely, having indicated that there is no
			// syncing thread running by swapping a '0' into the sync state.
			return
		}
		// Indicate that all existing syncing jobs will be completed.
		atomic.StoreUint32(&w.atomicSyncStatus, 1)

		// Create the syncState for the next iteration and then swap it for the
		// syncState that covers this iteration.
		newSS := new(syncState)
		newSS.rwMu.Lock()
		oldSS := (*syncState)(atomic.SwapPointer(&w.atomicSyncState, unsafe.Pointer(newSS)))

		// Sync, and set the syncErr. Then unlock the rwMu, which will inform
		// all waiting threads that the sync has completed.
		oldSS.err = w.logFile.Sync()
		oldSS.rwMu.Unlock()

		// Add a sleep to allow for more efficient batching of syncs. This
		// drastically improves the multithreaded performance on Windows while
		// slightly decreasing the singlethreaded performance. Unfortunately it
		// slightly decreases the overall performance on Linux and Mac OS.
		time.Sleep(time.Microsecond)
	}
}

// fSync will synchronize with the existing threadedSync thread to batch this
// fsync. If no threadedSync thread exists, it will create one.
func (w *WAL) fSync() error {
	// Fetch the lock for the next fsync, and the error for the next fsync.
	// Need to fetch these values before indicating that there is a pending
	// fsync, to guarantee that an fsync will run after fetching these vaules.
	ss := (*syncState)(atomic.LoadPointer(&w.atomicSyncState))

	// Set the sync status to '2' to indicate that there is at least one thread
	// waiting for a sync. When we do this, we guarantee that an fsync is going
	// to run following the update. It is possible, due to asynchrony, that the
	// fsync has already completed and so we are running an unecessary fsync,
	// but that is acceptable and should not impact performance.
	oldStatus := atomic.SwapUint32(&w.atomicSyncStatus, 2)
	// If the status was previously '0', there is no syncing thread, and a new
	// one must be spawned.
	if oldStatus == 0 {
		w.wg.Add(1)
		go w.threadedSync()
	}

	// The syncMu will hold a writelock until the fsync is completed. When we
	// are able to grab a readlock, we know that the fsync has completed, and
	// that the error has been written. We can safely read and return the
	// error.
	ss.rwMu.RLock()
	return ss.err
}
