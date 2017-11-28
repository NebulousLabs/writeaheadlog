package writeaheadlog

import (
	"sync"
)

// threadedSync syncs the WAL in regular intervals
func (w *WAL) threadedSync() {
	for {
		w.syncMu.Lock()
		if w.syncStatus == 1 {
			w.syncStatus = 0
			w.syncMu.Unlock()
			return
		}
		w.syncStatus = 1
		syncErr := w.syncErr
		w.syncErr = new(error)
		blockLock := w.syncRWMu
		w.syncRWMu = new(sync.RWMutex)
		w.syncRWMu.Lock()
		w.syncMu.Unlock()

		*syncErr = w.logFile.Sync()
		blockLock.Unlock()
	}
}

// fSync syncs the WAL's underlying file.
func (w *WAL) fSync() error {
	// Add oursevles to the sync queue.
	w.syncMu.Lock()
	if w.syncStatus == 0 {
		// No syncing thread active, create one.
		go w.threadedSync()
	}
	w.syncStatus = 2 // Indicate that there is a thread in the queue.
	blockLock := w.syncRWMu
	syncErr := w.syncErr
	w.syncMu.Unlock()

	// Block until the sync lock is released.
	blockLock.RLock()
	return *syncErr
}
