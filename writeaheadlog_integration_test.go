package wal

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/errors"
)

// addCountUpdate is a single instruction to increase a count on disk.
type addCountUpdate struct {
	index    uint64
	newCount uint64
}

// marshal will pack an update into a byte slice.
func (acu addCountUpdate) marshal() []byte {
	ret := make([]byte, 16)
	binary.LittleEndian.PutUint64(ret[:8], acu.index)
	binary.LittleEndian.PutUint64(ret[8:], acu.newCount)
	return ret
}

// unmarshalUpdates will unpack a set of updates.
func unmarshalUpdate(updateBytes []byte) addCountUpdate {
	return addCountUpdate{
		index:    binary.LittleEndian.Uint64(updateBytes[:8]),
		newCount: binary.LittleEndian.Uint64(updateBytes[8:]),
	}
}

// countingArray is a sample object that we wish to persist to disk using the
// wal.
type countdownArray struct {
	// Should always be {2, 1, 0}, or {3, 2, 1, 0}... etc.
	countdown []uint64
	file      *os.File
	wal       *WAL
}

// addCount will increment every counter in the array, and then append a '1',
// but using the wal to maintain consistency on disk.
func (ca *countdownArray) addCount() error {
	// Increment the count in memory, creating a list of updates as we go.
	var updates []Update
	for i := 0; i < len(ca.countdown); i++ {
		ca.countdown[i]++
		updates = append(updates, Update{
			Name:    "addCountUpdate - with an intentionally extra long name to force updates in the WAL to be a lot larger than they would be otherwise.",
			Version: "1.0.0",
			Instructions: addCountUpdate{
				index:    uint64(i),
				newCount: ca.countdown[i],
			}.marshal(),
		})
	}
	ca.countdown = append(ca.countdown, 0)

	// Corner case - if there are no updates, because this is the first time we
	// add to the count, just write out the file piece.
	if len(updates) == 0 {
		writeBytes := make([]byte, 8)
		_, err := ca.file.WriteAt(writeBytes, 8*int64(len(ca.countdown)-1))
		if err != nil {
			return err
		}
		err = ca.file.Sync()
		if err != nil {
			return err
		}
		return nil
	}

	// Create the WAL transaction.
	tx, err := ca.wal.NewTransaction(updates)
	if err != nil {
		return err
	}
	// Perform the setup write on the file.
	writeBytes := make([]byte, 8)
	_, err = ca.file.WriteAt(writeBytes, 8*int64(len(ca.countdown)-1))
	if err != nil {
		return err
	}
	err = ca.file.Sync()
	if err != nil {
		return err
	}
	// Signal completed setup and then wait for the commitment to finish.
	errChan := tx.SignalSetupComplete()
	err = <-errChan
	if err != nil {
		return err
	}
	// Apply the updates.
	err = ca.applyUpdates(updates)
	if err != nil {
		return err
	}
	err = tx.SignalUpdatesApplied()
	if err != nil {
		return err
	}
	return nil
}

// applyUpdates will apply a bunch of wal updates to the ca persist file.
func (ca *countdownArray) applyUpdates(updates []Update) error {
	for _, update := range updates {
		acu := unmarshalUpdate(update.Instructions)
		writeBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(writeBytes, acu.newCount)
		_, err := ca.file.WriteAt(writeBytes, 8*int64(acu.index))
		if err != nil {
			return err
		}
	}
	return nil
}

// Close will cleanly shut down the countdownArray.
func (ca *countdownArray) Close() error {
	return errors.Compose(ca.wal.Close(), ca.file.Close())
}

// newCountdown will initialze a countdown using the wal.
func newCountdown(dir string) (*countdownArray, error) {
	// Open the counting file.
	countingFilename := filepath.Join(dir, "counting.dat")
	file, err := os.OpenFile(countingFilename, os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		return nil, err
	}

	// Open the WAL.
	walFilename := filepath.Join(dir, "wal.dat")
	updates, wal, err := New(walFilename)
	if err != nil {
		return nil, err
	}

	// Create the countdownArray and apply any updates from the wal.
	ca := &countdownArray{
		file: file,
		wal:  wal,
	}
	err = ca.applyUpdates(updates)
	if err != nil {
		return nil, err
	}

	// Load the file into the array.
	fileBytes, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	for i := 0; i+7 < len(fileBytes); i += 8 {
		count := binary.LittleEndian.Uint64(fileBytes[i : i+8])
		ca.countdown = append(ca.countdown, count)
	}
	if len(ca.countdown) == 0 {
		// Count must at least start at zero.
		err = ca.addCount()
		if err != nil {
			return nil, err
		}
		return ca, nil
	}
	start := ca.countdown[0]
	if uint64(len(ca.countdown)) < start+1 {
		return nil, errors.New("count is incorrect length")
	}
	for i, num := range ca.countdown {
		if num != start-uint64(i) {
			return nil, errors.New("count is incorrect representation")
		}
	}
	return ca, nil
}

// TestWALIntegration creates a plausable use case for the WAL and then
// attempts to utilize all functions of the WAL.
func TestWALIntegration(t *testing.T) {
	// Create a folder to house everything we are working with.
	dir := build.TempDir("wal", t.Name())
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		t.Fatal(err)
	}

	// Create the countdown.
	for i := 0; i < 250; i++ {
		cd, err := newCountdown(dir)
		if err != nil {
			t.Fatal(err)
		}
		err = cd.addCount()
		if err != nil {
			t.Fatal(err)
		}
		err = cd.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}
