package wal

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/errors"
	"github.com/NebulousLabs/fastrand"
)

const (
	// addCountName is the name of the addCount update.
	addCountName = "addCountUpdate - with an intentionally extra long name to force updates in the WAL to be a lot larger than they would be otherwise."

	// changeSplotchName is the name of the changeSplotch update.
	changeSplotchName = "changeSplotchName"
)

// addCountUpdate is a single instruction to increase a count on disk.
type addCountUpdate struct {
	index    uint64
	newCount uint64
}

// marshal will pack an add count update into a byte slice.
func (acu addCountUpdate) marshal() []byte {
	ret := make([]byte, 16)
	binary.LittleEndian.PutUint64(ret[:8], acu.index)
	binary.LittleEndian.PutUint64(ret[8:], acu.newCount)
	return ret
}

// unmarshalAddCountUpdate will unpack an add count update.
func unmarshalAddCountUpdate(updateBytes []byte) addCountUpdate {
	return addCountUpdate{
		index:    binary.LittleEndian.Uint64(updateBytes[:8]),
		newCount: binary.LittleEndian.Uint64(updateBytes[8:]),
	}
}

// changeSplotchUpdate is a single instruction to replace the data in a splotch
// of the splotchFile.
type changeSplotchUpdate struct {
	index       uint64
	splotchData []byte
}

// marshal will pack a change splotch update into a byte slice.
func (csu changeSplotchUpdate) marshal() []byte {
	prefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefix, csu.index)
	return append(prefix, csu.splotchData...)
}

// unmarshalChangeSplotchUpdate will unpack a set of updates.
func unmarshalChangeSplotchUpdate(updateBytes []byte) changeSplotchUpdate {
	return changeSplotchUpdate{
		index:       binary.LittleEndian.Uint64(updateBytes[:8]),
		splotchData: updateBytes[8:],
	}
}

// countingArray is a sample object that we wish to persist to disk using the
// wal.
type countdownArray struct {
	// Should always be {2, 1, 0}, or {3, 2, 1, 0}... etc. The file contains an
	// array on disk that should have the exact same property.
	countdown []uint64
	file      *os.File

	// splotchFile is an extension to the countdown file, where large pages of
	// the file are all required to have a continuous countdown. The count can
	// start at any value, and then it will count down for the whole page,
	// underflowing if required.
	//
	// The first 80 bytes must be 10 8-byte values that count down and underflow.
	// The next 160 bytes must be 20 8-byte values that count down and underflow.
	// The next 240 bytes must be 30 8-byte values that count down and underflow.
	// ...
	//
	// Different parts of this file can easily be edited in parallel
	// transactions, while at the same time there are clear consistency
	// requirements.
	//
	// '0' is used as a special starting value to indicate that the whole
	// splotch should be empty.
	splotchFile *os.File

	wal *WAL
}

// addCount will increment every counter in the array, and then append a '1',
// but using the wal to maintain consistency on disk.
func (ca *countdownArray) addCount() error {
	// Increment the count in memory, creating a list of updates as we go.
	var updates []Update
	for i := 0; i < len(ca.countdown); i++ {
		ca.countdown[i]++
		updates = append(updates, Update{
			Name:    addCountName,
			Version: "1.0.0",
			Instructions: addCountUpdate{
				index:    uint64(i),
				newCount: ca.countdown[i],
			}.marshal(),
		})
	}

	// Corner case - if there are no updates, because this is the first time we
	// add to the count, just write out the file piece.
	if len(updates) == 0 {
		writeBytes := make([]byte, 8)
		_, err := ca.file.WriteAt(writeBytes, 8*int64(len(ca.countdown)))
		if err != nil {
			return err
		}
		err = ca.file.Sync()
		if err != nil {
			return err
		}
		ca.countdown = append(ca.countdown, 0)
		return nil
	}

	// Create the WAL transaction.
	tx, err := ca.wal.NewTransaction(updates)
	if err != nil {
		return err
	}
	// Perform the setup write on the file.
	writeBytes := make([]byte, 8)
	_, err = ca.file.WriteAt(writeBytes, 8*int64(len(ca.countdown)))
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
	ca.countdown = append(ca.countdown, 0)
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

// addCountBroken is a copy of addCount, but we never apply the updates, and we
// return an error.
func (ca *countdownArray) addCountBroken() error {
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
		_, err := ca.file.WriteAt(writeBytes, 8*int64(len(ca.countdown)))
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
	_, err = ca.file.WriteAt(writeBytes, 8*int64(len(ca.countdown)))
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
	return nil
}

// addCountApply applies an add count update to the countdown array.
func (ca *countdownArray) addCountApply(update Update) error {
	acu := unmarshalAddCountUpdate(update.Instructions)
	writeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(writeBytes, acu.newCount)
	_, err := ca.file.WriteAt(writeBytes, 8*int64(acu.index))
	return err
}

// changeSplotch will change the targeted splotch file on disk to count down
// form the provided value.
func (ca *countdownArray) changeSplotch(splotchIndex, newVal uint64) error {
	// The splotch update is intentionally encoded as the full write to
	// simulate what a full write would be from a real application.
	newSplotch := make([]byte, 10*8*(splotchIndex+1))
	if newVal != 0 {
		for i := uint64(0); i < (splotchIndex+1)*10; i++ {
			binary.LittleEndian.PutUint64(newSplotch[8*i:(8*i)+8], newVal-i)
		}
	}
	update := Update{
		Name:    changeSplotchName,
		Version: "1.0.0",
		Instructions: changeSplotchUpdate{
			index:       uint64(splotchIndex),
			splotchData: newSplotch,
		}.marshal(),
	}

	// Create the WAL transaction.
	tx, err := ca.wal.NewTransaction([]Update{update})
	if err != nil {
		return err
	}
	// Signal completed setup and then wait for the commitment to finish.
	// (there was no setup, but we still have to signal it)
	errChan := tx.SignalSetupComplete()
	err = <-errChan
	if err != nil {
		return err
	}
	// Apply the updates.
	err = ca.applyUpdates([]Update{update})
	if err != nil {
		return err
	}
	err = tx.SignalUpdatesApplied()
	if err != nil {
		return err
	}
	return nil
}

// changeSplotchApply applies a change splotch update to the countdown array.
func (ca *countdownArray) changeSplotchApply(update Update) error {
	csu := unmarshalChangeSplotchUpdate(update.Instructions)
	offset := int64(csu.index*(csu.index+1)/2) * 8 * 10
	_, err := ca.splotchFile.WriteAt(csu.splotchData, offset)
	return err
}

// applyUpdates will apply a bunch of wal updates to the ca persist file.
func (ca *countdownArray) applyUpdates(updates []Update) error {
	for _, update := range updates {
		if update.Name == addCountName {
			err := ca.addCountApply(update)
			if err != nil {
				return nil
			}
		} else if update.Name == changeSplotchName {
			err := ca.changeSplotchApply(update)
			if err != nil {
				return nil
			}
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
	splotchFilename := filepath.Join(dir, "splotch.dat")
	splotchFile, err := os.OpenFile(splotchFilename, os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		return nil, err
	}

	// Open the WAL.
	walFilename := filepath.Join(dir, "wal.dat")
	updates, wal, err := newWal(walFilename, dependencyUncleanShutdown{})
	if err != nil {
		return nil, err
	}

	// Create the countdownArray and apply any updates from the wal.
	ca := &countdownArray{
		file:        file,
		splotchFile: splotchFile,
		wal:         wal,
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
		if num == 0 {
			ca.countdown = ca.countdown[:i+1]
			break
		}
		if num != start-uint64(i) {
			return nil, fmt.Errorf("count is incorrect representation %v != (%v - %v)", num, start, uint64(i))
		}
	}

	// Check that the splotch file follows the splotch rules.
	splotchData, err := ioutil.ReadAll(ca.splotchFile)
	if err != nil {
		return nil, err
	}
	skip := 0
	for i := 0; i < len(splotchData); i += skip {
		skip += 80
		current := binary.LittleEndian.Uint64(splotchData[i : i+8])
		if current == 0 {
			continue
		}
		for j := 8; j < skip; j += 8 {
			next := binary.LittleEndian.Uint64(splotchData[j+i : j+i+8])
			if next != current-1 {
				return nil, fmt.Errorf("splotch does not count down correctly: %v != (%v - %v)", current, next, 1)
			}
			current = next
		}
	}

	// Signal that the recovery is complete
	if err := wal.RecoveryComplete(); err != nil {
		return nil, err
	}
	return ca, nil
}

// TestWALIntegration creates a plausable use case for the WAL and then
// attempts to utilize all functions of the WAL.
func TestWALIntegration(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	// Create a folder to house everything we are working with.
	dir := build.TempDir("wal", t.Name())
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		t.Fatal(err)
	}

	// Create the countdown and extend the count out to 250, closing and
	// re-opening the wal each time. The number of WAL pages required by each
	// 'addCount' will incrase over time, giving good confidence that the WAL
	// is correctly handling multi-page updates.
	//
	// 'newCountdown' will chcek that the file is consistent, and detect that
	// updates are being applied correctly.
	expectedCountdownLen := 1
	if fastrand.Intn(2) != 0 {
		for i := 0; i < 300; i++ {
			cd, err := newCountdown(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(cd.countdown) != expectedCountdownLen {
				t.Fatal("coundown is incorrect", len(cd.countdown), expectedCountdownLen)
			}
			err = cd.addCount()
			if err != nil {
				t.Fatal(err)
			}
			expectedCountdownLen++
			err = cd.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	// Continue increasing the count, but this time start performing multiple
	// transactions between each opening and closing.
	if fastrand.Intn(2) != 0 {
		for i := 0; i < 10; i++ {
			cd, err := newCountdown(dir)
			if err != nil {
				t.Fatal(err)
			}

			if len(cd.countdown) != expectedCountdownLen {
				t.Fatal("coundown is incorrect", len(cd.countdown), expectedCountdownLen)
			}
			for j := 0; j < i; j++ {
				err = cd.addCount()
				if err != nil {
					t.Fatal(err)
				}
				expectedCountdownLen++
			}
			err = cd.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	// Test the durability of the WAL. We'll initialize to simulate a disk
	// failure after the WAL commits, but before we are able to apply the
	// commit.
	if fastrand.Intn(2) != 0 {
		for i := 0; i < 25; i++ {
			cd, err := newCountdown(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(cd.countdown) != expectedCountdownLen {
				t.Fatal("coundown is incorrect", len(cd.countdown), expectedCountdownLen)
			}

			// Add some legitimate counts.
			for j := 0; j < i; j++ {
				err = cd.addCount()
				if err != nil {
					t.Fatal(err)
				}
				expectedCountdownLen++
			}

			// Add a broken count. Because the break is after the WAL commits, the
			// count should still restore correctly on the next iteration where we
			// call 'newCountdown'.
			err = cd.addCountBroken()
			if err != nil {
				t.Fatal(err)
			}
			expectedCountdownLen++
			err = cd.Close()
			if err == nil {
				t.Fatal("Should have returned an error but didn't")
			}
		}

		// Check at this point that the wal is less than 100 pages.
		info, err := os.Stat(filepath.Join(dir, "wal.dat"))
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > 100*pageSize {
			t.Error("the wal is too large")
		}
	}

	// Test the parallelism. Basic way to do that is to have a second file that
	// we update in parallel transactions. But I'd also like to be able to test
	// parallel transactions that act on the same file? Not sure if that's
	// strictly necessary. But we could have a second file that perhaps tracks
	// two unrelated fields, like 5 integer arrays that are all intialized with
	// the same integers, over 10kb or something. And then that file could have
	// 3 independent sets of these things, so they all have clear dependence
	// within but no dependence next to. Then we'll update all of them and the
	// count as well in parallel transactions.
	if fastrand.Intn(2) != 0 {
		cd, err := newCountdown(dir)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for i := uint64(0); i < 50; i++ {
			wg.Add(1)
			go func(i uint64) {
				defer wg.Done()
				for j := uint64(1); j < 200; j++ {
					err := cd.changeSplotch(i, j)
					if err != nil {
						t.Error(err)
					}
				}
			}(i)
		}
		wg.Wait()
		err = cd.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Open and close the cd to allow the consistency checks to run.
	cd, err := newCountdown(dir)
	if err != nil {
		t.Fatal(err)
	}
	err = cd.Close()
	if err != nil {
		t.Fatal(err)
	}
}
