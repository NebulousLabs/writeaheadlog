package writeaheadlog

// TODO: Make it so that not every update changes the silo data. That will
// probably make the test run a lot faster overall.

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/NebulousLabs/errors"
	"github.com/NebulousLabs/fastrand"
	"golang.org/x/crypto/blake2b"
)

type (
	// A silo is a simulated group of data that requires using the full
	// featureset of the wal for maximum performance. The silo is a list of
	// numbers followed by a checksum. The numbers form a ring (the first
	// number follows the last number) that has strictly increasing values,
	// except for one place within the ring where the next number is smaller
	// than the previous.
	//
	// The checksum is the checksum of the silo data plus some external data.
	// The external data exists in a file with the same name as checksum. The
	// external data should always be available if the silo exists. There
	// should also be exactly one external file per file and not more if the
	// full database is intact and has been consistently managed.
	silo struct {
		// List of numbers in silo and the index of the next number that will
		// be incremented.
		nextNumber uint32
		numbers    []uint32

		// Utilities
		deps   dependencies
		f      file  // database file holding silo
		offset int64 // offset within database
	}

	// A siloUpdate contains an update to the silo. There is a number that
	// needs to be written (it's the smallest number in the silo), as well as
	// an update to the checksum because the blob file associated with the silo
	// has changed.
	siloUpdate struct {
		offset int64  // Location in the file where we need to write a number.
		number uint32 // The number we need to write.
		silo   int64  // Offset of the silo within the file.

		// Datafile checksum management.
		prevChecksum   checksum // Need to remove the corresponding file.
		newChecksum    checksum // Need to write the new checksum. File already exists.
		checksumOffset int64    // Location to write the checksum.
	}
)

// newSilo creates a new silo of a certain length at a specific offset in the
// file
func newSilo(offset int64, length int, deps dependencies, f file) *silo {
	if length == 0 {
		panic("numbers shouldn't be empty")
	}

	return &silo{
		offset:  offset,
		numbers: make([]uint32, length, length),
		f:       f,
		deps:    deps,
	}
}

// marshal marshals a siloUpdate
func (su siloUpdate) marshal() []byte {
	data := make([]byte, 28+2*checksumSize)
	binary.LittleEndian.PutUint64(data[0:8], uint64(su.offset))
	binary.LittleEndian.PutUint32(data[8:12], su.number)
	binary.LittleEndian.PutUint64(data[12:20], uint64(su.silo))
	binary.LittleEndian.PutUint64(data[20:28], uint64(su.checksumOffset))
	copy(data[28:28+checksumSize], su.prevChecksum[:])
	copy(data[28+checksumSize:], su.newChecksum[:])
	return data
}

// unmarshal unmarshals a siloUpdate from data
func (su *siloUpdate) unmarshal(data []byte) {
	if len(data) != 28+2*checksumSize {
		panic("data has wrong size")
	}
	su.offset = int64(binary.LittleEndian.Uint64(data[0:8]))
	su.number = binary.LittleEndian.Uint32(data[8:12])
	su.silo = int64(binary.LittleEndian.Uint64(data[12:20]))
	su.checksumOffset = int64(binary.LittleEndian.Uint64(data[20:28]))
	copy(su.prevChecksum[:], data[28:28+checksumSize])
	copy(su.newChecksum[:], data[28+checksumSize:])
	return
}

// newUpdate create a WAL update from a siloUpdate
func (su *siloUpdate) newUpdate() Update {
	update := Update{
		Name:         "This is my update. There are others like it but this one is mine",
		Instructions: su.marshal(),
	}
	return update
}

// newSiloUpdate creates a new Silo update for a number at a specific index
func (s *silo) newSiloUpdate(index uint32, number uint32, ocs checksum) *siloUpdate {
	return &siloUpdate{
		number:         number,
		offset:         s.offset + int64(4*(index)),
		silo:           s.offset,
		prevChecksum:   ocs,
		checksumOffset: s.offset + int64(len(s.numbers)*4),
	}
}

// checksum calculates the silos's current checksum
func (s *silo) checksum() (cs checksum) {
	buf := make([]byte, 4*len(s.numbers))
	for i := 0; i < len(s.numbers); i++ {
		binary.LittleEndian.PutUint32(buf[i*4:i*4+4], s.numbers[i])
	}
	c := blake2b.Sum256(buf)
	copy(cs[:], c[:])
	return
}

// applyUpdate applies an update to a silo on disk
func (su siloUpdate) applyUpdate(silo *silo, dataPath string) error {
	if silo == nil {
		panic("silo shouldn't be nil")
	}

	// Write number
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data[:], su.number)
	_, err := silo.f.WriteAt(data[:], su.offset)
	if err != nil {
		return err
	}

	// Write new checksum
	_, err = silo.f.WriteAt(su.newChecksum[:], su.checksumOffset)
	if err != nil {
		return err
	}

	// Delete old data file if it still exists
	err = silo.deps.remove(filepath.Join(dataPath, hex.EncodeToString(su.prevChecksum[:])))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// init calculates the checksum of the silo and writes it to disk
func (s *silo) init() error {
	cs := s.checksum()
	_, err := s.f.WriteAt(cs[:], s.offset+int64(len(s.numbers)*4))
	return err
}

// threadedSetupWrite simulates a setup by updating the checksum and writing
// random data to a file which uses the checksum as a filename
func (s *silo) threadedSetupWrite(done chan error, dataPath string, ncs checksum) {
	// signal completion
	defer close(done)

	// write new data file
	newFile, err := s.deps.create(filepath.Join(dataPath, hex.EncodeToString(ncs[:])))
	if err != nil {
		done <- err
		return
	}
	_, err = newFile.Write(fastrand.Bytes(10 * pageSize))
	if err != nil {
		done <- err
		return
	}
	syncErr := newFile.Sync()
	if err := newFile.Close(); err != nil {
		done <- err
		return
	}

	// sync changes
	done <- syncErr
}

// threadedUpdate updates a silo until a sync fails
func (s *silo) threadedUpdate(t *testing.T, w *WAL, dataPath string, wg *sync.WaitGroup) {
	defer wg.Done()

	// Allocate some memory for the updates
	sus := make([]*siloUpdate, 0, len(s.numbers))

	// This thread will execute until the dependency of the silo causes a file
	// sync to fail
	for {
		// Change between 1 and len(s.numbers)
		length := fastrand.Intn(len(s.numbers)) + 1
		ocs := s.checksum()
		appendFrom := length
		var ncs checksum
		for j := 0; j < length; j++ {
			if appendFrom == length && j > 0 && fastrand.Intn(500) == 0 {
				// There is a 0.5% chance that the remaing updates will be
				// appended after the transaction was created
				appendFrom = j
			}
			if s.nextNumber == 0 {
				s.numbers[s.nextNumber] = (s.numbers[len(s.numbers)-1] + 1)
			} else {
				s.numbers[s.nextNumber] = (s.numbers[s.nextNumber-1] + 1)
			}
			ncs = s.checksum()

			// Create siloUpdate
			su := s.newSiloUpdate(s.nextNumber, s.numbers[s.nextNumber], ocs)
			sus = append(sus, su)

			// Increment the index. If that means we reach the end, set it to 0
			s.nextNumber = (s.nextNumber + 1) % uint32(len(s.numbers))
		}

		// Set the siloUpdates checksum and create the corresponding update
		updates := make([]Update, 0, len(s.numbers))
		for _, su := range sus {
			copy(su.newChecksum[:], ncs[:])
			updates = append(updates, su.newUpdate())
		}

		// Start setup write
		wait := make(chan error)
		go s.threadedSetupWrite(wait, dataPath, ncs)

		// Create txn
		txn, err := w.NewTransaction(updates[:appendFrom])
		if err != nil {
			t.Error(err)
			return
		}

		// Append the remaining updates
		if err := <-txn.Append(updates[appendFrom:]); err != nil {
			return
		}

		// Wait for setup to finish. If it wasn't successful there is no need
		// to continue
		if err := <-wait; err != nil {
			return
		}

		// Signal setup complete
		if err := <-txn.SignalSetupComplete(); err != nil {
			return
		}

		// Apply the updates
		for _, su := range sus {
			if err := su.applyUpdate(s, dataPath); err != nil {
				t.Error(err)
				return
			}
		}

		// Sync the updates
		if err := s.f.Sync(); err != nil {
			return
		}

		// Signal release complete
		if err := txn.SignalUpdatesApplied(); err != nil {
			return
		}
		// Reset
		sus = sus[:0]
		updates = updates[:0]
	}
}

// toUint32Slice is a helper function to convert a byte slice to a uint32
// slice
func toUint32Slice(d []byte) []uint32 {
	buf := bytes.NewBuffer(d)
	converted := make([]uint32, len(d)/4, len(d)/4)
	for i := 0; i < len(converted); i++ {
		converted[i] = binary.LittleEndian.Uint32(buf.Next(4))
	}
	return converted
}

// recoverSiloWAL recovers the WAL after a crash. This will be called
// repeatedly until it finishes.
func recoverSiloWAL(walPath string, deps *dependencyFaultyDisk, silos map[int64]*silo, testdir string, file file, numSilos int64, numIncrease int) (err error) {
	// Reload wal.
	updates, wal, err := newWal(walPath, deps)
	if err != nil {
		return errors.Extend(errors.New("failed to reload WAL"), err)
	}
	defer func() {
		if err != nil {
			wal.logFile.Close()
		}
	}()

	// Unmarshal updates and apply them
	var checksums = make(map[string]struct{})
	for _, update := range updates {
		var su siloUpdate
		su.unmarshal(update.Instructions)
		if err := su.applyUpdate(silos[su.silo], testdir); err != nil {
			return errors.Extend(errors.New("Failed to apply update"), err)
		}
		// Remember new checksums to be able to remove unnecessary setup files
		// later
		checksums[hex.EncodeToString(su.newChecksum[:])] = struct{}{}
	}

	// Sync the applied updates
	if err := file.Sync(); err != nil {
		return errors.Extend(errors.New("Failed to sync database"), err)
	}

	// Remove unnecessary setup files
	files, err := ioutil.ReadDir(testdir)
	if err != nil {
		return errors.Extend(errors.New("Failed to get list of files in testdir"), err)
	}
	for _, f := range files {
		_, exists := checksums[f.Name()]
		if len(f.Name()) == 32 && !exists {
			if err := deps.remove(filepath.Join(testdir, f.Name())); err != nil && !os.IsNotExist(err) {

				return errors.Extend(errors.New("Failed to remove setup file"), err)
			}
		}
	}

	// Check if the checksums match the data
	numbers := make([]byte, numSilos*int64(numIncrease)*4)
	var cs checksum
	for _, silo := range silos {
		// Adjust the size of numbers
		numbers = numbers[:4*len(silo.numbers)]

		// Read numbers and checksum
		if _, err := silo.f.ReadAt(numbers, silo.offset); err != nil {
			return errors.Extend(errors.New("Failed to read numbers of silo"), err)
		}
		if _, err := silo.f.ReadAt(cs[:], silo.offset+int64(4*len(silo.numbers))); err != nil {
			return errors.Extend(errors.New("Failed to read checksum of silo"), err)
		}

		// The checksum should match
		c := blake2b.Sum256(numbers)
		if bytes.Compare(c[:checksumSize], cs[:]) != 0 {
			return errors.New("Checksums don't match")
		}

		// Reset silo numbers in memory
		silo.numbers = toUint32Slice(numbers)
	}

	if err := wal.RecoveryComplete(); err != nil {
		return errors.Extend(errors.New("Failed to signal completed recovery"), err)
	}

	// Close the wal
	if err := wal.Close(); err != nil {
		return errors.Extend(errors.New("Failed to close WAL"), err)
	}
	return nil
}

// newSiloDatabase will create a silo database that overwrites any existing
// silo database.
func newSiloDatabase(deps *dependencyFaultyDisk, dbPath, walPath string, numSilos int64, numIncrease int) (map[int64]*silo, *WAL, file, error) {
	// Create new fake database file
	file, err := deps.create(dbPath)
	if err != nil {
		return nil, nil, nil, err
	}

	// Create a new wal.
	updates, wal, err := newWal(walPath, deps)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(updates) > 0 || wal.recoveryComplete == false {
		return nil, nil, nil, fmt.Errorf("len(updates) was %v and recoveryComplete was %v", len(updates), wal.recoveryComplete)
	}

	// Create and initialize the silos.
	var siloOffset int64
	var siloOffsets []int64
	var silos = make(map[int64]*silo)
	for i := 0; int64(i) < numSilos; i++ {
		silo := newSilo(siloOffset, 1+i*numIncrease, deps, file)
		if err := silo.init(); err != nil {
			return nil, nil, nil, errors.Extend(errors.New("failed to init silo"), err)
		}
		siloOffsets = append(siloOffsets, siloOffset)
		silos[siloOffset] = silo
		siloOffset += int64(len(silo.numbers)*4) + checksumSize
	}
	return silos, wal, file, nil
}

// TestSilo is an integration test that is supposed to test all the features of
// the WAL in a single testcase. It uses 100 silos updating 1000 times each.
func TestSilo(t *testing.T) {
	// Declare some vars to configure the loop
	numSilos := int64(100)
	numIncrease := 20
	maxIters := 50
	endTime := time.Now().Add(5 * time.Minute)
	// Test should only run 10 seconds in short mode.
	if testing.Short() {
		endTime = time.Now().Add(30 * time.Second)
	}

	// Create the folder and establis hthe filepaths.
	deps := newFaultyDiskDependency(10e6)
	testdir := tempDir("wal", t.Name())
	os.MkdirAll(testdir, 0777)
	dbPath := filepath.Join(testdir, "database.dat")
	walPath := filepath.Join(testdir, "wal.dat")

	// Create the silo database. Disable deps before doing that. Otherwise the
	// test might fail right away
	deps.disable()
	silos, wal, file, err := newSiloDatabase(deps, dbPath, walPath, numSilos, numIncrease)
	if err != nil {
		t.Fatal(err)
	}
	deps.enable()

	// Write silos, pull the plug and verify repeatedly
	i := 0
	maxRetries := 0
	totalRetries := 0
	for i = 0; i < maxIters; i++ {
		if time.Now().After(endTime) {
			// Stop if test takes too long
			break
		}
		// Reset the dependencies.
		deps.reset()

		// Randomly decide between resetting the database entirely and opening
		// the database from the previous iteration.
		deps.disable()
		if fastrand.Intn(3) == 0 {
			// Reset database.
			silos, wal, file, err = newSiloDatabase(deps, dbPath, walPath, numSilos, numIncrease)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			// Resume with existing database.
			var updates []Update
			updates, wal, err = newWal(walPath, deps)
			if len(updates) > 0 || err != nil {
				t.Fatal(updates, err)
			}
		}
		deps.enable()

		// Spin up all of the silo threads.
		var wg sync.WaitGroup
		for _, silo := range silos {
			wg.Add(1)
			go silo.threadedUpdate(t, wal, testdir, &wg)
		}
		// Wait for all the threads to fail
		wg.Wait()
		// Close wal.
		if err := wal.logFile.Close(); err != nil {
			t.Fatal(err)
		}

		// Repeatedly try to recover WAL.
		retries := 0
		err = retry(100, time.Millisecond, func() error {
			retries++
			deps.reset()
			if retries >= 100 {
				// Could be getting unlucky - try to disable the faulty disk and
				// see if we can recover all the way.
				t.Log("Disabling dependencies during recovery - recovery filed 100 times in a row")
				deps.disable()
				defer deps.enable()
			}

			// Try to recover WAL
			return recoverSiloWAL(walPath, deps, silos, testdir, file, numSilos, numIncrease)
		})
		if err != nil {
			t.Fatalf("WAL never recovered: %v", err)
		}

		// Statistics
		totalRetries += retries - 1
		if retries > maxRetries {
			maxRetries = retries - 1
		}
	}

	walFile, err := os.Open(walPath)
	if err != nil {
		t.Errorf("Failed to open wal: %v", err)
	}
	fi, err := walFile.Stat()
	if err != nil {
		t.Errorf("Failed to get wal fileinfo: %v", err)
	}

	t.Logf("Number of iterations: %v", i)
	t.Logf("Max number of retries: %v", maxRetries)
	t.Logf("Average number of retries: %v", totalRetries/i)
	t.Logf("WAL size: %v bytes", fi.Size())
}
