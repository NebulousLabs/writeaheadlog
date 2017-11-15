package writeaheadlog

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/errors"
	"github.com/NebulousLabs/fastrand"
	"golang.org/x/crypto/blake2b"
)

type (
	silo struct {
		// offset is the file offset in the fake database
		offset int64

		// numbers contains the set of numbers of the silo
		numbers []uint32

		// f is the file on which the silo is stored
		f file

		// i is the index of the number that needs to be incremented next
		i uint32

		// deps are the dependencies that is used to handle file IO
		deps dependencies
	}

	siloUpdate struct {
		// offset is the in the file at which the number should be written
		offset int64

		// number is the number which is written at offset
		number uint32

		// silo is the offset of the silo so we can apply the update to the right silo
		silo int64

		// ocs is the checksum of the file that became obsolete after changing a
		// number. It needs to be removed when the update is applied.
		ocs checksum

		// ncs is the new checkum of the silo and the corresponding file
		ncs checksum

		// ncso is the offset of the new checksum
		ncso int64
	}
)

// newSilo creates a new silo of a certain length at s specific offset in the
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
	binary.LittleEndian.PutUint64(data[20:28], uint64(su.ncso))
	copy(data[28:28+checksumSize], su.ocs[:])
	copy(data[28+checksumSize:], su.ncs[:])
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
	su.ncso = int64(binary.LittleEndian.Uint64(data[20:28]))
	copy(su.ocs[:], data[28:28+checksumSize])
	copy(su.ncs[:], data[28+checksumSize:])
	return
}

// newUpdate create a WAL update from a siloUpdate
func newUpdate(su *siloUpdate) Update {
	update := Update{
		Name:         "This is my update. There are others like it but this one is mine",
		Version:      "v0.9.8.7.6.5.4.3.2.1.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z",
		Instructions: su.marshal(),
	}
	return update
}

// newSiloUpdate creates a new Silo update for a number at a specific index
func (s *silo) newSiloUpdate(index uint32, number uint32, ocs checksum) *siloUpdate {
	return &siloUpdate{
		number: number,
		offset: s.offset + int64(4*(index)),
		silo:   s.offset,
		ocs:    ocs,
		ncso:   s.offset + int64(len(s.numbers)*4),
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
	_, err = silo.f.WriteAt(su.ncs[:], su.ncso)
	if err != nil {
		return err
	}

	// Delete old data file if it still exists
	err = silo.deps.remove(filepath.Join(dataPath, hex.EncodeToString(su.ocs[:])))
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
	done <- build.ComposeErrors(syncErr)
}

// threadedUpdate updates a silo 1000 times and leaves the last transaction unapplied.
func (s *silo) threadedUpdate(t *testing.T, w *WAL, dataPath string, wg *sync.WaitGroup) {
	defer wg.Done()

	// Allocate some memory for the updates
	sus := make([]*siloUpdate, 0, len(s.numbers))
	for {
		// Change between 1 and len(s.numbers)
		length := rand.Intn(len(s.numbers)) + 1
		var ocs = s.checksum()
		var ncs checksum
		for j := 0; j < length; j++ {
			if s.i == 0 {
				s.numbers[s.i] = (s.numbers[len(s.numbers)-1] + 1)
			} else {
				s.numbers[s.i] = (s.numbers[s.i-1] + 1)
			}
			ncs = s.checksum()

			// Create siloUpdate
			su := s.newSiloUpdate(s.i, s.numbers[s.i], ocs)
			sus = append(sus, su)

			// Increment the index. If that means we reach the end, set it to 0
			s.i = (s.i + 1) % uint32(len(s.numbers))
		}

		// Set the siloUpdates checksum and create the corresponding update
		updates := make([]Update, 0, len(s.numbers))
		for _, su := range sus {
			copy(su.ncs[:], ncs[:])
			updates = append(updates, newUpdate(su))
		}

		// Start setup write
		wait := make(chan error)
		go s.threadedSetupWrite(wait, dataPath, ncs)

		// Create txn
		txn, err := w.NewTransaction(updates)
		if err != nil {
			t.Error(err)
			return
		}

		// Wait for setup to finish. If it wasn't successful there is no need
		// to continue
		if err := <-wait; err != nil {
			return
		}

		// Signal setup complete
		if err := <-txn.SignalSetupComplete(); err != nil {
			t.Error(err)
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
			t.Error(err)
			return
		}

		// Reset
		sus = sus[:0]
		updates = updates[:0]
	}
}

// recoverSiloWAL recovers the WAL after a crash. This will be called
// repeatedly until it finishes.
func recoverSiloWAL(walPath string, deps dependencies, silos map[int64]*silo, testdir string, file file, numSilos int64, numIncrease int) (err error) {
	// Reload wal.
	updates, wal, err := newWal(walPath, deps)
	if err != nil {
		return build.ExtendErr("failed to reload WAL", err)
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
			return build.ExtendErr("Failed to apply update", err)
		}
		// Remember new checksums to be able to remove unnecessary setup files
		// later
		checksums[hex.EncodeToString(su.ncs[:])] = struct{}{}
	}

	// Sync the applied updates
	if err := file.Sync(); err != nil {
		return build.ExtendErr("Failed to sync database", err)
	}

	// Remove unnecessary setup files
	files, err := ioutil.ReadDir(testdir)
	if err != nil {
		return build.ExtendErr("Failed to get list of files in testdir", err)
	}
	for _, f := range files {
		_, exists := checksums[f.Name()]
		if len(f.Name()) == 32 && !exists {
			if err := deps.remove(filepath.Join(testdir, f.Name())); err != nil && !os.IsNotExist(err) {

				return build.ExtendErr("Failed to remove setup file", err)
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
			return build.ExtendErr("Failed to read numbers of silo", err)
		}
		if _, err := silo.f.ReadAt(cs[:], silo.offset+int64(4*len(silo.numbers))); err != nil {
			return build.ExtendErr("Failed to read checksum of silo", err)
		}

		// The checksum should match
		c := blake2b.Sum256(numbers)
		if bytes.Compare(c[:checksumSize], cs[:]) != 0 {
			return errors.New("Checksums don't match")
		}
	}

	if err := wal.RecoveryComplete(); err != nil {
		return build.ExtendErr("Failed to signal completed recovery: %v", err)
	}

	// Close the wal
	if err := wal.Close(); err != nil {
		return build.ExtendErr("Failed to close WAL", err)
	}
	return nil
}

// TestSilo is an integration test that is supposed to test all the features of
// the WAL in a single testcase. It uses 100 silos updating 1000 times each.
func TestSilo(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	deps := newFaultyDiskDependency(1000)
	testdir := build.TempDir("wal", t.Name())
	dbPath := filepath.Join(testdir, "database.dat")
	walPath := filepath.Join(testdir, "wal.dat")

	// Create the test dir
	os.MkdirAll(testdir, 0777)

	// Disable dependencies for the initial files
	deps.disable(true)

	// Declare some vars to configure the loop
	var numSilos = int64(250)
	var numIncrease = 20
	var maxCntr = 20
	var numRetries = 1000
	var wg sync.WaitGroup
	var counters = make([]int, maxCntr, maxCntr)

	// Write silos, pull the plug and verify repeatedly
	for cntr := 0; cntr < maxCntr; cntr++ {
		deps.disable(true)

		// Create new fake database file
		os.Remove(dbPath)
		file, err := deps.create(dbPath)
		if err != nil {
			t.Fatal(err)
		}

		// Create wal with disabled dependencies
		updates, wal, err := newWal(walPath, deps)
		if err != nil {
			t.Fatal(err)
		}
		if len(updates) > 0 || wal.recoveryComplete == false {
			t.Fatalf("len(updates) was %v and recoveryComplete was %v",
				len(updates), wal.recoveryComplete)
		}

		// Reset dependencies and increase the write limit to allow for higher
		// success rate
		deps.reset()
		*deps.writeLimit = 5000

		var siloOff int64
		var siloOffsets []int64
		var silos = make(map[int64]*silo)

		// Start the threaded update after creating and initializing the silos
		for i := 0; int64(i) < numSilos; i++ {
			wg.Add(1)
			silo := newSilo(siloOff, 1+i*numIncrease, deps, file)
			if err := silo.init(); err != nil {
				t.Fatalf("Failed to init silo: %v", err)
			}

			go silo.threadedUpdate(t, wal, testdir, &wg)
			siloOffsets = append(siloOffsets, siloOff)
			silos[siloOff] = silo
			siloOff += int64(len(silo.numbers)*4) + checksumSize
		}

		// Corrupt the disk
		deps.disable(false)

		// Wait for all the threads to fail
		wg.Wait()

		// Close wal
		if err := wal.logFile.Close(); err != nil {
			t.Error(err)
		}

		// Repeatedly try to recover WAL
		deps.reset()
		*deps.writeLimit = 10000000
		deps.disable(true)
		err = build.Retry(numRetries, time.Millisecond, func() error {
			counters[cntr]++
			// Reset deps if the failDenominator is already above the
			// writeLimitl. Otherwise we only need to reset deps.failed. This
			// reduces the chance of disk corruption after every iteration.
			deps.mu.Lock()
			*deps.failed = false
			if *deps.failDenominator > *deps.writeLimit {
				deps.mu.Unlock()
				deps.reset()
			} else {
				deps.mu.Unlock()
			}

			// Try to recover WAL
			return recoverSiloWAL(walPath, deps, silos, testdir, file, numSilos, numIncrease)
		})
		deps.disable(false)
		if err != nil {
			t.Fatalf("WAL never recovered: %v", err)
		}
	}
	avgCounter := 0
	for _, n := range counters {
		avgCounter += n
	}
	avgCounter /= len(counters)
	t.Logf("Average number of retries: %v", avgCounter)
}
