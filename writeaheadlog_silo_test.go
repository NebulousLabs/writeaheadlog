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

	"github.com/NebulousLabs/Sia/build"
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

		// cs is the checksum of the silo's numbers
		cs checksum

		// dep is the dependency that is used to handle file IO
		dep dependency
	}

	siloUpdate struct {
		// offset is the in the file at which the number should be written
		offset int64

		// number is the number which is written at offset
		number uint32

		// silo is the offset of the silo so we can apply the update to the right silo
		silo int64

		// cs is the checksum of the file that became obsolete after changing a
		// number. It needs to be removed when the update is applied.
		cs checksum
	}
)

// newSilo creates a new silo of a certain length at s specific offset in the
// file
func newSilo(offset int64, length int, f file) *silo {
	if length == 0 {
		panic("numbers shouldn't be empty")
	}

	return &silo{
		offset:  offset,
		numbers: make([]uint32, length, length),
		f:       f,
	}
}

// marshal marshals a siloUpdate
func (su siloUpdate) marshal() []byte {
	data := make([]byte, 20+checksumSize)
	binary.LittleEndian.PutUint64(data[0:8], uint64(su.offset))
	binary.LittleEndian.PutUint32(data[8:12], su.number)
	binary.LittleEndian.PutUint64(data[12:20], uint64(su.silo))
	copy(data[20:], su.cs[:])
	return data
}

// unmarshal unmarshals a siloUpdate from data
func (su *siloUpdate) unmarshal(data []byte) {
	if len(data) != 20+checksumSize {
		panic("data has wrong size")
	}
	su.offset = int64(binary.LittleEndian.Uint64(data[0:8]))
	su.number = binary.LittleEndian.Uint32(data[8:12])
	su.silo = int64(binary.LittleEndian.Uint64(data[12:20]))
	copy(su.cs[:], data[20:])
	return
}

// newUpdate create a WAL update from a siloUpdate
func newUpdate(su siloUpdate) Update {
	update := Update{
		Name:         "This is my update. There are others like it but this one is mine",
		Version:      "v0.9.8.7.6.5.4.3.2.1.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z",
		Instructions: su.marshal(),
	}
	return update
}

// newSiloUpdate creates a new Silo update for a number at a specific index
func (s *silo) newSiloUpdate(index uint32, number uint32) siloUpdate {
	return siloUpdate{
		number: number,
		offset: s.offset + int64(4*(index)),
		silo:   s.offset,
		cs:     s.cs,
	}
}

// updateChecksum calculates the silos checksum and updates the silo's
// checksum field
func (s *silo) updateChecksum() {
	buf := make([]byte, 4*len(s.numbers))
	for i := 0; i < len(s.numbers); i++ {
		binary.LittleEndian.PutUint32(buf[i*4:i*4+4], s.numbers[i])
	}
	cs := blake2b.Sum256(buf)
	copy(s.cs[:], cs[:])
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

	// Delete old data file if it still exists
	err = dep.Remove(filepath.Join(dataPath, hex.EncodeToString(su.cs[:])))
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// threadedSetupWrite simulates a setup by updating the checksum and writing
// random data to a file which uses the checksum as a filename
func (s *silo) threadedSetupWrite(done chan error, dataPath string) {
	// signal completion
	defer close(done)

	// write checksum
	s.updateChecksum()
	_, err := s.f.WriteAt(s.cs[:], s.offset+int64(len(s.numbers)*4))
	if err != nil {
		done <- err
		return
	}

	// write new data file
	newFile, err := s.dep.Create(filepath.Join(dataPath, hex.EncodeToString(s.cs[:])))
	if err != nil {
		done <- err
		return
	}
	_, err = newFile.Write(fastrand.Bytes(10 * pageSize))
	if err != nil {
		done <- err
		return
	}
	if err := newFile.Close(); err != nil {
		done <- err
		return
	}

	// sync changes
	//newFile.Sync()
	//s.f.Sync()
}

// threadedUpdate updates a silo 1000 times and leaves the last transaction unapplied.
func (s *silo) threadedUpdate(t *testing.T, w *WAL, dataPath string, wg *sync.WaitGroup) {
	defer wg.Done()

	sus := make([]siloUpdate, 0, len(s.numbers))
	updates := make([]Update, 0, len(s.numbers))
	iterations := 100
	for i := 0; i < iterations; i++ {
		// Change between 1 and len(s.numbers)
		length := rand.Intn(len(s.numbers)) + 1
		for j := 0; j < length; j++ {
			if s.i == 0 {
				s.numbers[s.i] = (s.numbers[len(s.numbers)-1] + 1)
			} else {
				s.numbers[s.i] = (s.numbers[s.i-1] + 1)
			}

			// Create siloUpdate and WAL update and append them to the
			// corresponding slice
			su := s.newSiloUpdate(s.i, s.numbers[s.i])
			sus = append(sus, su)
			updates = append(updates, newUpdate(su))

			// Increment the index. If that means we reach the end, set it to 0
			s.i = (s.i + 1) % uint32(len(s.numbers))
		}

		// Start setup write
		wait := make(chan error)
		go s.threadedSetupWrite(wait, dataPath)

		// Create txn
		txn, err := w.NewTransaction(updates)
		if err != nil {
			t.Fatal(err)
		}

		// Wait for setup to finish
		if err := <-wait; err != nil {
			t.Fatal(err)
		}

		// Signal setup complete
		if err := <-txn.SignalSetupComplete(); err != nil {
			t.Fatal(err)
		}

		// Corrupt the last iteration by not finishing it
		if i == iterations-1 {
			return
		}

		// Apply the updates
		for _, su := range sus {
			su.applyUpdate(s, dataPath)
		}

		// Signal release complete
		if err := txn.SignalUpdatesApplied(); err != nil {
			t.Fatal(err)
		}

		// Reset
		sus = sus[:0]
		updates = updates[:0]
	}
}

// TestSilo is an integration test that is supposed to test all the features of
// the WAL in a single testcase. It uses 100 silos updating 1000 times each.
func TestSilo(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	dep := faultyDiskDependency{}
	testdir := build.TempDir("wal", t.Name())
	dbPath := filepath.Join(testdir, "database.dat")
	walPath := filepath.Join(testdir, "wal.dat")

	// Create the test dir
	os.MkdirAll(testdir, 0777)

	// Create fake database file
	file, err := dep.Create(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	// Create wal
	updates, wal, err := newWal(walPath, dep)
	if err != nil {
		t.Fatal(err)
	}

	// Do
	var numSilos = int64(250)
	var numIncrease = 20
	var siloOff int64
	var siloOffsets []int64
	var wg sync.WaitGroup
	var silos = make(map[int64]*silo)
	for i := 0; int64(i) < numSilos; i++ {
		wg.Add(1)
		silo := newSilo(siloOff, 1+i+numIncrease, file)
		go silo.threadedUpdate(t, wal, testdir, &wg)

		siloOffsets = append(siloOffsets, siloOff)
		silos[siloOff] = silo
		siloOff += int64(len(silo.numbers)*4) + checksumSize
	}

	// Wait for threads to finish
	wg.Wait()

	// Close wal
	if err := wal.logFile.Close(); err != nil {
		t.Error(err)
	}

	// Reload wal
	updates, wal, err = New(walPath)
	if err != nil {
		t.Errorf("Failed to load wal: %v", err)
	}

	// Unmarshal updates and apply them
	for _, update := range updates {
		var su siloUpdate
		su.unmarshal(update.Instructions)
		su.applyUpdate(silos[su.silo], testdir)
	}

	// Remove unnecessary setup files
	checksums := make(map[string]struct{})
	for k := range silos {
		checksums[hex.EncodeToString(silos[k].cs[:])] = struct{}{}
	}
	files, err := ioutil.ReadDir(testdir)
	if err != nil {
		t.Errorf("Failed to get list of files in testdir: %v", err)
	}
	for _, f := range files {
		_, exists := checksums[f.Name()]
		if len(f.Name()) == 32 && !exists {
			if err := dep.Remove(filepath.Join(testdir, f.Name())); err != nil && !os.IsNotExist(err) {
				t.Errorf("Failed to remove setup file: %v", err)
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
			t.Errorf("Failed to read numbers of silo %v", err)
		}
		if _, err := silo.f.ReadAt(cs[:], silo.offset+int64(4*len(silo.numbers))); err != nil {
			t.Errorf("Failed to read checksum of silo %v", err)
		}

		// The checksum should match
		c := blake2b.Sum256(numbers)
		if bytes.Compare(c[:checksumSize], cs[:]) != 0 {
			t.Errorf("Checksums don't match \n %v\n %v", cs, c[:checksumSize])
		}
	}

	// There should be numSilos + 2 files in the directory
	files, err = ioutil.ReadDir(testdir)
	if err != nil {
		t.Error(err)
	}
	if int64(len(files)) != numSilos+2 {
		t.Errorf("Wrong number of files. Was %v but should be %v", len(files), numSilos+2)
	}

	// Signal a completed recovery and close the wal
	if err := wal.RecoveryComplete(); err != nil {
		t.Errorf("Failed to signal completed recovery: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Errorf("Failed to close WAL: %v", err)
	}
}
