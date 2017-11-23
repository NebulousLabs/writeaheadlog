package writeaheadlog

import (
	"io/ioutil"
	"os"
	"sync"

	"github.com/NebulousLabs/errors"
	"github.com/NebulousLabs/fastrand"
)

// scrambleData takes some data as input and replaces parts of it randomly with
// random data
func scrambleData(d []byte) []byte {
	randomData := fastrand.Bytes(len(d))
	scrambled := make([]byte, len(d), len(d))
	for i := 0; i < len(d); i++ {
		if fastrand.Intn(4) == 0 { // 25% chance to replace byte
			scrambled[i] = randomData[i]
		} else {
			scrambled[i] = d[i]
		}
	}
	return scrambled
}

// dependencyFaultyDisk implements dependencies that simulate a faulty disk.
type dependencyFaultyDisk struct {
	// failDenominator determines how likely it is that a write will fail,
	// defined as 1/failDenominator. Each write call increments
	// failDenominator, and it starts at 2. This means that the more calls to
	// WriteAt, the less likely the write is to fail. All calls will start
	// automatically failing after writeLimit writes.
	disabled        bool
	failed          bool
	failDenominator int
	totalWrites     int
	writeLimit      int

	mu sync.Mutex
}

// newFaultyDiskDependency creates a dependency that can be used to simulate a
// failing disk. writeLimit is the maximum number of writes the disk will
// endure before failing
func newFaultyDiskDependency(writeLimit int) *dependencyFaultyDisk {
	return &dependencyFaultyDisk{
		writeLimit: writeLimit,
	}
}

func (d *dependencyFaultyDisk) create(path string) (file, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.tryFail() {
		return nil, errors.New("failed to create file (faulty disk)")
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return d.newFaultyFile(f), nil
}

// disabled allows the caller to temporarily disable the dependency
func (d *dependencyFaultyDisk) disable() {
	d.mu.Lock()
	d.disabled = true
	d.mu.Unlock()
}
func (*dependencyFaultyDisk) disrupt(s string) bool {
	return s == "FaultyDisk"
}
func (d *dependencyFaultyDisk) enable() {
	d.mu.Lock()
	d.disabled = false
	d.mu.Unlock()
}

// tryFail will check if the disk has failed yet, and if not, it'll rng to see
// if the disk should fail now. Returns 'true' if the disk has failed.
func (d *dependencyFaultyDisk) tryFail() bool {
	d.totalWrites++
	if d.disabled {
		return false
	}
	if d.failed {
		return true
	}

	d.failDenominator += fastrand.Intn(13)
	fail := fastrand.Intn(int(d.failDenominator+1)) == 0 // +1 to prevent 0 from being passed in.
	if fail || d.failDenominator >= d.writeLimit {
		d.failed = true
		return true
	}
	return false
}

// newFaultyFile creates a new faulty file around the provided file handle.
func (d *dependencyFaultyDisk) newFaultyFile(f *os.File) *faultyFile {
	return &faultyFile{d: d, file: f}
}
func (*dependencyFaultyDisk) readFile(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}
func (d *dependencyFaultyDisk) remove(path string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.tryFail() {
		return nil
	}
	return os.Remove(path)
}

// reset resets the failDenominator and the failed flag of the dependency
func (d *dependencyFaultyDisk) reset() {
	d.mu.Lock()
	d.failDenominator = 0
	d.failed = false
	d.mu.Unlock()
}
func (d *dependencyFaultyDisk) openFile(path string, flag int, perm os.FileMode) (file, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return d.newFaultyFile(f), nil
}

// faultyFile implements a file that simulates a faulty disk.
type faultyFile struct {
	d    *dependencyFaultyDisk
	file *os.File
}

func (f *faultyFile) Read(p []byte) (int, error) {
	return f.file.Read(p)
}
func (f *faultyFile) Write(p []byte) (int, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	if f.d.tryFail() {
		return f.file.Write(scrambleData(p))
	}
	return f.file.Write(p)
}
func (f *faultyFile) Close() error { return f.file.Close() }
func (f *faultyFile) Name() string {
	return f.file.Name()
}
func (f *faultyFile) ReadAt(p []byte, off int64) (int, error) {
	return f.file.ReadAt(p, off)
}
func (f *faultyFile) WriteAt(p []byte, off int64) (int, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	if f.d.tryFail() {
		return f.file.WriteAt(scrambleData(p), off)
	}
	return f.file.WriteAt(p, off)
}
func (f *faultyFile) Stat() (os.FileInfo, error) {
	return f.file.Stat()
}
func (f *faultyFile) Sync() error {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	if f.d.tryFail() {
		return errors.New("could not write to disk (faultyDisk)")
	}
	return f.file.Sync()
}

// dependencyCommitFail corrupts the first page of a transaction when it
// is committed
type dependencyCommitFail struct {
	dependencyProduction
}

func (*dependencyCommitFail) disrupt(s string) bool {
	if s == "CommitFail" {
		return true
	}
	return false
}

// dependencyRecoveryFail causes the RecoveryComplete function to fail after
// the metadata was changed to wipe
type dependencyRecoveryFail struct {
	dependencyProduction
}

func (dependencyRecoveryFail) disrupt(s string) bool {
	if s == "RecoveryFail" {
		return true
	}
	if s == "UncleanShutdown" {
		return true
	}
	return false
}

// dependencyReleaseFail corrupts the first page of a transaction when it
// is released
type dependencyReleaseFail struct {
	dependencyProduction
}

func (*dependencyReleaseFail) disrupt(s string) bool {
	if s == "ReleaseFail" {
		return true
	}
	return false
}

// dependencyUncleanShutdown prevnts the wal from marking the logfile as clean
// upon shutdown
type dependencyUncleanShutdown struct {
	dependencyProduction
}

func (dependencyUncleanShutdown) disrupt(s string) bool {
	if s == "UncleanShutdown" {
		return true
	}
	return false
}
