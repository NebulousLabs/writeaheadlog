package writeaheadlog

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"github.com/NebulousLabs/fastrand"
)

// These interfaces define the wal's dependencies. Using the smallest
// interface possible makes it easier to mock these dependencies in testing.
type (
	dependencies interface {
		disrupt(string) bool
		readFile(string) ([]byte, error)
		openFile(string, int, os.FileMode) (file, error)
		create(string) (file, error)
		remove(string) error
	}

	// file implements all of the methods that can be called on an os.File.
	file interface {
		io.ReadWriteCloser
		Name() string
		ReadAt([]byte, int64) (int, error)
		Sync() error
		WriteAt([]byte, int64) (int, error)
		Stat() (os.FileInfo, error)
	}
)

// dependencyCommitFail corrupts the first page of a transaction when it
// is committed
type dependencyCommitFail struct {
	prodDependencies
}

func (dependencyCommitFail) disrupt(s string) bool {
	if s == "CommitFail" {
		return true
	}
	return false
}

// dependencyReleaseFail corrupts the first page of a transaction when it
// is released
type dependencyReleaseFail struct {
	prodDependencies
}

func (dependencyReleaseFail) disrupt(s string) bool {
	if s == "ReleaseFail" {
		return true
	}
	return false
}

// prodDependencies is a passthrough to the standard library calls
type prodDependencies struct{}

func (prodDependencies) disrupt(string) bool { return false }
func (prodDependencies) readFile(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}
func (prodDependencies) openFile(path string, flag int, perm os.FileMode) (file, error) {
	return os.OpenFile(path, flag, perm)
}
func (prodDependencies) create(path string) (file, error) {
	return os.Create(path)
}
func (prodDependencies) remove(path string) error {
	return os.Remove(path)
}

// faultyDiskDependency implements dependencies that simulate a faulty disk.
type faultyDiskDependency struct {
	// failDenominator determines how likely it is that a write will fail,
	// defined as 1/failDenominator. Each write call increments
	// failDenominator, and it starts at 2. This means that the more calls to
	// WriteAt, the less likely the write is to fail. All calls will start
	// automatically failing after writeLimit writes.
	failDenominator *int
	writeLimit      *int
	failed          *bool
	disabled        *bool
	mu              *sync.Mutex
}

// newFaultyDiskDependency creates a dependency that can be used to simulate a
// failing disk. writeLimit is the maximum number of writes the disk will
// endure before failing
func newFaultyDiskDependency(writeLimit int) faultyDiskDependency {
	var denominator = 3
	var failed = false
	var disabled = false
	return faultyDiskDependency{
		failDenominator: &denominator,
		failed:          &failed,
		writeLimit:      &writeLimit,
		disabled:        &disabled,
		mu:              new(sync.Mutex),
	}
}

func (faultyDiskDependency) disrupt(s string) bool {
	return s == "FaultyDisk"
}
func (faultyDiskDependency) readFile(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}
func (d faultyDiskDependency) openFile(path string, flag int, perm os.FileMode) (file, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return d.newFaultyFile(f), nil
}
func (d faultyDiskDependency) create(path string) (file, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return d.newFaultyFile(f), nil
}
func (d faultyDiskDependency) remove(path string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !*d.disabled {
		fail := fastrand.Intn(*d.failDenominator) == 0
		*d.failDenominator++
		if fail || *d.failDenominator >= *d.writeLimit {
			*d.failed = true
			return nil
		}
	}
	return os.Remove(path)
}

// faultyFile implements a file that simulates a faulty disk.
type faultyFile struct {
	d    *faultyDiskDependency
	file *os.File
}

func (f *faultyFile) Read(p []byte) (int, error) {
	return f.file.Read(p)
}
func (f *faultyFile) Write(p []byte) (int, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()

	if !*f.d.disabled {
		fail := fastrand.Intn(*f.d.failDenominator) == 0
		*f.d.failDenominator++
		if fail || *f.d.failDenominator >= *f.d.writeLimit {
			*f.d.failed = true
			return len(p), nil
		}
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

	if !*f.d.disabled {
		fail := fastrand.Intn(*f.d.failDenominator) == 0
		*f.d.failDenominator++
		if fail || *f.d.failDenominator >= *f.d.writeLimit {
			*f.d.failed = true
			return len(p), nil
		}
	}
	return f.file.WriteAt(p, off)
}
func (f *faultyFile) Stat() (os.FileInfo, error) {
	return f.file.Stat()
}
func (f *faultyFile) Sync() error {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()

	if !*f.d.disabled && *f.d.failed {
		return errors.New("could not write to disk (faultyDisk)")
	}
	return f.file.Sync()
}

// newFaultyFile creates a new faulty file around the provided file handle.
func (d *faultyDiskDependency) newFaultyFile(f *os.File) *faultyFile {
	return &faultyFile{d: d, file: f}
}

// reset resets the failDenominator and the failed flag of the dependency
func (d *faultyDiskDependency) reset() {
	d.mu.Lock()
	*d.failDenominator = 3
	*d.failed = false
	d.mu.Unlock()
}

// disabled allows the caller to temporarily disable the dependency
func (d *faultyDiskDependency) disable(b bool) {
	d.mu.Lock()
	*d.disabled = b
	d.mu.Unlock()
}
