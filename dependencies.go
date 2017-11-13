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

// faultyFile implements a file that simulates a faulty disk.
type faultyFile struct {
	file   *os.File
	failed bool

	// failDenominator determines how likely it is that a write will fail, defined
	// as 1/failDenominator. Each write call increments failDenominator, and it
	// starts at 2. This means that the more calls to WriteAt, the less likely
	// the write is to fail. All calls will start automatically failing after
	// 5000 writes.
	failDenominator int

	mu sync.Mutex
}

func (f *faultyFile) Read(p []byte) (int, error) {
	return f.file.Read(p)
}
func (f *faultyFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	fail := fastrand.Intn(f.failDenominator) == 0
	f.failDenominator++
	if fail || f.failDenominator >= 5000 {
		f.failed = true
		return len(p), nil
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
	f.mu.Lock()
	defer f.mu.Unlock()

	fail := fastrand.Intn(f.failDenominator) == 0
	f.failDenominator++
	if fail || f.failDenominator >= 5000 {
		f.failed = true
		return len(p), nil
	}
	return f.file.WriteAt(p, off)
}
func (f *faultyFile) Stat() (os.FileInfo, error) {
	return f.file.Stat()
}
func (f *faultyFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failed {
		return errors.New("could not write to disk (faultyDisk)")
	}
	return f.file.Sync()
}

// newFaultyFile creates a new faulty file around the provided file handle.
func newFaultyFile(f *os.File) *faultyFile {
	return &faultyFile{file: f, failed: false, failDenominator: 3}
}

// faultyDiskDependency implements dependencies that simulate a faulty disk.
type faultyDiskDependency struct{}

func (faultyDiskDependency) disrupt(s string) bool {
	return s == "FaultyDisk"
}
func (faultyDiskDependency) readFile(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}
func (faultyDiskDependency) openFile(path string, flag int, perm os.FileMode) (file, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return newFaultyFile(f), nil
}
func (faultyDiskDependency) create(path string) (file, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return newFaultyFile(f), nil
}
