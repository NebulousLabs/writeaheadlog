package writeaheadlog

import (
	"io"
	"io/ioutil"
	"os"
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

// dependencyProduction is a passthrough to the standard library calls
type dependencyProduction struct{}

func (*dependencyProduction) disrupt(string) bool { return false }
func (*dependencyProduction) readFile(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}
func (*dependencyProduction) openFile(path string, flag int, perm os.FileMode) (file, error) {
	return os.OpenFile(path, flag, perm)
}
func (*dependencyProduction) create(path string) (file, error) {
	return os.Create(path)
}
func (*dependencyProduction) remove(path string) error {
	return os.Remove(path)
}
