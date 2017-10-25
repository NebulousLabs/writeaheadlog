package wal

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
	}

	// file implements all of the methods that can be called on an os.File.
	file interface {
		io.ReadWriteCloser
		Name() string
		ReadAt([]byte, int64) (int, error)
		Sync() error
		WriteAt([]byte, int64) (int, error)
		Seek(int64, int) (int64, error)
	}
)

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

type prodFile struct {
	*os.File
}

func (f *prodFile) Seek(offset int64, whence int) (int64, error) {
	return f.Seek(offset, whence)
}
