package writeaheadlog

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"golang.org/x/sys/unix"
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
		io.Closer
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

type dependencyMmap struct{}

func (*dependencyMmap) disrupt(string) bool { return false }
func (*dependencyMmap) readFile(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}
func (*dependencyMmap) openFile(path string, flag int, perm os.FileMode) (file, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	sz := int(stat.Size())
	if sz == 0 {
		sz = 10e6
		if err := f.Truncate(10e6); err != nil {
			return nil, err
		}
	}
	b, err := unix.Mmap(int(f.Fd()), 0, sz, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &mmapFile{
		f:    f,
		data: b,
	}, nil
}
func (d *dependencyMmap) create(path string) (file, error) {
	fmt.Println("create")
	f, err := d.openFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}
	return f, nil
}
func (*dependencyMmap) remove(path string) error {
	return os.Remove(path)
}

type mmapFile struct {
	f    *os.File
	data []byte
}

func (f *mmapFile) Close() error {
	unix.Munmap(f.data)
	f.data = nil
	return f.f.Close()
}

func (f *mmapFile) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off > int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	return n, nil
}

func (f *mmapFile) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 || off > int64(len(f.data)) {
		return 0, io.EOF
	}
	if int(off)+len(p) >= len(f.data) {
		//	fmt.Println("write")
		if _, err := f.f.WriteAt(p, off); err != nil {
			return 0, err
		}
		unix.Munmap(f.data)
		b, err := unix.Mmap(int(f.f.Fd()), 0, int(off)+len(p), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		if err != nil {
			return 0, err
		}
		f.data = b
	} else {
		copy(f.data[off:], p)
	}
	return len(p), nil
}

func (f *mmapFile) Sync() error {
	return unix.Msync(f.data, unix.MS_SYNC)
}

func (f *mmapFile) Stat() (os.FileInfo, error) {
	return f.f.Stat()
}
