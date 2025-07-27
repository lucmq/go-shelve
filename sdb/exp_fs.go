package sdb

import (
	"io/fs"
	"os"
)

// TODO: Leverage fs.WalkDir when needed (no need to put Walk into the interface).

// TODO: Consider dropping Open() and using only OpenFile()

// TODO: Consider dropping ReadDir() and using Open() + fs.ReadDir()
//  Or, replacing it with `ReadDirNames(name string) ([]string, error)`

type fileSystem interface {
	Open(name string) (fs.File, error)
	OpenFile(name string, flag int, perm os.FileMode) (fs.File, error)

	Stat(name string) (os.FileInfo, error)

	ReadFile(name string) ([]byte, error)
	ReadDir(name string) ([]fs.DirEntry, error)

	Remove(name string) error
	Rename(oldpath, newpath string) error

	MkdirAll(path string, perm os.FileMode) error
}

type osFS struct {
}

func (fs *osFS) Open(name string) (fs.File, error) {
	return os.Open(name)
}

func (fs *osFS) OpenFile(name string, flag int, perm os.FileMode) (fs.File, error) {
	return os.OpenFile(name, flag, perm)
}

func (fs *osFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (fs *osFS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}

func (fs *osFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return os.ReadDir(name)
}

func (fs *osFS) Remove(name string) error {
	return os.Remove(name)
}

func (fs *osFS) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (fs *osFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

// // Open opens the named file for reading. If successful, methods on
// // the returned file can be used for reading; the associated file
// // descriptor has mode O_RDONLY.
// // If there is an error, it will be of type *PathError.
// func Open(name string) (*File, error) {
//	return OpenFile(name, O_RDONLY, 0)
// }
