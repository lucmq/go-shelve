package sdb

import (
	"io/fs"
	"os"
)

// TODO: merge `exp_fs.go` and `fs.go` files.

// TODO: Consider dropping Open() and using only OpenFile() (create a fsOpen(fsys fileSystem) helper)

// TODO: Consider dropping ReadDir() and using Open() + fs.ReadDir()
//  Or, replacing it with `ReadDirNames(name string) ([]string, error)`

// TODO: Use the following interfaces? (Note that we might be able to replace
//  fileReader with just fs.FS)
//
//  type fileReader interface {
//  	Stat(name string) (fs.FileInfo, error)
//
//  	ReadFile(name string) ([]byte, error)
//  	ReadDirNames(name string) ([]string, error) --> No, use Open + Readdirnames
//  }
//
//  type fileWriter interface {
//  	WriteFileAtomic(path string, data []byte, excl bool)
//
//  	Remove(name string) error
//  	Rename(oldpath, newpath string) error
//
//  	MkdirAll(path string, perm fs.FileMode) error
//  }

type fileSystem interface {
	Open(name string) (fs.File, error)
	OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error)

	Stat(name string) (fs.FileInfo, error)

	ReadFile(name string) ([]byte, error)
	ReadDir(name string) ([]fs.DirEntry, error)

	Remove(name string) error

	// Rename should atomically replace the destination file or directory with the
	// source. It should guarantee to either replace the target file entirely, or not
	// change either file.
	Rename(oldpath, newpath string) error

	MkdirAll(path string, perm fs.FileMode) error
}

// OS Filesystem

type osFS struct {
}

var _ fileSystem = (*osFS)(nil)

func (fs *osFS) Open(name string) (fs.File, error) {
	return os.Open(name)
}

func (fs *osFS) OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error) {
	return os.OpenFile(name, flag, perm)
}

func (fs *osFS) Stat(name string) (fs.FileInfo, error) {
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
	return renameFile(oldpath, newpath)
}

func (fs *osFS) MkdirAll(path string, perm fs.FileMode) error {
	return os.MkdirAll(path, perm)
}

// FS Utilities

// // Open opens the named file for reading. If successful, methods on
// // the returned file can be used for reading; the associated file
// // descriptor has mode O_RDONLY.
// // If there is an error, it will be of type *PathError.
// func Open(name string) (*File, error) {
//	return OpenFile(name, O_RDONLY, 0)
// }
