package sdb

import (
	"io/fs"
)

// Mock Filesystem

// mockFS is a lightweight test double that lets unit-tests inject behaviour
// for any subset of fileSystem operations.
type mockFS struct {
	openFunc     func(name string) (fs.File, error)
	openFileFunc func(name string, flag int, perm fs.FileMode) (fs.File, error)
	statFunc     func(name string) (fs.FileInfo, error)
	readFileFunc func(name string) ([]byte, error)
	removeFunc   func(name string) error
	renameFunc   func(oldpath, newpath string) error
	mkdirAllFunc func(path string, perm fs.FileMode) error
}

// Compile-time interface check.
var _ fileSystem = (*mockFS)(nil)

func (fs *mockFS) Open(name string) (fs.File, error) {
	if fs.openFunc != nil {
		return fs.openFunc(name)
	}
	return (&osFS{}).Open(name)
}

func (fs *mockFS) OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error) {
	if fs.openFileFunc != nil {
		return fs.openFileFunc(name, flag, perm)
	}
	return (&osFS{}).OpenFile(name, flag, perm)
}

func (fs *mockFS) Stat(name string) (fs.FileInfo, error) {
	if fs.statFunc != nil {
		return fs.statFunc(name)
	}
	return (&osFS{}).Stat(name)
}

func (fs *mockFS) ReadFile(name string) ([]byte, error) {
	if fs.readFileFunc != nil {
		return fs.readFileFunc(name)
	}
	return (&osFS{}).ReadFile(name)
}

func (fs *mockFS) Remove(name string) error {
	if fs.removeFunc != nil {
		return fs.removeFunc(name)
	}
	return (&osFS{}).Remove(name)
}

func (fs *mockFS) Rename(oldpath, newpath string) error {
	if fs.renameFunc != nil {
		return fs.renameFunc(oldpath, newpath)
	}
	return (&osFS{}).Rename(oldpath, newpath)
}

func (fs *mockFS) MkdirAll(path string, perm fs.FileMode) error {
	if fs.mkdirAllFunc != nil {
		return fs.mkdirAllFunc(path, perm)
	}
	return (&osFS{}).MkdirAll(path, perm)
}

// Mock File

// mockFile is an in-memory test double that pretends to be an *os.File.
type mockFile struct {
	statFunc  func() (fs.FileInfo, error)
	readFunc  func(bytes []byte) (int, error)
	closeFunc func() error

	readdirnamesFunc func(n int) ([]string, error)
	writeFunc        func(p []byte) (n int, err error)
	syncFunc         func() error
}

// Compile-time interface check.
var _ fs.File = (*mockFile)(nil)

func (f *mockFile) Stat() (fs.FileInfo, error) {
	if f.statFunc != nil {
		return f.statFunc()
	}
	return nil, nil
}

func (f *mockFile) Read(bytes []byte) (int, error) {
	if f.readFunc != nil {
		return f.readFunc(bytes)
	}
	return 0, nil
}

func (f *mockFile) Close() error {
	if f.closeFunc != nil {
		return f.closeFunc()
	}
	return nil
}

func (f *mockFile) Readdirnames(n int) ([]string, error) {
	if f.readdirnamesFunc != nil {
		return f.readdirnamesFunc(n)
	}
	return nil, nil
}

func (f *mockFile) Write(p []byte) (n int, err error) {
	if f.writeFunc != nil {
		return f.writeFunc(p)
	}
	return 0, nil
}

func (f *mockFile) Sync() error {
	if f.syncFunc != nil {
		return f.syncFunc()
	}
	return nil
}
