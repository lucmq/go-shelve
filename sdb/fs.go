package sdb

import (
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"
)

var (
	defaultDiskSectorSize = 4096
	defaultPermissions    = os.FileMode(0600)
	defaultDirPermissions = os.FileMode(0700)
)

// fileSystem abstracts the subset of os/fs operations that SDB needs.
//
// Implementations are expected to wrap a concrete filesystem (e.g. the real
// os filesystem, an in-memory mock, or an overlay that injects faults for
// testing).  All methods MUST be safe for concurrent use by multiple
// goroutines.
//
// Notes:
//
//   - Rename MUST be atomic: it should guarantee to either replace the target
//     file entirely, or not change either the destination or the source.
type fileSystem interface {
	fs.FS

	OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error)
	Remove(name string) error
	Rename(oldpath, newpath string) error
	MkdirAll(path string, perm fs.FileMode) error
}

// OS Filesystem

// osFS is the implementation of fileSystem that delegates every call to the
// standard library’s os package. The zero value is ready to use.
type osFS struct{}

// Compile-time interface check.
var _ fileSystem = (*osFS)(nil)

func (*osFS) Open(name string) (fs.File, error) {
	return os.Open(name)
}

func (*osFS) OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error) {
	return os.OpenFile(name, flag, perm)
}

func (*osFS) Stat(name string) (fs.FileInfo, error) {
	return os.Stat(name)
}

func (*osFS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}

func (*osFS) Remove(name string) error {
	return os.Remove(name)
}

func (*osFS) Rename(oldpath, newpath string) error {
	return renameFile(oldpath, newpath)
}

func (*osFS) MkdirAll(path string, perm fs.FileMode) error {
	return os.MkdirAll(path, perm)
}

// Atomic Writer

// The main object of atomicWrite is to protect against incomplete writes.
// When used together with O_SYNC, atomicWrite also provides some additional
// durability guarantees.
type atomicWriter struct {
	fs             fileSystem
	syncWrites     bool
	diskSectorSize int
	perm           os.FileMode
}

func newAtomicWriter(fsys fileSystem, syncWrites bool) *atomicWriter {
	// Note: If we decide to ask the host system for the disk sector size,
	// we can use the go `init` function for that and keep this constructor
	// cleaner, without the need to return an error and also, without the
	// need to query the os multiple times.
	diskSectorSize := defaultDiskSectorSize
	return &atomicWriter{
		fs:             fsys,
		syncWrites:     syncWrites,
		diskSectorSize: diskSectorSize,
		perm:           defaultPermissions,
	}
}

func (w *atomicWriter) flag(excl bool) int {
	flag := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if w.syncWrites {
		flag |= os.O_SYNC
	}
	if excl {
		flag |= os.O_EXCL
	}
	return flag
}

func (w *atomicWriter) WriteFile(path string, data []byte, excl bool) (err error) {
	defer func() {
		// Sync the parent directory for more durability guarantees. See:
		// - https://lwn.net/Articles/457667/#:~:text=When%20should%20you%20Fsync
		if err == nil && w.syncWrites {
			err = syncFile(w.fs, filepath.Dir(path))
		}
	}()

	if runtime.GOOS == "linux" && len(data) <= w.diskSectorSize {
		// Optimization: Write directly if the data fits in a single sector,
		// since a single-sector write can be assumed to be atomic. See:
		//
		// - https://stackoverflow.com/questions/2009063/are-disk-sector-writes-atomic
		// - https://web.cs.ucla.edu/classes/spring07/cs111-2/scribe/lecture14.html
		//
		// This optimization assumes that the host supports atomic writes to a
		// disk sector.
		return w._writeFile(path, data, excl)
	}

	tmpPath := makeTempPath(path)

	// w.writeFile will sync, if configured to do so.
	err = w._writeFile(tmpPath, data, excl)
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}

	return renameFile(tmpPath, path)
}

// writeFile writes data to the named file, creating it if necessary.
// If the file does not exist, WriteFile creates it with permissions perm (before umask);
// otherwise writeFile truncates it before writing, without changing permissions.
// Since writeFile requires multiple system calls to complete, a failure mid-operation
// can leave the file in a partially written state.
func (w *atomicWriter) _writeFile(name string, data []byte, excl bool) error {
	// Adapted from `os.WriteFile()`
	f, err := w.fs.OpenFile(name, w.flag(excl), w.perm)
	if err != nil {
		return err
	}
	_, err = f.(io.Writer).Write(data)
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}

// Utilities

func mkdirs(fs fileSystem, paths []string, perm os.FileMode) error {
	for _, path := range paths {
		if err := fs.MkdirAll(path, perm); err != nil {
			return fmt.Errorf("MkdirAll: %w", err)
		}
	}
	return nil
}

func streamDir(fs fileSystem, dir, start string, order int, fn func(filename string) (bool, error)) (bool, error) {
	asc := order > Desc
	needFilter := start != ""

	filenames, err := readDir(fs, dir, order)
	if err != nil {
		return false, fmt.Errorf("readDir: %w", err)
	}

	for _, name := range filenames {
		if needFilter {
			if asc && name < start {
				continue // still before the start
			}
			if !asc && name > start {
				continue // still before the start (descending case)
			}
			needFilter = false // boundary crossed -- stop filtering
		}

		keep, err := fn(name)
		if err != nil {
			return false, fmt.Errorf("fn: %w", err)
		}
		if !keep {
			return false, nil
		}
	}

	return true, nil
}

func readDir(fsys fileSystem, dir string, order int) ([]string, error) {
	names, err := readdirnames(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("readdirnames: %w", err)
	}

	sort.Slice(names, func(i, j int) bool {
		if order > Desc {
			return names[i] < names[j]
		}
		return names[i] > names[j]
	})

	return names, nil
}

func readdirnames(fsys fs.FS, name string) ([]string, error) {
	f, err := fsys.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	type dirReader interface{ Readdirnames(n int) ([]string, error) }
	dir, _ := f.(dirReader)

	return dir.Readdirnames(-1)
}

// countRegularFiles walks the directory tree rooted at path and returns the
// number of regular (non-directory) files it finds.
func countRegularFiles(fsys fileSystem, path string) (uint64, error) {
	var count uint64
	err := fs.WalkDir(fsys, path, func(_ string, d fs.DirEntry, err error) error {
		if d != nil && d.Type().IsRegular() {
			count++
		}
		// propagate I/O or permission errors
		return err
	})
	return count, err
}

// Helpers

func makeTempPath(path string) string {
	tmpBase := fmt.Sprintf(
		"%s-%d-%d",
		filepath.Base(path),
		rand.Uint32(),
		time.Now().UnixNano(),
	)
	tmpPath := filepath.Join(os.TempDir(), tmpBase)
	return tmpPath
}

func syncFile(fsys fileSystem, path string) error {
	f, err := fsys.Open(path)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}

	type syncer interface{ Sync() error }
	ff := f.(syncer)

	err = ff.Sync()
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}
