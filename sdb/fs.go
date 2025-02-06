package sdb

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

var (
	defaultDiskSectorSize = 4096
	defaultPermissions    = os.FileMode(0600)
	defaultDirPermissions = os.FileMode(0700)
)

// The main object of atomicWrite is to protect against incomplete writes.
// When used together with O_SYNC, atomicWrite also provides some additional
// durability guarantees.
type atomicWriter struct {
	syncWrites     bool
	diskSectorSize int
	perm           os.FileMode
}

func newAtomicWriter(syncWrites bool) *atomicWriter {
	// Note: If we decide to ask the host system for the disk sector size,
	// we can use the go `init` function for that and keep this constructor
	// cleaner, without the need to return an error and also, without the
	// need to query the os multiple times.
	diskSectorSize := defaultDiskSectorSize
	return &atomicWriter{
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

func (w *atomicWriter) WriteFile(path string, data []byte, excl bool) error {
	var err error
	defer func() {
		// Sync the parent directory for more durability guarantees. See:
		// - https://lwn.net/Articles/457667/#:~:text=When%20should%20you%20Fsync
		if err == nil && w.syncWrites {
			_ = syncFile(filepath.Dir(path))
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
	f, err := os.OpenFile(name, w.flag(excl), w.perm)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}

func mkdirs(paths []string, perm os.FileMode) error {
	for _, path := range paths {
		if err := os.MkdirAll(path, perm); err != nil {
			return fmt.Errorf("MkdirAll: %w", err)
		}
	}
	return nil
}

func countFiles(path string) (uint64, error) {
	var count uint64
	err := readDir(path, func(name string) (bool, error) {
		count++
		return true, nil
	})
	return count, err
}

func readDir(path string, fn func(name string) (bool, error)) (err error) {
	var f *os.File
	f, err = os.Open(path)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() {
		// Safely close the file and assign to the return value
		if err1 := f.Close(); err1 != nil && err == nil {
			err = err1
		}
	}()
	for {
		var names []string
		batchSize := 256
		names, err = f.Readdirnames(batchSize)
		if err != nil {
			// EOF or unreadable dir
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return err
		}
		for _, name := range names {
			ok, err := fn(name)
			if err != nil {
				return fmt.Errorf("fn: %w", err)
			}
			if !ok {
				// Stop Iteration
				return nil
			}
		}
	}
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

func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	err = f.Sync()
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}
