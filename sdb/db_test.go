package sdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Helpers

var (
	TestDirectory = filepath.Join(os.TempDir(), "sdb-test")
	TestError     = errors.New("test error")
)

type TDB = *DB

// Provides an Open function that creates a clean test database.
func OpenTestDB() (*DB, error) {
	open := NewOpenFunc(true)
	return open()
}

// Same as OpenTestDB, but without the cleaning the database directory.
func ReopenTestDB() (*DB, error) {
	open := NewOpenFunc(false)
	return open()
}

// NewOpenFunc is a factory for Open functions. If clean is true, then
// the database directory is cleaned before creating the database.
func NewOpenFunc(clean bool, opts ...Option) OpenFunc {
	return func() (TDB, error) {
		path := TestDirectory
		if clean {
			err := os.RemoveAll(path)
			if err != nil && !os.IsNotExist(err) {
				return nil, fmt.Errorf("remove path: %w", err)
			}
		}
		return Open(path, opts...)
	}
}

// Tests

func TestDB(t *testing.T) {
	tests := NewDBTests(NewOpenFunc(true), NewOpenFunc(false))
	tests.CheckInitialization = CheckInitialization
	tests.TestAll(t)
}

func TestDB_NoCache(t *testing.T) {
	tests := NewDBTests(
		NewOpenFunc(true, WithCacheSize(0)),
		NewOpenFunc(false, WithCacheSize(0)),
	)
	tests.CheckInitialization = CheckInitialization
	tests.TestAll(t)
}

func TestOpen_WithOptions(t *testing.T) {
	path := TestDirectory
	err := os.RemoveAll(path)
	if err != nil && !os.IsNotExist(err) {
		t.Errorf("remove path: %s", err)
	}
	cacheSize := int64(1024 * 1024)

	db, err := Open(path,
		WithCacheSize(cacheSize),
		WithSynchronousWrites(true),
	)
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}
	if !db.syncWrites {
		t.Errorf("Expected true, but got %v", db.syncWrites)
	}
}

// Tests for miscellaneous boundary cases.
func TestDB_Error(t *testing.T) {
	t.Run("Put - key exceeds maximum length", func(t *testing.T) {
		db, err := OpenTestDB()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		defer db.Close()
		key := bytes.Repeat([]byte{0xFF}, 2*MaxKeyLength)

		err = db.Put(key, []byte("value"))
		if !errors.Is(err, ErrKeyTooLarge) {
			t.Errorf("Expected ErrKeyTooLarge, but got %v", err)
		}
	})
}

// Tests for boundary cases where the file that represent a database
// record got in a inconsistent state.
func TestDB_FileError(t *testing.T) {
	t.Run("Has - File cannot be read", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		open := NewOpenFunc(true, WithCacheSize(0))
		db := StartDatabase(t, open, seed)
		defer db.Close()

		// Make the files unreadable
		path := filepath.Join(db.path, dataDirectory)
		defer func() {
			err := os.Chmod(path, defaultDirPermissions)
			if err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
		}()
		if err := os.Chmod(path, 0o400); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		_, err := db.Has([]byte("key-1"))
		if !errors.Is(err, os.ErrPermission) {
			t.Errorf("Expected os.ErrPermission, but got %v", err)
		}
	})

	t.Run("Get - File cannot be read", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		open := NewOpenFunc(true, WithCacheSize(0))
		db := StartDatabase(t, open, seed)
		defer db.Close()

		// Make the files unreadable
		path := filepath.Join(db.path, dataDirectory)
		defer func() {
			err := os.Chmod(path, defaultDirPermissions)
			if err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
		}()
		if err := os.Chmod(path, 0o400); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		_, err := db.Get([]byte("key-1"))
		if !errors.Is(err, os.ErrPermission) {
			t.Errorf("Expected os.ErrPermission, but got %v", err)
		}
	})

	t.Run("Put - File cannot be written", func(t *testing.T) {
		open := NewOpenFunc(true, WithCacheSize(0))
		db := StartDatabase(t, open, nil)
		defer db.Close()

		// Make the files not writable
		path := filepath.Join(db.path, dataDirectory)
		defer func() {
			err := os.Chmod(path, defaultDirPermissions)
			if err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
		}()
		if err := os.Chmod(path, 0o200); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		err := db.Put([]byte("key-1"), []byte("value-1"))
		if !errors.Is(err, os.ErrPermission) {
			t.Errorf("Expected os.ErrPermission, but got %v", err)
		}
	})

	t.Run("Delete - File cannot be read", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		open := NewOpenFunc(true, WithCacheSize(0))
		db := StartDatabase(t, open, seed)
		defer db.Close()

		// Make the files unreadable
		path := filepath.Join(db.path, dataDirectory)
		defer func() {
			err := os.Chmod(path, defaultDirPermissions)
			if err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
		}()
		if err := os.Chmod(path, 0o400); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		err := db.Delete([]byte("key-1"))
		if !errors.Is(err, os.ErrPermission) {
			t.Errorf("Expected os.ErrPermission, but got %v", err)
		}
	})
}

// Tests for boundary cases where the file that represent a database
// record got in a inconsistent state. Specific for the DB.Items method.
func TestDB_FileError_Items(t *testing.T) {
	t.Run("Items - File cannot be read", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		open := NewOpenFunc(true, WithCacheSize(0))
		db := StartDatabase(t, open, seed)
		defer db.Close()

		// Make the files unreadable
		path := filepath.Join(db.path, dataDirectory)
		defer func() {
			err := os.Chmod(path, defaultDirPermissions)
			if err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
		}()
		if err := os.Chmod(path, 0o400); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		err := db.Items(nil, 1, func(k, v []byte) (bool, error) {
			return false, errors.New("should not be called")
		})
		if !errors.Is(err, os.ErrPermission) {
			t.Errorf("Expected os.ErrPermission, but got %v", err)
		}
	})

	t.Run("Items - Filename not base32hex", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		open := NewOpenFunc(true, WithCacheSize(0))
		db := StartDatabase(t, open, seed)
		defer db.Close()

		// Make the files unreadable
		path := keyPath(db, []byte("key-1"))
		wrongPath := filepath.Join(db.path, dataDirectory, "0000")
		err := os.Rename(path, wrongPath)
		if err != nil {
			t.Fatalf("Expected no error, but got %v", err)
		}

		err = db.Items(nil, 1, func(k, v []byte) (bool, error) {
			return true, nil
		})
		if err == nil {
			t.Errorf("Expected error, but got none")
		}
	})
}
