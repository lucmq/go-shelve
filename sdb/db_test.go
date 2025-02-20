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

// Tests boundary cases where a file representing a database record
// becomes inconsistent or corrupted.
func TestDB_FileError(t *testing.T) {
	// Note: This function serves as a placeholder for future tests. It may be used
	// if we decide to implement CRC checks in the data file to ensure consistency.
}

// Tests for boundary cases where the file that represent a database
// record got in an inconsistent state. Specific for the DB.Items method.
func TestDB_FileError_Items(t *testing.T) {
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

func TestOperationsOnClosedDB(t *testing.T) {
	// Open a new test database.
	db, err := OpenTestDB()
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}

	// Close the DB to mark it as unusable.
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close DB: %v", err)
	}

	// Test Put: should return ErrDatabaseClosed.
	err = db.Put([]byte("testKey"), []byte("testValue"))
	if !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("Put after Close: expected ErrDatabaseClosed, got: %v", err)
	}

	// Test Delete.
	err = db.Delete([]byte("testKey"))
	if !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("Delete after Close: expected ErrDatabaseClosed, got: %v", err)
	}

	// Test Sync.
	err = db.Sync()
	if !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("Sync after Close: expected ErrDatabaseClosed, got: %v", err)
	}

	// Test Items.
	err = db.Items(nil, 1, func(k, v []byte) (bool, error) {
		return true, nil
	})
	if !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("Items after Close: expected ErrDatabaseClosed, got: %v", err)
	}

	// Test Has.
	_, err = db.Has([]byte("testKey"))
	if !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("Has after Close: expected ErrDatabaseClosed, got: %v", err)
	}

	// Test Get.
	value, err := db.Get([]byte("testKey"))
	if !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("Get after Close: expected ErrDatabaseClosed, got: %v", err)
	}
	if value != nil {
		t.Errorf("Get after Close: expected nil value, got: %v", value)
	}

	// Test Len.
	if n := db.Len(); n != -1 {
		t.Errorf("Len after Close: expected -1, got: %v", n)
	}

	// Test Close.
	err = db.Close()
	if err != nil {
		t.Errorf("Close after Close: expected no error, got: %v", err)
	}
}
