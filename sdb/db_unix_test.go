//go:build !windows
// +build !windows

package sdb

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Tests for boundary cases where the file that represent a database
// record got in an inconsistent state (specific for Unix-like systems).
func TestDB_FileError_Unix(t *testing.T) {
	t.Run("Has - File cannot be read", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		open := NewOpenFunc(true, WithCacheSize(0))
		db := StartDatabase(t, open, seed)
		defer db.Close()

		// Make the files unreadable.
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
// record got in an inconsistent state. Specific for the DB.Items method
// and Unix-like systems.
func TestDB_FileError_Items_Unix(t *testing.T) {
	t.Run("Items - File cannot be read", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		open := NewOpenFunc(true, WithCacheSize(0))
		db := StartDatabase(t, open, seed)
		defer db.Close()

		// Make the files unreadable
		path := filepath.Join(db.path, dataDirectory, sentinelDir)
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
}
