package sdb

import (
	"os"
	"path/filepath"
	"testing"
)

// Helpers

func CheckInitialization(t *testing.T, db *DB) {
	t.Helper()
	if db == nil {
		t.Errorf("Expected db to be non-nil")
		return
	}

	CheckFileStructure(t, db)
}

func CheckFileStructure(t *testing.T, db *DB) {
	t.Helper()
	AssertExists(t, db.path)
	AssertExists(t, filepath.Join(db.path, dataDirectory))
	AssertExists(t, filepath.Join(db.path, metadataDirectory))
	AssertExists(t, filepath.Join(db.path, db.metadata.FilePath()))
}

func checkMetadata(
	t *testing.T,
	m *metadata,
	expectedTotalEntries uint64,
	expectedGeneration uint64,
) {
	t.Helper()
	if m.Version != version {
		t.Errorf("Expected version to be %v, but got %v",
			version, m.Version)
	}
	if m.TotalEntries != expectedTotalEntries {
		t.Errorf("Expected total entries to be %v, but got %v",
			expectedTotalEntries, m.TotalEntries)
	}
	if m.Generation != expectedGeneration {
		t.Errorf("Expected generation to be %v, but got %v",
			expectedGeneration, m.Generation)
	}
	if m.Checkpoint != expectedGeneration {
		t.Errorf("Expected checkpoint to be %v, but got %v",
			expectedGeneration, m.Checkpoint)
	}
}

func AssertExists(t *testing.T, path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("Expected %s to exist", path)
	}
}

// Tests

func TestDB_Init(t *testing.T) {
	// New database:
	//  - The Open() function should create it along with the
	//  associated file structure.
	t.Run("New database", func(t *testing.T) {
		db, err := OpenTestDB()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		CheckInitialization(t, db)
	})

	// Database exists:
	//  - The Open() function should load the saved state.
	t.Run("Database exists", func(t *testing.T) {
		// Arrange
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := StartDatabase(t, OpenTestDB, seed)
		CheckInitialization(t, db)
		db.Close() // Call close to ensure we restart with a correct state

		// Reopen and verify
		db, err := ReopenTestDB()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		CheckInitialization(t, db)

		// Verify the metadata
		totalEntries := uint64(len(seed))
		expectedGeneration := totalEntries + 1
		checkMetadata(t, &db.metadata, totalEntries, expectedGeneration)

		// Verify the data
		checkDatabase(t, db, seed)
	})

	// Corrupted database (not closed):
	//  - The db initialization should be able to recover and rebuild
	//  the metadata information.
	t.Run("Corrupted database (not closed)", func(t *testing.T) {
		// Arrange
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := StartDatabase(t, OpenTestDB, seed)
		CheckInitialization(t, db)

		// Reopen without closing
		db, err := ReopenTestDB()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		CheckInitialization(t, db)

		// Verify the metadata
		totalEntries := uint64(len(seed))
		expectedGeneration := uint64(1)
		checkMetadata(t, &db.metadata, totalEntries, expectedGeneration)

		// Verify the data
		checkDatabase(t, db, seed)
	})
}

func TestDB_Init_MetadataError(t *testing.T) {
	// Corrupted metadata file
	//  - If the database metadata file is corrupted, the database should
	//  not be loaded.
	t.Run("Corrupted metadata file", func(t *testing.T) {
		// Arrange
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := StartDatabase(t, OpenTestDB, seed)
		CheckInitialization(t, db)
		db.Close()

		// Corrupt the metadata file
		metadataFile := filepath.Join(db.path, db.metadata.FilePath())
		if err := os.Truncate(metadataFile, 2); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Try to Reopen
		_, err := ReopenTestDB()
		if err == nil {
			t.Errorf("Expected error, but got nil")
		}
	})

	// Bad metadata:
	//  - If the database metadata file contain values that are not valid,
	//  the database should not be loaded.
	t.Run("Bad metadata", func(t *testing.T) {
		t.Run("Wrong version", func(t *testing.T) {
			// Arrange
			seed := map[string]string{
				"key-1": "value-1", "key-2": "value-2",
				"key-3": "value-3", "key-4": "value-4",
			}
			db := StartDatabase(t, OpenTestDB, seed)
			CheckInitialization(t, db)
			db.Close()

			// Corrupt the metadata file
			var m metadata
			if err := m.Load(db.path); err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
			m.Version = 0
			if err := m.Save(db.path); err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}

			// Try to Reopen
			_, err := ReopenTestDB()
			if err == nil {
				t.Errorf("Expected error, but got nil")
			}
		})
	})
}

func TestDB_Init_FileError(t *testing.T) {
	// DB Path is a file
	t.Run("DB Path is a file", func(t *testing.T) {
		// Arrange
		db, err := OpenTestDB()
		if err != nil {
			t.Fatalf("Expected no error, but got %v", err)
		}
		defer db.Close()
		if err = os.RemoveAll(db.path); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		if _, err = os.Create(db.path); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Act / Assert
		_, err = ReopenTestDB()
		if err == nil {
			t.Errorf("Expected error, but got nil")
		}
	})

	// Bad Permissions
	t.Run("Bad Permissions", func(t *testing.T) {
		// Arrange
		db, err := OpenTestDB()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		defer db.Close()
		if err = os.RemoveAll(db.path); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		// Recreate the directory with incorrect permissions
		perm := os.FileMode(0400)
		if err = os.Mkdir(db.path, perm); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Act / Assert
		_, err = ReopenTestDB()
		if err == nil {
			t.Errorf("Expected error, but got nil")
		}
	})
}
