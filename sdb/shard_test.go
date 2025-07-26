package sdb

import (
	"path/filepath"
	"runtime"
	"testing"
)

// TestLoadShards_EdgeCases should fail when directories are unreadable.
func TestLoadShards_EdgeCases(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod permission tests are UNIX-only")
	}

	t.Run("DB directory unreadable", func(t *testing.T) {
		db, err := OpenTestDB()
		if err != nil {
			t.Fatalf("OpenTestDB: %v", err)
		}
		defer db.Close()

		RequirePermErr(t, db.path, db.loadShards)
	})

	t.Run("Data directory unreadable", func(t *testing.T) {
		db, err := OpenTestDB()
		if err != nil {
			t.Fatalf("OpenTestDB: %v", err)
		}
		defer db.Close()

		dataDir := filepath.Join(db.path, "data")
		RequirePermErr(t, dataDir, db.loadShards)
	})
}

// TestSplitShard_EdgeCases should fail if it cannot read/write the data directory.
func TestSplitShard_EdgeCases(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod permission tests are UNIX-only")
	}

	seed := map[string]string{
		"k1": "v1", "k2": "v2",
		"k3": "v3", "k4": "v4",
	}
	db := StartDatabase(t, OpenTestDB, seed)
	defer db.Close()

	dataDir := filepath.Join(db.path, "data")
	RequirePermErr(t, dataDir, func() error { return db.splitShard(0) })
}
