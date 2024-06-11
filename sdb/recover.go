package sdb

import (
	"fmt"
	"path/filepath"
)

// Recover the database from a corrupted state, detected at initialization.
func recoverDatabase(db *DB) error {
	// Note: We have the following:
	// - The DB design is simple, and all operations require at most one file
	// mutation.
	// - The metadata is stored in a single file, and all operations require
	// at most one file mutation.
	// - Currently, the only thing that can get corrupted is the metadata, in
	// particular, the metadata.TotalEntries count.
	//
	// Thus, the recovery process can be limited to counting the number of
	// files in the data folder and updating the metadata.
	dataRoot := filepath.Join(db.path, dataDirectory)

	totalItems, err := countItems(dataRoot)
	if err != nil {
		return fmt.Errorf("count items: %w", err)
	}

	db.metadata.TotalEntries = totalItems
	db.metadata.Checkpoint = db.metadata.Generation

	err = db.metadata.Save(db.path)
	if err != nil {
		return fmt.Errorf("sync metadata: %w", err)
	}
	return nil
}

func countItems(path string) (uint64, error) {
	// Each database record is represented by a regular file.
	return countFiles(path)
}
