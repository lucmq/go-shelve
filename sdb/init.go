package sdb

import (
	"fmt"
	"os"
	"path/filepath"
)

func initializeDatabase(db *DB) error {
	// Check if the database already exists
	fi, err := os.Stat(db.path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("stat path: %w", err)
	}

	if os.IsNotExist(err) {
		return createDatabaseStorage(db)
	} else {
		// Check permissions
		if !fi.IsDir() {
			return fmt.Errorf("path is not a directory")
		} else if fi.Mode().Perm()&0700 != 0700 {
			return fmt.Errorf("path permissions are not 0700")
		}
	}

	// Load the metadata
	db.metadata = metadata{}
	err = db.metadata.Load(db.path)
	if err != nil {
		return fmt.Errorf("load metadata: %w", err)
	}

	// Load the shards
	if err = db.loadShards(); err != nil {
		return fmt.Errorf("load shards: %w", err)
	}

	// Check the DB consistency and possibly recover from a corrupted
	// state
	return sanityCheck(db)
}

func createDatabaseStorage(db *DB) error {
	paths := []string{
		db.path,
		filepath.Join(db.path, dataDirectory),
		filepath.Join(db.path, dataDirectory, sentinelDir),
		filepath.Join(db.path, metadataDirectory),
	}

	if err := mkdirs(paths, defaultDirPermissions); err != nil {
		return fmt.Errorf("create directories: %w", err)
	}

	// Sync the database
	err := db.Sync()
	if err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	return nil
}

// Check version, totalBuckets and the generations. Recover from a corrupted
// database if the generation checkpoint doesn't match the current generation.
func sanityCheck(db *DB) error {
	if err := db.metadata.Validate(); err != nil {
		return err
	}
	// Check generations
	if db.metadata.Generation != db.metadata.Checkpoint {
		return recoverDatabase(db)
	}
	return nil
}
