// Package sdb offers a simple key-value database that can be utilized with the
// go-shelve project.
//
// It should be suitable for a wide range of applications, but the driver
// directory (go-shelve/driver) provides additional options for configuring the
// Shelf with other supported databases from the Go ecosystem.
//
// # DB Records
//
// In sdb, each database record is represented by a distinct file stored in a
// bucket, which is a corresponding filesystem directory. The number of
// documents stored in each bucket is unlimited, and modern filesystems should
// be able to handle large buckets without significantly affecting performance.
//
// Each file record's name is "base32hex" encoding of the key, which preserves
// lexical sort order [1]. Keys are limited to 128 characters. The record file
// is stored as binary data. With this design, Users do not need to worry about
// hitting the maximum filename length or storing keys with forbidden
// characters.
//
// # Cache
//
// The sdb database uses a memory-based cache to speed up operations. By
// default, the cache size is unlimited, but it can be configured to a fixed
// size or disabled altogether.
//
// The cache's design, albeit simple, can enhance the performance of "DB.Get"
// and "DB.Items" to more than 1 million reads per second on standard hardware.
//
// # Atomicity
//
// New records are written atomically to the key-value store. With a
// file-per-record design, sdb achieves this by using atomic file writes, which
// consist of creating a temporary file and then renaming it [2].
//
// This ensures that the database's methods are always performed with one
// atomic operation, significantly simplifying the recovery process.
//
// Currently, the only data that can become inconsistent is the count of stored
// records, but if this happens, it is detected and corrected at the DB
// initialization.
//
// As an optimization, records might be written directly without needing a
// temporary file if the data fits in a single sector since a single-sector
// write can be assumed to be atomic on some systems [3] [4].
//
// # Durability
//
// By default, sdb leverages the filesystem cache to speed up the database
// writes. This is generally suitable for most applications for which sdb is
// intended, as modern hardware can offer sufficient protection against
// crashes and ensure durability.
//
// For the highest level of durability, the WithSynchronousWrites option makes
// the database synchronize data to persistent storage on each write.
//
// # Notes
//
// [1] https://datatracker.ietf.org/doc/html/rfc4648#section-7
//
// [2] On Windows, additional configuration is involved.
//
// [3] https://stackoverflow.com/questions/2009063/are-disk-sector-writes-atomic
//
// [4] https://web.cs.ucla.edu/classes/spring07/cs111-2/scribe/lecture14.html
package sdb

import (
	"encoding/base32"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lucmq/go-shelve/sdb/internal"
)

const (
	// DefaultCacheSize is the default size of the cache used to speed up the
	// database operations. A value of -1 represents an unlimited cache.
	DefaultCacheSize = -1

	// MaxKeyLength is the maximum size of a key.
	MaxKeyLength = 128

	// metadataSyncInterval is the interval at which the metadata is synced to
	// disk.
	metadataSyncInterval = 1 * time.Minute
)

const (
	dataDirectory     = "data"
	metadataDirectory = "meta"
)

const version = 1

var (
	// ErrKeyTooLarge is returned when a key exceeds the maximum length.
	ErrKeyTooLarge = errors.New("key exceeds maximum length")
)

// Yield is a function called when iterating over key-value pairs in the
// database. If Yield returns false or an error, the iteration stops.
type Yield = func(key, value []byte) (bool, error)

// DB represents a database, which is created with the Open function.
//
// Client applications must call DB.Close() when done with the database.
//
// A DB is safe for concurrent use by multiple goroutines.
type DB struct {
	mu         sync.RWMutex
	path       string
	metadata   metadata
	cache      internal.Cache[cacheEntry]
	syncWrites bool
}

// cacheEntry represents an entry in the cache.
type cacheEntry = []byte

// Open opens the database at the given path. If the path does not exist, it is
// created.
//
// Client applications must call DB.Close() when done with the database.
func Open(path string, options ...Option) (*DB, error) {
	db := DB{
		path:       path,
		metadata:   makeMetadata(),
		cache:      internal.NewCache[cacheEntry](-1),
		syncWrites: false,
	}

	// Apply options
	for _, option := range options {
		option(&db)
	}

	err := initializeDatabase(&db)
	if err != nil {
		return nil, fmt.Errorf("initialize database: %w", err)
	}
	go syncMetadata(&db)

	return &db, nil
}

// Close synchronizes and closes the database.
func (db *DB) Close() error {
	return db.Sync()
}

// Len returns the number of items in the database. If an error occurs, it
// returns -1.
func (db *DB) Len() int64 {
	return int64(db.metadata.TotalEntries)
}

// Sync synchronizes the database to persistent storage.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Mark as consistent
	db.metadata.Checkpoint = db.metadata.Generation

	return db.metadata.Save(db.path)
}

// Has reports whether a key exists in the database.
func (db *DB) Has(key []byte) (bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	_, ok := db.cache.Get(string(key))
	if ok {
		return true, nil
	}

	_, err := os.Stat(keyPath(db, key))
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("stat: %w", err)
	}

	return !os.IsNotExist(err), nil
}

// Get retrieves the value associated with a key from the database. If the key
// is not found, it returns nil.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	v, ok := db.cache.Get(string(key))
	if ok {
		return v, nil
	}

	value, err := os.ReadFile(keyPath(db, key))
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read file: %w", err)
	}
	if os.IsNotExist(err) {
		return nil, nil
	}

	return value, err
}

// Put adds a key-value pair to the database. If the key already exists, it
// overwrites the existing value.
//
// It returns an error if the key is greater than [MaxKeyLength].
func (db *DB) Put(key, value []byte) error {
	if err := prepareForMutation(db); err != nil {
		return fmt.Errorf("prepare for mutation: %w", err)
	}
	if len(key) > MaxKeyLength {
		return ErrKeyTooLarge
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	updated, err := putPath(db, keyPath(db, key), value)
	if err != nil {
		return fmt.Errorf("put path: %w", err)
	}

	if !updated {
		db.metadata.TotalEntries++
	}
	db.metadata.Generation++

	// Cache aside
	db.cache.Put(string(key), value)
	return nil
}

func putPath(db *DB, path string, value []byte) (updated bool, err error) {
	_, err = os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("stat: %w", err)
	}
	if err == nil {
		updated = true
	}

	writer := newAtomicWriter(db.syncWrites)
	err = writer.WriteFile(path, value, !updated)
	return updated, err
}

// Delete removes a key-value pair from the database.
func (db *DB) Delete(key []byte) error {
	if err := prepareForMutation(db); err != nil {
		return fmt.Errorf("prepare for mutation: %w", err)
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	var deleted bool
	err := os.Remove(keyPath(db, key))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove: %w", err)
	}
	if err == nil {
		deleted = true
	}

	if deleted {
		db.metadata.TotalEntries--
	}
	db.metadata.Generation++

	db.cache.Delete(string(key))
	return nil
}

// Items iterates over key-value pairs in the database, calling fn(k, v)
// for each pair in the sequence. The iteration stops early if the function
// fn returns false.
//
// The start and order parameters exist for compatibility with the shelve.DB
// interface and are not currently used.
//
// The operation will acquire a read lock everytime a database record is read
// and will hold for the duration of the fn callback. Implementations that need
// to quickly release the lock, should copy the key-value pair and return as
// soon as possible from the callback.
func (db *DB) Items(start []byte, order int, fn Yield) error {
	_, _ = start, order
	root := filepath.Join(db.path, dataDirectory)
	_, err := items(db, root, fn)
	if err != nil {
		return fmt.Errorf("walk data directory: %w", err)
	}
	return nil
}

func items(
	db *DB,
	root string,
	fn func(key, value []byte) (bool, error),
) (
	count int,
	err error,
) {
	err = readDir(root, func(name string) (bool, error) {
		path := filepath.Join(root, name)
		count++
		return handlePathWithLock(db, path, fn)
	})
	return count, err
}

func handlePathWithLock(
	db *DB,
	path string,
	fn func(key, value []byte) (bool, error),
) (bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Note: Hold the lock while the callback fn is being executed. Do not
	// assume we can release it earlier (after the record read).

	key, err := parseKey(path)
	if err != nil {
		return false, fmt.Errorf("parse key: %w", err)
	}

	// Use the cache (but do not cache aside while iterating)
	value, ok := db.cache.Get(string(key))
	if ok {
		return fn(key, value)
	}

	// Read from the disk
	v, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		// Deleted while iterating? Ignore.
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read key-value: %w", err)
	}

	return fn(key, v)
}

// Helpers

func keyPath(db *DB, key []byte) string {
	base := base32.HexEncoding.EncodeToString(key)
	return filepath.Join(db.path, dataDirectory, base)
}

func parseKey(path string) ([]byte, error) {
	base := filepath.Base(path)
	return base32.HexEncoding.DecodeString(base)
}

// prepareForMutation ensures we have enough information saved in persistent
// storage to be able to recover the database in the event of an error.
//
// Before each mutation, we compare the database generation value with the
// checkpoint. If they are equal, we increase generation and sync the metadata.
// Different values for generation and checkpoint indicates that the database
// has pending state to be synced to persistent storage.
//
// The I/O done by this function should be amortized between many mutations.
func prepareForMutation(db *DB) error {
	ok := db.mu.TryLock()
	defer db.mu.Unlock()

	if !ok {
		return nil
	}
	if db.metadata.Generation != db.metadata.Checkpoint {
		// Already drifted
		return nil
	}

	// Mark as loaded
	db.metadata.Generation = db.metadata.Checkpoint + 1

	// Sync the metadata
	return db.metadata.Save(db.path)
}

// syncMetadata periodically syncs the metadata to persistent storage.
func syncMetadata(d *DB) {
	// Note: This is only done to decrease the chance of a recovery triggered
	// in the initialization due to a user forgetting to call DB.Close() or a
	// system crash. The database doesn't really depend on this mechanism and
	// errors here can be ignored.
	for {
		time.Sleep(metadataSyncInterval)
		_ = d.Sync()
	}
}
