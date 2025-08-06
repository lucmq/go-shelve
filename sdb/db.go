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
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unsafe"

	"github.com/lucmq/go-shelve/sdb/internal"
)

const (
	// Asc and Desc can be used with the DB.Items method to make the
	// iteration order ascending or descending respectively.
	//
	// They are just syntactic sugar to make the iteration order more
	// explicit.
	Asc = 1

	// Desc is the opposite of Asc.
	Desc = -1
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
	metadataFilename  = "meta.gob"
)

const version = 1

var (
	// ErrKeyTooLarge is returned when a key exceeds the maximum length.
	ErrKeyTooLarge = errors.New("key exceeds maximum length")

	// ErrDatabaseClosed is returned when the database is closed.
	ErrDatabaseClosed = errors.New("database is closed")
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
	mu            sync.RWMutex
	path          string
	metadata      metadata
	metadataStore *metadataStore
	shards        []shard
	cache         internal.Cache[cacheEntry]
	fs            fileSystem
	closed        bool

	// Controls the background sync loop.
	done chan struct{}
	wg   sync.WaitGroup

	maxFilesPerShard int64
	syncWrites       bool

	// autoSync enables the background sync loop. Can be removed if a WAL
	// is adopted for consistency, since the WAL would handle the sync
	// loop unnecessary.
	autoSync     bool
	syncInterval time.Duration
}

// cacheEntry represents an entry in the cache.
type cacheEntry = []byte

// Open opens the database at the given path. If the path does not exist, it is
// created.
//
// Client applications must call DB.Close() when done with the database.
func Open(path string, options ...Option) (*DB, error) {
	db := DB{
		path:             path,
		metadata:         makeMetadata(),
		shards:           []shard{{maxKey: sentinelDir}},
		cache:            internal.NewCache[cacheEntry](-1),
		fs:               &osFS{},
		done:             make(chan struct{}),
		maxFilesPerShard: defaultMaxFilesPerShard,
		syncWrites:       false,
		autoSync:         true,
		syncInterval:     metadataSyncInterval,
	}

	// Apply options.
	for _, option := range options {
		option(&db)
	}

	if err := initializeDatabase(&db); err != nil {
		return nil, fmt.Errorf("initialize database: %w", err)
	}

	// Start the background loop if autoSync is enabled.
	if db.autoSync {
		db.wg.Add(1)
		go syncMetadata(&db)
	}

	return &db, nil
}

// Close synchronizes and closes the database. Users must ensure no pending
// operations are in progress before calling Close().
//
// Example:
//
//	var wg sync.WaitGroup
//	db, _ := sdb.Open("path")
//
//	// Start concurrent writes
//	for i := 0; i < 10; i++ {
//	    wg.Add(1)
//	    go func(i int) {
//	        defer wg.Done()
//	        db.Put([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
//	    }(i)
//	}
//
//	wg.Wait()  // Ensure all writes are done
//	db.Close() // Safe to close now
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}
	db.closed = true

	// Signal the background goroutine to stop.
	close(db.done)
	db.wg.Wait()

	// Final sync.
	return syncInternal(db)
}

// Len returns the number of items in the database. If an error occurs, it
// returns -1.
func (db *DB) Len() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return -1
	}

	return int64(db.metadata.TotalEntries)
}

// Sync synchronizes the database to persistent storage.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}

	return syncInternal(db)
}

// Has reports whether a key exists in the database.
func (db *DB) Has(key []byte) (bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return false, ErrDatabaseClosed
	}

	_, ok := cacheGet(db, key)
	if ok {
		return true, nil
	}

	path, _ := keyPath(db, key)

	_, err := fs.Stat(db.fs, path)
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

	if db.closed {
		return nil, ErrDatabaseClosed
	}

	v, ok := cacheGet(db, key)
	if ok {
		return v, nil
	}

	path, _ := keyPath(db, key)

	value, err := fs.ReadFile(db.fs, path)
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

	if db.closed {
		return ErrDatabaseClosed
	}

	path, shardID := keyPath(db, key)
	sh := &db.shards[shardID]

	updated, err := putPath(db, path, value)
	if err != nil {
		return fmt.Errorf("put path: %w", err)
	}

	if !updated {
		sh.count++
		db.metadata.TotalEntries++
	}
	db.metadata.Generation++

	if int64(sh.count) > db.maxFilesPerShard {
		if err = db.splitShard(shardID); err != nil {
			return fmt.Errorf("split shard: %w", err)
		}
	}

	// Cache aside
	db.cache.Put(string(key), value)
	return nil
}

func putPath(db *DB, path string, value []byte) (updated bool, err error) {
	_, err = fs.Stat(db.fs, path)
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("stat: %w", err)
	}
	if err == nil {
		updated = true
	}

	writer := newAtomicWriter(db.fs, db.syncWrites)
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

	if db.closed {
		return ErrDatabaseClosed
	}

	path, shardID := keyPath(db, key)
	sh := &db.shards[shardID]

	var deleted bool
	err := db.fs.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove: %w", err)
	}
	if err == nil {
		deleted = true
	}

	if deleted {
		sh.count--
		db.metadata.TotalEntries--
	}
	db.metadata.Generation++

	db.cache.Delete(string(key))
	return nil
}

// Items iterates over key-value pairs in the database, invoking fn(k, v)
// for each pair. Iteration stops early if fn returns false.
//
// start is the first key to include in the iteration (inclusive).
// If start is nil or empty, iteration begins at the logical extremity
// determined by order.
//
// order controls the traversal direction:
//
//	Asc  (value +1) – ascending lexical order
//	Desc (value –1) – descending lexical order
//
// Keys are streamed in that order until Yield returns false or an
// error occurs.
//
// This operation acquires a read lock each time a database record is read
// and holds it for the duration of the fn callback. Implementations that
// require faster lock release should copy the key-value pair and return
// from the callback as quickly as possible.
//
// The user-provided fn(k, v) must not modify the database within the same
// goroutine as the iteration, as this would cause a deadlock.
func (db *DB) Items(start []byte, order int, fn Yield) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return ErrDatabaseClosed
	}

	n := len(db.shards)
	asc := order == Asc
	encStart := encodeKey(start)

	// Pick initial shard (Use the db.shards slice to prune the
	// search space).
	idx := 0
	if len(start) != 0 {
		idx = db.shardForKey(encStart)
	} else if !asc {
		idx = n - 1
	}

	step := 1
	stop := n
	if !asc {
		step = -1
		stop = -1
	}

	for k := idx; k != stop; k += step {
		sh := db.shards[k]
		dir := filepath.Join(db.path, dataDirectory, sh.maxKey)

		keep, err := streamDir(db.fs, dir, encStart, order, func(filename string) (bool, error) {
			return handleFileWithLock(db, dir, filename, fn)
		})
		if err != nil {
			return err
		}
		if !keep {
			return nil
		}
	}

	return nil
}

func handleFileWithLock(db *DB, dir string, name string, fn Yield) (bool, error) {
	key, err := decodeKey(name)
	if err != nil {
		return false, fmt.Errorf("decode key: %w", err)
	}

	// Use the cache (but do not cache aside while iterating) because that would
	// result in a lot of cache turnover with keys that might not be needed to be
	// cached.
	value, ok := cacheGet(db, key)
	if ok {
		return fn(key, value)
	}

	// Read from the disk.
	v, err := fs.ReadFile(db.fs, filepath.Join(dir, name))
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

func keyPath(db *DB, key []byte) (path string, shardID int) {
	base := encodeKey(key)
	i := db.shardForKey(base)
	dir := db.shardPath(i)
	return filepath.Join(dir, base), i
}

func encodeKey(key []byte) string {
	return base32.HexEncoding.EncodeToString(key)
}

func decodeKey(key string) ([]byte, error) {
	return base32.HexEncoding.DecodeString(key)
}

func cacheGet(db *DB, key []byte) (cacheEntry, bool) {
	s := unsafe.String(&key[0], len(key))
	return db.cache.Get(s)
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
	if !ok {
		return nil
	}
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}

	if db.metadata.Generation != db.metadata.Checkpoint {
		// Already drifted
		return nil
	}

	// Mark as loaded
	db.metadata.Generation = db.metadata.Checkpoint + 1

	// Sync the metadata
	return db.metadataStore.Save(db.metadata)
}

func syncInternal(db *DB) error {
	// Mark as consistent
	db.metadata.Checkpoint = db.metadata.Generation

	return db.metadataStore.Save(db.metadata)
}

// syncMetadata periodically syncs the metadata to persistent storage.
//
// Note: This is only done to decrease the chance of a recovery triggered
// in the initialization due to a user forgetting to call DB.Close() or a
// system crash. The database doesn't really depend on this mechanism and
// errors here can be ignored.
func syncMetadata(db *DB) {
	defer db.wg.Done()

	ticker := time.NewTicker(db.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = db.Sync()

		case <-db.done:
			// The channel is closed in Close(); exit the goroutine.
			return
		}
	}
}
