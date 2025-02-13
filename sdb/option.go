package sdb

import (
	"github.com/lucmq/go-shelve/sdb/internal"
)

// Option is passed to the Open function to create a customized DB.
type Option func(*DB)

// WithCacheSize sets the size of the cache used by the database. A value of -1
// represents an unlimited cache and a value of 0 disables the cache. The
// default cache size is -1.
func WithCacheSize(size int64) Option {
	return func(db *DB) {
		db.cache = internal.NewCache[cacheEntry](int(size))
	}
}

func WithFileCacheEnabled(enabled bool) Option {
	return func(db *DB) {
		// TODO: set a flag in the db to enable keeping a many open files.

		// TODO: Might be a FS Option if we decide not to add *os.File to the cacheEntry struct.
	}
}

func WithMaxOpenFiles(max int) Option {
	return func(db *DB) {
		// TODO: This option might not be needed if we decide to add *os.File to the cacheEntry struct.
	}
}

// WithSynchronousWrites enables synchronous writes to the database. By default,
// synchronous writes are disabled.
func WithSynchronousWrites(sync bool) Option {
	return func(db *DB) {
		db.syncWrites = sync
	}
}
