package sdb

import (
	"time"

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

// WithSynchronousWrites enables synchronous writes to the database. By default,
// synchronous writes are disabled.
func WithSynchronousWrites(sync bool) Option {
	return func(db *DB) {
		db.syncWrites = sync
	}
}

// withMaxFilesPerShard returns an Option that limits how many regular data
// files may reside in a single shard directory before SDB triggers a split.
//
// Currently, it is intended for tests: using a very small threshold speeds up
// exercises that need to observe shard-splitting behaviour.
func withMaxFilesPerShard(maxFilesPerShard int64) Option {
	return func(db *DB) {
		db.maxFilesPerShard = maxFilesPerShard
	}
}

// withSyncInterval returns an Option that overrides the background metadata
// sync interval. Some tests shorten the default (one minute) to stress
// time-dependent logic.
func withSyncInterval(d time.Duration) Option {
	return func(db *DB) {
		db.syncInterval = d
	}
}

// withFileSystem returns an Option that injects a custom fileSystem
// implementation (e.g. an in-memory or fault-injecting mock). Production code
// normally relies on the default os-backed implementation; tests can supply a
// mock to avoid touching the real disk or to verify error-handling paths.
func withFileSystem(fsys fileSystem) Option {
	return func(db *DB) {
		db.fs = fsys
	}
}
