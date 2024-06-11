package sdb

import "go-shelve/sdb/internal"

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
