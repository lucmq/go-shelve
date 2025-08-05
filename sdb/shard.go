package sdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

const (
	// This limit is arbitrary and chosen to balance between:
	// - the (small) cost of creating many directories
	// - the (small) cost of walking a large number of files
	defaultMaxFilesPerShard = 50_000

	// The sentinelDir is a special directory that is guaranteed to have a
	// higher name than any other directory. It is created by the db and is
	// used to simplify the logic of sharding.
	sentinelDir = "_"
)

type shard struct {
	maxKey string // upper bound, inclusive
	count  uint32
}

// shardForKey returns the index whose *upper* bound >= encStart.
func (db *DB) shardForKey(enc string) int {
	return sort.Search(len(db.shards), func(j int) bool { return enc <= db.shards[j].maxKey })
}

func (db *DB) shardPath(i int) string {
	return filepath.Join(db.path, dataDirectory, db.shards[i].maxKey)
}

func (db *DB) loadShards() error {
	entries, err := db.fs.ReadDir(filepath.Join(db.path, dataDirectory))
	if err != nil {
		return fmt.Errorf("read dir: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	db.shards = make([]shard, len(entries))
	for i, e := range entries {
		shardEntries, err := db.fs.ReadDir(filepath.Join(db.path, dataDirectory, e.Name()))
		if err != nil {
			return fmt.Errorf("read shard dir: %w", err)
		}

		db.shards[i] = shard{
			maxKey: e.Name(),
			count:  uint32(len(shardEntries)),
		}
	}
	return nil
}

// splitShard splits shard `idx` in two. It moves the _lower_ half of shard
// `idx` into a freshly-created directory whose name is the *highest* key that
// stays inside that new shard (`names[mid-1]`). The original directory keeps
// the upper half unchanged.
func (db *DB) splitShard(idx int) error {
	// 1. Enumerate & sort entries in the *old* directory.
	oldPath := db.shardPath(idx)

	files, err := db.fs.ReadDir(oldPath)
	if err != nil {
		return fmt.Errorf("read shard dir: %w", err)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })

	mid := len(files) / 2
	lowerHalf := files[:mid]
	newLowMax := files[mid-1].Name()
	newPath := filepath.Join(db.path, dataDirectory, newLowMax)

	// 2. Create the new directory and move the lower-half files into it.
	if err = db.fs.MkdirAll(newPath, defaultDirPermissions); err != nil && !os.IsExist(err) {
		return fmt.Errorf("mkdir: %w", err)
	}
	for _, e := range lowerHalf {
		if err = db.fs.Rename(
			filepath.Join(oldPath, e.Name()),
			filepath.Join(newPath, e.Name()),
		); err != nil {
			return fmt.Errorf("rename: %w", err)
		}
	}

	// 3. Update the in-memory shard slice.
	updateSplitShards(db, idx, files)

	// 4. Sync the parent directory.
	if db.syncWrites {
		// Sync the parent directory for more durability guarantees. See:
		// - https://lwn.net/Articles/457667/#:~:text=When%20should%20you%20Fsync
		_ = syncFile(db.fs, newPath)
	}

	return nil
}

// updateSplitShards updates the in-memory shard slice after a shard has been
// split: it inserts a new shard in the middle, and updates the two Counts so
// that the next split will happen at the correct boundary.
//
// The files argument must be sorted by file name.
func updateSplitShards(db *DB, idx int, files []os.DirEntry) {
	mid := len(files) / 2

	lowerHalf := files[:mid]
	upperHalf := files[mid:]
	newLowMax := files[mid-1].Name()

	// make room for one more element (shift right)
	db.shards = append(db.shards, shard{})
	copy(db.shards[idx+1:], db.shards[idx:])

	// fill the freshly-created slot
	db.shards[idx] = shard{maxKey: newLowMax, count: uint32(len(lowerHalf))}

	// fix the old shard’s count (it’s now the *upper* shard)
	db.shards[idx+1].count = uint32(len(upperHalf))
}
