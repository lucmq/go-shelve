package sdb

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Helpers

var (
	TestDirectory           = filepath.Join(os.TempDir(), "sdb-test")
	TestFilesPerShardOption = withMaxFilesPerShard(3)
	TestError               = errors.New("test error")
)

type TDB = *DB

// Provides an Open function that creates a clean test database.
func OpenTestDB() (*DB, error) {
	open := NewOpenFunc(true, TestFilesPerShardOption)
	return open()
}

// Same as OpenTestDB, but without the cleaning the database directory.
func ReopenTestDB() (*DB, error) {
	open := NewOpenFunc(false, TestFilesPerShardOption)
	return open()
}

// NewOpenFunc is a factory for Open functions. If clean is true, then
// the database directory is cleaned before creating the database.
func NewOpenFunc(clean bool, opts ...Option) OpenFunc {
	return func() (TDB, error) {
		path := TestDirectory
		if clean {
			err := os.RemoveAll(path)
			if err != nil && !os.IsNotExist(err) {
				return nil, fmt.Errorf("remove path: %w", err)
			}
		}
		return Open(path, opts...)
	}
}

func CheckShardLayout(t *testing.T, db TDB, _ map[string]string) {
	// Get the list of shards on disk
	shards, err := os.ReadDir(filepath.Join(db.path, dataDirectory))
	if err != nil {
		t.Fatalf("read shard dir: %v", err)
	}

	for _, dir := range shards {
		shardFiles, err := os.ReadDir(filepath.Join(db.path, dataDirectory, dir.Name()))
		if err != nil {
			t.Errorf("read shard dir: %v", err)
		}

		// Check the number of files in the shard
		if int64(len(shardFiles)) > db.maxFilesPerShard {
			t.Errorf("Too many files (%d) in shard: %s", len(shardFiles), dir.Name())
		}
	}
}

func AssertItems(t *testing.T, db *DB, start []byte, order int, expect []string) {
	t.Helper()

	var got []string
	err := db.Items(start, order, func(k, _ []byte) (bool, error) {
		got = append(got, string(k))
		return true, nil
	})
	if err != nil {
		t.Fatalf("Items: %v", err)
	}

	if len(got) != len(expect) {
		t.Errorf("Expected len to be %v, but got %v", expect, got)
	}
	if !reflect.DeepEqual(got, expect) {
		t.Fatalf("start=%q order=%d\nwant %v\ngot  %v",
			start, order, expect, got)
	}
}

func getClosedDB(t *testing.T, seed map[string]string) *DB {
	t.Helper()

	open := NewOpenFunc(true, WithCacheSize(0))
	db := StartDatabase(t, open, seed)
	if err := db.Close(); err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}

	return db
}

// Tests

func TestDB(t *testing.T) {
	tests := NewDBTests(
		NewOpenFunc(true, TestFilesPerShardOption),
		NewOpenFunc(false, TestFilesPerShardOption),
	)
	tests.CheckInitialization = CheckInitialization
	tests.SupportsSeeking = true
	tests.SupportsReverseIteration = true
	tests.TestAll(t)
}

func TestDB_NoCache(t *testing.T) {
	tests := NewDBTests(
		NewOpenFunc(true, WithCacheSize(0), TestFilesPerShardOption),
		NewOpenFunc(false, WithCacheSize(0), TestFilesPerShardOption),
	)
	tests.CheckInitialization = CheckInitialization
	tests.SupportsSeeking = true
	tests.SupportsReverseIteration = true
	tests.TestAll(t)
}

func TestDB_SyncWrites(t *testing.T) {
	tests := NewDBTests(
		NewOpenFunc(true, WithSynchronousWrites(true), TestFilesPerShardOption),
		NewOpenFunc(false, WithSynchronousWrites(true), TestFilesPerShardOption),
	)
	tests.CheckInitialization = CheckInitialization
	tests.SupportsSeeking = true
	tests.SupportsReverseIteration = true
	tests.TestAll(t)
}

func TestOpen_WithOptions(t *testing.T) {
	path := TestDirectory
	err := os.RemoveAll(path)
	if err != nil && !os.IsNotExist(err) {
		t.Errorf("remove path: %s", err)
	}
	cacheSize := int64(1024 * 1024)

	db, err := Open(path,
		WithCacheSize(cacheSize),
		WithSynchronousWrites(true),
		TestFilesPerShardOption,
	)
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}
	if !db.syncWrites {
		t.Errorf("Expected true, but got %v", db.syncWrites)
	}
}

// Tests for miscellaneous boundary cases.
func TestDB_Error(t *testing.T) {
	t.Run("Put - key exceeds maximum length", func(t *testing.T) {
		db, err := OpenTestDB()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		defer db.Close()
		key := bytes.Repeat([]byte{0xFF}, 2*MaxKeyLength)

		err = db.Put(key, []byte("value"))
		if !errors.Is(err, ErrKeyTooLarge) {
			t.Errorf("Expected ErrKeyTooLarge, but got %v", err)
		}
	})
}

// Tests boundary cases where a file representing a database record
// becomes inconsistent or corrupted.
func TestDB_FileError(t *testing.T) {
	// Note: This function serves as a placeholder for future tests. It may be used
	// if we decide to implement CRC checks in the data file to ensure consistency.
}

// Tests for boundary cases where the file that represent a database
// record got in an inconsistent state. Specific for the DB.Items method.
func TestDB_FileError_Items(t *testing.T) {
	t.Run("Items - Filename not base32hex", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		open := NewOpenFunc(true, WithCacheSize(0))
		db := StartDatabase(t, open, seed)
		defer db.Close()

		// Make the files unreadable
		path, _ := keyPath(db, []byte("key-1"))
		wrongPath := filepath.Join(db.path, dataDirectory, sentinelDir, "0000")
		err := os.Rename(path, wrongPath)
		if err != nil {
			t.Fatalf("Expected no error, but got %v", err)
		}

		err = db.Items(nil, 1, func(k, v []byte) (bool, error) {
			return true, nil
		})
		if err == nil {
			t.Errorf("Expected error, but got none")
		}
	})
}

// Tests to check the internal sharding layout consistency.
func TestDB_ShardLayout(t *testing.T) {
	t.Run("Count < MaxCount after insertions", func(t *testing.T) {
		// Arrange
		seed := make(map[string]string)
		for i := 0; i < 100; i++ {
			key := []byte("key-" + strconv.Itoa(i))
			value := []byte("value-" + strconv.Itoa(i))
			seed[string(key)] = string(value)
		}

		// Act
		open := NewOpenFunc(true, WithCacheSize(0))
		db := StartDatabase(t, open, seed)
		defer db.Close()

		// Assert
		CheckShardLayout(t, db, seed)
	})
}

func TestDB_Items_ShardBoundaries(t *testing.T) {
	seed := make(map[string]string)
	for i := 0; i < 30; i++ {
		k := fmt.Sprintf("%03d", i)
		seed[k] = "v" + k
	}

	db := StartDatabase(t, OpenTestDB, seed)
	defer db.Close()

	// Expected key slices
	keysAsc := slices.Sorted(maps.Keys(seed))
	keysDesc := slices.Clone(keysAsc)
	slices.Reverse(keysDesc)

	cases := []struct {
		name  string
		order int
		keys  []string
	}{
		{"asc", Asc, keysAsc},
		{"desc", Desc, keysDesc},
	}

	for _, tc := range cases {
		tc := tc
		for i, start := range tc.keys {
			t.Run(fmt.Sprintf("%s/from=%s", tc.name, start), func(t *testing.T) {
				expect := tc.keys[i:]
				AssertItems(t, db, []byte(start), tc.order, expect)
			})
		}
	}
}

func TestDB_Items_ShardBoundariesAfterDelete(t *testing.T) {
	// Seed & Delete
	seed := make(map[string]string)
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("%03d", i)
		seed[k] = "v" + k
	}

	db := StartDatabase(t, OpenTestDB, seed)
	defer db.Close()

	// Delete the first 30 keys (000 … 029)
	for i := 0; i < 30; i++ {
		k := fmt.Sprintf("%03d", i)
		delete(seed, k)
		if err := db.Delete([]byte(k)); err != nil {
			t.Fatalf("Delete(%s): %v", k, err)
		}
	}

	// Expected key slices
	keysAsc := slices.Sorted(maps.Keys(seed)) // [030 … 039]
	keysDesc := slices.Clone(keysAsc)
	slices.Reverse(keysDesc) // [039 … 030]

	cases := []struct {
		name  string
		order int
		keys  []string
	}{
		{"asc", Asc, keysAsc},
		{"desc", Desc, keysDesc},
	}

	for _, tc := range cases {
		tc := tc
		for i, start := range tc.keys {
			t.Run(fmt.Sprintf("%s/from=%s", tc.name, start), func(t *testing.T) {
				expect := tc.keys[i:]
				AssertItems(t, db, []byte(start), tc.order, expect)
			})
		}
	}
}

func TestOperationsOnClosedDB(t *testing.T) {
	t.Run("Close", func(t *testing.T) {
		db := getClosedDB(t, nil)

		err := db.Close()
		if err != nil { // Close() is idempotent.
			t.Errorf("Expected no error, but got %v", err)
		}
	})

	t.Run("Sync", func(t *testing.T) {
		db := getClosedDB(t, nil)

		err := db.Sync()
		if !errors.Is(err, ErrDatabaseClosed) {
			t.Errorf("Sync after Close: expected ErrDatabaseClosed, got: %v", err)
		}
	})

	t.Run("Len", func(t *testing.T) {
		db := getClosedDB(t, nil)

		if db.Len() != -1 {
			t.Errorf("Len after Close: expected -1, got: %v", db.Len())
		}
	})

	t.Run("Has", func(t *testing.T) {
		db := getClosedDB(t, nil)

		_, err := db.Has([]byte("testKey"))
		if !errors.Is(err, ErrDatabaseClosed) {
			t.Errorf("Has after Close: expected ErrDatabaseClosed, got: %v", err)
		}
	})

	t.Run("Get", func(t *testing.T) {
		db := getClosedDB(t, nil)

		_, err := db.Get([]byte("testKey"))
		if !errors.Is(err, ErrDatabaseClosed) {
			t.Errorf("Get after Close: expected ErrDatabaseClosed, got: %v", err)
		}
	})

	t.Run("Put", func(t *testing.T) {
		db := getClosedDB(t, nil)

		err := db.Put([]byte("testKey"), []byte("testValue"))
		if !errors.Is(err, ErrDatabaseClosed) {
			t.Errorf("Put after Close: expected ErrDatabaseClosed, got: %v", err)
		}
	})

	t.Run("Put - Concurrent and Some After Close()", func(t *testing.T) {
		db := StartDatabase(
			t,
			NewOpenFunc(true, WithCacheSize(0)),
			map[string]string{
				"key-1": "value-1", "key-2": "value-2",
				"key-3": "value-3", "key-4": "value-4",
				"key-5": "value-5", "key-6": "value-6",
			},
		)

		N := 100
		closeN := 10 // Close() is called when we reach this index.

		wg := sync.WaitGroup{}

		mu := sync.Mutex{}
		var errs []error

		// Act
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if i == closeN {
					if err := db.Close(); err != nil {
						t.Errorf("Expected no error, but got %v", err)
						return
					}
				}

				key := fmt.Sprintf("key-%d", i)
				value := fmt.Sprintf("value-%d", i)

				if err := db.Put([]byte(key), []byte(value)); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}(i)
		}

		wg.Wait()

		// Assert
		if len(errs) == 0 {
			t.Errorf("Expected at least one error, but got none")
		}
		for _, err := range errs {
			if !errors.Is(err, ErrDatabaseClosed) {
				t.Errorf("Put after Close: expected ErrDatabaseClosed, got: %v", err)
			}
		}
	})

	t.Run("Delete", func(t *testing.T) {
		db := getClosedDB(t, nil)

		err := db.Delete([]byte("testKey"))
		if !errors.Is(err, ErrDatabaseClosed) {
			t.Errorf("Delete after Close: expected ErrDatabaseClosed, got: %v", err)
		}
	})

	t.Run("Delete - Concurrent and Some After Close()", func(t *testing.T) {
		db := StartDatabase(
			t,
			NewOpenFunc(true, WithCacheSize(0)),
			map[string]string{
				"key-1": "value-1", "key-2": "value-2",
				"key-3": "value-3", "key-4": "value-4",
				"key-5": "value-5", "key-6": "value-6",
			},
		)

		N := 100
		closeN := 10 // Close() is called when we reach this index.

		wg := sync.WaitGroup{}

		mu := sync.Mutex{}
		var errs []error

		// Act
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if i == closeN {
					if err := db.Close(); err != nil {
						t.Errorf("Expected no error, but got %v", err)
						return
					}
				}

				key := fmt.Sprintf("key-%d", i)

				if err := db.Delete([]byte(key)); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}(i)
		}
		wg.Wait()

		// Assert
		if len(errs) == 0 {
			t.Errorf("Expected at least one error, but got none")
		}
		for _, err := range errs {
			if !errors.Is(err, ErrDatabaseClosed) {
				t.Errorf("Put after Close: expected ErrDatabaseClosed, got: %v", err)
			}
		}
	})

	t.Run("Items - Empty DB", func(t *testing.T) {
		db := getClosedDB(t, nil)

		err := db.Items(nil, 1, func(k, v []byte) (bool, error) {
			return true, nil
		})
		if !errors.Is(err, ErrDatabaseClosed) {
			t.Errorf("Items after Close: expected ErrDatabaseClosed, got: %v", err)
		}
	})

	t.Run("Items - Non-Empty DB", func(t *testing.T) {
		db := getClosedDB(t, map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		})

		err := db.Items(nil, 1, func(k, v []byte) (bool, error) {
			return true, nil
		})
		if !errors.Is(err, ErrDatabaseClosed) {
			t.Errorf("Items after Close: expected ErrDatabaseClosed, got: %v", err)
		}
	})

	// Note: Items() and Close() can run in parallel because Items() takes the
	// global lock at the start. Update this test if the locking strategy changes.
	t.Run("Items - While Iterating", func(t *testing.T) {
		db := StartDatabase(
			t,
			NewOpenFunc(true, WithCacheSize(0)),
			map[string]string{
				"key-1": "value-1", "key-2": "value-2",
				"key-3": "value-3", "key-4": "value-4",
				"key-5": "value-5", "key-6": "value-6",
			},
		)

		i := 0
		wg := sync.WaitGroup{}

		err := db.Items(nil, 1, func(k, v []byte) (bool, error) {
			if i == 0 {
				// Close the database while iterating, on a separate goroutine
				// so that the iteration is not blocked.
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := db.Close(); err != nil {
						t.Errorf("Expected no error, but got %v", err)
						return
					}
				}()
			} else {
				// Give the goroutine a chance to close the database.
				time.Sleep(10 * time.Millisecond)
			}
			i++

			return true, nil
		})
		if err != nil {
			t.Errorf("Items: %v", err)
		}
		wg.Wait()
	})
}
