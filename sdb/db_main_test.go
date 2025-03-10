package sdb

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Test Suite

// NOTE: The tests in this file are also included in the suite located at
// driver/test/db_main.go. Therefore, only tests that are universally
// applicable to all shelve.DB implementations should be placed here.

// OpenFunc is a function that opens a new database.
type OpenFunc func() (TDB, error)

// DBTests is a collection of tests for a database.
type DBTests struct {
	Open   OpenFunc // Open the database in a clean state
	Reopen OpenFunc // Reopen the database without cleaning it

	// Run additional checks after initialization
	CheckInitialization func(t *testing.T, db TDB)

	// Informs that the database supports seeking to a start
	// position and enable additional tests.
	SupportsSeeking bool

	// Informs that the database supports iterating in
	// descending order and enable additional tests.
	SupportsReverseIteration bool
}

// NewDBTests creates a new instance of DBTests. It can be used to test
// different implementations of the shelve.DB interface.
func NewDBTests(open, reopen OpenFunc) *DBTests {
	return &DBTests{
		Open:                open,
		Reopen:              reopen,
		CheckInitialization: func(t *testing.T, db TDB) {},
	}
}

// TestAll is the entrypoint to the test suite.
func (T *DBTests) TestAll(t *testing.T) {
	var db TDB
	t.Run("Open succeeds", func(t *testing.T) {
		var err error
		db, err = T.Open()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		if db == nil {
			t.Errorf("Expected db to be non-nil")
		}
		db.Close()
	})

	T.TestClose(t)
	T.TestLen(t)
	T.TestSync(t)
	T.TestHas(t)
	T.TestGet(t)
	T.TestPut(t)
	T.TestDelete(t)
	T.TestItems(t)

	if T.SupportsSeeking {
		T.TestItems_Seek(t)
	}
	if T.SupportsReverseIteration {
		T.TestItems_Reverse(t)
	}
	if T.SupportsSeeking &&
		T.SupportsReverseIteration {
		T.TestItems_SeekReverse(t)
	}

	T.TestPersistence(t)
}

func (T *DBTests) TestClose(t *testing.T) {
	t.Run("Close succeeds", func(t *testing.T) {
		db, err := T.Open()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		err = db.Close()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
	})
}

func (T *DBTests) TestLen(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		db, err := T.Open()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		defer db.Close()

		if db.Len() != 0 {
			t.Errorf("Expected len to be 0, but got %v", db.Len())
		}
	})

	t.Run("Non-empty", func(t *testing.T) {
		db, err := T.Open()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		defer db.Close()
		if err = db.Put([]byte("key"), []byte("value")); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		if db.Len() != 1 {
			t.Errorf("Expected len to be 1, but got %v", db.Len())
		}
	})
}

func (T *DBTests) TestSync(t *testing.T) {
	t.Run("Sync succeeds", func(t *testing.T) {
		db, err := T.Open()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		defer db.Close()

		err = db.Sync()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
	})
}

func (T *DBTests) TestHas(t *testing.T) {
	t.Run("Has succeeds", func(t *testing.T) {
		// Arrange
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()
		key := []byte("key-1")

		// Act
		has, err := db.Has(key)
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		if !has {
			t.Errorf("Expected has to be true, but got %v", has)
		}
	})

	t.Run("Has fails", func(t *testing.T) {
		// Arrange
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()

		// Act
		has, err := db.Has([]byte("key-99"))
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		if has {
			t.Errorf("Expected has to be false, but got %v", has)
		}
	})
}

func (T *DBTests) TestGet(t *testing.T) {
	t.Run("Get succeeds", func(t *testing.T) {
		// Arrange
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()
		key := "key-3"

		// Act
		v, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		value := seed[key]
		if string(v) != value {
			t.Errorf("Expected value to be %v, but got %v", value, v)
		}
	})

	t.Run("Get non-existing key", func(t *testing.T) {
		// Arrange
		seed := map[string]string{}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()

		// Act
		v, err := db.Get([]byte("key"))
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		if v != nil {
			t.Errorf("Expected value to be nil, but got %v", v)
		}
	})
}

func (T *DBTests) TestPut(t *testing.T) {
	t.Run("Put succeeds", func(t *testing.T) {
		// Arrange
		seed := map[string]string{}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()

		// Act
		if err := db.Put([]byte("key-1"), []byte("value-1")); err != nil {
			t.Errorf("Expected error, but got nil")
		}

		// Assert
		seed["key-1"] = "value-1"
		checkDatabase(t, db, seed)
	})

	t.Run("Put existing key", func(t *testing.T) {
		// Arrange
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()

		// Act
		if err := db.Put([]byte("key-2"), []byte("value-99")); err != nil {
			t.Errorf("Expected error, but got nil")
		}

		// Assert
		seed["key-2"] = "value-99"
		checkDatabase(t, db, seed)
	})

	t.Run("Put many", func(t *testing.T) {
		// Arrange
		seed := make(map[string]string)
		for i := 0; i < 100; i++ {
			key := []byte("key-" + strconv.Itoa(i))
			value := []byte("value-" + strconv.Itoa(i))
			seed[string(key)] = string(value)
		}

		// Act
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()

		// Assert
		checkDatabase(t, db, seed)
	})

	t.Run("Put concurrent", func(t *testing.T) {
		// Arrange
		N := 1000
		items := make([][2][]byte, N)
		for i := 0; i < N; i++ {
			items[i] = [2][]byte{
				[]byte(fmt.Sprintf("key-%d", i)),
				[]byte(fmt.Sprintf("value-%d", i)),
			}
		}
		db := StartDatabase(t, T.Open, nil)
		defer db.Close()

		inserted := make(map[string]string)
		mu := sync.Mutex{}

		C := 30 // Number of goroutines

		// Act
		var wg sync.WaitGroup
		for i := 0; i < C; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < N/C; j++ {
					// Use rand to avoid many simultaneous writes on the same file.
					x := rand.Int()
					err := db.Put(items[x%N][0], items[x%N][1])
					if err != nil {
						t.Errorf("put: %s", err)
						return
					}

					mu.Lock()
					inserted[string(items[x%N][0])] = string(items[x%N][1])
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		// Assert
		checkDatabase(t, db, inserted)
	})

	// Test that many goroutines putting the *same* key concurrently do not fail or
	// corrupt the database. In particular, it validates that our single‐sector‐write
	// optimization (which can use O_EXCL when creating new files) does not break
	// under concurrency, thanks to the global lock in Put(). If we removed the
	// internal lock calls, we'd see the second goroutine's O_EXCL fail and cause
	// an error. This test guards against inadvertently dropping that lock or
	// otherwise breaking concurrency in future changes.
	t.Run("Put concurrent - Same key", func(t *testing.T) {
		// Arrange
		N := 1000
		item := [2][]byte{
			[]byte(fmt.Sprintf("key-%d", 0)),
			[]byte(fmt.Sprintf("value-%d", 0)),
		}
		db := StartDatabase(t, T.Open, nil)
		defer db.Close()

		inserted := make(map[string]string)
		mu := sync.Mutex{}

		C := 30 // Number of goroutines

		// Act
		var wg sync.WaitGroup
		for i := 0; i < C; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < N/C; j++ {
					err := db.Put(item[0], item[1])
					if err != nil {
						t.Errorf("put: %s", err)
						return
					}

					mu.Lock()
					inserted[string(item[0])] = string(item[1])
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		// Assert
		checkDatabase(t, db, inserted)
	})

}

func (T *DBTests) TestDelete(t *testing.T) {
	t.Run("Delete succeeds", func(t *testing.T) {
		// Arrange
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()

		// Act
		if err := db.Delete([]byte("key-3")); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		delete(seed, "key-3")
		checkDatabase(t, db, seed)
	})

	t.Run("Key not found", func(t *testing.T) {
		// Arrange
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()

		// Act
		if err := db.Delete([]byte("key-99")); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		checkDatabase(t, db, seed)
	})

	t.Run("Delete many", func(t *testing.T) {
		// Arrange
		seed := make(map[string]string)
		for i := 0; i < 100; i++ {
			key := []byte("key-" + strconv.Itoa(i))
			value := []byte("value-" + strconv.Itoa(i))
			seed[string(key)] = string(value)
		}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()

		// Act
		for key, _ := range seed {
			if err := db.Delete([]byte(key)); err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
		}

		// Assert
		checkDatabase(t, db, nil)
	})

	t.Run("Delete concurrent", func(t *testing.T) {
		// Arrange
		seed := make(map[string]string)
		for i := 0; i < 100; i++ {
			key := []byte("key-" + strconv.Itoa(i))
			value := []byte("value-" + strconv.Itoa(i))
			seed[string(key)] = string(value)
		}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()

		// Act
		var wg sync.WaitGroup
		for k, _ := range seed {
			key := k // Capture
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := db.Delete([]byte(key)); err != nil {
					t.Errorf("Expected no error, but got %v", err)
				}
			}()
		}
		wg.Wait()

		// Assert
		checkDatabase(t, db, nil)
	})
}

func (T *DBTests) TestItems(t *testing.T) {
	t.Run("Items succeeds", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()

		// Act
		gotItems := make(map[string]string)
		err := db.Items([]byte{}, 1, func(k, v []byte) (bool, error) {
			gotItems[string(k)] = string(v)
			return true, nil
		})
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		if len(gotItems) != len(seed) {
			t.Errorf("Expected len to be %v, but got %v",
				len(seed), len(gotItems))
		}
		if !reflect.DeepEqual(gotItems, seed) {
			t.Errorf("Expected %v, but got %v", seed, gotItems)
		}
	})

	t.Run("Items stopped early", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()
		stopAfter := 2

		// Act
		gotItems := make(map[string]string)
		n := 0
		err := db.Items([]byte{}, 1, func(k, v []byte) (bool, error) {
			if n == stopAfter {
				return false, nil // stop early
			}
			gotItems[string(k)] = string(v)
			n++
			return true, nil
		})
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		if len(gotItems) != stopAfter {
			t.Errorf("Expected len to be %v, but got %v",
				stopAfter, len(gotItems))
		}
	})

	t.Run("Items with nil start", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()

		// Act
		gotItems := make(map[string]string)
		err := db.Items(nil, 1, func(k, v []byte) (bool, error) {
			gotItems[string(k)] = string(v)
			return true, nil
		})
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		if len(gotItems) != len(seed) {
			t.Errorf("Expected len to be %v, but got %v",
				len(seed), len(gotItems))
		}
		if !reflect.DeepEqual(gotItems, seed) {
			t.Errorf("Expected %v, but got %v", seed, gotItems)
		}
	})

	t.Run("Items fails", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()

		// Act / Assert
		err := db.Items([]byte{}, 1, func(k, v []byte) (bool, error) {
			return true, TestError
		})
		if !errors.Is(err, TestError) {
			t.Errorf("Expected %v, but got %v", TestError, err)
		}
	})

	t.Run("Items - delete while iterating", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()

		i := 0
		wg := sync.WaitGroup{}
		gotItems := make(map[string]string)

		// Act
		err := db.Items(nil, 1, func(k, v []byte) (bool, error) {
			if i == 0 {
				// Delete from the database while iterating, on a separate
				// goroutine so that the iteration is not blocked.
				wg.Add(1)
				go func() {
					defer wg.Done()
					for k := range seed {
						if err := db.Delete([]byte(k)); err != nil {
							t.Errorf("Expected no error, but got %v", err)
						}
					}
				}()
			} else {
				// Give the goroutine a chance to delete some items.
				time.Sleep(10 * time.Millisecond)
			}

			gotItems[string(k)] = string(v)
			i++

			return true, nil
		})
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		wg.Wait()

		// Assert
		if len(gotItems) > len(seed) {
			t.Errorf("Expected len to be at most %v, but got %v",
				len(seed), len(gotItems))
		}
		if len(gotItems) <= 1 {
			t.Errorf("Expected len to be at least 1, but got %v",
				len(gotItems))
		}
	})
}

func (T *DBTests) TestItems_Seek(t *testing.T) {
	seed := map[string]string{
		"key-1": "value-1", "key-2": "value-2",
		"key-3": "value-3", "key-4": "value-4",
	}
	tests := []struct {
		name     string
		start    string
		expected map[string]string
	}{
		{
			name:     "Items succeeds - empty start",
			start:    "",
			expected: seed,
		},
		{
			name:     "Items succeeds - first key",
			start:    "key-1",
			expected: seed,
		},
		{
			name:  "Items succeeds - subset",
			start: "key-2",
			expected: map[string]string{
				"key-2": "value-2",
				"key-3": "value-3",
				"key-4": "value-4",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Arrange
			db := StartDatabase(t, T.Open, seed)
			defer db.Close()
			start := []byte(test.start)
			expected := test.expected

			// Act
			items := make(map[string]string)
			err := db.Items(start, 1, func(k, v []byte) (bool, error) {
				items[string(k)] = string(v)
				return true, nil
			})
			if err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}

			// Assert
			if len(items) != len(expected) {
				t.Errorf("Expected len to be %v, but got %v",
					len(expected), len(items))
			}
			if !reflect.DeepEqual(items, expected) {
				t.Errorf("Expected %v, but got %v", expected, items)
			}
		})
	}
}

func (T *DBTests) TestItems_Reverse(t *testing.T) {
	t.Run("Items succeeds - descending", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := StartDatabase(t, T.Open, seed)
		defer db.Close()

		// Act
		gotItems := make(map[string]string)
		err := db.Items([]byte{}, -1, func(k, v []byte) (bool, error) {
			gotItems[string(k)] = string(v)
			return true, nil
		})
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		if len(gotItems) != len(seed) {
			t.Errorf("Expected len to be %v, but got %v",
				len(seed), len(gotItems))
		}
		if !reflect.DeepEqual(gotItems, seed) {
			t.Errorf("Expected %v, but got %v", seed, gotItems)
		}
	})
}

func (T *DBTests) TestItems_SeekReverse(t *testing.T) {
	seed := map[string]string{
		"key-1": "value-1", "key-2": "value-2",
		"key-3": "value-3", "key-4": "value-4",
	}
	tests := []struct {
		name     string
		start    string
		expected map[string]string
	}{
		{
			name:     "Items succeeds - empty start",
			start:    "",
			expected: seed,
		},
		{
			name:     "Items succeeds - first key",
			start:    "key-4",
			expected: seed,
		},
		{
			name:  "Items succeeds - subset",
			start: "key-3",
			expected: map[string]string{
				"key-3": "value-3",
				"key-2": "value-2",
				"key-1": "value-1",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Arrange
			db := StartDatabase(t, T.Open, seed)
			defer db.Close()
			start := []byte(test.start)
			expected := test.expected

			// Act
			items := make(map[string]string)
			err := db.Items(start, -1, func(k, v []byte) (bool, error) {
				items[string(k)] = string(v)
				return true, nil
			})
			if err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}

			// Assert
			if len(items) != len(expected) {
				t.Errorf("Expected len to be %v, but got %v",
					len(expected), len(items))
			}
			if !reflect.DeepEqual(items, expected) {
				t.Errorf("Expected %v, but got %v", expected, items)
			}
		})
	}
}

func (T *DBTests) TestPersistence(t *testing.T) {
	t.Run("Reopen", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		// Start the database, but only close after reopening.
		db := StartDatabase(t, T.Open, seed)

		T.CheckInitialization(t, db)

		// Reopen and verify
		if err := db.Close(); err != nil {
			t.Fatalf("Close DB failed: %v", err)
		}
		db, err := T.Reopen()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		defer db.Close()

		T.CheckInitialization(t, db)

		for k, v := range seed {
			if val, err := db.Get([]byte(k)); err != nil {
				t.Errorf("Expected no error, but got %v", err)
			} else if string(val) != v {
				t.Errorf("Expected %s, but got %s", v, string(val))
			}
		}
	})
}

// Test Suite - Helpers

func StartDatabase(t testing.TB, open OpenFunc, seed map[string]string) TDB {
	t.Helper()
	db, err := open()
	if err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}

	for k, v := range seed {
		if err = db.Put([]byte(k), []byte(v)); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
	}

	if db.Len() != int64(len(seed)) {
		t.Errorf("Expected len to be %v, but got %v", len(seed), db.Len())
	}
	return db
}

func checkDatabase(t testing.TB, db TDB, expected map[string]string) {
	t.Helper()
	if db.Len() != int64(len(expected)) {
		t.Errorf("Expected len to be %v, but got %v", len(expected), db.Len())
	}
	for k, v := range expected {
		got, err := db.Get([]byte(k))
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		if string(got) != v {
			t.Errorf("Expected value to be %v, but got %v", v, got)
		}
	}
}
