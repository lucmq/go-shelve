package shelvetest

import (
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

// Test Suite

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

		if db.Len() != 0 {
			t.Errorf("Expected len to be 0, but got %v", db.Len())
		}
	})

	t.Run("Non-empty", func(t *testing.T) {
		db, err := T.Open()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
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

		// Assert
		checkDatabase(t, db, seed)
	})

	t.Run("Put concurrent", func(t *testing.T) {
		// Arrange
		seed := make(map[string]string)
		for i := 0; i < 100; i++ {
			key := []byte("key-" + strconv.Itoa(i))
			value := []byte("value-" + strconv.Itoa(i))
			seed[string(key)] = string(value)
		}
		db := StartDatabase(t, T.Open, nil)

		// Act
		var wg sync.WaitGroup
		for k, v := range seed {
			key, value := k, v // Capture
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := db.Put([]byte(key), []byte(value))
				if err != nil {
					t.Errorf("Expected no error, but got %v", err)
				}
			}()
		}
		wg.Wait()

		// Assert
		checkDatabase(t, db, seed)
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

		// Act / Assert
		err := db.Items([]byte{}, 1, func(k, v []byte) (bool, error) {
			return true, TestError
		})
		if !errors.Is(err, TestError) {
			t.Errorf("Expected %v, but got %v", TestError, err)
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
