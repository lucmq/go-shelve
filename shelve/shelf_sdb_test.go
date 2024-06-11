package shelve

import (
	"math/rand"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/lucmq/go-shelve/sdb"
)

type TestStruct struct {
	S string

	I64 int64
	I8  int8

	U64 uint64
	U8  uint8

	F64 float64
	F32 float32

	B bool

	StrSlice []string
	I16Slice []int16
	U64slice []uint64
	U8slice  []uint8
	BSlice   []bool

	IPtrSlice []*int64

	MStrI64 map[string]int64
}

func MakeTestStruct() TestStruct {
	rand.Seed(time.Now().UnixNano())
	i64 := int64(123)
	return TestStruct{
		S:   randString(10),
		I64: rand.Int63(),
		I8:  int8(rand.Int31n(127)),
		U64: rand.Uint64(),
		U8:  uint8(rand.Uint32()),
		F64: rand.Float64(),
		F32: float32(rand.Float64()),
		B:   rand.Intn(2) == 0,
		StrSlice: []string{
			randString(5), randString(5), randString(5),
		},
		I16Slice: []int16{
			int16(rand.Int31n(32767)),
			int16(rand.Int31n(32767)),
			int16(rand.Int31n(32767)),
		},
		U64slice: []uint64{
			rand.Uint64(), rand.Uint64(), rand.Uint64(),
		},
		U8slice: []uint8{
			uint8(rand.Uint32()), uint8(rand.Uint32()), uint8(rand.Uint32()),
		},
		BSlice: []bool{
			rand.Intn(2) == 0, rand.Intn(2) == 0, rand.Intn(2) == 0,
		},
		IPtrSlice: []*int64{
			&i64, &i64, &i64,
		},
		MStrI64: map[string]int64{
			"a": rand.Int63(), "b": rand.Int63(), "c": rand.Int63(),
		},
	}
}

func randString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func OpenTestShelf[K comparable, V any](t *testing.T) *Shelf[K, V] {
	t.Helper()
	path := TestDirectory

	// Clean-up the shelf directory
	err := os.RemoveAll(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove shelf: %s", err)
	}

	db, err := sdb.Open(path)
	if err != nil {
		t.Fatalf("open db: %s", err)
	}

	shelf, err := Open[K, V](
		path,
		WithDatabase(db),
		WithCodec(&gobCodec{}),
	)
	if err != nil {
		t.Fatalf("open shelf: %s", err)
	}
	return shelf
}

// Tests

// TestShelf_SDB runs the tests in `ShelfSDBTests` with multiple types.
func TestShelf_SDB(t *testing.T) {
	t.Run("Shelf[string, string]", func(t *testing.T) {
		keys := []string{"key-1", "key-2", "key-3", "key-4"}
		values := []string{"value-1", "value-2", "value-3", "value-4"}

		shelf := OpenTestShelf[string, string]

		ShelfSDBTests[string, string](t, shelf, keys, values)
		ShelfSDBTests_Iteration[string, string](t, shelf, keys, values)
	})

	t.Run("Shelf[string, int64]", func(t *testing.T) {
		keys := []string{"key-1", "key-2", "key-3", "key-4"}
		values := []int64{1, 2, 3, 4}

		shelf := OpenTestShelf[string, int64]

		ShelfSDBTests[string, int64](t, shelf, keys, values)
		ShelfSDBTests_Iteration[string, int64](t, shelf, keys, values)
	})

	t.Run("Shelf[int64, string]", func(t *testing.T) {
		keys := []int64{1, 2, 3, 4}
		values := []string{"value-1", "value-2", "value-3", "value-4"}

		shelf := OpenTestShelf[int64, string]

		ShelfSDBTests[int64, string](t, shelf, keys, values)
		ShelfSDBTests_Iteration[int64, string](t, shelf, keys, values)
	})

	t.Run("Shelf[string, TestStruct]", func(t *testing.T) {
		keys := []string{"key-1", "key-2", "key-3", "key-4"}
		values := []TestStruct{
			MakeTestStruct(), MakeTestStruct(),
			MakeTestStruct(), MakeTestStruct(),
		}

		shelf := OpenTestShelf[string, TestStruct]

		ShelfSDBTests[string, TestStruct](t, shelf, keys, values)
		ShelfSDBTests_Iteration[string, TestStruct](t, shelf, keys, values)
	})

	t.Run("Shelf[string, *TestStruct]", func(t *testing.T) {
		var values []*TestStruct
		keys := []string{"key-1", "key-2", "key-3", "key-4"}
		for i := 0; i < 4; i++ {
			v := MakeTestStruct()
			values = append(values, &v)
		}

		shelf := OpenTestShelf[string, *TestStruct]

		ShelfSDBTests[string, *TestStruct](t, shelf, keys, values)
		ShelfSDBTests_Iteration[string, *TestStruct](t, shelf, keys, values)
	})

	t.Run("Shelf[int64, TestStruct]", func(t *testing.T) {
		keys := []int64{1, 2, 3, 4}
		values := []TestStruct{
			MakeTestStruct(), MakeTestStruct(),
			MakeTestStruct(), MakeTestStruct(),
		}

		shelf := OpenTestShelf[int64, TestStruct]

		ShelfSDBTests[int64, TestStruct](t, shelf, keys, values)
		ShelfSDBTests_Iteration[int64, TestStruct](t, shelf, keys, values)
	})

	t.Run("Shelf[int64, *TestStruct]", func(t *testing.T) {
		var values []*TestStruct
		keys := []int64{1, 2, 3, 4}
		for i := 0; i < 4; i++ {
			v := MakeTestStruct()
			values = append(values, &v)
		}

		shelf := OpenTestShelf[int64, *TestStruct]

		ShelfSDBTests[int64, *TestStruct](t, shelf, keys, values)
		ShelfSDBTests_Iteration[int64, *TestStruct](t, shelf, keys, values)
	})

	t.Run("Shelf[ByteArray, TestStruct]", func(t *testing.T) {
		// Tests that we can use go-shelf with ID generators that return
		// byte arrays (12 bytes is the size of a xID).
		keys := [][12]byte{
			{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C},
			{0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18},
			{0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20, 0x21, 0x22, 0x23, 0x24},
			{0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30},
		}
		values := []TestStruct{
			MakeTestStruct(), MakeTestStruct(), MakeTestStruct(), MakeTestStruct(),
		}

		shelf := OpenTestShelf[[12]byte, TestStruct]

		ShelfSDBTests[[12]byte, TestStruct](t, shelf, keys, values)
		ShelfSDBTests_Iteration[[12]byte, TestStruct](t, shelf, keys, values)
	})
}

// ShelfSDBTests provides a set of tests for Shelf with the sdb database. The
// keys and values provided must be of the same length and have at least 4
// elements.
func ShelfSDBTests[K comparable, V any](
	t *testing.T,
	open func(t *testing.T) *Shelf[K, V],
	keys []K,
	values []V,
) {
	// Before starting, validate the test input parameters
	if open == nil {
		t.Fatalf("expected open func, but got nil")
	}
	if len(keys) != len(values) {
		t.Fatalf("expected %d keys, but got %d", len(keys), len(values))
	}
	if len(keys) < 4 {
		t.Fatalf("expected at least 4 keys, but got %d", len(keys))
	}

	t.Run("Put", func(t *testing.T) {
		// Arrange
		shelf := open(t)
		expected := values[0]

		// Act
		if err := shelf.Put(keys[0], expected); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		value, ok, err := shelf.Get(keys[0])
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		if !ok {
			t.Errorf("Expected key %v to exist", keys[0])
		}
		if !reflect.DeepEqual(value, expected) {
			t.Errorf("Expected %v, but got %v", expected, value)
		}
	})

	t.Run("Put existing key", func(t *testing.T) {
		// Arrange
		shelf := open(t)
		key := keys[0]
		value := values[0]
		newValue := values[1]

		if err := shelf.Put(key, value); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Act
		if err := shelf.Put(key, newValue); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		value, ok, err := shelf.Get(key)
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		if !ok {
			t.Errorf("Expected key %v to exist", key)
		}
		if !reflect.DeepEqual(value, newValue) {
			t.Errorf("Expected %v, but got %v", newValue, value)
		}
	})

	t.Run("Len", func(t *testing.T) {
		// Arrange
		shelf := open(t)

		if err := shelf.Put(keys[0], values[0]); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Act
		length := shelf.Len()

		// Assert
		if length != 1 {
			t.Errorf("Expected length to be 1, but got %v", length)
		}
	})

	t.Run("Has", func(t *testing.T) {
		// Arrange
		shelf := open(t)
		key := keys[0]

		if err := shelf.Put(key, values[0]); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Act
		has, err := shelf.Has(key)
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
		shelf := open(t)

		if err := shelf.Put(keys[0], values[0]); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Act
		has, err := shelf.Has(keys[1])
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		if has {
			t.Errorf("Expected has to be false, but got %v", has)
		}
	})

	t.Run("Get", func(t *testing.T) {
		// Arrange
		shelf := open(t)
		key := keys[0]
		expected := values[0]

		if err := shelf.Put(key, expected); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Act
		value, ok, err := shelf.Get(key)
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		if !ok {
			t.Errorf("Expected key %v to exist", key)
		}
		if !reflect.DeepEqual(value, expected) {
			t.Errorf("Expected %v, but got %v", expected, value)
		}
	})

	t.Run("Get non-existing key", func(t *testing.T) {
		// Arrange
		shelf := open(t)
		expected := values[0]

		if err := shelf.Put(keys[0], expected); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Act
		value, ok, err := shelf.Get(keys[1])
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		if ok {
			t.Errorf("Expected key %v to not exist", keys[1])
		}
		var zero V
		if !reflect.DeepEqual(value, zero) {
			t.Errorf("Expected value to be zero, but got %v", value)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		// Arrange
		shelf := open(t)
		key, value := keys[0], values[0]

		if err := shelf.Put(key, value); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Act
		if err := shelf.Delete(key); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		value, ok, err := shelf.Get(key)
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		if ok {
			t.Errorf("Expected key %v to not exist", key)
		}
		var zero V
		if !reflect.DeepEqual(value, zero) {
			t.Errorf("Expected value to be zero, but got %v", value)
		}
	})

	t.Run("Delete non-existing key", func(t *testing.T) {
		// Arrange
		shelf := open(t)

		if err := shelf.Put(keys[0], values[0]); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Act
		if err := shelf.Delete(keys[1]); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		value, ok, err := shelf.Get(keys[1])
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		if ok {
			t.Errorf("Expected key %v to not exist", keys[1])
		}
		var zero V
		if !reflect.DeepEqual(value, zero) {
			t.Errorf("Expected value to be zero, but got %v", value)
		}
	})

	t.Run("Close", func(t *testing.T) {
		// Arrange
		shelf := open(t)

		if err := shelf.Put(keys[0], values[0]); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Act
		if err := shelf.Close(); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
	})

	t.Run("Sync", func(t *testing.T) {
		// Arrange
		shelf := open(t)

		if err := shelf.Put(keys[0], values[0]); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Act
		if err := shelf.Sync(); err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
	})
}

// ShelfSDBTests_Iteration provides a set of tests for Shelf with the sdb
// database. This tests specifically the Items, Keys and Values methods. The
// keys and values provided must be of the same length and have at least 4
// elements.
func ShelfSDBTests_Iteration[K comparable, V any](
	t *testing.T,
	open func(t *testing.T) *Shelf[K, V],
	keys []K,
	values []V,
) {
	// Before starting, validate the test input parameters
	if open == nil {
		t.Fatalf("expected open func, but got nil")
	}
	if len(keys) != len(values) {
		t.Fatalf("expected %d keys, but got %d", len(keys), len(values))
	}
	if len(keys) < 4 {
		t.Fatalf("expected at least 4 keys, but got %d", len(keys))
	}

	t.Run("Items", func(t *testing.T) {
		// Arrange
		shelf := open(t)
		items := make(map[K]V)
		for i := 0; i < len(keys); i++ {
			if err := shelf.Put(keys[i], values[i]); err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
			items[keys[i]] = values[i]
		}
		var start K

		// Act
		var (
			actualItems = make(map[K]V)
		)
		err := shelf.Items(&start, All, Asc, func(key K, value V) (
			bool, error,
		) {
			actualItems[key] = value
			return true, nil
		})
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		if !reflect.DeepEqual(actualItems, items) {
			t.Errorf("Expected items to be %v, but got %v", items, actualItems)
		}
	})

	t.Run("Items subset", func(t *testing.T) {
		// Arrange
		shelf := open(t)
		for i := 0; i < len(keys); i++ {
			if err := shelf.Put(keys[i], values[i]); err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
		}
		var start K
		n := 2

		// Act
		var (
			actualKeys   []K
			actualValues []V
		)
		err := shelf.Items(&start, n, Asc, func(key K, value V) (
			bool, error,
		) {
			actualKeys = append(actualKeys, key)
			actualValues = append(actualValues, value)
			return true, nil
		})
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		for _, key := range actualKeys {
			if !slices.Contains(keys, key) {
				t.Errorf("Expected key to be %v, but got %v", keys, key)
			}
		}
		if len(actualKeys) != n {
			t.Errorf("Expected %v keys, but got %v", n, len(actualKeys))
		}
	})

	t.Run("Keys", func(t *testing.T) {
		// Arrange
		shelf := open(t)
		m := make(map[K]struct{})
		for i := 0; i < len(keys); i++ {
			if err := shelf.Put(keys[i], values[i]); err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
			m[keys[i]] = struct{}{}
		}
		var start K

		// Act
		var (
			actualKeys = make(map[K]struct{})
		)
		err := shelf.Items(&start, All, Asc, func(key K, value V) (
			bool, error,
		) {
			actualKeys[key] = struct{}{}
			return true, nil
		})
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		if !reflect.DeepEqual(actualKeys, m) {
			t.Errorf("Expected keys to be %v, but got %v", m, actualKeys)
		}
	})

	t.Run("Keys subset", func(t *testing.T) {
		// Arrange
		shelf := open(t)
		for i := 0; i < len(keys); i++ {
			if err := shelf.Put(keys[i], values[i]); err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
		}
		var start K
		n := 2

		// Act
		var (
			actualKeys []K
		)
		err := shelf.Items(&start, n, Asc, func(key K, _ V) (
			bool, error,
		) {
			actualKeys = append(actualKeys, key)
			return true, nil
		})
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		for _, key := range actualKeys {
			if !slices.Contains(keys, key) {
				t.Errorf("Expected key to be %v, but got %v", keys, key)
			}
		}
		if len(actualKeys) != n {
			t.Errorf("Expected %v keys, but got %v", n, len(actualKeys))
		}
	})

	t.Run("Values", func(t *testing.T) {
		// Arrange
		shelf := open(t)
		for i := 0; i < len(keys); i++ {
			if err := shelf.Put(keys[i], values[i]); err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
		}
		var start K

		// Act
		var (
			actualValues []V
		)
		err := shelf.Items(&start, All, Asc, func(key K, value V) (
			bool, error,
		) {
			actualValues = append(actualValues, value)
			return true, nil
		})
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		if len(actualValues) != len(values) {
			t.Errorf("Expected %v values, but got %v",
				len(values), len(actualValues))
		}
	})

	t.Run("Values subset", func(t *testing.T) {
		// Arrange
		shelf := open(t)
		for i := 0; i < len(keys); i++ {
			if err := shelf.Put(keys[i], values[i]); err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
		}
		var start K
		n := 2

		// Act
		var (
			actualValues []V
		)
		err := shelf.Items(&start, n, Asc, func(_ K, value V) (
			bool, error,
		) {
			actualValues = append(actualValues, value)
			return true, nil
		})
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}

		// Assert
		if len(actualValues) != n {
			t.Errorf("Expected %v values, but got %v", n, len(actualValues))
		}
	})
}
