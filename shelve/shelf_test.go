package shelve

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Helpers

var (
	TestDirectory = filepath.Join(os.TempDir(), "go-shelve")
	TestError     = errors.New("test error")
)

// YieldData is a syntactic sugar for the DB.Items callback
// to use when testing.
type YieldData = func(key, value []byte) (bool, error)

// Item is a struct used for testing.
type Item struct{ Key, Value string }

// NewTestShelf creates a new Shelf for testing, with basic mocks already
// set up.
//
// By default, a MockDB is used, with the default mock behavior.
//
// For the Codec, a customized MockCodec that adds the "encoded_" prefix to
// the values is used.
//
// It is possible to override the mocks by passing in options.
func NewTestShelf(t *testing.T, opts ...Option) *Shelf[string, string] {
	t.Helper()

	// Create default mocks
	var db MockDB
	var codec MockCodec
	codec.EncodeFunc = func(x any) ([]byte, error) {
		s := x.(string)
		return []byte("encoded_" + s), nil
	}
	codec.DecodeFunc = func(data []byte, value any) error {
		p := value.(*string)
		*p = strings.TrimPrefix(string(data), "encoded_")
		return nil
	}

	// Set up the Shelf options, possibly overriding the default mocks
	opts = append(
		[]Option{
			WithDatabase(&db),
			WithCodec(&codec),
			WithKeyCodec(&codec),
		},
		opts...,
	)

	s, err := Open[string, string](TestDirectory, opts...)
	if err != nil {
		t.Fatalf("Failed to open shelf: %v", err)
	}
	return s
}

// MakeItems returns a slice of Item with the given seed. The map keys are used
// as the Item.Key and the map values as the Item.Value.
func MakeItems(seed map[string]string) []Item {
	items := make([]Item, 0, len(seed))
	for k, v := range seed {
		items = append(items, Item{Key: k, Value: v})
	}
	return items
}

// NewMockItemsFunc returns an ItemsFunc that can be used in tests. The seed
// data is used as the database data and is yielded in sorted order.
func NewMockItemsFunc(seed []Item) func(
	start []byte,
	order int,
	fn YieldData,
) error {
	return func(start []byte, order int, fn YieldData) error {
		var errs error
		if order == Asc {
			sort.Slice(seed, func(i, j int) bool {
				return seed[i].Key < seed[j].Key
			})
		} else {
			sort.Slice(seed, func(i, j int) bool {
				return seed[i].Value > seed[j].Value
			})
		}
		for _, i := range seed {
			key, value := "encoded_"+i.Key, "encoded_"+i.Value

			// Discard items until start
			if order == Asc && key < string(start) {
				continue
			}
			if order == Desc && start != nil && key > string(start) {
				continue
			}
			// Call the user-provided function
			ok, err := fn([]byte(key), []byte(value))
			if err != nil {
				errs = errors.Join(errs, err)
			}
			if !ok {
				// Stop iteration
				break
			}
		}
		return errs
	}
}

// Tests

func TestShelf_Open(t *testing.T) {
	t.Run("Default options", func(t *testing.T) {
		path := TestDirectory
		shelf, err := Open[string, int](path)
		if err != nil {
			t.Errorf("Error opening shelf: %v", err)
		}
		if shelf == nil {
			t.Errorf("Expected shelf to be non-nil")
		}
		if shelf.db == nil {
			t.Errorf("Expected shelf.db to be non-nil")
		}
		if shelf.codec == nil {
			t.Errorf("Expected shelf.codec to be non-nil")
		}
	})

	t.Run("Custom options", func(t *testing.T) {
		customDB := &MockDB{}
		customCodec := &MockCodec{}
		shelf, err := Open[string, int](
			TestDirectory,
			WithDatabase(customDB),
			WithCodec(customCodec),
		)
		if err != nil {
			t.Errorf("Error opening shelf: %v", err)
		}
		if shelf == nil {
			t.Errorf("Expected shelf to be non-nil")
		}
		if shelf.db != customDB {
			t.Errorf("Expected shelf.db to be customDB")
		}
		if shelf.codec != customCodec {
			t.Errorf("Expected shelf.codec to be customCodec")
		}
	})

	t.Run("Error opening default database", func(t *testing.T) {
		// Keep the path empty to trigger an error
		shelf, err := Open[string, int]("")
		if err == nil {
			t.Errorf("Expected error opening database, but got nil")
		}
		if shelf != nil {
			t.Errorf("Expected shelf to be nil")
		}
	})
}

func TestShelf_DefaultKeyCodec(t *testing.T) {
	t.Run("Int keys", func(t *testing.T) {
		shelf, _ := Open[int, struct{}](TestDirectory)
		if _, ok := shelf.keyCodec.(stringCodec); !ok {
			t.Errorf("Expected key codec to be stringCodec")
		}
	})

	t.Run("Int64 keys", func(t *testing.T) {
		shelf, _ := Open[int, struct{}](TestDirectory)
		if _, ok := shelf.keyCodec.(stringCodec); !ok {
			t.Errorf("Expected key codec to be stringCodec")
		}
	})

	t.Run("Uint keys", func(t *testing.T) {
		shelf, _ := Open[uint, struct{}](TestDirectory)
		if _, ok := shelf.keyCodec.(stringCodec); !ok {
			t.Errorf("Expected key codec to be stringCodec")
		}
	})

	t.Run("Uint64 keys", func(t *testing.T) {
		shelf, _ := Open[uint64, struct{}](TestDirectory)
		if _, ok := shelf.keyCodec.(stringCodec); !ok {
			t.Errorf("Expected key codec to be stringCodec")
		}
	})

	t.Run("String keys", func(t *testing.T) {
		shelf, _ := Open[string, struct{}](TestDirectory)
		if _, ok := shelf.keyCodec.(stringCodec); !ok {
			t.Errorf("Expected key codec to be stringCodec")
		}
	})

	t.Run("Struct keys", func(t *testing.T) {
		shelf, _ := Open[struct{}, struct{}](TestDirectory)
		if _, ok := shelf.keyCodec.(gobCodec); !ok {
			t.Errorf("Expected key codec to be gobCodec")
		}
	})
}

func TestShelf_Close(t *testing.T) {
	t.Run("Close succeeds", func(t *testing.T) {
		shelf, _ := Open[string, int](
			TestDirectory,
			WithDatabase(&MockDB{}),
		)
		err := shelf.Close()
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
	})
}

func TestShelf_Len(t *testing.T) {
	t.Run("Len succeeds", func(t *testing.T) {
		var db MockDB
		db.LenFunc = func() int64 {
			return 10
		}
		shelf, _ := Open[string, int](
			TestDirectory,
			WithDatabase(&db),
		)

		length := shelf.Len()

		if length != 10 {
			t.Errorf("Expected length to be 10, but got %d", length)
		}
	})
}

func TestShelf_Sync(t *testing.T) {
	t.Run("Sync succeeds", func(t *testing.T) {
		var db MockDB
		db.SyncFunc = func() error {
			return nil
		}
		shelf, _ := Open[string, int](
			TestDirectory,
			WithDatabase(&db),
		)

		err := shelf.Sync()

		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
	})
}

func TestShelf_Has(t *testing.T) {
	t.Run("Has succeeds", func(t *testing.T) {
		var db MockDB
		db.HasFunc = func(key []byte) (bool, error) {
			return true, nil
		}
		shelf, _ := Open[string, int](
			TestDirectory,
			WithDatabase(&db),
		)

		has, err := shelf.Has("key")

		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		if !has {
			t.Errorf("Expected has to be true, but got %v", has)
		}
	})

	t.Run("Has fails", func(t *testing.T) {
		var db MockDB
		db.HasFunc = func(key []byte) (bool, error) {
			return false, nil
		}
		shelf, _ := Open[string, int](
			TestDirectory,
			WithDatabase(&db),
		)

		has, err := shelf.Has("key")

		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		if has {
			t.Errorf("Expected has to be false, but got %v", has)
		}
	})

	t.Run("DB error", func(t *testing.T) {
		var db MockDB
		db.HasFunc = func(key []byte) (bool, error) {
			return false, TestError
		}
		shelf, _ := Open[string, int](
			TestDirectory,
			WithDatabase(&db),
		)

		has, err := shelf.Has("key")

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
		if has {
			t.Errorf("Expected has to be false, but got %v", has)
		}
	})

	t.Run("Codec error", func(t *testing.T) {
		var db MockDB
		var codec MockCodec
		codec.EncodeFunc = func(key any) ([]byte, error) {
			return nil, TestError
		}
		shelf, _ := Open[string, int](
			TestDirectory,
			WithDatabase(&db),
			WithCodec(&codec),
			WithKeyCodec(&codec),
		)

		has, err := shelf.Has("key")

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
		if has {
			t.Errorf("Expected has to be false, but got %v", has)
		}
	})
}

func TestShelf_Get(t *testing.T) {
	t.Run("Get succeeds", func(t *testing.T) {
		var db MockDB
		db.GetFunc = func(key []byte) ([]byte, error) {
			return []byte("value"), nil
		}
		shelf := NewTestShelf(t, WithDatabase(&db))

		key := "key"
		value, ok, err := shelf.Get(key)

		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		if !ok {
			t.Errorf("Expected key %v to exist", key)
		}
		if value != "value" {
			t.Errorf("Expected value to be 'value', but got %v", value)
		}
	})

	t.Run("Get fails", func(t *testing.T) {
		var db MockDB
		db.GetFunc = func(key []byte) ([]byte, error) {
			return nil, TestError
		}
		shelf := NewTestShelf(t, WithDatabase(&db))

		key := "key"
		value, ok, err := shelf.Get("key")

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
		if ok {
			t.Errorf("Expected key %v to not exist", key)
		}
		if value != "" {
			t.Errorf("Expected value to be '', but got %v", value)
		}
	})

	t.Run("DB error", func(t *testing.T) {
		var db MockDB
		db.GetFunc = func(key []byte) ([]byte, error) {
			return nil, TestError
		}
		shelf := NewTestShelf(t, WithDatabase(&db))

		key := "key"
		value, ok, err := shelf.Get(key)

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
		if ok {
			t.Errorf("Expected key %v to not exist", key)
		}
		if value != "" {
			t.Errorf("Expected value to be '', but got %v", value)
		}
	})

	t.Run("Codec error", func(t *testing.T) {
		var codec MockCodec
		codec.EncodeFunc = func(key any) ([]byte, error) {
			return nil, TestError
		}
		shelf := NewTestShelf(t, WithKeyCodec(&codec))

		key := "key"
		value, ok, err := shelf.Get("key")

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
		if ok {
			t.Errorf("Expected key %v to not exist", key)
		}
		if value != "" {
			t.Errorf("Expected value to be '', but got %v", value)
		}
	})
}

func TestShelf_Put(t *testing.T) {
	t.Run("Put succeeds", func(t *testing.T) {
		shelf := NewTestShelf(t)

		err := shelf.Put("key", "value")

		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
	})

	t.Run("Encode key error", func(t *testing.T) {
		var codec MockCodec
		codec.EncodeFunc = func(value any) ([]byte, error) {
			return nil, TestError
		}
		shelf := NewTestShelf(t, WithKeyCodec(&codec))

		err := shelf.Put("key", "value")

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})

	t.Run("Encode value error", func(t *testing.T) {
		var codec MockCodec
		codec.EncodeFunc = func(value any) ([]byte, error) {
			return nil, TestError
		}
		shelf := NewTestShelf(t, WithCodec(&codec))

		err := shelf.Put("key", "value")

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})

	t.Run("DB error", func(t *testing.T) {
		var db MockDB
		db.PutFunc = func(key []byte, value []byte) error {
			return TestError
		}
		shelf := NewTestShelf(t, WithDatabase(&db))

		err := shelf.Put("key", "value")

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})
}

func TestShelf_Delete(t *testing.T) {
	t.Run("Delete succeeds", func(t *testing.T) {
		shelf := NewTestShelf(t)

		err := shelf.Delete("key")

		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
	})

	t.Run("DB error", func(t *testing.T) {
		var db MockDB
		db.DeleteFunc = func(key []byte) error {
			return TestError
		}
		shelf := NewTestShelf(t, WithDatabase(&db))

		err := shelf.Delete("key")

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})

	t.Run("Encode key error", func(t *testing.T) {
		var codec MockCodec
		codec.EncodeFunc = func(value any) ([]byte, error) {
			return nil, TestError
		}
		shelf := NewTestShelf(t, WithKeyCodec(&codec))

		err := shelf.Delete("key")

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})
}

func TestShelf_Items(t *testing.T) {
	seed := []Item{
		{"key-01", "value-01"}, {"key-02", "value-02"},
		{"key-03", "value-03"}, {"key-04", "value-04"},
		{"key-05", "value-05"}, {"key-06", "value-06"},
		{"key-07", "value-07"}, {"key-08", "value-08"},
		{"key-09", "value-09"},
	}
	type args struct {
		start int
		n     int
		step  int
	}
	tests := []struct {
		args args
		want []int
	}{
		// Step = 0
		{args{0, All, 0}, []int{}},

		// All items, Ascendant (Step = 1)
		{args{0, All, Asc}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{args{1, All, Asc}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{args{5, All, Asc}, []int{5, 6, 7, 8, 9}},
		{args{9, All, Asc}, []int{9}},
		{args{10, All, Asc}, []int{}},

		// All items, Ascendant (Step = 2)
		{args{0, All, 2}, []int{1, 3, 5, 7, 9}},
		{args{1, All, 2}, []int{1, 3, 5, 7, 9}},
		{args{5, All, 2}, []int{5, 7, 9}},
		{args{9, All, 2}, []int{9}},
		{args{10, All, 2}, []int{}},

		// All items, Ascendant (Step = 3)
		{args{0, All, 3}, []int{1, 4, 7}},
		{args{1, All, 3}, []int{1, 4, 7}},
		{args{5, All, 3}, []int{5, 8}},
		{args{9, All, 3}, []int{9}},
		{args{10, All, 3}, []int{}},

		// All items, Descendant (Step = -1)
		{args{10, All, Desc}, []int{9, 8, 7, 6, 5, 4, 3, 2, 1}},
		{args{9, All, Desc}, []int{9, 8, 7, 6, 5, 4, 3, 2, 1}},
		{args{5, All, Desc}, []int{5, 4, 3, 2, 1}},
		{args{1, All, Desc}, []int{1}},
		{args{0, All, Desc}, []int{}},

		// All items, Descendant (Step = -3)
		{args{10, All, -3}, []int{9, 6, 3}},
		{args{9, All, -3}, []int{9, 6, 3}},
		{args{5, All, -3}, []int{5, 2}},
		{args{1, All, -3}, []int{1}},
		{args{0, All, -3}, []int{}},

		// 3 items, Ascendant (Step = 1)
		{args{0, 3, Asc}, []int{1, 2, 3}},
		{args{1, 3, Asc}, []int{1, 2, 3}},
		{args{5, 3, Asc}, []int{5, 6, 7}},
		{args{9, 3, Asc}, []int{9}},
		{args{10, 3, Asc}, []int{}},

		// 3 items, Ascendant (Step = 3)
		{args{0, 3, 3}, []int{1, 4, 7}},
		{args{1, 3, 3}, []int{1, 4, 7}},
		{args{5, 3, 3}, []int{5, 8}},
		{args{9, 3, 3}, []int{9}},
		{args{10, 3, 3}, []int{}},

		// 3 items, Descendant (Step = -1)
		{args{10, 3, Desc}, []int{9, 8, 7}},
		{args{9, 3, Desc}, []int{9, 8, 7}},
		{args{5, 3, Desc}, []int{5, 4, 3}},
		{args{1, 3, Desc}, []int{1}},
		{args{0, 3, Desc}, []int{}},

		// 3 items, Descendant (Step = -3)
		{args{10, 3, -3}, []int{9, 6, 3}},
		{args{9, 3, -3}, []int{9, 6, 3}},
		{args{5, 3, -3}, []int{5, 2}},
		{args{1, 3, -3}, []int{1}},
		{args{0, 3, -3}, []int{}},
	}
	for _, tt := range tests {
		t.Run("Items succeeds", func(t *testing.T) {
			var db MockDB
			db.ItemsFunc = NewMockItemsFunc(seed)

			shelf := NewTestShelf(t, WithDatabase(&db))

			start := fmt.Sprintf("key-%02d", tt.args.start)
			n := tt.args.n
			step := tt.args.step

			var expectedItems []Item
			for _, i := range tt.want {
				key := fmt.Sprintf("key-%02d", i)
				value := fmt.Sprintf("value-%02d", i)
				item := Item{Key: key, Value: value}
				expectedItems = append(expectedItems, item)
			}

			var gotItems []Item
			err := shelf.Items(&start, n, step, func(key, value string) (
				bool, error,
			) {
				item := Item{Key: key, Value: value}
				gotItems = append(gotItems, item)
				return true, nil
			})

			if err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
			if !reflect.DeepEqual(gotItems, expectedItems) {
				t.Errorf("Expected %v, but got %v", expectedItems, gotItems)
			}
		})
	}
}

func TestShelf_Items_Error(t *testing.T) {
	t.Run("DB error", func(t *testing.T) {
		var db MockDB
		db.ItemsFunc = func(start []byte, order int, fn YieldData) error {
			return TestError
		}
		shelf := NewTestShelf(t, WithDatabase(&db))
		start := ""

		err := shelf.Items(&start, All, Asc, func(key, value string) (
			bool, error,
		) {
			return true, nil
		})

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})

	t.Run("Yield error", func(t *testing.T) {
		var db MockDB
		items := MakeItems(map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		})
		db.ItemsFunc = NewMockItemsFunc(items)
		shelf := NewTestShelf(t, WithDatabase(&db))
		start := ""

		err := shelf.Items(&start, All, Asc, func(key, value string) (
			bool, error,
		) {
			return true, TestError
		})

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})

	t.Run("Encode start key error", func(t *testing.T) {
		var db MockDB
		var codec MockCodec
		start := "key-1"
		codec.EncodeFunc = func(x any) ([]byte, error) {
			if x.(string) == start {
				return nil, TestError
			}
			return []byte(x.(string)), nil
		}
		shelf := NewTestShelf(t, WithDatabase(&db), WithKeyCodec(&codec))

		err := shelf.Items(&start, All, Asc, func(key, value string) (
			bool, error,
		) {
			return true, nil
		})

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})

	t.Run("Decode key error", func(t *testing.T) {
		var db MockDB
		var codec MockCodec
		seed := MakeItems(map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		})
		db.ItemsFunc = NewMockItemsFunc(seed)
		codec.DecodeFunc = func(data []byte, value any) error {
			return TestError
		}
		shelf := NewTestShelf(t, WithDatabase(&db), WithKeyCodec(&codec))
		start := ""

		err := shelf.Items(&start, All, Asc, func(key, value string) (
			bool, error,
		) {
			return true, nil
		})

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})

	t.Run("Decode value error", func(t *testing.T) {
		var db MockDB
		var codec MockCodec
		seed := MakeItems(map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		})
		db.ItemsFunc = NewMockItemsFunc(seed)
		codec.DecodeFunc = func(data []byte, value any) error {
			return TestError
		}
		shelf := NewTestShelf(t, WithDatabase(&db), WithCodec(&codec))
		start := ""

		err := shelf.Items(&start, All, Asc, func(key, value string) (
			bool, error,
		) {
			return true, nil
		})

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})
}

func TestShelf_Keys(t *testing.T) {
	seed := MakeItems(map[string]string{
		"key-1": "value-1", "key-2": "value-2",
		"key-3": "value-3", "key-4": "value-4",
	})
	tests := []struct {
		name         string
		start        string
		hasStart     bool
		n            int
		order        int
		expectedKeys []string
	}{
		{
			name:         "All keys",
			start:        "",
			n:            All,
			order:        Asc,
			expectedKeys: []string{"key-1", "key-2", "key-3", "key-4"},
		},
		{
			name:         "All keys - Descending",
			start:        "",
			n:            All,
			order:        Desc,
			expectedKeys: []string{"key-4", "key-3", "key-2", "key-1"},
		},
		{
			name:         "First subset",
			start:        "key-2",
			hasStart:     true,
			n:            All,
			order:        Asc,
			expectedKeys: []string{"key-2", "key-3", "key-4"},
		},
		{
			name:         "Second subset",
			start:        "",
			n:            3,
			order:        Asc,
			expectedKeys: []string{"key-1", "key-2", "key-3"},
		},
		{
			name:         "Third subset",
			start:        "key-2",
			hasStart:     true,
			n:            2,
			order:        Asc,
			expectedKeys: []string{"key-2", "key-3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var db MockDB
			db.ItemsFunc = NewMockItemsFunc(seed)
			shelf := NewTestShelf(t, WithDatabase(&db))
			var start *string
			s := tt.start
			if tt.hasStart {
				start = &s
			}

			gotKeys := make([]string, 0)
			err := shelf.Keys(start, tt.n, tt.order, func(key, value string) (
				bool, error,
			) {
				gotKeys = append(gotKeys, key)
				return true, nil
			})

			if err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
			if !reflect.DeepEqual(gotKeys, tt.expectedKeys) {
				t.Errorf("Expected keys %v, but got %v",
					tt.expectedKeys, gotKeys)
			}
		})
	}
}

func TestShelf_Keys_Error(t *testing.T) {
	t.Run("DB Items error", func(t *testing.T) {
		var db MockDB
		db.ItemsFunc = func(start []byte, order int, fn YieldData) error {
			return TestError
		}
		shelf := NewTestShelf(t, WithDatabase(&db))
		start := ""

		err := shelf.Keys(&start, All, Asc, func(key, value string) (
			bool, error,
		) {
			return true, nil
		})

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})

	t.Run("Encode start key error", func(t *testing.T) {
		var db MockDB
		var codec MockCodec
		start := "key-1"
		codec.EncodeFunc = func(x any) ([]byte, error) {
			if x.(string) == start {
				return nil, TestError
			}
			return []byte(x.(string)), nil
		}
		shelf := NewTestShelf(t, WithDatabase(&db), WithKeyCodec(&codec))

		err := shelf.Keys(&start, All, Asc, func(key, value string) (
			bool, error,
		) {
			return true, nil
		})

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})

	t.Run("Decode key error", func(t *testing.T) {
		var db MockDB
		var codec MockCodec
		seed := MakeItems(map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		})
		db.ItemsFunc = NewMockItemsFunc(seed)
		codec.DecodeFunc = func(data []byte, value any) error {
			return TestError
		}
		shelf := NewTestShelf(t, WithDatabase(&db), WithKeyCodec(&codec))
		start := ""

		err := shelf.Keys(&start, All, Asc, func(key, value string) (
			bool, error,
		) {
			return true, nil
		})

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})
}

func TestShelf_Values(t *testing.T) {
	seed := MakeItems(map[string]string{
		"key-1": "value-1", "key-2": "value-2",
		"key-3": "value-3", "key-4": "value-4",
	})
	tests := []struct {
		name           string
		start          string
		hasStart       bool
		n              int
		order          int
		expectedValues []string
	}{
		{
			name:           "All values",
			start:          "",
			n:              All,
			order:          Asc,
			expectedValues: []string{"value-1", "value-2", "value-3", "value-4"},
		},
		{
			name:           "All values - Descending",
			start:          "",
			n:              All,
			order:          Desc,
			expectedValues: []string{"value-4", "value-3", "value-2", "value-1"},
		},
		{
			name:           "First subset",
			start:          "key-2",
			hasStart:       true,
			n:              All,
			order:          Asc,
			expectedValues: []string{"value-2", "value-3", "value-4"},
		},
		{
			name:           "Second subset",
			start:          "",
			n:              3,
			order:          Asc,
			expectedValues: []string{"value-1", "value-2", "value-3"},
		},
		{
			name:           "Third subset",
			start:          "key-2",
			hasStart:       true,
			n:              2,
			order:          Asc,
			expectedValues: []string{"value-2", "value-3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var db MockDB
			db.ItemsFunc = NewMockItemsFunc(seed)
			shelf := NewTestShelf(t, WithDatabase(&db))
			var start *string
			s := tt.start
			if tt.hasStart {
				start = &s
			}

			values := make([]string, 0)
			err := shelf.Values(start, tt.n, tt.order, func(key, value string) (
				bool, error,
			) {
				values = append(values, value)
				return true, nil
			})

			if err != nil {
				t.Errorf("Expected no error, but got %v", err)
			}
			if !reflect.DeepEqual(values, tt.expectedValues) {
				t.Errorf("Expected values %v, but got %v",
					tt.expectedValues, values)
			}
		})
	}
}

func TestShelf_Values_Error(t *testing.T) {
	t.Run("DB Items error", func(t *testing.T) {
		var db MockDB
		db.ItemsFunc = func(start []byte, order int, fn YieldData) error {
			return TestError
		}
		shelf := NewTestShelf(t, WithDatabase(&db))
		start := ""

		err := shelf.Values(&start, All, Asc, func(key, value string) (
			bool, error,
		) {
			return true, nil
		})

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})

	t.Run("Encode start key error", func(t *testing.T) {
		var db MockDB
		var codec MockCodec
		start := "key-1"
		codec.EncodeFunc = func(x any) ([]byte, error) {
			if x.(string) == start {
				return nil, TestError
			}
			return []byte(x.(string)), nil
		}
		shelf := NewTestShelf(t, WithDatabase(&db), WithKeyCodec(&codec))

		err := shelf.Values(&start, All, Asc, func(key, value string) (
			bool, error,
		) {
			return true, nil
		})

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})

	t.Run("Decode value error", func(t *testing.T) {
		var db MockDB
		var codec MockCodec
		seed := MakeItems(map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		})
		db.ItemsFunc = NewMockItemsFunc(seed)
		codec.DecodeFunc = func(data []byte, value any) error {
			return TestError
		}
		shelf := NewTestShelf(t, WithDatabase(&db), WithCodec(&codec))
		start := ""

		err := shelf.Values(&start, All, Asc, func(key, value string) (
			bool, error,
		) {
			return true, nil
		})

		if !errors.Is(err, TestError) {
			t.Errorf("Expected error %v, but got %v", TestError, err)
		}
	})
}
