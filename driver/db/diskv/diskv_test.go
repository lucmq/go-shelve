package diskvd

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/peterbourgon/diskv/v3"

	shelvetest "github.com/lucmq/go-shelve/driver/test"
	"github.com/lucmq/go-shelve/shelve"
)

var (
	dbPath = filepath.Join(os.TempDir(), "diskv-test")

	testError = errors.New("test error")

	recordExtension = "json"
)

type CustomIndexLen struct {
	diskv.Index
	len int
}

func (i *CustomIndexLen) Len() int { return i.len }

func OpenTestDB() (shelve.DB, error) {
	// Clean-up the database directory
	err := os.RemoveAll(dbPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return NewDefault(dbPath, "")
}

func ReopenTestDB() (shelve.DB, error) {
	return NewDefault(dbPath, "")
}

func OpenTestDBWithExtension() (shelve.DB, error) {
	// Clean-up the database directory
	err := os.RemoveAll(dbPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return NewDefault(dbPath, recordExtension)
}

func ReopenTestDBWithExtension() (shelve.DB, error) {
	return NewDefault(dbPath, recordExtension)
}

func TestDB(t *testing.T) {
	shelvetest.NewDBTests(OpenTestDB, ReopenTestDB).TestAll(t)
}

func TestDB_WithRecordExtension(t *testing.T) {
	tests := shelvetest.NewDBTests(
		OpenTestDBWithExtension,
		ReopenTestDBWithExtension,
	)
	tests.TestAll(t)
}

func TestNewDefault(t *testing.T) {
	t.Run("Error", func(t *testing.T) {
		db, err := NewDefault("", "")
		if err == nil {
			t.Fatalf("expected error")
		}
		if db != nil {
			defer db.Close()
		}
	})
}

func TestLen(t *testing.T) {
	t.Run("Custom IndexLen", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := shelvetest.StartDatabase(t, OpenTestDB, seed)
		// Replace the Index
		index := CustomIndexLen{len: 4}
		db.(*Store).db.Index = &index

		if n := db.Len(); n != int64(index.len) {
			t.Errorf("expected len to be %d, but got %d", index.len, n)
		}
	})

	t.Run("No Index", func(t *testing.T) {
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := shelvetest.StartDatabase(t, OpenTestDB, seed)
		// Replace the Index
		db.(*Store).db.Index = nil

		if n := db.Len(); n != int64(len(seed)) {
			t.Errorf("expected len to be %d, but got %d", len(seed), n)
		}
	})
}

func TestItems(t *testing.T) {
	t.Run("Get error", func(t *testing.T) {
		// Arrange
		seed := map[string]string{
			"key-1": "value-1", "key-2": "value-2",
			"key-3": "value-3", "key-4": "value-4",
		}
		db := shelvetest.StartDatabase(t, OpenTestDB, seed)
		// Replace the get function
		db.(*Store).get = func(s *Store, key []byte) ([]byte, error) {
			return nil, testError
		}

		// Act
		err := db.Items(
			[]byte("key-1"),
			shelve.Asc,
			func(k, v []byte) (bool, error) { return true, nil },
		)

		// Assert
		if !errors.Is(err, testError) {
			t.Errorf("expected error %v, but got %v", testError, err)
		}
	})
}

func TestSplitFilepath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty input",
			input:    "",
			expected: []string{},
		},
		{
			name:     "single-level path",
			input:    "foo",
			expected: []string{"foo"},
		},
		{
			name:     "multi-level path",
			input:    "foo/bar/baz",
			expected: []string{"foo", "bar", "baz"},
		},
		{
			name:     "path with leading slash",
			input:    "/foo/bar/baz",
			expected: []string{"foo", "bar", "baz"},
		},
		{
			name:     "path with trailing slash",
			input:    "foo/bar/baz/",
			expected: []string{"foo", "bar", "baz"},
		},
		{
			name:     "path with both leading and trailing slashes",
			input:    "/foo/bar/baz/",
			expected: []string{"foo", "bar", "baz"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := SplitFilepath(tt.input)
			if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("expected %v, got %v", tt.expected, actual)
			}
		})
	}
}

func TestFilepathToPathKey(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *diskv.PathKey
	}{
		{
			name:     "empty input",
			input:    "",
			expected: &diskv.PathKey{},
		},
		{
			name:     "root path",
			input:    "/",
			expected: &diskv.PathKey{},
		},
		{
			name:     "single-level path",
			input:    "foo",
			expected: &diskv.PathKey{FileName: "foo"},
		},
		{
			name:     "multi-level path",
			input:    "foo/bar/baz",
			expected: &diskv.PathKey{Path: []string{"foo", "bar"}, FileName: "baz"},
		},
		{
			name:     "path with leading slash",
			input:    "/foo/bar/baz",
			expected: &diskv.PathKey{Path: []string{"foo", "bar"}, FileName: "baz"},
		},
		{
			name:     "path with trailing slash",
			input:    "foo/bar/baz/",
			expected: &diskv.PathKey{Path: []string{"foo", "bar"}, FileName: "baz"},
		},
		{
			name:     "path with both leading and trailing slashes",
			input:    "/foo/bar/baz/",
			expected: &diskv.PathKey{Path: []string{"foo", "bar"}, FileName: "baz"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := FilepathToPathKey(tt.input)
			if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("expected %v, got %v", tt.expected, actual)
			}
		})
	}
}
