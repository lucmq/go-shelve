package pebbled

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	shelvetest "github.com/lucmq/go-shelve/driver/test"
	"github.com/lucmq/go-shelve/shelve"
)

var (
	dbPath = filepath.Join(os.TempDir(), "pebble-test")
)

func OpenTestDB() (shelve.DB, error) {
	// Clean-up the database directory
	err := os.RemoveAll(dbPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return NewDefault(dbPath)
}

func ReopenTestDB() (shelve.DB, error) {
	return NewDefault(dbPath)
}

func TestDB(t *testing.T) {
	tests := shelvetest.NewDBTests(OpenTestDB, ReopenTestDB)
	tests.SupportsSeeking = true
	tests.SupportsReverseIteration = true
	tests.TestAll(t)
}

func TestNewDefault(t *testing.T) {
	t.Run("Error", func(t *testing.T) {
		db, err := NewDefault("")
		if err == nil {
			t.Fatalf("expected error")
		}
		if db != nil {
			defer db.Close()
		}
	})
}

// Driver Specific Tests

var TestError = errors.New("test error")

func TestFailingIteration(t *testing.T) {
	t.Run("Len", func(t *testing.T) {
		db, err := OpenTestDB()
		if err != nil {
			t.Fatalf("open: %s", err)
		}
		defer db.Close()

		db.(*Store).newIterFn = func(*pebble.DB, *pebble.IterOptions) (*pebble.Iterator, error) {
			return nil, TestError
		}

		if db.Len() != -1 {
			t.Fatalf("expected len to be -1, but got %v", db.Len())
		}
	})

	t.Run("Items", func(t *testing.T) {
		db, err := OpenTestDB()
		if err != nil {
			t.Fatalf("open: %s", err)
		}
		defer db.Close()

		db.(*Store).newIterFn = func(*pebble.DB, *pebble.IterOptions) (*pebble.Iterator, error) {
			return nil, TestError
		}

		err = db.Items(nil, 1, func(k, v []byte) (bool, error) {
			t.Fatalf("expected error")
			return false, nil
		})
		if !errors.Is(err, TestError) {
			t.Fatalf("expected error to be %v, but got %v", TestError, err)
		}
	})
}
