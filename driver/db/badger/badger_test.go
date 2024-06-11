package badgerd

import (
	"errors"
	"github.com/dgraph-io/badger/v3"
	shelvetest "go-shelve/driver/test"
	"go-shelve/shelve"
	"os"
	"path/filepath"
	"testing"
)

var dbPath = filepath.Join(os.TempDir(), "badger-test")

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
			t.Errorf("expected error")
		}
		if db != nil {
			defer db.Close()
		}
	})
}

func TestDB_EmptyKeys(t *testing.T) {
	t.Run("Has", func(t *testing.T) {
		db, err := NewDefault(dbPath)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		defer db.Close()

		_, err = db.Has([]byte{})
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("Get", func(t *testing.T) {
		db, err := NewDefault(dbPath)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		defer db.Close()

		_, err = db.Get([]byte{})
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("Put", func(t *testing.T) {
		db, err := NewDefault(dbPath)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		defer db.Close()

		err = db.Put([]byte{}, []byte{})
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		db, err := NewDefault(dbPath)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		defer db.Close()

		err = db.Delete([]byte{})
		if err == nil {
			t.Fatalf("expected error")
		}
	})
}

func TestDB_Items_CopyError(t *testing.T) {
	db, err := NewDefault(dbPath)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer db.Close()

	// Replace the copy function to return an error
	db.valueCopy = func(item *badger.Item, dst []byte) ([]byte, error) {
		return nil, shelvetest.TestError
	}

	err = db.Put([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = db.Items(nil, 1, nil)

	if !errors.Is(err, shelvetest.TestError) {
		t.Errorf("Expected %v, but got %v", shelvetest.TestError, err)
	}
}
