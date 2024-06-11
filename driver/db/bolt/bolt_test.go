package boltd

import (
	"os"
	"path/filepath"
	"testing"

	shelvetest "github.com/lucmq/go-shelve/driver/test"
	"github.com/lucmq/go-shelve/shelve"
)

var (
	dbPath         = filepath.Join(os.TempDir(), "bolt-test")
	boltBucketName = []byte("go-shelve")
)

func OpenTestDB() (shelve.DB, error) {
	// Clean-up the database directory
	err := os.RemoveAll(dbPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return NewDefault(dbPath, boltBucketName)
}

func ReopenTestDB() (shelve.DB, error) {
	return NewDefault(dbPath, boltBucketName)
}

func TestDB(t *testing.T) {
	tests := shelvetest.NewDBTests(OpenTestDB, ReopenTestDB)
	tests.SupportsSeeking = true
	tests.SupportsReverseIteration = true
	tests.TestAll(t)
}

func TestNewDefault(t *testing.T) {
	t.Run("Error", func(t *testing.T) {
		db, err := NewDefault("", boltBucketName)
		if err == nil {
			t.Fatalf("expected error")
		}
		if db != nil {
			defer db.Close()
		}
	})
}

func TestDB_BucketError(t *testing.T) {
	var badBucket []byte // Invalid bucket name

	db, err := OpenTestDB()
	if err != nil {
		t.Fatalf("open: %s", err)
	}
	defer db.Close()

	t.Run("New", func(t *testing.T) {
		_, err := New(db.(*Store).db, badBucket)
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("Len", func(t *testing.T) {
		db.(*Store).bucket = badBucket
		n := db.Len()
		if n != -1 {
			t.Fatalf("expected error (len = -1), got %d", n)
		}
	})

	t.Run("Has", func(t *testing.T) {
		db.(*Store).bucket = badBucket
		_, err := db.Has([]byte("key"))
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("Get", func(t *testing.T) {
		db.(*Store).bucket = badBucket
		_, err := db.Get([]byte("key"))
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("Put", func(t *testing.T) {
		db.(*Store).bucket = badBucket
		err := db.Put([]byte("key"), []byte("value"))
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		db.(*Store).bucket = badBucket
		err := db.Delete([]byte("key"))
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("Items", func(t *testing.T) {
		db.(*Store).bucket = badBucket
		err := db.Items([]byte{}, shelve.Asc, func(k, v []byte) (bool, error) {
			return true, nil
		})
		if err == nil {
			t.Fatalf("expected error")
		}
	})
}
