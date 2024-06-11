// Package boltd provides a BoltDB driver for go-shelve.
package boltd

import (
	"fmt"
	"os"

	"github.com/boltdb/bolt"
	"github.com/lucmq/go-shelve/shelve"
)

// Store is a BoltDB driver for [shelve.Shelf].
type Store struct {
	db     *bolt.DB
	bucket []byte
}

// Assert Store implements shelve.DB
var _ shelve.DB = (*Store)(nil)

// New creates a new BoltDB store. The bucket is created if it doesn't exist.
func New(db *bolt.DB, bucket []byte) (*Store, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket(bucket) != nil {
			// Bucket already exists
			return nil
		}
		_, err := tx.CreateBucket(bucket)
		return err
	})
	return &Store{db: db, bucket: bucket}, err
}

// NewDefault creates a new BoltDB store with sensible default values. The
// bucket is created if it doesn't exist.
func NewDefault(path string, bucket []byte) (*Store, error) {
	db, err := Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	return New(db, bucket)
}

// Open opens an existing BoltDB store. It is a wrapper around bolt.Open.
func Open(path string, mode os.FileMode, options *bolt.Options) (*bolt.DB, error) {
	return bolt.Open(path, mode, options)
}

// Close closes the underlying BoltDB database.
func (s *Store) Close() error {
	return s.db.Close()
}

// Len returns the number of items in the store. It returns -1 if an error
// occurs.
func (s *Store) Len() int64 {
	var count int64
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		c := b.Stats().KeyN
		count = int64(c)
		return nil
	})
	if err != nil {
		return -1
	}
	return count
}

// Sync synchronizes the BoltDB contents to persistent storage.
func (s *Store) Sync() error {
	return s.db.Sync()
}

// Has reports whether a key exists in the store.
func (s *Store) Has(key []byte) (bool, error) {
	var exists bool
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		value := b.Get(key)
		if value != nil {
			exists = true
		}
		return nil
	})
	return exists, err
}

// Get retrieves the value associated with a key from the store. If the key is
// not found, it returns nil.
func (s *Store) Get(key []byte) ([]byte, error) {
	var val []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		val = b.Get(key)
		return nil
	})
	return val, err
}

// Put adds a key-value pair to the store. If the key already exists, it
// overwrites the existing value.
func (s *Store) Put(key, value []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		return b.Put(key, value)
	})
}

// Delete removes a key-value pair from the store.
func (s *Store) Delete(key []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		return b.Delete(key)
	})
}

// Items iterates over key-value pairs in the database, calling fn(k, v)
// for each pair in the sequence. The iteration stops early if the function
// fn returns false.
//
// The start parameter specifies the key from which the iteration should
// start. If the start parameter is nil, the iteration will begin from the
// first key in the database.
//
// The order parameter specifies the order in which the items should be
// yielded. A negative value for order will cause the iteration to occur in
// reverse order.
//
// The key-value pairs are returned in lexicographically sorted order.
func (s *Store) Items(
	start []byte,
	order int,
	fn func(key, value []byte) (bool, error),
) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		c := b.Cursor()

		for k, v := seek(c, start, order); k != nil; {
			ok, err := fn(k, v)
			if err != nil {
				return fmt.Errorf("call fn: %w", err)
			}
			if !ok {
				return nil
			}
			k, v = next(c, order)
		}

		return nil
	})
}

func seek(c *bolt.Cursor, start []byte, order int) (k, v []byte) {
	if len(start) != 0 {
		return c.Seek(start)
	}
	if order < 0 {
		return c.Last()
	}
	return c.First()
}

func next(c *bolt.Cursor, order int) (k, v []byte) {
	if order < 0 {
		return c.Prev()
	}
	return c.Next()
}
