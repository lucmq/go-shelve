// Package badgerd provides a BadgerDB driver for go-shelve.
package badgerd

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"slices"
)

// Store is a BadgerDB driver for go-shelve/Shelf.
type Store struct {
	db        *badger.DB
	valueCopy copyFunc
}

type copyFunc func(item *badger.Item, dest []byte) ([]byte, error)

// New creates a new BadgerDB store.
func New(db *badger.DB) (*Store, error) {
	return &Store{db: db, valueCopy: valueCopy}, nil
}

// NewDefault creates a new BadgerDB store with sensible default values.
func NewDefault(path string) (*Store, error) {
	opts := badger.DefaultOptions(path)

	// Badger uses mmap and recommends using async writes for
	// most scenarios
	opts = opts.WithSyncWrites(false)
	opts = opts.WithDir(path)
	opts = opts.WithValueDir(path)
	opts = opts.WithLoggingLevel(badger.ERROR)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	return New(db)
}

// Close closes the underlying BadgerDB database.
func (s *Store) Close() error {
	return s.db.Close()
}

// Len returns the number of items in the store. It returns -1 if an error
// occurs.
//
// Warning: BadgerDB doesn't provide a method to count the number of stored
// items and Len will iterate through the entire DB to retrieve this
// information.
//
// See: https://discuss.dgraph.io/t/count-of-items-in-db/7549/2
func (s *Store) Len() int64 {
	var count int64 = -1
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Only fetch keys
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}

		return nil
	})
	if err == nil {
		count++ // We started from -1
	}
	return count
}

// Sync synchronizes the underlying BadgerDB database to persistent storage.
func (s *Store) Sync() error {
	// Perform a sync operation on the underlying BadgerDB database
	return s.db.Sync()
}

// Has reports whether a key exists in the store.
func (s *Store) Has(key []byte) (bool, error) {
	var has bool
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("get: %w", err)
		}
		has = true
		return nil
	})
	return has, err
}

// Get retrieves the value associated with a key from the store. If the key is
// not found, it returns nil.
func (s *Store) Get(key []byte) ([]byte, error) {
	var val []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("get: %w", err)
		}
		slices.Grow(val, int(item.ValueSize()))
		val, err = item.ValueCopy(val)
		return err
	})
	return val, err
}

// Put stores a key-value pair in the store. If the key already exists, it
// overwrites the existing value.
func (s *Store) Put(key, value []byte) error {
	return s.db.Update(func(tx *badger.Txn) error {
		return tx.Set(key, value)
	})
}

// Delete removes a key-value pair from the store.
func (s *Store) Delete(key []byte) error {
	return s.db.Update(func(tx *badger.Txn) error {
		return tx.Delete(key)
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
//
// The value parameter must only be used inside fn, as it is reused during the
// iteration.
func (s *Store) Items(
	start []byte,
	order int,
	fn func(key, value []byte) (bool, error),
) error {
	// Initialize the value variable as a buffer to be reused during
	// the iteration
	var value []byte

	// Iterate over the items in the store within the given range
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Only fetch keys
		if order < 0 {
			opts.Reverse = true
		}

		it := txn.NewIterator(opts)
		defer it.Close()

		// Iterate over the items until the end key is reached
		for it.Seek(start); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			// Fetch the value for the key
			var err error
			value, err = s.valueCopy(item, value)
			if err != nil {
				return fmt.Errorf("copy value: %w", err)
			}

			// Call the callback function with the key and value
			ok, err := fn(key, value)
			if err != nil {
				return fmt.Errorf("call fn: %w", err)
			}
			// Stop iterating if the callback function returns false
			if !ok {
				return nil
			}
		}

		return nil
	})
}

func valueCopy(item *badger.Item, dst []byte) ([]byte, error) {
	return item.ValueCopy(dst)
}
