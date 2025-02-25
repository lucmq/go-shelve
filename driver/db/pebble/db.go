// Package pebbled provides a Pebble driver for go-shelve.
package pebbled

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/lucmq/go-shelve/sdb"
	"github.com/lucmq/go-shelve/shelve"
)

// Store is a Pebble driver for [shelve.Shelf].
type Store struct {
	db        *pebble.DB
	newIterFn newIterFunc
}

// Assert Store implements shelve.DB
var _ shelve.DB = (*Store)(nil)

type newIterFunc func(*pebble.DB, *pebble.IterOptions) (*pebble.Iterator, error)

// New creates a new Pebble store.
func New(db *pebble.DB) (*Store, error) {
	return &Store{
		db:        db,
		newIterFn: defaultNewIterFn(),
	}, nil
}

// NewDefault creates a new Pebble store with sensible default values.
func NewDefault(path string) (*Store, error) {
	db, err := Open(path, nil)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	return New(db)
}

// Open is a wrapper around pebble.Open.
func Open(dirname string, opts *pebble.Options) (*pebble.DB, error) {
	return pebble.Open(dirname, opts)
}

// Close closes the Pebble database.
func (s *Store) Close() error {
	return s.db.Close()
}

// Len returns the number of items in the store. It returns -1 if an error
// occurs.
//
// Warning: Pebble doesn't provide a method to count the number of stored
// items and Len will iterate through the entire DB to retrieve this
// information.
func (s *Store) Len() int64 {
	var count int64
	err := s.Items(nil, 1, func(k, v []byte) (bool, error) {
		count++
		return true, nil
	})
	if err != nil {
		return -1
	}
	return count
}

// Sync synchronizes the underlying Pebble database to persistent storage.
func (s *Store) Sync() error {
	return s.db.Flush()
}

// Has reports whether a key exists in the store.
func (s *Store) Has(key []byte) (bool, error) {
	value, closer, err := s.db.Get(key)
	if err != nil && errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	defer closer.Close()
	return value != nil, err
}

// Get retrieves the value associated with a key from the store. If the key is
// not found, it returns nil.
func (s *Store) Get(key []byte) ([]byte, error) {
	value, closer, err := s.db.Get(key)
	if err != nil && errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	defer func() {
		if err == nil {
			closer.Close()
		}
	}()
	return value, err
}

// Put stores a key-value pair in the store. If the key already exists, it
// overwrites the existing value.
func (s *Store) Put(key, value []byte) error {
	return s.db.Set(key, value, pebble.Sync)
}

// Delete removes a key-value pair from the store.
func (s *Store) Delete(key []byte) error {
	return s.db.Delete(key, pebble.Sync)
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
func (s *Store) Items(start []byte, order int, fn sdb.Yield) error {
	iter, err := s.newIterFn(s.db, nil)
	if err != nil {
		return fmt.Errorf("new iter: %w", err)
	}
	defer iter.Close()

	// Note: Seek will move the iterator to the first element greater than or
	// equal to start (or less than or equal to start if order < 0).
	seek(iter, start, order)

	for iter.Valid() {
		key := iter.Key()
		value := iter.Value()
		cont, err := fn(key, value)
		if err != nil {
			return fmt.Errorf("call fn: %w", err)
		}
		if !cont {
			break
		}
		if !next(iter, order) {
			break
		}
	}
	return iter.Error()
}

func seek(it *pebble.Iterator, start []byte, order int) {
	if len(start) != 0 {
		seekWithStart(it, start, order)
		return
	}
	if order >= 0 {
		it.First()
		return
	}
	it.Last()
}

func seekWithStart(it *pebble.Iterator, start []byte, order int) {
	if order >= 0 {
		it.SeekGE(start)
	} else {
		it.SeekLT(append(start, 0x00))
	}
}

func next(it *pebble.Iterator, order int) bool {
	if order < 0 {
		return it.Prev()
	}
	return it.Next()
}

func defaultNewIterFn() newIterFunc {
	return func(db *pebble.DB, o *pebble.IterOptions) (*pebble.Iterator, error) {
		return db.NewIter(o)
	}
}
