// Package shelve provides a persistent, map-like object called Shelf. It lets you
// store and retrieve Go objects directly, with the serialization and storage handled
// automatically by the Shelf. Additionally, you can customize the underlying
// key-value storage and serialization codec to better suit your application's needs.
//
// This package is inspired by the `shelve` module from the Python standard library
// and aims to provide a similar set of functionalities.
//
// By default, a Shelf serializes data using the Gob format and stores it using `sdb`
// (for "shelve-db"), a simple key-value storage created for this project. This
// database should be good enough for a broad range of applications, but the modules
// in [go-shelve/driver] provide additional options for configuring the `Shelf` with
// other databases and Codecs.
//
// [go-shelve/driver]: https://pkg.go.dev/github.com/lucmq/go-shelve/driver
package shelve

import (
	"fmt"
	"go-shelve/sdb"
)

const (
	// Asc and Desc can be used with the Shelf.Items method to make the
	// iteration order ascending or descending respectively.
	//
	// They are just syntactic sugar to make the iteration order more
	// explicit.
	Asc = 1

	// Desc is the opposite of Asc.
	Desc = -1

	// All can be used with the Shelf.Items method to iterate over all
	// items in the database. It is the same as the -1 value.
	All = -1
)

// Yield is a function called when iterating over key-value pairs in the
// Shelf. If Yield returns false or an error, the iteration stops.
type Yield[K, V any] func(key K, value V) (bool, error)

// A Shelf is a persistent, map-like object. It is used together with an
// underlying key-value storage to store Go objects directly.
//
// Stored values can be of arbitrary types, but the keys must be comparable.
//
// The values are encoded as [Gob], and keys, if they are strings or integers,
// are encoded as strings. Otherwise, they will also be encoded as Gob.
//
// For storage, the underlying database is an instance of the [sdb.DB]
// ("shelve-db") key-value store.
//
// The underlying storage and codec Shelf uses can be configured with the
// [Option] functions.
//
// [Gob]: https://pkg.go.dev/encoding/gob
type Shelf[K comparable, V any] struct {
	db       DB
	codec    Codec
	keyCodec Codec
}

// Option is passed to the Open function to create a customized Shelf.
type Option func(any)

type options struct {
	DB       DB
	Codec    Codec
	KeyCodec Codec
}

// WithDatabase specifies the underlying database to use. By default, the
// [sdb.DB] ("shelve-db") key-value storage is used.
//
// The packages in [driver/db] packages provide support for others databases in
// the Go ecosystem, like [Bolt] and [Badger].
//
// [driver/db]: https://pkg.go.dev/github.com/lucmq/go-shelve/driver/db
// [Bolt]: https://pkg.go.dev/github.com/etcd-io/bbolt
// [Badger]: https://pkg.go.dev/github.com/dgraph-io/badger
func WithDatabase(db DB) Option {
	return func(v any) {
		opt := v.(*options)
		opt.DB = db
	}
}

// WithCodec specifies the Codec to use. By default, a codec for the Gob format
// from the standard library ([encoding/gob]) is used.
//
// Additional Codecs can be found in the packages in [driver/encoding].
//
// [driver/encoding]: https://pkg.go.dev/github.com/lucmq/go-shelve/driver/encoding
func WithCodec(c Codec) Option {
	return func(v any) {
		opt := v.(*options)
		opt.Codec = c
	}
}

// WithKeyCodec specifies the Codec to use with keys. By default, if the key is
// a string or an integer type (both signed and unsigned), the [StringCodec] is
// used. Otherwise, keys are encoded as Gob ([encoding/gob]).
//
// Additional Codecs can be found in the packages in [driver/encoding].
//
// [driver/encoding]: https://pkg.go.dev/github.com/lucmq/go-shelve/driver/encoding
func WithKeyCodec(c Codec) Option {
	return func(v any) {
		opt := v.(*options)
		opt.KeyCodec = c
	}
}

// Open creates a new Shelf.
//
// The path parameter specifies the filesystem path to the database files. It
// can be a directory or a regular file, depending on the underlying database
// implementation. With the default database [sdb.DB], it will point to a
// directory.
func Open[K comparable, V any](path string, opts ...Option) (
	*Shelf[K, V],
	error,
) {
	var k K
	o := options{
		Codec:    GobCodec(),
		KeyCodec: defaultKeyCodec(k),
	}
	for _, option := range opts {
		option(&o)
	}

	if o.DB == nil {
		db, err := sdb.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open db: %w", err)
		}
		o.DB = db
	}

	return &Shelf[K, V]{
		db:       o.DB,
		codec:    o.Codec,
		keyCodec: o.KeyCodec,
	}, nil
}

// Close synchronizes and closes the Shelf.
func (s *Shelf[K, V]) Close() error {
	return s.db.Close()
}

// Len returns the number of items in the Shelf. It returns the number
// of items as an int64. If an error occurs, it returns -1.
func (s *Shelf[K, V]) Len() int64 {
	return s.db.Len()
}

// Sync synchronizes the Shelf contents to persistent storage.
func (s *Shelf[K, V]) Sync() error {
	return s.db.Sync()
}

// Has reports whether a key exists in the Shelf.
func (s *Shelf[K, V]) Has(key K) (bool, error) {
	data, err := s.keyCodec.Encode(key)
	if err != nil {
		return false, fmt.Errorf("encode: %w", err)
	}
	ok, err := s.db.Has(data)
	if err != nil {
		return false, fmt.Errorf("has: %w", err)
	}
	return ok, nil
}

// Get retrieves the value associated with a key from the Shelf. If the key is
// not found, it returns nil.
func (s *Shelf[K, V]) Get(key K) (value V, ok bool, err error) {
	data, err := s.keyCodec.Encode(key)
	if err != nil {
		return *new(V), false, fmt.Errorf("encode: %w", err)
	}
	vData, err := s.db.Get(data)
	if err != nil {
		return *new(V), false, fmt.Errorf("get: %w", err)
	}
	if vData == nil {
		return *new(V), false, nil
	}
	var v V
	err = s.codec.Decode(vData, &v)
	return v, true, err
}

// Put adds a key-value pair to the Shelf. If the key already exists, it
// overwrites the existing value.
func (s *Shelf[K, V]) Put(key K, value V) error {
	data, err := s.keyCodec.Encode(key)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}
	vData, err := s.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}
	err = s.db.Put(data, vData)
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	return nil
}

// Delete removes a key-value pair from the Shelf.
func (s *Shelf[K, V]) Delete(key K) error {
	data, err := s.keyCodec.Encode(key)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	err = s.db.Delete(data)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	return nil
}

// Items iterates over key-value pairs in the Shelf, calling fn(k, v) for each
// pair in the sequence. The iteration stops early if the function fn returns
// false.
//
// The start parameter specifies the key from which the iteration should start.
// If the start parameter is nil, the iteration will begin from the first key
// in the Shelf.
//
// The n parameter specifies the maximum number of items to iterate over. If n
// is -1 or less, all items will be iterated.
//
// The step parameter specifies the number of items to skip between each
// iteration. A negative value for step will cause the iteration to occur in
// reverse order.
//
// When iterating over key-value pairs in a Shelf, the order of iteration may
// not be sorted. Some database implementations may ignore the start parameter
// or not support iteration in reverse order.
//
// The default database used with Shelf (sdb.DB) does not yield items in any
// particular order and ignores the start parameter.
func (s *Shelf[K, V]) Items(start *K, n, step int, fn Yield[K, V]) error {
	dbFn := func(k, v []byte) (bool, error) {
		var key K
		var value V
		err := s.keyCodec.Decode(k, &key)
		if err != nil {
			return false, fmt.Errorf("decode key: %w", err)
		}
		if len(v) != 0 {
			err = s.codec.Decode(v, &value)
			if err != nil {
				return false, fmt.Errorf("decode value: %w", err)
			}
		}
		return fn(key, value)
	}
	return s.iterate(start, n, step, dbFn)
}

// Keys iterates over all keys in the Shelf and calls the user-provided
// function fn for each key. The details of the iteration are the same as
// for [Shelf.Items].
//
// The value parameter for fn will always be the zero value for the type V.
func (s *Shelf[K, V]) Keys(start *K, n, step int, fn Yield[K, V]) error {
	dbFn := func(k, _ []byte) (bool, error) {
		var key K
		var zero V
		err := s.keyCodec.Decode(k, &key)
		if err != nil {
			return false, fmt.Errorf("decode: %w", err)
		}
		return fn(key, zero)
	}
	return s.iterate(start, n, step, dbFn)
}

// Values iterates over all values in the Shelf and calls the user-provided
// function fn for each value. The details of the iteration are the same as
// for [Shelf.Items].
//
// The key parameter for fn will always be the zero value for the type K.
func (s *Shelf[K, V]) Values(start *K, n, step int, fn Yield[K, V]) error {
	dbFn := func(_, v []byte) (bool, error) {
		var zero K
		var value V
		err := s.codec.Decode(v, &value)
		if err != nil {
			return false, fmt.Errorf("decode: %w", err)
		}
		return fn(zero, value)
	}
	return s.iterate(start, n, step, dbFn)
}

func (s *Shelf[K, V]) iterate(
	start *K,
	n, step int,
	fn func(k, v []byte) (bool, error),
) error {
	var from []byte = nil
	var err error
	if start != nil {
		from, err = s.keyCodec.Encode(*start)
		if err != nil {
			return fmt.Errorf("encode start: %w", err)
		}
	}

	var order int
	if step > 0 {
		order = Asc
	} else if step < 0 {
		order = Desc
		step = -step
	} else {
		return nil
	}

	var total int
	var counter = step - 1 // 0, 1, ..., step - 1

	return s.db.Items(from, order, func(k, v []byte) (bool, error) {
		if n > 0 && total >= n {
			return false, nil
		}

		// Increase counter until the step is reached
		if counter < step-1 {
			counter++
			return true, nil
		}
		counter = 0

		total++
		return fn(k, v)
	})
}

// Helpers

func defaultKeyCodec(key any) Codec {
	switch key.(type) {
	case int8, int16, int32, int64, int:
		return StringCodec()
	case uint8, uint16, uint32, uint64, uint:
		return StringCodec()
	case string:
		return StringCodec()
	default:
		return GobCodec()
	}
}
