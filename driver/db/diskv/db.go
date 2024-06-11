// Package diskvd provides a Diskv driver for go-shelve.
package diskvd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/google/btree"
	"github.com/peterbourgon/diskv/v3"

	"github.com/lucmq/go-shelve/shelve"
)

var (
	// DefaultCacheSize is the default size of the cache used when a Store is
	// created with NewDefault.
	DefaultCacheSize uint64 = 1024 * 1024

	// DefaultAdvancedTransform is the default function used to transform keys
	// to filesystem paths when a Store is created with NewDefault.
	DefaultAdvancedTransform = FilepathToPathKey

	// DefaultInverseTransform is the default function used map filesystem
	// paths back to keys when a Store is created with NewDefault. It is the
	// inverse of DefaultAdvancedTransform.
	DefaultInverseTransform = PathKeyToFilepath

	// DefaultLessFunction is the default function used to compare keys when a
	// Store is created with NewDefault.
	DefaultLessFunction = StringLess

	// DefaultBTreeDegree is the degree of the BTree used as the diskv index
	// when a Store is created with NewDefault.
	DefaultBTreeDegree = 8
)

// IndexLen is an interface that implementations of diskv.Index can use to make
// the Store.Len's method a no-op. By default, an instance of diskv.BTreeIndex
// is used, and it is not necessary to implement this interface.
type IndexLen interface {
	// Len returns the number of items in the index.
	Len() int
}

// Store is a [shelve.DB] driver backed by a diskv.Diskv instance.
type Store struct {
	db *diskv.Diskv

	// Gets a value associated with a key, from the file system.
	get func(s *Store, key []byte) ([]byte, error)
}

// Assert that Store implements the shelve.DB interface.
var _ shelve.DB = (*Store)(nil)

// New creates a new Store with the given diskv.Diskv instance.
func New(db *diskv.Diskv) (*Store, error) {
	return &Store{db: db, get: getValue}, nil
}

// NewDefault creates a new Store rooted at path with sensible default values.
// If extension is not empty, it will be appended to the path of each db record
// file.
func NewDefault(path, extension string) (*Store, error) {
	if path == "" {
		return nil, errors.New("path is required")
	}
	db := diskv.New(diskv.Options{
		BasePath:          path,
		AdvancedTransform: makeAdvancedTransform(extension),
		InverseTransform:  makeInverseTransform(extension),
		CacheSizeMax:      DefaultCacheSize,
		TempDir:           os.TempDir(),
		Index:             newBTreeIndex(),
		IndexLess:         DefaultLessFunction,
	})
	return New(db)
}

func newBTreeIndex() *diskv.BTreeIndex {
	return &diskv.BTreeIndex{
		RWMutex:      sync.RWMutex{},
		LessFunction: DefaultLessFunction,
		BTree:        btree.New(DefaultBTreeDegree),
	}
}

// Close closes the underlying diskv.Diskv instance. In diskv, this is a no-op.
func (*Store) Close() error {
	return nil
}

// Len returns the number of items in the store. It returns -1 if an error
// occurs.
//
// Warning: Diskv doesn't provide a method to count the number of stored items
// and Len may iterate through the entire DB to retrieve this information.
//
// If Diskv.Index is the default diskv.BTreeIndex, or satisfies the IndexLen
// interface from this package, this method is a no-op.
//
// Otherwise, Len will default to iterating through the entire DB.
func (s *Store) Len() int64 {
	switch index := s.db.Index.(type) {
	case *diskv.BTreeIndex:
		index.RLock()
		defer index.RUnlock()
		return int64(index.Len())

	case IndexLen:
		return int64(index.Len())

	default:
		count := int64(0)
		for range s.db.Keys(nil) {
			count++
		}
		return count
	}
}

// Sync synchronizes the underlying diskv database to persistent storage. In
// diskv, this is a no-op.
func (*Store) Sync() error {
	return nil
}

// Has reports whether a key exists in the store.
func (s *Store) Has(key []byte) (bool, error) {
	return s.db.Has(string(key)), nil
}

// Get retrieves the value associated with a key from the store. If the key is
// not found, it returns nil.
func (s *Store) Get(key []byte) ([]byte, error) {
	value, err := s.db.Read(string(key))
	if errors.Is(err, os.ErrNotExist) {
		// Key not found
		return nil, nil
	}
	return value, err
}

// Put stores a key-value pair in the store. If the key already exists, it
// overwrites the existing value.
func (s *Store) Put(key, value []byte) error {
	return s.db.Write(string(key), value)
}

// Delete removes a key-value pair from the store.
func (s *Store) Delete(key []byte) error {
	err := s.db.Erase(string(key))
	if errors.Is(err, os.ErrNotExist) {
		// Key not found
		return nil
	}
	return err
}

// Items iterates over key-value pairs in the database, calling fn(k, v)
// for each pair in the sequence. The iteration stops early if the function
// fn returns false.
//
// The start parameter specifies the key from which the iteration should
// start. If the start parameter is nil, the iteration will begin from the
// first key in the database.
//
// The key-value pairs are returned in lexicographically sorted order but only
// forward iteration is supported. The order parameter exists only for
// compatibility with the shelve.DB interface and is ignored by this method.
func (s *Store) Items(
	start []byte,
	_ int,
	fn func(key, value []byte) (bool, error),
) error {
	batchSize := 256
	from := string(start)
	for {
		keys := s.db.Index.Keys(from, batchSize)
		if len(keys) == 0 {
			break
		}

		// Add the start key because diskv.Index.Keys will not include it
		if len(start) != 0 {
			keys = append([]string{from}, keys...)
		}

		from = keys[len(keys)-1]
		for _, key := range keys {
			value, err := s.get(s, []byte(key))
			if err != nil {
				return fmt.Errorf("get: %w", err)
			}
			ok, err := fn([]byte(key), value)
			if err != nil {
				return fmt.Errorf("fn: %w", err)
			}
			if !ok {
				return nil
			}
		}
	}
	return nil
}

// SplitFilepath splits a path into its components. Examples:
//   - "/a/b/c" -> ["a", "b", "c"]
//   - "a/b/c" -> ["a", "b", "c"]
//   - "a/b/c/" -> ["a", "b", "c"]
//   - "a/b/c.txt" -> ["a", "b", "c.txt"]
func SplitFilepath(path string) []string {
	if path == "" {
		return []string{}
	}
	var parts []string
	for {
		path = filepath.Clean(path)
		dir, base := filepath.Split(path)
		if base != "" {
			parts = append(parts, base)
		}
		if dir == path || dir == "" {
			break
		}
		path = dir
	}
	slices.Reverse(parts)
	return parts
}

// FilepathToPathKey converts a filepath to a PathKey. using the SplitFilepath
// function.
func FilepathToPathKey(path string) *diskv.PathKey {
	if !strings.Contains(path, string(os.PathSeparator)) {
		// Shortcut: the path represents a file
		return &diskv.PathKey{FileName: path}
	}
	parts := SplitFilepath(path)
	if len(parts) == 0 {
		return &diskv.PathKey{}
	}
	return &diskv.PathKey{
		Path:     parts[:len(parts)-1],
		FileName: parts[len(parts)-1],
	}
}

// PathKeyToFilepath converts a diskv.PathKey to a filepath. It is the inverse
// of FilepathToPathKey.
func PathKeyToFilepath(pk *diskv.PathKey) string {
	return filepath.Join(append(pk.Path, pk.FileName)...)
}

// StringLess returns true if a is less than b lexicographically.
func StringLess(a, b string) bool { return a < b }

func getValue(s *Store, key []byte) ([]byte, error) {
	return s.Get(key)
}

func makeAdvancedTransform(extension string) diskv.AdvancedTransformFunction {
	return func(s string) *diskv.PathKey {
		p := DefaultAdvancedTransform(s)
		if extension != "" {
			p.FileName = fmt.Sprintf("%s.%s", p.FileName, extension)
		}
		return p
	}
}

func makeInverseTransform(extension string) diskv.InverseTransformFunction {
	return func(pathKey *diskv.PathKey) string {
		if extension != "" {
			pathKey.FileName = strings.TrimSuffix(
				pathKey.FileName,
				fmt.Sprintf(".%s", extension),
			)
		}
		return DefaultInverseTransform(pathKey)
	}
}
