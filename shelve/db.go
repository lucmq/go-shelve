package shelve

// DB is an interface that defines the methods for a database that can be used
// with Shelf. It takes the encoded binary representation of keys and values.
type DB interface {
	// Close synchronizes and closes the database.
	Close() error

	// Len returns the number of items in the database. It returns the number
	// of items as an int64. If an error occurs, it returns -1.
	Len() int64

	// Sync synchronizes the database to persistent storage.
	Sync() error

	// Has reports whether a key exists in the database.
	Has(key []byte) (bool, error)

	// Get retrieves the value associated with a key from the database. If the
	// key is not found, it returns nil.
	Get(key []byte) ([]byte, error)

	// Put adds a key-value pair to the database. If the key already exists, it
	// overwrites the existing value.
	Put(key, value []byte) error

	// Delete removes a key-value pair from the database.
	Delete(key []byte) error

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
	// When iterating over key-value pairs in a database, the order of
	// iteration may not be sorted. Some database implementations may ignore
	// the start parameter or not support iteration in reverse order.
	Items(
		start []byte,
		order int,
		fn func(key, value []byte) (bool, error),
	) error
}
