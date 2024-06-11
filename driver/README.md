# Go-Shelve Drivers
This module contains independent submodules for different database drivers and
codecs, meant to be used as part of the go-shelve project.

### Databases:
#### Supported:
- [Bolt](https://pkg.go.dev/github.com/lucmq/go-shelve/driver/db/bolt)
- [BBolt](https://pkg.go.dev/github.com/lucmq/go-shelve/driver/db/bboltd)
- [Badger](https://pkg.go.dev/github.com/lucmq/go-shelve/driver/db/badger)
- [Diskv](https://pkg.go.dev/github.com/lucmq/go-shelve/driver/db/diskv)

#### Beta:
- None

#### Wishlist:
- Berkeley DB / DBM (For Python interoperability)

<br/>

### Codecs:
#### Supported:
- [MessagePack](https://pkg.go.dev/github.com/lucmq/go-shelve/driver/encoding/msgpack)

### Beta
- None

### Wishlist
- Pickle (Python)

<br/>

## Implementation Guidelines
### Database Drivers
- Drivers should be implemented for key-value stores. For SQL databases, ORMs already provide a mapper from objects to database types.
  - With this in mind, a driver for SQLite probably won't be provided, even if it is a popular option for embedded databases.
- Drivers must implement the shelve.DB interface.
- Optionally, drivers can implement the shelve.Sorted interface, if the underlying database supports sorted iteration.
- Drivers must have a `New` function to create new instances.
- Optionally, a `NewDefault` function, which creates a driver with sensible defaults, can also be provided.
- When a key is not found, DB.Get() must return a nil value and no error.

<br/>

### Codecs
- Codecs must implement the shelve.Codec interface.
