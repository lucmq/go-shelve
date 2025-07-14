# Go-Shelve
[![Go Reference](https://pkg.go.dev/badge/github.com/lucmq/go-shelve/shelve.svg)](https://pkg.go.dev/github.com/lucmq/go-shelve/shelve)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/lucmq/go-shelve)](https://goreportcard.com/report/github.com/lucmq/go-shelve)
[![Go Coverage](https://github.com/lucmq/go-shelve/wiki/coverage.svg)](https://raw.githack.com/wiki/lucmq/go-shelve/coverage.html)
[![DeepSource](https://app.deepsource.com/gh/lucmq/go-shelve.svg/?label=active+issues&show_trend=false&token=iZaN7kSfuZGm1KppBKaqMHME)](https://app.deepsource.com/gh/lucmq/go-shelve/)

Go-Shelve is a dependencies-free Go package that provides a persistent, map-like
object called `Shelf`. It lets you store and retrieve Go objects directly, with
the serialization and storage handled automatically by the `Shelf`. Additionally,
you can customize the underlying key-value storage and serialization Codec to
better suit your application's needs.

This project is inspired by the `shelve` module from the Python standard
library and aims to provide a similar set of functionalities.

Check the [driver's directory](./driver/README.md) for additional storage and
Codec options.

## Installation
To use this package in your Go project, you can install it using `go get`:
```bash
go get github.com/lucmq/go-shelve
```

## Usage
Here are some examples of how to use `go-shelve`:

### Basic
The following example illustrates the usage of a `Shelf` with string keys and
values. The `Shelf` type uses generics and can be instantiated with any Go type
as a value and any comparable type for the key.
```go
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/lucmq/go-shelve/shelve"
)

func main() {
	// Note: In a real application be sure to replace the
	// os.TempDir() with a non temporary directory and
	// provide better error treatment. 
	path := filepath.Join(os.TempDir(), "go-shelve")

	// Open the shelf with default options
	shelf, err := shelve.Open[string, string](path)
	if err != nil {
		log.Fatal(err)
	}
	defer shelf.Close()

	// Use the database
	shelf.Put("language", "Go")
	shelf.Put("module", "Go-Shelve")

	// Note: Saved values will be available between restarts
	value, ok, _ := shelf.Get("language")
	fmt.Println(value, ok)
	
	// Output: Go true
}
```

### Custom Database and Codec
By default, a `Shelf` serializes data using the JSON format and stores it using
`sdb` (for "shelve-db"), a simple key-value storage created for this project.

This database should be suitable for a wide range of applications, but the
[driver's directory](./driver/README.md) provides additional options for
configuring a `Shelf` with other databases from the Go ecosystem.

The example below shows how to customize a `Shelf` to use `BoltDB` for storage
together with `MessagePack` for serialization:
```go
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	bboltd "github.com/lucmq/go-shelve/driver/db/bbolt"
	"github.com/lucmq/go-shelve/driver/encoding/msgpack"
	"github.com/lucmq/go-shelve/shelve"
)

func main() {
	path := filepath.Join(os.TempDir(), "bolt-example")

	db, _ := bboltd.NewDefault(path, []byte("example-bucket"))
	codec := msgpack.NewDefault()

	// Open the shelf with custom options
	shelf, err := shelve.Open[string, string](
		path,
		shelve.WithDatabase(db),
		shelve.WithCodec(codec),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer shelf.Close()

	// Use the database
	shelf.Put("language", "Go")
	shelf.Put("module", "Go-Shelve")

	value, ok, _ := shelf.Get("language")
	fmt.Println(value, ok)

	// Output: Go true
}
```

### Readable files with `diskv` and `JSON`
An interesting use case for `Shelf` is storing data in files that can be read
transparently with the `JSON` format, each named by a semantically meaningful
key. This can be used to save configuration and application data as
human-readable files.

The `diskv` driver in [`driver/db/diskv`](./driver/db/diskv) provides a
database driver that uses a key-value store based on a file-per-record design
that would suit this purpose.

The example below provides a simple illustration of how this could be done to
save data for an imaginary game:
```go
package main

import (
	"fmt"
	"os"
	"path/filepath"

	diskvd "github.com/lucmq/go-shelve/driver/db/diskv"
	"github.com/lucmq/go-shelve/shelve"
)

var StoragePath = filepath.Join(os.TempDir(), "game-test", "db")

type Player struct {
	Name  string
	Level int
	Gold  int
	Items []string
}

type Config struct {
	Difficulty string
}

// NewShelf creates a customized Shelf using Diskv and JSON.
func NewShelf[V any](path string) (*shelve.Shelf[string, V], error) {
	path = filepath.Join(StoragePath, path)
	extension := "json" // Extension of the record files

	db, err := diskvd.NewDefault(path, extension)
	if err != nil {
		return nil, err
	}

	return shelve.Open[string, V](
		path,
		shelve.WithDatabase(db),
		shelve.WithCodec(shelve.JSONCodec()),
	)
}

func main() {
	// Open the shelf with custom options
	players, _ := NewShelf[Player]("players")
	config, _ := NewShelf[Config]("config")

	defer players.Close()
	defer config.Close()

	// Create the game data
	player := Player{
		Name:  "Frodo",
		Level: 14,
		Gold:  9999,
		Items: []string{"Sting", "Lembas"},
	}
	cfg := Config{
		Difficulty: "Hard",
	}

	// Save the data. Serialization and persistence will be
	// handled automatically by the Shelf.
	players.Put(player.Name, player)
	config.Put("config", cfg)

	// The app storage will contain readable JSON files with
	// configuration and game state, that can be retrieved
	// back to a Go type:
	value, ok, _ := players.Get("Frodo")
	fmt.Println(ok, value.Name, value.Items)

	// Output: true Frodo [Sting Lembas]
}
```

# Contributing
Contributions to this package are welcome! If you find any issues or have suggestions
for improvements, please feel free to open an issue or submit a pull request.

In particular, if you are interested in contributing a driver for a key-value storage
or a encoding format, check this [guideline and wishlist](./driver/README.md).

# License
This project is licensed under the MIT License - see the LICENSE file for details.
