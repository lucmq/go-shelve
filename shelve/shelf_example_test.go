package shelve_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/lucmq/go-shelve/sdb"

	"github.com/lucmq/go-shelve/shelve"
)

func ExampleOpen() {
	// Note: In a real application be sure to replace the
	// os.TempDir() with a non temporary directory and
	// provide better error treatment.
	path := filepath.Join(os.TempDir(), "go-shelve")

	// Open the shelf with default options
	shelf, err := shelve.Open[string, string](path)
	if err != nil {
		log.Printf("open: %s", err)
		return
	}
	defer shelf.Close()

	// Use the database
	shelf.Put("language", "Go")
	shelf.Put("module", "go-shelve")

	// Note: Saved values will be available between restarts
	value, ok, _ := shelf.Get("language")
	fmt.Println(value, ok)

	// Output: Go true
}

func ExampleOpen_withDatabase() {
	path := filepath.Join(os.TempDir(), "go-shelve")

	// Configure some options for the default go-shelf database. This
	// can also be used to open go-shelf with another database (like
	// boltdb, badger, etc). Check the driver packages for more
	// options.
	db, _ := sdb.Open(
		path,
		sdb.WithCacheSize(1024*1024),
		sdb.WithSynchronousWrites(true),
	)
	shelf, err := shelve.Open[string, string](
		path,
		shelve.WithDatabase(db),
		shelve.WithCodec(shelve.JSONCodec()),
		shelve.WithKeyCodec(shelve.TextCodec()),
	)
	if err != nil {
		log.Printf("open: %s", err)
		return
	}
	defer shelf.Close()

	// Use the database
	shelf.Put("example", "go-shelve")

	value, ok, _ := shelf.Get("example")
	fmt.Println(value, ok)

	// Output: go-shelve true
}

func ExampleShelf_Put() {
	path := filepath.Join(os.TempDir(), "go-shelve")
	Clean(path) // Only for the example

	type Example struct {
		I int
		M map[string]string
	}

	shelf, err := shelve.Open[string, Example](path)
	if err != nil {
		log.Printf("open: %s", err)
		return
	}
	defer shelf.Close()

	// Put an item
	shelf.Put("a", Example{I: 42, M: map[string]string{"a": "1"}})

	// Get the item
	value, ok, _ := shelf.Get("a")
	fmt.Println(value, ok)

	// Mutate the item. Remember to Save the mutated item to
	// make the change persistent.
	value.M["b"] = "2"
	shelf.Put("a", value)

	newValue, ok, _ := shelf.Get("a")
	fmt.Println(newValue, ok)

	// Output:
	// {42 map[a:1]} true
	// {42 map[a:1 b:2]} true
}

func ExampleShelf_Items() {
	path := filepath.Join(os.TempDir(), "go-shelve")
	Clean(path) // Only for the example

	shelf, err := shelve.Open[string, string](path)
	if err != nil {
		log.Printf("open: %s", err)
		return
	}
	defer shelf.Close()

	// Put some items
	shelf.Put("a", "1")
	shelf.Put("b", "2")
	shelf.Put("c", "3")

	fn := func(key, value string) (bool, error) {
		fmt.Println(key, value)
		return true, nil
	}
	start := "a"

	err = shelf.Items(&start, shelve.All, shelve.Asc, fn)
	if err != nil {
		log.Printf("items: %s", err)
		return
	}

	// Note: The call above is the same as shelve.Items(&start, -1, 1, fn)

	// Note: The output will be sorted by key if the underlying
	// database driver supports ordered iteration.

	// Unordered output:
	// a 1
	// b 2
	// c 3
}

// Clean is a function used with example code to start with a clean storage.
func Clean(path string) error {
	return os.RemoveAll(path)
}
