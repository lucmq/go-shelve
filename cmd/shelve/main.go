package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/lucmq/go-shelve/shelve"
)

type (
	K = string
	V = string
)

var exitOnError = true

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "run failed: %v", err)
		if exitOnError {
			os.Exit(1)
		}
	}
}

func run() error {
	flag.Usage = printUsage

	storePath := flag.String("path", ".store", "Path to the shelve store")
	codecName := flag.String("codec", "json", "value serialization format: gob, json, or string")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		return nil
	}

	command := args[0]
	commandArgs := args[1:]

	codec, err := getCodec(*codecName)
	if err != nil {
		return fmt.Errorf("get codec: %w", err)
	}

	// Open the shelve store
	store, err := shelve.Open[K, V](
		*storePath,
		shelve.WithCodec(codec),
	)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	// Execute the appropriate command
	switch command {
	case "put":
		return handlePut(store, commandArgs)
	case "get":
		return handleGet(store, commandArgs)
	case "has":
		return handleHas(store, commandArgs)
	case "delete":
		return handleDelete(store, commandArgs)
	case "len":
		return handleLen(store)
	case "items":
		return handleItems(store, "items", commandArgs)
	case "keys":
		return handleItems(store, "keys", commandArgs)
	case "values":
		return handleItems(store, "values", commandArgs)
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}

func getCodec(name string) (shelve.Codec, error) {
	switch name {
	case "gob":
		return shelve.GobCodec(), nil
	case "json":
		return shelve.JSONCodec(), nil
	case "string":
		return shelve.StringCodec(), nil
	default:
		return nil, fmt.Errorf("unsupported codec: %s", name)
	}
}

// Put key-value pairs.
func handlePut(store *shelve.Shelf[K, V], args []string) error {
	if len(args) < 2 || len(args)%2 != 0 {
		return errors.New("usage: shelve put <key> <value> [<key> <value> ...]")
	}

	for i := 0; i < len(args); i += 2 {
		key := args[i]
		value := args[i+1]

		if err := store.Put(key, value); err != nil {
			return fmt.Errorf("put key-value pair (%s, %s): %w", key, value, err)
		}
	}
	fmt.Println("OK")
	return nil
}

// Get value by key.
func handleGet(store *shelve.Shelf[K, V], args []string) error {
	if len(args) < 1 {
		return errors.New("usage: shelve get <key>")
	}

	key := args[0]
	value, _, err := store.Get(key)
	if err != nil {
		return fmt.Errorf("get key: %w", err)
	}

	fmt.Println(value)
	return nil
}

// Check if a key exists.
func handleHas(store *shelve.Shelf[K, V], args []string) error {
	if len(args) < 1 {
		return errors.New("usage: shelve has <key>")
	}

	key := args[0]
	ok, err := store.Has(key)
	if err != nil {
		return fmt.Errorf("check key existence: %w", err)
	}

	if ok {
		fmt.Println("true")
	} else {
		fmt.Println("false")
	}
	return nil
}

// Delete a key.
func handleDelete(store *shelve.Shelf[K, V], args []string) error {
	if len(args) < 1 {
		return errors.New("usage: shelve delete <key>")
	}

	key := args[0]
	if err := store.Delete(key); err != nil {
		return fmt.Errorf("delete key: %w", err)
	}

	fmt.Println("OK")
	return nil
}

// Get total number of keys.
func handleLen(store *shelve.Shelf[K, V]) error {
	count := store.Len()
	if count == -1 {
		return errors.New("failed to get length")
	}

	fmt.Println(count)
	return nil
}

// List items, keys, or values with optional filters.
func handleItems(store *shelve.Shelf[K, V], mode string, args []string) error {
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	start := fs.String("start", "", "Start key (inclusive)")
	end := fs.String("end", "", "End key (exclusive)")
	limit := fs.Int("limit", shelve.All, "Maximum number of items")
	_ = fs.Bool("paged", false, "Enable pagination (future feature)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	switch mode {
	case "items":
		return printItems(store, start, end, *limit)
	case "keys":
		return printKeys(store, start, end, *limit)
	case "values":
		return printValues(store, start, end, *limit)
	default:
		return fmt.Errorf("invalid mode: %s", mode)
	}
}

// Helper: Print key-value pairs.
func printItems(store *shelve.Shelf[K, V], start, end *string, limit int) error {
	return store.Items(start, limit, shelve.Asc, func(key K, value V) (bool, error) {
		if *end != "" && key >= *end {
			return false, nil
		}
		fmt.Println(key, value)
		return true, nil
	})
}

// Helper: Print keys only.
func printKeys(store *shelve.Shelf[K, V], start, end *string, limit int) error {
	return store.Keys(start, limit, shelve.Asc, func(key K, _ V) (bool, error) {
		if *end != "" && key >= *end {
			return false, nil
		}
		fmt.Println(key)
		return true, nil
	})
}

// Helper: Print values only.
func printValues(store *shelve.Shelf[K, V], start, end *string, limit int) error {
	return store.Items(start, limit, shelve.Asc, func(key K, value V) (bool, error) {
		if *end != "" && key >= *end {
			return false, nil
		}
		fmt.Println(value)
		return true, nil
	})
}

func printUsage() {
	fmt.Println(`shelve is a CLI tool for managing a shelve key-value store.

Usage:

    shelve [options] <command> [arguments]

The commands are:

    put         store one or more key-value pairs
    get         retrieve the value of a key
    has         check if a key exists
    delete      remove a key
    len         count total keys in the store
    items       list key-value pairs
    keys        list only the keys
    values      list only the values

Options:
 `)
	flag.PrintDefaults()
}
