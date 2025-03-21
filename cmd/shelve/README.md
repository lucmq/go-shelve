# Go-Shelve CLI

A CLI tool for managing a shelve key-value store.

## Installing

```shell
go install github.com/lucmq/go-shelve/cmd/shelve@latest
```

## Limitations

- Currently, it only supports string keys and values (`shelve.Shelf[string, string]`).
- Only the default database `SDB` is supported.

## Usage

```
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

  -codec string
        value serialization format: gob, json, or string (default "gob")
  -path string
        Path to the shelve store (default ".store")
```

## Examples

```shell
# Put a key-value pair
shelve .store put key1 value1

# Get a value
shelve .store get key1

# Get all items with filters
shelve .store items -start "key-2" -end "key-4" -limit 10 -paged
```

```shell
shelve .store put key1 value1 key2 value2 key3 value3
shelve .store get key1
# Output: value1

shelve .store has key2
# Output: true

shelve .store len
# Output: 3

shelve .store all
# Output:
# key1 value1
# key2 value2
# key3 value3

shelve .store keys
# Output:
# key1
# key2
# key3

shelve .store values
# Output:
# value1
# value2
# value3
```

```shell
# Using as a TODO list
shelve put `date +%s` "Do the laundry"

shelve items | sort
```
