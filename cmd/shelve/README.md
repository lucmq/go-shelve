# Go-Shelve CLI

A simple and extensible CLI tool for managing a [go-shelve](https://github.com/lucmq/go-shelve) key-value store.

## Installation

```sh
go install github.com/lucmq/go-shelve/cmd/shelve@latest
```

## Key Features

* Simple CLI for interacting with shelve stores
* Supports multiple codecs: `json`, `gob`, and `text`
* Filtered listing (`--start`, `--end`, `--limit`)
* Composable with shell tools (e.g. `sort`, `grep`)
* Defaults to JSON serialization for compatibility and readability

## Limitations

* Currently assumes `string` keys and values when used from the CLI (`shelve.Shelf[string, string]`)
* Only the default shelf backend is used (no plug-in store drivers yet)
* Best used with the **JSON codec** for interoperability

---

## Usage

```
Usage:

    shelve [options] <command> [arguments]

Commands:

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
        Value serialization format: gob, json, or text (default "json")
  -path string
        Path to the shelve store (default ".store")
```

---

## Examples

### Basic Operations

```sh
# Store key-value pairs
shelve put key1 value1 key2 value2

# Retrieve a value
shelve get key1
# Output: value1

# Check key existence
shelve has key2
# Output: true

# Delete a key
shelve delete key2

# Count entries
shelve len
# Output: 1
```

### Listing and Filtering

```sh
# List all items
shelve items
# Output:
# key1 value1

# List keys
shelve keys
# Output:
# key1

# List values
shelve values
# Output:
# value1

# Filtered listing
shelve items -start "key1" -end "key9" -limit 10
```

### Use Case: TODO List

```sh
# Add a task with a timestamp key
shelve put $(date +%s) "Do the laundry"

# View all tasks (sorted)
shelve items | sort
```

---

## Storage Format

Shelves are stored as key-sorted files under the path specified by `-path`. By default, this is `.store` in the current directory.

Each key-value entry is serialized using the selected codec (e.g., JSON). The store maintains a persistent, ordered key-value log on disk.

* Text-based codecs like `json` allow human-readable inspection
* You can back up, copy, or version `.store` directories safely

---

## ⚠️ Codec Compatibility

> The CLI is most compatible with **JSON-based shelves**.
>
> Using codecs like `gob` or `text` may result in limited or unreadable output, especially if values are complex Go structs.
>
> To ensure full CLI support, prefer the default JSON codec:

```go
db, _ := shelve.Open("my.shelve") // Uses JSON codec by default
```
