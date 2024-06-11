package sdb_test

import (
	"bytes"
	"encoding/gob"
	"go-shelve/sdb"
	"log"
	"os"
	"path/filepath"
)

func Example() {
	path := filepath.Join(os.TempDir(), "sdb")

	// Open the database
	db, err := sdb.Open(path)
	if err != nil {
		log.Printf("open: %s", err)
		return
	}
	defer db.Close()

	item := struct {
		Name     string
		Price    float64
		Quantity int
		Active   bool
	}{
		Name:     "Apple",
		Price:    1.99,
		Quantity: 10,
		Active:   true,
	}

	// Encode data as gob
	data := new(bytes.Buffer)
	enc := gob.NewEncoder(data)
	if err = enc.Encode(item); err != nil {
		log.Printf("encode gob: %s", err)
		return
	}

	// Save the data
	err = db.Put([]byte("apple"), data.Bytes())
	if err != nil {
		log.Printf("put: %s", err)
		return
	}
}
