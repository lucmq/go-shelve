package sdb_test

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"

	"github.com/lucmq/go-shelve/sdb"
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

	// Encode data as json
	data, err := json.Marshal(item)
	if err != nil {
		log.Printf("marshal json: %s", err)
		return
	}

	// Save the data
	err = db.Put([]byte("apple"), data)
	if err != nil {
		log.Printf("put: %s", err)
		return
	}
}
