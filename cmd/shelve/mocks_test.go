package main

import (
	"errors"
	"testing"

	"github.com/lucmq/go-shelve/shelve"
)

var TestError = errors.New("test error")

func newFakeShelve(t *testing.T) *Shelf {
	s, err := shelve.Open[string, string](
		dbPath,
		shelve.WithKeyCodec(fakeCodec{}),
		shelve.WithDatabase(fakeDB{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type fakeCodec struct{}

func (fakeCodec) Encode(any) ([]byte, error) { return nil, TestError }
func (fakeCodec) Decode([]byte, any) error   { return TestError }

type fakeDB struct{}

func (fakeDB) Close() error {
	return nil
}

func (fakeDB) Len() int64 {
	return -1
}

func (fakeDB) Sync() error {
	return TestError
}

func (fakeDB) Has([]byte) (bool, error) {
	return false, TestError
}

func (fakeDB) Get([]byte) ([]byte, error) {
	return nil, TestError
}

func (fakeDB) Put([]byte, []byte) error {
	return TestError
}

func (fakeDB) Delete([]byte) error {
	return TestError
}

func (fakeDB) Items([]byte, int, func(key []byte, value []byte) (bool, error)) error {
	return TestError
}
