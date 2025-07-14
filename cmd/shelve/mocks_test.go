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

func (f fakeCodec) Encode(any) ([]byte, error) { return nil, TestError }
func (f fakeCodec) Decode([]byte, any) error   { return TestError }

type fakeDB struct{}

func (f fakeDB) Close() error {
	return nil
}

func (f fakeDB) Len() int64 {
	return -1
}

func (f fakeDB) Sync() error {
	return TestError
}

func (f fakeDB) Has([]byte) (bool, error) {
	return false, TestError
}

func (f fakeDB) Get([]byte) ([]byte, error) {
	return nil, TestError
}

func (f fakeDB) Put([]byte, []byte) error {
	return TestError
}

func (f fakeDB) Delete([]byte) error {
	return TestError
}

func (f fakeDB) Items([]byte, int, func(key []byte, value []byte) (bool, error)) error {
	return TestError
}
