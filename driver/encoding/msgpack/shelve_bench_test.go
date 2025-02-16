package msgpack

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/lucmq/go-shelve/shelve"
)

func BenchmarkShelf_Put_SDB_Msgpack(b *testing.B) {
	path := filepath.Join(os.TempDir(), "go-shelve")
	if err := os.RemoveAll(path); err != nil {
		b.Fatalf("remove db: %s", err)
	}

	s, err := shelve.Open[uint64, *Product](
		path,
		shelve.WithCodec(NewDefault()),
	)
	if err != nil {
		b.Fatalf("open db: %s", err)
	}
	defer s.Close()

	// Create test data
	N := 100_000
	items := make([]Product, N)
	for i := 0; i < N; i++ {
		items[i] = Product{
			ID:       uint64(i),
			Name:     fmt.Sprintf("Product %d", i),
			Price:    float64(i),
			Quantity: rand.N(10000),
			Active:   true,
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Put(items[i%N].ID, &items[i%N]); err != nil {
			b.Fatalf("put error: %v", err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

func BenchmarkShelf_Get_SDB_Msgpack(b *testing.B) {
	path := filepath.Join(os.TempDir(), "go-shelve")
	if err := os.RemoveAll(path); err != nil {
		b.Fatalf("remove db: %s", err)
	}

	s, err := shelve.Open[uint64, *Product](
		path,
		shelve.WithCodec(NewDefault()),
	)
	if err != nil {
		b.Fatalf("open db: %s", err)
	}
	defer s.Close()

	// Create test data
	N := 100_000
	items := make([]Product, N)
	for i := 0; i < N; i++ {
		items[i] = Product{
			ID:       uint64(i),
			Name:     fmt.Sprintf("Product %d", i),
			Price:    float64(i),
			Quantity: rand.N(10000),
			Active:   true,
		}
	}

	// Insert test data before running Get benchmark
	for i := 0; i < N; i++ {
		if err := s.Put(items[i].ID, &items[i]); err != nil {
			b.Fatalf("put error: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok, err := s.Get(items[i%N].ID)
		if err != nil {
			b.Fatalf("get error: %v", err)
		}
		if !ok {
			b.Fatalf("expected key %v to exist", items[i%N].ID)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}
