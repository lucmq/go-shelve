package msgpack

import (
	"fmt"
	"go-shelve/shelve"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkShelf_Get_SDB_Msgpack(b *testing.B) {
	path := filepath.Join(os.TempDir(), "go-shelve")
	err := os.RemoveAll(path)
	if err != nil {
		b.Fatalf("remove db: %s", err)
	}

	s, err := shelve.Open[uint64, *Product](
		path,
		shelve.WithCodec(NewDefault()),
	)
	if err != nil {
		b.Fatalf("open db: %s", err)
	}

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

	// Insert test data
	b.ResetTimer()
	inserted := 0
	for i := 0; i < N; i++ {
		err := s.Put(items[i].ID, &items[i])
		if err != nil {
			b.Errorf("Expected no error, but got %v", err)
		}
		inserted++
	}

	b.Logf(
		"Inserted: %d (%.0f items/s)",
		inserted,
		float64(inserted)/b.Elapsed().Seconds(),
	)

	b.Run("Get", func(b *testing.B) {
		total := 0

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, ok, err := s.Get(items[i%N].ID)
			if err != nil {
				b.Errorf("Expected no error, but got %v", err)
			}
			if !ok {
				b.Errorf("Expected key %v to exist", items[i%N].ID)
			}

			total++
		}

		b.Logf(
			"Get: %d (%.0f items/s)",
			total,
			float64(total)/b.Elapsed().Seconds(),
		)
	})
}
