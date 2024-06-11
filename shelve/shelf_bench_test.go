package shelve

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkShelf_Get(b *testing.B) {
	path := TestDirectory
	os.RemoveAll(path)

	s, err := Open[string, string](path)
	if err != nil {
		b.Fatalf("open db: %s", err)
	}

	// Create test data
	N := 100_000
	items := make([]string, N)
	for i := 0; i < N; i++ {
		items[i] = fmt.Sprintf("key-%d", i)
	}

	// Insert test data
	b.ResetTimer()
	inserted := 0
	for i := 0; i < N; i++ {
		err := s.Put(items[i], items[i])
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
			_, ok, err := s.Get(items[i%N])
			if err != nil {
				b.Errorf("Expected no error, but got %v", err)
			}
			if !ok {
				b.Errorf("Expected key %v to exist", items[i%N])
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
