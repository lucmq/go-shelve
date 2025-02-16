package shelve

import (
	"fmt"
	"os"
	"testing"
)

// Note: The benchmarks below are performed using the default Go-Shelve database
// (sdb). Results may vary when using different database drivers.

func BenchmarkShelf_Put(b *testing.B) {
	path := TestDirectory
	os.RemoveAll(path)

	s, err := Open[string, string](path)
	if err != nil {
		b.Fatalf("open db: %s", err)
	}
	defer s.Close()

	// Create test data
	N := 100_000
	items := make([]string, N)
	for i := 0; i < N; i++ {
		items[i] = fmt.Sprintf("key-%d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Put(items[i%N], items[i%N]); err != nil {
			b.Fatalf("put error: %v", err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

func BenchmarkShelf_Get(b *testing.B) {
	path := TestDirectory
	os.RemoveAll(path)

	s, err := Open[string, string](path)
	if err != nil {
		b.Fatalf("open db: %s", err)
	}
	defer s.Close()

	// Create test data
	N := 100_000
	items := make([]string, N)
	for i := 0; i < N; i++ {
		items[i] = fmt.Sprintf("key-%d", i)
	}

	// Insert test data before running the Get benchmark
	for i := 0; i < N; i++ {
		if err := s.Put(items[i], items[i]); err != nil {
			b.Fatalf("put error: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok, err := s.Get(items[i%N])
		if err != nil {
			b.Fatalf("get error: %v", err)
		}
		if !ok {
			b.Fatalf("expected key %v to exist", items[i%N])
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}
