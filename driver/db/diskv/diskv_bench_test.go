package diskvd

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	shelvetest "github.com/lucmq/go-shelve/driver/test"
	"github.com/peterbourgon/diskv/v3"
)

func CreateBenchDiskvDB(b *testing.B) *diskv.Diskv {
	path := filepath.Join(os.TempDir(), dbPath)

	// Clean-up database directory
	err := os.RemoveAll(path)
	if err != nil {
		b.Fatalf("remove db: %s", err)
	}

	// Initialize a new diskv store, rooted at `path`, with a 1GB cache.
	db := diskv.New(diskv.Options{
		BasePath:     path,
		Transform:    func(s string) []string { return []string{s} },
		CacheSizeMax: 1024 * 1024 * 1024,
		Index:        newBTreeIndex(),
	})
	return db
}

func BenchmarkDiskvPut(b *testing.B) {
	db := CreateBenchDiskvDB(b)

	// Generate 1M entries
	N := 1000000
	keys := make([]string, N)
	values := make([][]byte, N)
	for i := 0; i < N; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
		values[i] = []byte(fmt.Sprintf("value-%d", i))
	}

	b.Run("Put", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := db.Write(keys[i%N], values[i%N]); err != nil {
				b.Fatalf("put: %s", err)
			}
		}

		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
	})
}

func BenchmarkDiskvGet(b *testing.B) {
	db := CreateBenchDiskvDB(b)

	// Generate 100K entries
	N := 100000
	keys := make([]string, N)
	values := make([][]byte, N)
	for i := 0; i < N; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
		values[i] = []byte(fmt.Sprintf("value-%d", i))
	}

	// Insert test data before running the Get benchmark.
	for i := 0; i < N; i++ {
		if err := db.Write(keys[i], values[i]); err != nil {
			b.Fatalf("put: %s", err)
		}
	}

	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := db.Read(keys[i%N])
			if err != nil {
				b.Fatalf("get: %s", err)
			}
		}

		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
	})
}

func BenchmarkStore_Items(b *testing.B) {
	N := 100000
	batchSize := 100

	// Create and populate a database.
	seed := make(map[string]string, N)
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key-%d", i)
		seed[key] = fmt.Sprintf("value-%d", i)
	}
	tdb := shelvetest.StartDatabase(b, OpenTestDB, seed)

	b.Run("Items", func(b *testing.B) {
		read := 0 // Track actual number of items read

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := tdb.Items(nil, 1, func(k, v []byte) (bool, error) {
				read++
				if read%batchSize == 0 {
					return false, nil // Stop batch after batchSize iterations
				}
				return true, nil // Continue iteration
			})
			if err != nil {
				b.Fatalf("items: %s", err)
			}
		}

		b.ReportMetric(float64(read)/b.Elapsed().Seconds(), "ops/sec")
	})
}
