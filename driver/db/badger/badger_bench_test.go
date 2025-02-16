package badgerd

import (
	"fmt"
	"testing"

	shelvetest "github.com/lucmq/go-shelve/driver/test"
)

func BenchmarkStore_Put(b *testing.B) {
	N := 100000

	// Generate sample entries.
	keys := make([][]byte, N)
	values := make([][]byte, N)
	for i := 0; i < N; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		values[i] = []byte(fmt.Sprintf("value-%d", i))
	}

	db := shelvetest.StartDatabase(b, OpenTestDB, nil)

	b.Run("Put", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := db.Put(keys[i%N], values[i%N]); err != nil {
				b.Fatalf("put error: %v", err)
			}
		}

		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
	})
}

func BenchmarkStore_Get(b *testing.B) {
	N := 100000

	// Create and populate a database.
	seed := make(map[string]string, N)
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key-%d", i)
		seed[key] = fmt.Sprintf("value-%d", i)
	}
	db := shelvetest.StartDatabase(b, OpenTestDB, seed)

	b.Run("Get", func(b *testing.B) {
		itemsRead := 0
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key-%d", i%N)
			_, err := db.Get([]byte(key))
			if err != nil {
				b.Fatalf("get error: %v", err)
			}
			itemsRead++
		}

		b.ReportMetric(float64(itemsRead)/b.Elapsed().Seconds(), "ops/sec")
	})
}

// Note: The `Items` operation is really fast for BadgerDB.
func BenchmarkStore_Items(b *testing.B) {
	N := 100000
	batchSize := 100

	// Create a database initialized with test data.
	seed := make(map[string]string, N)
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		seed[key] = value
	}
	db := shelvetest.StartDatabase(b, OpenTestDB, seed)

	b.Run("Items", func(b *testing.B) {
		read := 0 // Track the actual number of items read
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := db.Items(nil, 1, func(k, v []byte) (bool, error) {
				read++
				if read%batchSize == 0 {
					return false, nil // Stop batch after batchSize iterations
				}
				return true, nil
			})
			if err != nil {
				b.Fatalf("expected no error, got %v", err)
			}
		}

		b.ReportMetric(float64(read)/b.Elapsed().Seconds(), "ops/sec")
	})
}
