package badgerd

import (
	"fmt"
	"testing"

	shelvetest "github.com/lucmq/go-shelve/driver/test"
)

func BenchmarkStore_Items(b *testing.B) {
	N := 100000
	batchSize := 100

	// Create a database initialized with test data
	seed := make(map[string]string, N)
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		seed[key] = value
	}
	db := shelvetest.StartDatabase(b, OpenTestDB, seed)

	b.Run("Items", func(b *testing.B) {
		read := 0
		n := 0
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			err := db.Items(nil, 1, func(k, v []byte) (bool, error) {
				read++
				n++
				if n >= batchSize {
					n = 0
					return false, nil
				}
				return true, nil
			})
			if err != nil {
				b.Fatalf("expected no error, got %v", err)
			}
		}
		b.Logf(
			"Read: %d (%d items/s)",
			read,
			int(float64(read)/b.Elapsed().Seconds()),
		)
	})
}
