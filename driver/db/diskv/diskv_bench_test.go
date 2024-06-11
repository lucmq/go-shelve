package diskvd

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

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
	items := make([][2][]byte, N)
	for i := 0; i < N; i++ {
		items[i] = [2][]byte{
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)),
		}
	}

	b.Run("Put", func(b *testing.B) {
		itemsInserted := 0

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := db.Write(string(items[i%N][0]), items[i%N][1])
			if err != nil {
				b.Fatalf("put: %s", err)
			}

			itemsInserted++
		}

		b.Logf(
			"Insert: %d (%d items/s)",
			itemsInserted,
			int(float64(itemsInserted)/b.Elapsed().Seconds()),
		)
	})
}

func BenchmarkDiskvGet(b *testing.B) {
	db := CreateBenchDiskvDB(b)

	// Generate 100K entries
	N := 100000
	items := make([][2][]byte, N)
	for i := 0; i < N; i++ {
		items[i] = [2][]byte{
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)),
		}
	}

	for i := 0; i < N; i++ {
		err := db.Write(string(items[i][0]), items[i][1])
		if err != nil {
			b.Fatalf("put: %s", err)
		}
	}

	b.Run("Get", func(b *testing.B) {
		itemsRead := 0

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := db.Read(string(items[i%N][0]))
			if err != nil {
				b.Fatalf("get: %s", err)
			}

			itemsRead++
		}

		b.Logf(
			"Read: %d (%d items/s)",
			itemsRead,
			int(float64(itemsRead)/b.Elapsed().Seconds()),
		)
	})
}
