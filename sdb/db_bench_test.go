package sdb

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
)

var BenchmarkOptions = []Option{
	WithSynchronousWrites(false),
	WithCacheSize(DefaultCacheSize),
}

func OpenBenchDB(b testing.TB, opts ...Option) *DB {
	opts = append(BenchmarkOptions, opts...)
	open := NewOpenFunc(true, opts...)
	return StartDatabase(b, open, nil)
}

func BenchmarkDB_Put(b *testing.B) {
	benchmarks := []struct {
		name string
		opts []Option // Additions to the default options
	}{
		{
			name: "Sync Writes",
			opts: []Option{WithSynchronousWrites(true)},
		},
		{
			name: "Async Writes",
			opts: []Option{WithSynchronousWrites(false)},
		},
		{
			name: "No Cache",
			opts: []Option{WithCacheSize(0)},
		},
	}

	// Generate 1M entries
	N := 1000000
	items := make([][2][]byte, N)
	for i := 0; i < N; i++ {
		items[i] = [2][]byte{
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)),
		}
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			db := OpenBenchDB(b, bm.opts...)
			defer db.Close()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := db.Put(items[i%N][0], items[i%N][1])
				if err != nil {
					b.Fatalf("put: %s", err)
				}
			}

			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}

func BenchmarkDB_Put_Concurrent(b *testing.B) {
	// Generate 1M entries
	N := 1000000
	items := make([][2][]byte, N)
	for i := 0; i < N; i++ {
		items[i] = [2][]byte{
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)),
		}
	}

	C := 100 // Number of goroutines
	b.Run(fmt.Sprintf("Put-%d", C), func(b *testing.B) {
		db := OpenBenchDB(b)
		defer db.Close()

		var wg sync.WaitGroup

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < C; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < b.N/C; j++ {
					// Use rand to avoid many simultaneous writes on the same file.
					x := rand.Int()
					err := db.Put(items[x%N][0], items[x%N][1])
					if err != nil {
						b.Errorf("put: %s", err)
						return
					}
				}
			}()
		}
		wg.Wait()

		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
	})
}

func BenchmarkDB_Get(b *testing.B) {
	benchmarks := []struct {
		name      string
		seedCount int
		opts      []Option // Additions to the default options
	}{
		{
			name:      "Sync Writes",
			seedCount: 10000,
			opts:      []Option{WithSynchronousWrites(true)},
		},
		{
			name:      "Async Writes",
			seedCount: 100000,
			opts:      []Option{WithSynchronousWrites(false)},
		},
		{
			name:      "No Cache",
			seedCount: 100000,
			opts:      []Option{WithCacheSize(0)},
		},
	}

	for _, bm := range benchmarks {
		// Generate sample entries
		N := bm.seedCount
		items := make([][2][]byte, N)
		for i := 0; i < N; i++ {
			items[i] = [2][]byte{
				[]byte(fmt.Sprintf("key-%d", i)),
				[]byte(fmt.Sprintf("value-%d", i)),
			}
		}

		db := OpenBenchDB(b, bm.opts...)

		for i := 0; i < N; i++ {
			err := db.Put(items[i][0], items[i][1])
			if err != nil {
				b.Fatalf("put: %s", err)
			}
		}

		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := db.Get(items[i%N][0])
				if err != nil {
					b.Fatalf("get: %s", err)
				}
			}

			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})

		db.Close()
	}
}

func BenchmarkDB_Items(b *testing.B) {
	benchmarks := []struct {
		name      string
		order     int
		seedSize  int
		batchSize int
		opts      []Option // Additions to the default options
	}{
		{
			name:      "Cache",
			order:     Asc,
			seedSize:  100000,
			batchSize: 1000,
			opts:      []Option{WithCacheSize(-1)},
		},
		{
			name:      "No Cache",
			order:     Asc,
			seedSize:  100000,
			batchSize: 1000,
			opts:      []Option{WithCacheSize(0)},
		},
	}

	for _, bm := range benchmarks {
		// Generate sample entries
		N := bm.seedSize
		items := make([][2][]byte, N)
		for i := 0; i < N; i++ {
			items[i] = [2][]byte{
				[]byte(fmt.Sprintf("key-%d", i)),
				[]byte(fmt.Sprintf("value-%d", i)),
			}
		}

		db := OpenBenchDB(b, bm.opts...)

		for i := 0; i < N; i++ {
			err := db.Put(items[i][0], items[i][1])
			if err != nil {
				b.Fatalf("put: %s", err)
			}
		}

		b.Run(bm.name, func(b *testing.B) {
			itemsRead := 0
			n := 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := db.Items(
					nil,
					bm.order,
					func(key, value []byte) (bool, error) {
						itemsRead++
						n++
						if n >= bm.batchSize {
							n = 0
							return false, nil
						}
						return true, nil
					},
				)
				if err != nil {
					b.Fatalf("items: %s", err)
				}
			}

			b.ReportMetric(float64(itemsRead)/b.Elapsed().Seconds(), "ops/sec")
		})

		db.Close()
	}
}
