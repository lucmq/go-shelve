package internal

import (
	"fmt"
	"strconv"
	"testing"
)

// Tests

func TestNewCache(t *testing.T) {
	t.Run("maxLength < 0", func(t *testing.T) {
		c := NewCache[int](-1)
		if _, ok := c.cache.(*unboundedCache[int]); !ok {
			t.Errorf("NewCache did not return the expected type " +
				"unboundedCache[int] for maxLength < 0")
		}
	})

	t.Run("maxLength = 0", func(t *testing.T) {
		c := NewCache[int](0)
		if _, ok := c.cache.(*passThroughCache[int]); !ok {
			t.Errorf("NewCache did not return the expected type " +
				"passThroughCache[int] for maxLength = 0")
		}
	})

	t.Run("maxLength >= 1", func(t *testing.T) {
		c := NewCache[int](1)
		if _, ok := c.cache.(*randomCache[int]); !ok {
			t.Errorf("NewCache did not return the expected type " +
				"randomCache[int] for maxLength >= 1")
		}
	})
}

func TestCacheStats(t *testing.T) {
	t.Run("Empty Cache", func(t *testing.T) {
		c := NewCache[int](-1)
		if c.Hits() != 0 {
			t.Errorf("Expected hits to be 0, got %d", c.Hits())
		}
		if c.Misses() != 0 {
			t.Errorf("Expected misses to be 0, got %d", c.Misses())
		}
	})

	t.Run("Non Empty Cache", func(t *testing.T) {
		c := NewCache[int](-1)

		c.Put("1", 10)
		c.Put("2", 20)
		c.Put("3", 30)

		c.Get("1")
		c.Get("2")
		c.Get("4")

		if c.Hits() != 2 {
			t.Errorf("Expected hits to be 2, got %d", c.Hits())
		}
		if c.Misses() != 1 {
			t.Errorf("Expected misses to be 1, got %d", c.Misses())
		}
	})

	t.Run("Reset Ratio", func(t *testing.T) {
		c := NewCache[int](-1)

		c.Put("1", 10)
		c.Put("2", 20)
		c.Put("3", 30)

		c.Get("1")
		c.Get("2")
		c.Get("4")

		c.ResetRatio()

		c.Get("1")
		c.Get("4")

		if c.Hits() != 1 {
			t.Errorf("Expected hits to be 1, got %d", c.Hits())
		}
		if c.Misses() != 1 {
			t.Errorf("Expected misses to be 1, got %d", c.Misses())
		}
	})

	t.Run("Delete", func(t *testing.T) {
		c := NewCache[int](-1)

		c.Put("1", 10)
		c.Put("2", 20)
		c.Put("3", 30)

		c.Get("1")
		c.Get("2")
		c.Get("4")

		c.Delete("2")
		c.Get("2")

		if c.Hits() != 2 {
			t.Errorf("Expected hits to be 2, got %d", c.Hits())
		}
		if c.Misses() != 2 {
			t.Errorf("Expected misses to be 2, got %d", c.Misses())
		}
	})
}

func TestRandomCache_Get(t *testing.T) {
	t.Run("Empty Cache", func(t *testing.T) {
		c := newRandomCache[int](10)
		value, ok := c.Get("1")
		if ok {
			t.Errorf("Expected ok to be false, got true")
		}
		var zeroValue int
		if value != zeroValue {
			t.Errorf("Expected value to be zero value, got %d", value)
		}
	})

	t.Run("Existing Key", func(t *testing.T) {
		c := newRandomCache[int](10)
		c.cache["1"] = 2
		value, ok := c.Get("1")
		if !ok {
			t.Errorf("Expected ok to be true, got false")
		}
		if value != 2 {
			t.Errorf("Expected value to be 2, got %d", value)
		}
	})

	t.Run("Non Existing Key", func(t *testing.T) {
		c := newRandomCache[int](10)
		c.cache["1"] = 2
		value, ok := c.Get("2")
		if ok {
			t.Errorf("Expected ok to be false, got true")
		}
		var zeroValue int
		if value != zeroValue {
			t.Errorf("Expected value to be zero value, got %d", value)
		}
	})

	t.Run("Max Size", func(t *testing.T) {
		c := newRandomCache[int](2)
		c.cache["1"] = 2
		c.cache["2"] = 3
		value, ok := c.Get("1")
		if !ok {
			t.Errorf("Expected ok to be true, got false")
		}
		if value != 2 {
			t.Errorf("Expected value to be 2, got %d", value)
		}
	})

	t.Run("Eviction", func(t *testing.T) {
		c := newRandomCache[int](2)
		c.cache["1"] = 2
		c.cache["2"] = 3
		c.cache["3"] = 4
		value, ok := c.Get("1")
		if !ok {
			t.Errorf("Expected ok to be true, got false")
		}
		if value != 2 {
			t.Errorf("Expected value to be 2, got %d", value)
		}
	})
}

func TestRandomCache_Put(t *testing.T) {
	t.Run("Put a new key-value pair", func(t *testing.T) {
		c := newRandomCache[int](2)
		c.Put("1", 10)
		if len(c.cache) != 1 {
			t.Errorf("Expected cache size to be 1, got %d", len(c.cache))
		}
	})

	t.Run("Update an existing key", func(t *testing.T) {
		c := newRandomCache[int](2)
		c.Put("1", 10)
		c.Put("1", 20)
		if len(c.cache) != 1 {
			t.Errorf("Expected cache size to remain 1 after "+
				"updating an existing key, got %d", len(c.cache))
		}
	})

	t.Run("Put more elements than maxSize", func(t *testing.T) {
		c := newRandomCache[int](2)
		c.Put("1", 10)
		c.Put("2", 30)
		c.Put("3", 40)
		if len(c.cache) != 2 {
			t.Errorf("Expected cache size to be limited "+
				"by maxSize, got %d", len(c.cache))
		}
	})
}

func TestRandomCache_Delete(t *testing.T) {
	t.Run("Empty Cache", func(_ *testing.T) {
		c := newRandomCache[int](10)
		c.Delete("1")
	})

	t.Run("Existing Key", func(t *testing.T) {
		c := newRandomCache[int](10)
		c.Put("1", 2)
		c.Delete("1")
		if len(c.cache) != 0 {
			t.Errorf("Expected cache size to be 0, got %d", len(c.cache))
		}
		value, ok := c.Get("1")
		if ok {
			t.Errorf("Expected ok to be false, got true")
		}
		var zeroValue int
		if value != zeroValue {
			t.Errorf("Expected value to be zero value, got %d", value)
		}
	})

	t.Run("Non Existing Key", func(t *testing.T) {
		c := newRandomCache[int](10)
		c.Put("1", 2)
		c.Delete("2")
		if len(c.cache) != 1 {
			t.Errorf("Expected cache size to be 1, got %d", len(c.cache))
		}
		value, ok := c.Get("1")
		if !ok {
			t.Errorf("Expected ok to be true, got false")
		}
		if value != 2 {
			t.Errorf("Expected value to be 2, got %d", value)
		}
	})
}

func TestUnboundedCache(t *testing.T) {
	c := newUnboundedCache[int]()
	c.Put("1", 2)

	value, ok := c.Get("1")
	if !ok {
		t.Errorf("Expected ok to be true, got false")
	}
	if value != 2 {
		t.Errorf("Expected value to be 2, got %d", value)
	}

	c.Delete("1")
	value, ok = c.Get("1")
	if ok {
		t.Errorf("Expected ok to be false, got true")
	}
	var zero int
	if value != zero {
		t.Errorf("Expected value to be zero value, got %d", value)
	}
}

func TestPassThroughCache(t *testing.T) {
	c := newPassThroughCache[int]()
	c.Put("1", 2)

	value, ok := c.Get("1")
	if ok {
		t.Errorf("Expected ok to be false, got true")
	}
	var zero int
	if value != zero {
		t.Errorf("Expected value to be zero value, got %d", value)
	}

	c.Delete("1")
	value, ok = c.Get("1")
	if ok {
		t.Errorf("Expected ok to be false, got true")
	}
	if value != zero {
		t.Errorf("Expected value to be zero value, got %d", value)
	}
}

// Benchmarks

func BenchmarkRandomCache_Get(b *testing.B) {
	benchmarks := []struct {
		name string
		size int
	}{
		{"Size 1", 1},
		{"Size 10", 10},
		{"Size 100", 100},
		{"Size 1000", 1000},
		{"Size 10000", 10000},
		{"Size 100000", 100000},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			c := newRandomCache[int](bm.size)
			keys := make([]string, bm.size)

			for i := 0; i < bm.size; i++ {
				keys[i] = fmt.Sprintf("%d", i)
				c.Put(keys[i], i)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = c.Get(keys[i%bm.size])
			}

			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}

func BenchmarkRandomCache_Put(b *testing.B) {
	benchmarks := []struct {
		name string
		size int
	}{
		{"Size 1", 1},
		{"Size 10", 10},
		{"Size 100", 100},
		{"Size 1000", 1000},
		{"Size 10000", 10000},
		{"Size 100000", 100000},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			c := newRandomCache[int](bm.size)
			keys := make([]string, bm.size)

			for i := 0; i < bm.size; i++ {
				keys[i] = fmt.Sprint(i)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.Put(keys[i%bm.size], 42)
			}

			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}

func BenchmarkRandomCache_Delete(b *testing.B) {
	N := 10000
	c := newRandomCache[int](N)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if len(c.cache) < N/8 {
			// Refill cache when it runs low
			b.StopTimer()
			c = newRandomCache[int](N)
			for j := 0; j < N; j++ {
				c.Put(strconv.Itoa(j), j)
			}
			b.StartTimer()
		}
		c.Delete(strconv.Itoa(i % N))
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}
