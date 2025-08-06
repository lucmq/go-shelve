package internal

import (
	"sync/atomic"
)

// TKey is the key type used by the cache.
type TKey = string

// Cache is a generic cache interface.
type Cache[TValue any] interface {
	// Get retrieves a value from the cache based on the provided key. It
	// returns the value and a boolean indicating whether the value was
	// found in the cache.
	Get(key TKey) (TValue, bool)

	// Put adds a new key-value pair to the cache.
	Put(key TKey, value TValue)

	// Delete removes a value from the cache based on the provided key.
	Delete(key TKey)
}

// Default Cache

// DefaultCache is the default implementation of the Cache interface. It is not
// safe for concurrent use, as it meant to be embedded in code that does the
// concurrency control.
type DefaultCache[TValue any] struct {
	cache  Cache[TValue]
	hits   atomic.Int64 // Atomic, since it's mutated by DefaultCache.Get.
	misses atomic.Int64
}

// Assert DefaultCache implements Cache
var _ Cache[any] = (*DefaultCache[any])(nil)

// NewCache creates a new cache based on the provided maximum length.
//
// Setting the maxLength to -1 or less will disable the eviction of elements
// from the cache. A maxLength of 0 will create a pass-through cache that
// does nothing.
func NewCache[TValue any](maxLength int) *DefaultCache[TValue] {
	var c Cache[TValue]
	switch {
	case maxLength <= -1:
		c = newUnboundedCache[TValue]()
	case maxLength == 0:
		c = newPassThroughCache[TValue]()
	default:
		c = newRandomCache[TValue](maxLength)
	}
	return newCacheWithBase[TValue](c)
}

func newCacheWithBase[TValue any](c Cache[TValue]) *DefaultCache[TValue] {
	return &DefaultCache[TValue]{cache: c}
}

// Get retrieves a value from the cache based on the provided key. It
// returns the value and a boolean indicating whether the value was
// found in the cache.
func (c *DefaultCache[TValue]) Get(key TKey) (TValue, bool) {
	v, ok := c.cache.Get(key)
	if !ok {
		c.misses.Add(1)
		return v, false
	}
	c.hits.Add(1)
	return v, true
}

// Put adds a new key-value pair to the cache.
func (c *DefaultCache[TValue]) Put(key TKey, value TValue) {
	c.cache.Put(key, value)
}

// Delete removes a value from the cache based on the provided key.
func (c *DefaultCache[TValue]) Delete(key TKey) { c.cache.Delete(key) }

// Hits returns the number of cache hits (i.e. the number of Get calls that
// found the value in the cache).
func (c *DefaultCache[TValue]) Hits() int { return int(c.hits.Load()) }

// Misses returns the number of cache misses (i.e. the number of Get calls that
// did not find the value in the cache).
func (c *DefaultCache[TValue]) Misses() int { return int(c.misses.Load()) }

// ResetRatio resets the ratio of hits to misses.
func (c *DefaultCache[TValue]) ResetRatio() {
	c.misses.Store(0)
	c.hits.Store(0)
}

// Unbounded Cache

type unboundedCache[TValue any] struct {
	m map[TKey]TValue
}

// Check unboundedCache implements Cache interface
var _ Cache[any] = (*unboundedCache[any])(nil)

func newUnboundedCache[TValue any]() *unboundedCache[TValue] {
	return &unboundedCache[TValue]{
		m: make(map[TKey]TValue),
	}
}

func (c *unboundedCache[TValue]) Get(key TKey) (TValue, bool) {
	v, ok := c.m[key]
	return v, ok
}

func (c *unboundedCache[TValue]) Put(key TKey, value TValue) {
	c.m[key] = value
}

func (c *unboundedCache[TValue]) Delete(key TKey) {
	delete(c.m, key)
}

// Pass-Through Cache

// passThroughCache is a simple pass-through cache.
type passThroughCache[TValue any] struct{}

// Check passThroughCache implements Cache interface
var _ Cache[any] = (*passThroughCache[any])(nil)

// newPassThroughCache creates a new pass-through cache.
func newPassThroughCache[TValue any]() *passThroughCache[TValue] {
	return &passThroughCache[TValue]{}
}

func (passThroughCache[TValue]) Get(TKey) (v TValue, ok bool) { return }

func (passThroughCache[TValue]) Put(TKey, TValue) {}

func (passThroughCache[TValue]) Delete(TKey) {}

// Random Cache

// randomCache provides a cache that evicts elements randomly.
type randomCache[TValue any] struct {
	cache   map[TKey]TValue
	maxSize int
}

// Check randomCache implements Cache interface
var _ Cache[any] = (*randomCache[any])(nil)

// newRandomCache creates a new instance of the randomCache struct, that can
// hold up to maxSize elements.
//
// Setting the maxSize to 0 or less will disable the eviction of elements
// from the cache.
func newRandomCache[TValue any](maxSize int) *randomCache[TValue] {
	return &randomCache[TValue]{
		cache:   make(map[TKey]TValue),
		maxSize: maxSize,
	}
}

func (c *randomCache[TValue]) Get(key TKey) (value TValue, ok bool) {
	value, ok = c.cache[key]
	return value, ok
}

func (c *randomCache[TValue]) Put(key TKey, value TValue) {
	if c.maxSize <= 0 || len(c.cache) < c.maxSize {
		c.cache[key] = value
		return
	}
	// Remove any key and save the new one
	for k := range c.cache {
		delete(c.cache, k)
		break
	}
	c.cache[key] = value
}

func (c *randomCache[TValue]) Delete(key TKey) {
	delete(c.cache, key)
}
