package cache

import (
	"errors"
	"sync"
	"time"
)

type Cache struct {
	mu                sync.RWMutex
	defaultExpiration time.Duration
	cleanupInterval   time.Duration
	items             map[int]CacheItem
}

type CacheItem struct {
	Value      interface{}
	Created    time.Time
	Expiration int64
}

func NewCache(defaultExpiration, cleanupInterval time.Duration) *Cache {

	items := make(map[int]CacheItem)

	cache := Cache{
		items:             items,
		defaultExpiration: defaultExpiration,
		cleanupInterval:   cleanupInterval,
	}

	if cleanupInterval > 0 {
		cache.StartGC()
	}

	return &cache
}

func (c *Cache) Set(key int, value interface{}, duration time.Duration) {

	var expiration int64

	if duration == 0 {
		duration = c.defaultExpiration
	}

	if duration > 0 {
		expiration = time.Now().Add(duration).UnixNano()
	}

	c.mu.Lock()

	defer c.mu.Unlock()

	c.items[key] = CacheItem{
		Value:      value,
		Expiration: expiration,
		Created:    time.Now(),
	}

}

func (c *Cache) Get(key int) (interface{}, bool) {

	c.mu.RLock()

	defer c.mu.RUnlock()

	item, found := c.items[key]

	if !found {
		return nil, false
	}

	if item.Expiration > 0 {

		if time.Now().UnixNano() > item.Expiration {
			return nil, false
		}

	}

	return item.Value, true
}

func (c *Cache) Delete(key int) error {

	c.mu.Lock()

	defer c.mu.Unlock()

	if _, found := c.items[key]; !found {
		return errors.New("key not found")
	}

	delete(c.items, key)

	return nil
}

func (c *Cache) StartGC() {
	go c.GC()
}

func (c *Cache) GC() {

	for {
		<-time.After(c.cleanupInterval)

		if c.items == nil {
			return
		}

		if keys := c.expiredKeys(); len(keys) != 0 {
			c.clearItems(keys)

		}

	}

}

func (c *Cache) expiredKeys() (keys []int) {

	c.mu.RLock()

	defer c.mu.RUnlock()

	for k, i := range c.items {
		if time.Now().UnixNano() > i.Expiration && i.Expiration > 0 {
			keys = append(keys, k)
		}
	}

	return
}

func (c *Cache) clearItems(keys []int) {

	c.mu.Lock()

	defer c.mu.Unlock()

	for _, k := range keys {
		delete(c.items, k)
	}
}
