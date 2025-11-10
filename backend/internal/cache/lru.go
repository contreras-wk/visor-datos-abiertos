package cache

import (
	"container/list"
	"sync"
)

type LRUCache struct {
	capacity  int
	size      int64
	maxSize   int64
	items     map[string]*list.Element
	evictList *list.List
	mu        sync.RWMutex
}

type entry struct {
	key   string
	value string
	size  int64
}

func NewLRUCache(maxSize int64) *LRUCache {
	return &LRUCache{
		capacity:  10, // Top 10 datasets
		maxSize:   maxSize,
		items:     make(map[string]*list.Element),
		evictList: list.New(),
	}
}

func (c *LRUCache) Get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.evictList.MoveToFront(elem)
		return elem.Value.(*entry).value, true
	}
	return "", false
}

func (c *LRUCache) Set(key, value string, size int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Si existe, actualizar
	if elem, ok := c.items[key]; ok {
		c.evictList.MoveToFront(elem)
		oldEntry := elem.Value.(*entry)
		c.size = c.size - oldEntry.size + size
		oldEntry.value = value
		oldEntry.size = size
		return
	}

	// Nuevo entry
	entry := &entry{key: key, value: value, size: size}
	elem := c.evictList.PushFront(entry)
	c.items[key] = elem
	c.size += size

	for c.evictList.Len() > c.capacity || c.size > c.maxSize {
		c.evictOldest()
	}
}

func (c *LRUCache) evictOldest() {
	elem := c.evictList.Back()
	if elem != nil {
		c.evictList.Remove(elem)
		entry := elem.Value.(*entry)
		delete(c.items, entry.key)
		c.size -= entry.size
	}
}

func (c *LRUCache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.evictList.Remove(elem)
		entry := elem.Value.(*entry)
		c.size -= entry.size
		delete(c.items, key)
	}
}

func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.evictList.Init()
	c.size = 0
}

func (c *LRUCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.evictList.Len()
}

func (c *LRUCache) Size() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.size
}
