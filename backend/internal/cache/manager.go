package cache

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Manager struct {
	redis       *redis.Client
	memoryCache *LRUCache
	diskCache   *DiskCache
	ctx         context.Context
}

func NewManager(redisURL string, memorySize, diskSize int64, cacheDir string) (*Manager, error) {
	// Redis
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("error parseando Redis URL: %w", err)
	}
	redisClient := redis.NewClient(opt)
	ctx := context.Background()

	// Test de conexión
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("error conectando a Redis %w", err)
	}

	// Memory cache
	memCache := NewLRUCache(memorySize)

	// Disk cache
	diskCache := NewDiskCache(cacheDir, diskSize)

	return &Manager{
		redis:       redisClient,
		memoryCache: memCache,
		diskCache:   diskCache,
		ctx:         ctx,
	}, nil
}

// Redis operaciones
func (m *Manager) GetFromRedis(key string) ([]byte, bool) {
	val, err := m.redis.Get(m.ctx, key).Bytes()
	if err != nil {
		return nil, false
	}
	return val, true
}

func (m *Manager) SetToRedis(key string, value interface{}, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return m.redis.Set(m.ctx, key, data, ttl).Err()
}

// Memory operaciones
func (m *Manager) GetFromMemory(uuid string) (string, bool) {
	return m.memoryCache.Get(uuid)
}

func (m *Manager) SetToMemory(uuid, dbPath string) {
	fi, err := os.Stat(dbPath)
	var size int64
	if err == nil {
		size = fi.Size()
	}
	m.memoryCache.Set(uuid, dbPath, size)
}

// Disk operaciones
func (m *Manager) GetFromDisk(uuid string) (string, bool) {
	return m.diskCache.Get(uuid)
}

func (m *Manager) SetToDisk(uuid, dbPath string) error {
	return m.diskCache.Set(uuid, dbPath)
}

// Helpers
func (m *Manager) GenerateKey(prefix string, data interface{}) string {
	jsonData, _ := json.Marshal(data)
	hash := md5.Sum(jsonData)
	return fmt.Sprintf("%s:%x", prefix, hash)
}

func (m *Manager) Close() error {
	return m.redis.Close()
}

type DiskCache struct {
	dir     string
	maxSize int64
	mu      sync.RWMutex
}

func NewDiskCache(dir string, maxSize int64) *DiskCache {
	os.MkdirAll(dir, 0755)
	return &DiskCache{
		dir:     dir,
		maxSize: maxSize,
	}
}

func (dc *DiskCache) Get(uuid string) (string, bool) {
	path := filepath.Join(dc.dir, uuid+".duckdb")
	if _, err := os.Stat(path); err == nil {
		return path, true
	}
	return "", false
}

func (dc *DiskCache) Set(uuid, srcPath string) error {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	dstPath := filepath.Join(dc.dir, uuid+".duckdb")

	// Si ya existe, no hacer nada
	if _, err := os.Stat(dstPath); err == nil {
		return nil
	}
	//  Mover o copiar
	return os.Rename(srcPath, dstPath)
}

func (m *Manager) GetCacheDir() string {
	return m.diskCache.dir
}
