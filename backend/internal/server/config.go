package server

type Config struct {
	Port          string
	CKANBaseURL   string
	RedisURL      string
	CacheDir      string
	MemoryCacheGB int64
	DiskCacheGB   int64
}
