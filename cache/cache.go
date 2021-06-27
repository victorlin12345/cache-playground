package cache

import (
	redisCache "github.com/victorlin12345/cache-playground/cache/redis-cache"
	"sync"
)

var Cache *redisCache.Cache
var CacheLock sync.Mutex

// NewCache : Singleton of Redis cache object
func NewCache() *redisCache.Cache {
	if Cache == nil {
		CacheLock.Lock()
		defer CacheLock.Unlock()

		if Cache == nil {
			redisRedis := redisCache.NewRedisPool("127.0.0.1:6379")
			Cache = &redisCache.Cache{
				R:                 redisRedis,
				L:                 redisCache.NewLocalCache(),
				LocalCacheTimeout: 10,
			}
		}
	}

	return Cache
}
