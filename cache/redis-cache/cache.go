package redis_cache

import (
	"github.com/gomodule/redigo/redis"
	"github.com/victorlin12345/cache-playground/utils"
	"log"
	"sync"
	"time"
)

// Cache has redis and local
type Cache struct {
	R                 RedisPool
	L                 *LocalCache
	LocalCacheTimeout int64
}

type LocalCache struct {
	mutex  sync.Mutex
	data   map[string]interface{}
	expire map[string]interface{}
}

// NewLocalCache every 60 sec run clear expired 40 items
func NewLocalCache() *LocalCache {

	queue := &LocalCache{
		data:   map[string]interface{}{},
		expire: map[string]interface{}{},
	}

	go func() {
		for {
			timer := time.NewTimer(5 * time.Second)
			for {
				select {
				case <-timer.C:
					queue.mutex.Lock()
					now := time.Now().Unix()
					checkItems := 40
					for i, e := range queue.expire {
						if e.(int64) < now {
							delete(queue.expire, i)
							delete(queue.data, i)
						} else {
							checkItems--
							if checkItems <= 0 {
								break
							}
						}
					}
					queue.mutex.Unlock()
					timer.Reset(5 * time.Second)
				}
			}
		}
	}()
	return queue
}

// Read read local cache by key
func (c *LocalCache) Read(key string) interface{} {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if expire := c.expire[key].(int64); expire < time.Now().Unix() {
		delete(c.expire, key)
		delete(c.data, key)
		return nil
	}
	return c.data[key]
}

// Remove remove local cache by key
func (c *LocalCache) Remove(key string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.expire, key)
	delete(c.data, key)
	return nil
}

// Set set local cache with key obj timeout
func (c *LocalCache) Set(key string, obj interface{}, timeout int64) interface{} {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.data[key] = obj

	timeout += time.Now().Unix()

	c.expire[key] = timeout

	return obj
}

// Read read from local cache first then redis
func (c *Cache) Read(key string) []byte {
	if c.L != nil {
		if ret, ok := c.L.data[key].([]byte); ok {
			log.Println("read from local")
			return ret
		}
	}

	if result, err := redis.Bytes(c.do("GET", key)); err != nil {
		return nil
	} else {
		log.Println("read from redis")
		return result
	}
}

// Remove remove local cache first then redis
func (c *Cache) Remove(key string) error {
	if c.L != nil {
		c.L.Remove(key)
	}

	_, err := c.do("DEL", key)
	return err
}

// CacheWithTimeout set key obj with timeout
func (c *Cache) CacheWithTimeout(key string, obj []byte, timeout int64) error {
	if c.L != nil {
		c.L.Set(key, obj, c.LocalCacheTimeout)
	}
    // set key timeout(seconds) obj
	if _, err := c.do("SETEX", key, timeout, obj); err != nil {
		return err
	}
	return nil
}

// Cache set key obj without timeout
func (c *Cache) Cache(key string, obj []byte) error {
	if c.L != nil {
		c.L.Set(key, obj, c.LocalCacheTimeout)
	}

	if _, err := c.do("SET", key, obj); err != nil {
		return err
	}
	return nil
}

// RemoveLock : remove redis lock
func (c *Cache) RemoveLock(key string) error {
	updateKey := "UPDATEING::" + key
	err := c.Remove(updateKey)
	return err
}

// Lock : redis lock
func (c *Cache) Lock(key string, expireTime int, waitLockTimeout int) bool {
	lockKey := "UPDATEING::" + key
	waitLockTimes := waitLockTimeout * (1000 / 200) // 200ms
	myToken := utils.RandSeq(20)
	// after waitLock times over release the lock
	for waitLockTimes > 0 {
		// Key : lockKey, Value : myToken,
		// NX – Only set the key if it does not already exist.
		// EX - expired time
		// try to set same key every 200 milli seconds. It will only success when key expired
		if result, err := c.do("SET", lockKey, myToken, "NX", "EX", expireTime); err == nil && result != nil {
			return true
		}
		time.Sleep(200 * time.Millisecond)
		waitLockTimes --
	}
	return false
}

// do command for redis
func (c *Cache) do(commandName string, args ...interface{}) (interface{}, error) {
	rd := c.R.Get()
	defer rd.Close()

	return rd.Do(commandName, args...)
}
