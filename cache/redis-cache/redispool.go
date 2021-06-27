package redis_cache

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc"
)

type RedisPool interface {
	Get() redis.Conn
	Dial() (redis.Conn, error)
}

func NewLegacyRedisPool(content string) RedisPool {
	redisPool := &legecyRedisPool{
		content: content,
		conns:   make([]*redisConn, 20),
	}

	for i := range redisPool.conns {
		redisPool.conns[i] = &redisConn{
			pool: redisPool,
		}
	}

	return redisPool
}

type clusterRedisPool redisc.Cluster

func NewRedisPool(content string) RedisPool {
	redisPool := &redisc.Cluster{
		StartupNodes: []string{content},
		DialOptions:  []redis.DialOption{redis.DialConnectTimeout(5 * time.Second)},
		CreatePool:   createPool,
	}

	if err := redisPool.Refresh(); err != nil {
		defer redisPool.Close()
		fmt.Printf("Can't initialize cluster because %#v, fallback to normal\n", err)
		return NewLegacyRedisPool(content)
	}

	return (*clusterRedisPool)(redisPool)
}

type clusterRedisConn struct {
	p *clusterRedisPool
	c redis.Conn
	s int
	m sync.Mutex
}

var _ redis.Conn = (*clusterRedisConn)(nil)

func (c *clusterRedisConn) Close() error {
	return c.c.Close()
}

func (c *clusterRedisConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.do(commandName, args...)
}

func (c *clusterRedisConn) do(commandName string, args ...interface{}) (reply interface{}, err error) {
	switch strings.ToLower(commandName) {
	case "exists":
		if len(args) > 1 { // check multiple exist
			var err error
			res := 0
			for _, key := range args {
				result, retErr := redis.Int(c.do("EXIST", key))
				if retErr != nil {
					err = retErr
				} else {
					res += result
				}
			}
			return []byte(strconv.Itoa(res)), err
		}
		fallthrough
	case "get", "set", "setex", "incr", "getset", "expire", "setnx", "decr", "incrby", "decrby", "sismember", "smembers", "srem", "sadd", "lrem", "lpush", "lrange", "del", "getrange", "psetex", "ttl":
		key := args[0].(string)
		if c.s == -1 {
			c.s = redisc.Slot(key)
			redisc.BindConn(c.c, key)
		}

		val, err := c.c.Do(commandName, args...)
		if err != nil && err.Error()[0:5] == "MOVED" {
			c.s = redisc.Slot(key)
			c.c.Close()
			c.c = c.p.Get()
			redisc.BindConn(c.c, key)
			val, err = c.c.Do(commandName, args...)
		}
		return val, err
	case "mget":
		var err error
		results := make([]interface{}, 0, len(args))
		for _, key := range args {
			result, retErr := c.do("GET", key)
			if retErr != nil {
				err = retErr
			}
			results = append(results, result)
		}
		return results, err
	case "mset":
	}
	return c.c.Do(commandName, args...)
}

func (c *clusterRedisConn) Err() error {
	return c.c.Err()
}

func (c *clusterRedisConn) Flush() error {
	return c.c.Flush()
}

func (c *clusterRedisConn) Receive() (reply interface{}, err error) {
	return c.c.Receive()
}

func (c *clusterRedisConn) Send(commandName string, args ...interface{}) error {
	return c.c.Send(commandName, args...)
}

func (r *clusterRedisPool) Get() redis.Conn {
	conn := (*redisc.Cluster)(r).Get()
	// retryConn, _ := redisc.RetryConn(conn, 3, 100*time.Millisecond)
	return &clusterRedisConn{
		p: r,
		c: conn,
		s: -1,
	}
}

func (r *clusterRedisPool) Dial() (redis.Conn, error) {
	conn, err := (*redisc.Cluster)(r).Dial()
	if err != nil {
		return nil, err
	}
	// retryConn, err := redisc.RetryConn(conn, 3, 100*time.Millisecond)
	return &clusterRedisConn{
		p: r,
		c: conn,
		s: -1,
	}, nil
}

func createPool(addr string, opts ...redis.DialOption) (*redis.Pool, error) {
	return &redis.Pool{
		MaxIdle:     5,
		MaxActive:   10,
		IdleTimeout: time.Minute,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", addr, opts...)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}, nil
}

type legecyRedisPool struct {
	content   string
	connMutex sync.Mutex
	mutex     sync.Mutex
	next      int
	conns     []*redisConn
}

type redisConn struct {
	pool  *legecyRedisPool
	mutex sync.Mutex
	conn  redis.Conn

	sendMutex   sync.Mutex
	connForSend redis.Conn
}

var _ redis.Conn = (*redisConn)(nil)

func (r *redisConn) Close() error {
	return nil
}

func (r *redisConn) Err() error {
	return nil
}

func (r *redisConn) retry(conn *redis.Conn, retryFunc func() error) (err error) {
	if *conn == nil {
		*conn, err = redis.Dial("tcp", r.pool.content)
		if err != nil {
			return err
		}
	}

	for retry := 3; retry > 0; retry-- {
		err = retryFunc()
		if err == nil {
			return err
		}

		if err == io.EOF ||
			strings.Contains(err.Error(), "use of closed network connection") ||
			strings.Contains(err.Error(), "connect: connection refused") {
			fmt.Printf("REDIS Error(Recovery retry %d): %#v\n", retry, err)
			(*conn).Close()
			*conn, err = redis.Dial("tcp", r.pool.content)
		} else {
			fmt.Printf("REDIS Error(Unrecoverable): %#v\n", err)
			return err
		}
	}
	return err
}

func (r *redisConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	var conn *redis.Conn

	if strings.ToLower(commandName) == "exec" {
		r.sendMutex.Lock()
		defer r.sendMutex.Unlock()
		conn = &r.connForSend
	} else {
		r.mutex.Lock()
		defer r.mutex.Unlock()
		conn = &r.conn
	}

	r.retry(conn, func() (err error) {
		reply, err = (*conn).Do(commandName, args...)
		return err
	})
	return reply, err
}

func (r *redisConn) Send(commandName string, args ...interface{}) (err error) {
	r.sendMutex.Lock()
	defer r.sendMutex.Unlock()

	if r.connForSend == nil {
		r.connForSend, err = redis.Dial("tcp", r.pool.content)
	}

	return r.retry(&r.connForSend, func() error {
		return r.connForSend.Send(commandName, args...)
	})
}

func (r *redisConn) Flush() error {
	return r.conn.Flush()
}

func (r *redisConn) Receive() (reply interface{}, err error) {
	return r.conn.Receive()
}

func (r *legecyRedisPool) Get() redis.Conn {
	r.connMutex.Lock()
	defer r.connMutex.Unlock()

	if r.next >= len(r.conns) {
		r.next = 0
	}

	conn := r.conns[r.next]
	r.next++

	return conn
}

func (r *legecyRedisPool) Dial() (redis.Conn, error) {
	return r.Get(), nil
}
