package datastore

import (
	"context"
	"errors"
	"fmt"
	secret "github.com/yetiz-org/goth-secret"
	"strconv"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/yetiz-org/goth-kklogger"
)

var DefaultRedisDialTimeout = 1000
var DefaultRedisMaxIdle = 20
var DefaultRedisIdleTimeout = 60000
var DefaultRedisMaxConnLifetime = 0
var DefaultRedisMaxActive = 0
var DefaultRedisWait = false

type Redis struct {
	name   string
	master *RedisOp
	slave  *RedisOp
}

func (r *Redis) Master() *RedisOp {
	return r.master
}

func (r *Redis) Slave() *RedisOp {
	return r.slave
}

type RedisOp struct {
	meta   secret.RedisMeta
	pool   *redis.Pool
	opLock sync.Mutex
}

func (o *RedisOp) Pool() *redis.Pool {
	return o.pool
}

func (o *RedisOp) Conn() redis.Conn {
	return o.pool.Get()
}

func (o *RedisOp) ActiveCount() int {
	if o.pool == nil {
		return 0
	}

	return o.pool.ActiveCount()
}

func (o *RedisOp) IdleCount() int {
	if o.pool == nil {
		return 0
	}

	return o.pool.IdleCount()
}

var RedisNotFound = fmt.Errorf("not_found")

func (o *RedisOp) Exec(f func(conn redis.Conn)) error {
	if conn, err := o.pool.GetContext(context.Background()); err != nil {
		return err
	} else {
		f(conn)
		return conn.Close()
	}
}

func (o *RedisOp) _Do(cmd string, args ...interface{}) *RedisResponse {
	conn := o.Conn()
	defer conn.Close()
	if r, err := conn.Do(cmd, args...); err == nil {
		if r == nil {
			return &RedisResponse{
				Error: RedisNotFound,
			}
		} else {
			return &RedisResponse{
				RedisResponseEntity: RedisResponseEntity{data: r},
				Error:               nil,
			}
		}
	} else {
		return &RedisResponse{
			Error: err,
		}
	}
}

func (o *RedisOp) Get(key interface{}) *RedisResponse {
	return o._Do("GET", key)
}

func (o *RedisOp) Set(key interface{}, val interface{}) *RedisResponse {
	return o._Do("SET", key, val)
}

func (o *RedisOp) Expire(key interface{}, ttl int64) *RedisResponse {
	return o._Do("EXPIRE", key, ttl)
}

func (o *RedisOp) Delete(key ...interface{}) *RedisResponse {
	return o._Do("DEL", key...)
}

func (o *RedisOp) Keys(key interface{}) *RedisResponse {
	return o._Do("KEYS", key)
}

func (o *RedisOp) Exists(key ...interface{}) *RedisResponse {
	return o._Do("EXISTS", key...)
}

func (o *RedisOp) SetExpire(key interface{}, val interface{}, ttl int64) *RedisResponse {
	return o._Do("SETEX", key, ttl, val)
}

func (o *RedisOp) HMSet(key interface{}, val map[interface{}]interface{}) *RedisResponse {
	vals := []interface{}{key}
	for mk, mv := range val {
		vals = append(vals, mk, mv)
	}

	return o._Do("HMSET", vals...)
}

func (o *RedisOp) HMGet(key interface{}, field ...interface{}) *RedisResponse {
	vals := []interface{}{key}
	for _, f := range field {
		vals = append(vals, f)
	}

	return o._Do("HMGET", vals...)
}

func (o *RedisOp) HSet(key, field, val interface{}) *RedisResponse {
	return o._Do("HSET", key, field, val)
}

func (o *RedisOp) HGet(key, field interface{}) *RedisResponse {
	return o._Do("HGET", key, field)
}

func (o *RedisOp) HExists(key, field interface{}) *RedisResponse {
	return o._Do("HEXISTS", key, field)
}

func (o *RedisOp) HDel(key interface{}, field ...interface{}) *RedisResponse {
	vals := []interface{}{key}
	for _, f := range field {
		vals = append(vals, f)
	}

	return o._Do("HDEL", vals...)
}

func (o *RedisOp) HGetAll(key interface{}) *RedisResponse {
	return o._Do("HGETALL", key)
}

func (o *RedisOp) HLen(key interface{}) *RedisResponse {
	return o._Do("HLEN", key)
}

func (o *RedisOp) HKeys(key interface{}) *RedisResponse {
	return o._Do("HKEYS", key)
}

func (o *RedisOp) HIncrBy(key interface{}, field interface{}, val int64) *RedisResponse {
	return o._Do("HINCRBY", key, field, val)
}

func (o *RedisOp) HVals(key interface{}) *RedisResponse {
	return o._Do("HVALS", key)
}

func (o *RedisOp) Incr(key interface{}) *RedisResponse {
	return o._Do("INCR", key)
}

func (o *RedisOp) IncrBy(key interface{}, val int64) *RedisResponse {
	return o._Do("INCRBY", key, val)
}

func (o *RedisOp) Publish(key interface{}, val interface{}) *RedisResponse {
	return o._Do("PUBLISH", key, val)
}

func (o *RedisOp) Ping() *RedisResponse {
	return o._Do("PING")
}

func (o *RedisOp) Close() error {
	if o.pool != nil {
		return o.pool.Close()
	}

	return nil
}

type RedisResponseEntity struct {
	data interface{}
}

func (k *RedisResponseEntity) GetInt64() int64 {
	switch v := k.data.(type) {
	case int64:
		return v
	case []byte:
		if p, err := strconv.ParseInt(string(v), 10, 64); err == nil {
			return p
		}

		return 0
	}

	return 0
}

func (k *RedisResponseEntity) GetString() string {
	switch v := k.data.(type) {
	case []byte:
		return string(v)
	case int64:
		return fmt.Sprintf("%d", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (k *RedisResponseEntity) GetBytes() []byte {
	switch v := k.data.(type) {
	case []byte:
		return v
	}

	return nil
}

func (k *RedisResponse) GetSlice() []RedisResponseEntity {
	var entities []RedisResponseEntity
	switch v := k.data.(type) {
	case []interface{}:
		for _, entity := range v {
			entities = append(entities, RedisResponseEntity{data: entity})
		}
	}

	return entities
}

type RedisResponse struct {
	RedisResponseEntity
	Error error
}

func (k *RedisResponse) RecordNotFound() bool {
	return errors.Is(k.Error, RedisNotFound)
}

func NewRedis(profileName string) *Redis {
	profile := &secret.Redis{}
	if err := secret.Load("redis", profileName, profile); err != nil {
		kklogger.ErrorJ("datastore.redis#Load", err.Error())
		return nil
	}

	r := &Redis{
		name: profileName,
	}

	mop := RedisOp{
		meta: profile.Master,
		pool: newRedisPool(profile.Master),
	}

	r.master = &mop

	sop := RedisOp{
		meta: profile.Slave,
		pool: newRedisPool(profile.Slave),
	}

	r.slave = &sop

	return r
}

func newRedisPool(meta secret.RedisMeta) *redis.Pool {
	redisPool := &redis.Pool{
		MaxActive:       DefaultRedisMaxActive,
		MaxIdle:         DefaultRedisMaxIdle,
		IdleTimeout:     time.Duration(DefaultRedisIdleTimeout) * time.Millisecond,
		MaxConnLifetime: time.Duration(DefaultRedisMaxConnLifetime) * time.Millisecond,
		Wait:            DefaultRedisWait,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL(fmt.Sprintf("redis://%s:%d", meta.Host,
				meta.Port), redis.DialConnectTimeout(time.Duration(DefaultRedisDialTimeout)*time.Millisecond))
			if err != nil {
				return nil, err
			}
			return c, err
		},
	}

	return redisPool
}
