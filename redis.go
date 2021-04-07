package datastore

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/kklab-com/goth-kklogger"
	"github.com/kklab-com/goth-kksecret"
)

var KKRedisLocker = sync.Mutex{}
var KKRedisProfiles = sync.Map{}
var KKRedisDialTimeout = 1000
var KKRedisMaxIdle = 20
var KKRedisIdleTimeout = 60000
var KKRedisMaxConnLifetime = 0
var KKRedisMaxActive = 0
var KKRedisWait = false
var KKRedisDebug = false
var _KKRedisDebug = sync.Once{}

type RedisOpType int

const (
	TypeMaster RedisOpType = 0
	TypeSlave  RedisOpType = 1
)

// can be improvement by add more connection
type KKRedis struct {
	name   string
	master *KKRedisOp
	slave  *KKRedisOp
}

func (k *KKRedis) Master() *KKRedisOp {
	return k.master
}

func (k *KKRedis) Slave() *KKRedisOp {
	return k.slave
}

type KKRedisOp struct {
	opType RedisOpType //0 master, 1 slave
	meta   kksecret.RedisMeta
	pool   *redis.Pool
	once   sync.Once
}

func (k *KKRedisOp) Conn() redis.Conn {
	if k.pool == nil {
		k.once.Do(func() {
			k.pool = newRedisPool(k.meta)
		})
	}

	return k.pool.Get()
}

func (k *KKRedisOp) ActiveCount() int {
	if k.pool == nil {
		return 0
	}

	return k.pool.ActiveCount()
}

func (k *KKRedisOp) IdleCount() int {
	if k.pool == nil {
		return 0
	}

	return k.pool.IdleCount()
}

var RedisNotFound = fmt.Errorf("not_found")

func (k *KKRedisOp) _Do(cmd string, args ...interface{}) *KKRedisOpResponse {
	conn := k.Conn()
	r, err := conn.Do(cmd, args...)
	if e := conn.Close(); e != nil {
		kklogger.ErrorJ("kkdatastore.redis#Close", e.Error())
	}

	if err == nil {
		if r == nil {
			return &KKRedisOpResponse{
				Error: RedisNotFound,
			}
		} else {
			return &KKRedisOpResponse{
				KKRedisOpResponseUnit: KKRedisOpResponseUnit{data: r},
				Error:                 nil,
			}
		}
	} else {
		return &KKRedisOpResponse{
			Error: err,
		}
	}
}

func (k *KKRedisOp) Get(key interface{}) *KKRedisOpResponse {
	return k._Do("GET", key)
}

func (k *KKRedisOp) Set(key interface{}, val interface{}) *KKRedisOpResponse {
	return k._Do("SET", key, val)
}

func (k *KKRedisOp) Expire(key interface{}, ttl int64) *KKRedisOpResponse {
	return k._Do("EXPIRE", key, ttl)
}

func (k *KKRedisOp) Delete(key ...interface{}) *KKRedisOpResponse {
	return k._Do("DEL", key...)
}

func (k *KKRedisOp) Keys(key interface{}) *KKRedisOpResponse {
	return k._Do("KEYS", key)
}

func (k *KKRedisOp) Exists(key ...interface{}) *KKRedisOpResponse {
	return k._Do("EXISTS", key...)
}

func (k *KKRedisOp) SetExpire(key interface{}, val interface{}, ttl int64) *KKRedisOpResponse {
	return k._Do("SETEX", key, ttl, val)
}

func (k *KKRedisOp) HMSet(key interface{}, val map[interface{}]interface{}) *KKRedisOpResponse {
	vals := []interface{}{key}
	for mk, mv := range val {
		vals = append(vals, mk, mv)
	}

	return k._Do("HMSET", vals...)
}

func (k *KKRedisOp) HMGet(key interface{}, field ...interface{}) *KKRedisOpResponse {
	vals := []interface{}{key}
	for _, f := range field {
		vals = append(vals, f)
	}

	return k._Do("HMGET", vals...)
}

func (k *KKRedisOp) HSet(key, field, val interface{}) *KKRedisOpResponse {
	return k._Do("HSET", key, field, val)
}

func (k *KKRedisOp) HGet(key, field interface{}) *KKRedisOpResponse {
	return k._Do("HGET", key, field)
}

func (k *KKRedisOp) HExists(key, field interface{}) *KKRedisOpResponse {
	return k._Do("HEXISTS", key, field)
}

func (k *KKRedisOp) HDel(key interface{}, field ...interface{}) *KKRedisOpResponse {
	vals := []interface{}{key}
	for _, f := range field {
		vals = append(vals, f)
	}

	return k._Do("HDEL", vals...)
}

func (k *KKRedisOp) HGetAll(key interface{}) *KKRedisOpResponse {
	return k._Do("HGETALL", key)
}

func (k *KKRedisOp) HLen(key interface{}) *KKRedisOpResponse {
	return k._Do("HLEN", key)
}

func (k *KKRedisOp) HKeys(key interface{}) *KKRedisOpResponse {
	return k._Do("HKEYS", key)
}

func (k *KKRedisOp) HIncrBy(key interface{}, field interface{}, val int64) *KKRedisOpResponse {
	return k._Do("HINCRBY", key, field, val)
}

func (k *KKRedisOp) HVals(key interface{}) *KKRedisOpResponse {
	return k._Do("HVALS", key)
}

func (k *KKRedisOp) Incr(key interface{}) *KKRedisOpResponse {
	return k._Do("INCR", key)
}

func (k *KKRedisOp) IncrBy(key interface{}, val int64) *KKRedisOpResponse {
	return k._Do("INCRBY", key, val)
}

func (k *KKRedisOp) Publish(key interface{}, val interface{}) *KKRedisOpResponse {
	return k._Do("PUBLISH", key, val)
}

func (k *KKRedisOp) Close() error {
	if k.pool != nil {
		return k.pool.Close()
	}

	return nil
}

type KKRedisOpResponseUnit struct {
	data interface{}
}

func (k *KKRedisOpResponseUnit) GetInt64() int64 {
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

func (k *KKRedisOpResponseUnit) GetString() string {
	switch v := k.data.(type) {
	case []byte:
		return string(v)
	case int64:
		return fmt.Sprintf("%d", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (k *KKRedisOpResponseUnit) GetBytes() []byte {
	switch v := k.data.(type) {
	case []byte:
		return v
	}

	return nil
}

func (k *KKRedisOpResponse) GetSlice() []KKRedisOpResponseUnit {
	var entities []KKRedisOpResponseUnit
	switch v := k.data.(type) {
	case []interface{}:
		for _, entity := range v {
			entities = append(entities, KKRedisOpResponseUnit{data: entity})
		}
	}

	return entities
}

type KKRedisOpResponse struct {
	KKRedisOpResponseUnit
	Error error
}

func (k *KKRedisOpResponse) RecordNotFound() bool {
	return k.Error == RedisNotFound
}

func KKREDIS(redisName string) *KKRedis {
	_KKRedisDebug.Do(func() {
		if !KKRedisDebug {
			KKRedisDebug = _IsKKDatastoreDebug()
		}
	})

	if r, f := KKRedisProfiles.Load(redisName); f && !KKRedisDebug {
		return r.(*KKRedis)
	}

	profile := kksecret.RedisProfile(redisName)
	if profile == nil {
		return nil
	}

	KKRedisLocker.Lock()
	defer KKRedisLocker.Unlock()
	if KKRedisDebug {
		KKRedisProfiles.Delete(redisName)
	}

	if r, f := KKRedisProfiles.Load(redisName); !f {
		r := &KKRedis{
			name: redisName,
		}

		mop := KKRedisOp{}
		mop.opType = TypeMaster
		mop.meta = profile.Master
		r.master = &mop

		sop := KKRedisOp{}
		sop.opType = TypeSlave
		sop.meta = profile.Slave
		r.slave = &sop

		KKRedisProfiles.Store(redisName, r)
		return r
	} else {
		return r.(*KKRedis)
	}
}

func newRedisPool(meta kksecret.RedisMeta) *redis.Pool {
	redisPool := &redis.Pool{
		MaxActive:       KKRedisMaxActive,
		MaxIdle:         KKRedisMaxIdle,
		IdleTimeout:     time.Duration(KKRedisIdleTimeout) * time.Millisecond,
		MaxConnLifetime: time.Duration(KKRedisMaxConnLifetime) * time.Millisecond,
		Wait:            KKRedisWait,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL(fmt.Sprintf("redis://%s:%d", meta.Host,
				meta.Port), redis.DialConnectTimeout(time.Duration(KKRedisDialTimeout)*time.Millisecond))
			if err != nil {
				return nil, err
			}
			return c, err
		},
	}

	return redisPool
}
