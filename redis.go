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

func (o *RedisOp) Meta() secret.RedisMeta {
	return o.meta
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

func (o *RedisOp) HScan(key interface{}, cursor int64, match string, count int64) *RedisResponse {
	args := []interface{}{key, cursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", count)
	}
	return o._Do("HSCAN", args...)
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

// String 系列補充指令
func (o *RedisOp) Append(key interface{}, val interface{}) *RedisResponse {
	return o._Do("APPEND", key, val)
}

func (o *RedisOp) StrLen(key interface{}) *RedisResponse {
	return o._Do("STRLEN", key)
}

func (o *RedisOp) GetRange(key interface{}, start, end int64) *RedisResponse {
	return o._Do("GETRANGE", key, start, end)
}

func (o *RedisOp) SetRange(key interface{}, offset int64, val interface{}) *RedisResponse {
	return o._Do("SETRANGE", key, offset, val)
}

// Key 系列指令補足
func (o *RedisOp) Copy(src, dst interface{}) *RedisResponse {
	return o._Do("COPY", src, dst)
}

func (o *RedisOp) Decr(key interface{}) *RedisResponse {
	return o._Do("DECR", key)
}

func (o *RedisOp) DecrBy(key interface{}, val int64) *RedisResponse {
	return o._Do("DECRBY", key, val)
}

func (o *RedisOp) Dump(key interface{}) *RedisResponse {
	return o._Do("DUMP", key)
}

func (o *RedisOp) TTL(key interface{}) *RedisResponse {
	return o._Do("TTL", key)
}

func (o *RedisOp) PTTL(key interface{}) *RedisResponse {
	return o._Do("PTTL", key)
}

func (o *RedisOp) Type(key interface{}) *RedisResponse {
	return o._Do("TYPE", key)
}

func (o *RedisOp) RandomKey() *RedisResponse {
	return o._Do("RANDOMKEY")
}

func (o *RedisOp) Rename(oldKey, newKey interface{}) *RedisResponse {
	return o._Do("RENAME", oldKey, newKey)
}

func (o *RedisOp) RenameNX(oldKey, newKey interface{}) *RedisResponse {
	return o._Do("RENAMENX", oldKey, newKey)
}

func (o *RedisOp) Touch(key ...interface{}) *RedisResponse {
	return o._Do("TOUCH", key...)
}

func (o *RedisOp) Unlink(key ...interface{}) *RedisResponse {
	return o._Do("UNLINK", key...)
}

func (o *RedisOp) Persist(key interface{}) *RedisResponse {
	return o._Do("PERSIST", key)
}

// List 系列指令
func (o *RedisOp) LIndex(key interface{}, index int64) *RedisResponse {
	return o._Do("LINDEX", key, index)
}

func (o *RedisOp) LInsert(key interface{}, where string, pivot, element interface{}) *RedisResponse {
	return o._Do("LINSERT", key, where, pivot, element)
}

func (o *RedisOp) LLen(key interface{}) *RedisResponse {
	return o._Do("LLEN", key)
}

func (o *RedisOp) LMove(source, destination interface{}, srcWhere, dstWhere string) *RedisResponse {
	return o._Do("LMOVE", source, destination, srcWhere, dstWhere)
}

func (o *RedisOp) LMPop(count int64, where string, key ...interface{}) *RedisResponse {
	args := []interface{}{count, where}
	args = append(args, key...)
	return o._Do("LMPOP", args...)
}

func (o *RedisOp) LPop(key interface{}) *RedisResponse {
	return o._Do("LPOP", key)
}

func (o *RedisOp) LPos(key, element interface{}) *RedisResponse {
	return o._Do("LPOS", key, element)
}

func (o *RedisOp) LPush(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return o._Do("LPUSH", args...)
}

func (o *RedisOp) LPushX(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return o._Do("LPUSHX", args...)
}

func (o *RedisOp) LRange(key interface{}, start, stop int64) *RedisResponse {
	return o._Do("LRANGE", key, start, stop)
}

func (o *RedisOp) LRem(key interface{}, count int64, element interface{}) *RedisResponse {
	return o._Do("LREM", key, count, element)
}

func (o *RedisOp) LSet(key interface{}, index int64, element interface{}) *RedisResponse {
	return o._Do("LSET", key, index, element)
}

func (o *RedisOp) LTrim(key interface{}, start, stop int64) *RedisResponse {
	return o._Do("LTRIM", key, start, stop)
}

func (o *RedisOp) RPop(key interface{}) *RedisResponse {
	return o._Do("RPOP", key)
}

func (o *RedisOp) RPopLPush(source, destination interface{}) *RedisResponse {
	return o._Do("RPOPLPUSH", source, destination)
}

func (o *RedisOp) RPush(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return o._Do("RPUSH", args...)
}

func (o *RedisOp) RPushX(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return o._Do("RPUSHX", args...)
}

// Set 系列指令
func (o *RedisOp) SAdd(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return o._Do("SADD", args...)
}

func (o *RedisOp) SCard(key interface{}) *RedisResponse {
	return o._Do("SCARD", key)
}

func (o *RedisOp) SDiff(key ...interface{}) *RedisResponse {
	return o._Do("SDIFF", key...)
}

func (o *RedisOp) SDiffStore(destination interface{}, key ...interface{}) *RedisResponse {
	args := []interface{}{destination}
	args = append(args, key...)
	return o._Do("SDIFFSTORE", args...)
}

func (o *RedisOp) SInter(key ...interface{}) *RedisResponse {
	return o._Do("SINTER", key...)
}

func (o *RedisOp) SInterCard(key ...interface{}) *RedisResponse {
	return o._Do("SINTERCARD", key...)
}

func (o *RedisOp) SInterStore(destination interface{}, key ...interface{}) *RedisResponse {
	args := []interface{}{destination}
	args = append(args, key...)
	return o._Do("SINTERSTORE", args...)
}

func (o *RedisOp) SIsMember(key, member interface{}) *RedisResponse {
	return o._Do("SISMEMBER", key, member)
}

func (o *RedisOp) SMembers(key interface{}) *RedisResponse {
	return o._Do("SMEMBERS", key)
}

func (o *RedisOp) SMIsMember(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return o._Do("SMISMEMBER", args...)
}

func (o *RedisOp) SMove(source, destination, member interface{}) *RedisResponse {
	return o._Do("SMOVE", source, destination, member)
}

func (o *RedisOp) SPop(key interface{}) *RedisResponse {
	return o._Do("SPOP", key)
}

func (o *RedisOp) SRandMember(key interface{}) *RedisResponse {
	return o._Do("SRANDMEMBER", key)
}

func (o *RedisOp) SRem(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return o._Do("SREM", args...)
}

func (o *RedisOp) SScan(key interface{}, cursor int64, match string, count int64) *RedisResponse {
	args := []interface{}{key, cursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", count)
	}
	return o._Do("SSCAN", args...)
}

func (o *RedisOp) SUnion(key ...interface{}) *RedisResponse {
	return o._Do("SUNION", key...)
}

func (o *RedisOp) SUnionStore(destination interface{}, key ...interface{}) *RedisResponse {
	args := []interface{}{destination}
	args = append(args, key...)
	return o._Do("SUNIONSTORE", args...)
}

func (o *RedisOp) FlushDB() *RedisResponse {
	return o._Do("FLUSHDB")
}

func (o *RedisOp) FlushAll() *RedisResponse {
	return o._Do("FLUSHALL")
}

func (o *RedisOp) Scan(cursor int64, match string, count int64) *RedisResponse {
	args := []interface{}{cursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", count)
	}
	return o._Do("SCAN", args...)
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

// Sorted Set 系列指令
func (o *RedisOp) ZAdd(key interface{}, score float64, member interface{}, pairs ...interface{}) *RedisResponse {
	args := []interface{}{key, score, member}
	args = append(args, pairs...)
	return o._Do("ZADD", args...)
}

func (o *RedisOp) ZCard(key interface{}) *RedisResponse {
	return o._Do("ZCARD", key)
}

func (o *RedisOp) ZCount(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZCOUNT", key, min, max)
}

func (o *RedisOp) ZDiff(key ...interface{}) *RedisResponse {
	return o._Do("ZDIFF", key...)
}

func (o *RedisOp) ZDiffStore(destination interface{}, key ...interface{}) *RedisResponse {
	args := []interface{}{destination}
	args = append(args, key...)
	return o._Do("ZDIFFSTORE", args...)
}

func (o *RedisOp) ZIncrBy(key interface{}, increment float64, member interface{}) *RedisResponse {
	return o._Do("ZINCRBY", key, increment, member)
}

func (o *RedisOp) ZInter(key ...interface{}) *RedisResponse {
	return o._Do("ZINTER", key...)
}

func (o *RedisOp) ZInterCard(key ...interface{}) *RedisResponse {
	return o._Do("ZINTERCARD", key...)
}

func (o *RedisOp) ZInterStore(destination interface{}, key ...interface{}) *RedisResponse {
	args := []interface{}{destination}
	args = append(args, key...)
	return o._Do("ZINTERSTORE", args...)
}

func (o *RedisOp) ZLexCount(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZLEXCOUNT", key, min, max)
}

func (o *RedisOp) ZMPop(count int64, where string, key ...interface{}) *RedisResponse {
	args := []interface{}{count, where}
	args = append(args, key...)
	return o._Do("ZMPOP", args...)
}

func (o *RedisOp) ZMScore(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return o._Do("ZMSCORE", args...)
}

func (o *RedisOp) ZPopMax(key interface{}) *RedisResponse {
	return o._Do("ZPOPMAX", key)
}

func (o *RedisOp) ZPopMin(key interface{}) *RedisResponse {
	return o._Do("ZPOPMIN", key)
}

func (o *RedisOp) ZRandMember(key interface{}) *RedisResponse {
	return o._Do("ZRANDMEMBER", key)
}

func (o *RedisOp) ZRange(key interface{}, start, stop int64) *RedisResponse {
	return o._Do("ZRANGE", key, start, stop)
}

func (o *RedisOp) ZRangeByLex(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZRANGEBYLEX", key, min, max)
}

func (o *RedisOp) ZRangeByScore(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZRANGEBYSCORE", key, min, max)
}

func (o *RedisOp) ZRangeStore(dst interface{}, src interface{}, min, max int64) *RedisResponse {
	return o._Do("ZRANGESTORE", dst, src, min, max)
}

func (o *RedisOp) ZRevRange(key interface{}, start, stop int64) *RedisResponse {
	return o._Do("ZREVRANGE", key, start, stop)
}

func (o *RedisOp) ZRevRangeByLex(key interface{}, max, min string) *RedisResponse {
	return o._Do("ZREVRANGEBYLEX", key, max, min)
}

func (o *RedisOp) ZRevRangeByScore(key interface{}, max, min string) *RedisResponse {
	return o._Do("ZREVRANGEBYSCORE", key, max, min)
}

func (o *RedisOp) ZRank(key, member interface{}) *RedisResponse {
	return o._Do("ZRANK", key, member)
}

func (o *RedisOp) ZRem(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return o._Do("ZREM", args...)
}

func (o *RedisOp) ZRemRangeByLex(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZREMRANGEBYLEX", key, min, max)
}

func (o *RedisOp) ZRemRangeByRank(key interface{}, start, stop int64) *RedisResponse {
	return o._Do("ZREMRANGEBYRANK", key, start, stop)
}

func (o *RedisOp) ZRemRangeByScore(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZREMRANGEBYSCORE", key, min, max)
}

func (o *RedisOp) ZRevRank(key, member interface{}) *RedisResponse {
	return o._Do("ZREVRANK", key, member)
}

func (o *RedisOp) ZScan(key interface{}, cursor int64, match string, count int64) *RedisResponse {
	args := []interface{}{key, cursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", count)
	}
	return o._Do("ZSCAN", args...)
}

func (o *RedisOp) ZScore(key, member interface{}) *RedisResponse {
	return o._Do("ZSCORE", key, member)
}

func (o *RedisOp) ZUnion(key ...interface{}) *RedisResponse {
	return o._Do("ZUNION", key...)
}

func (o *RedisOp) ZUnionStore(destination interface{}, key ...interface{}) *RedisResponse {
	args := []interface{}{destination}
	args = append(args, key...)
	return o._Do("ZUNIONSTORE", args...)
}
