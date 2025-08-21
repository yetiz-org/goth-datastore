package datastore

import (
	"github.com/gomodule/redigo/redis"
	secret "github.com/yetiz-org/goth-secret"
)

// RedisOperator defines the interface for Redis operations.
// This interface allows for both real and mock implementations,
// enabling comprehensive unit testing while maintaining API compatibility.
type RedisOperator interface {
	// Connection and pool management
	Meta() secret.RedisMeta
	Pool() *redis.Pool
	Conn() redis.Conn
	ActiveCount() int
	IdleCount() int
	Close() error
	Exec(f func(conn redis.Conn)) error

	// Pipeline operations
	Pipeline(cmds ...RedisPipelineCmd) []*RedisResponse

	// String operations
	Get(key interface{}) *RedisResponse
	Set(key interface{}, val interface{}) *RedisResponse
	SetExpire(key interface{}, val interface{}, ttl int64) *RedisResponse
	Incr(key interface{}) *RedisResponse
	IncrBy(key interface{}, val int64) *RedisResponse
	Decr(key interface{}) *RedisResponse
	DecrBy(key interface{}, val int64) *RedisResponse
	Append(key interface{}, val interface{}) *RedisResponse
	StrLen(key interface{}) *RedisResponse
	GetRange(key interface{}, start, end int64) *RedisResponse
	SetRange(key interface{}, offset int64, val interface{}) *RedisResponse

	// Hash operations
	HMSet(key interface{}, val map[interface{}]interface{}) *RedisResponse
	HMGet(key interface{}, field ...interface{}) *RedisResponse
	HSet(key, field, val interface{}) *RedisResponse
	HGet(key, field interface{}) *RedisResponse
	HExists(key, field interface{}) *RedisResponse
	HDel(key interface{}, field ...interface{}) *RedisResponse
	HGetAll(key interface{}) *RedisResponse
	HLen(key interface{}) *RedisResponse
	HKeys(key interface{}) *RedisResponse
	HIncrBy(key interface{}, field interface{}, val int64) *RedisResponse
	HVals(key interface{}) *RedisResponse
	HScan(key interface{}, cursor int64, match string, count int64) *RedisResponse

	// Key operations
	Expire(key interface{}, ttl int64) *RedisResponse
	Delete(key ...interface{}) *RedisResponse
	Keys(key interface{}) *RedisResponse
	Exists(key ...interface{}) *RedisResponse
	Copy(src, dst interface{}) *RedisResponse
	Dump(key interface{}) *RedisResponse
	TTL(key interface{}) *RedisResponse
	PTTL(key interface{}) *RedisResponse
	Type(key interface{}) *RedisResponse
	RandomKey() *RedisResponse
	Rename(oldKey, newKey interface{}) *RedisResponse
	RenameNX(oldKey, newKey interface{}) *RedisResponse
	Touch(key ...interface{}) *RedisResponse
	Unlink(key ...interface{}) *RedisResponse
	Persist(key interface{}) *RedisResponse

	// List operations
	LIndex(key interface{}, index int64) *RedisResponse
	LInsert(key interface{}, where string, pivot, element interface{}) *RedisResponse
	LLen(key interface{}) *RedisResponse
	LMove(source, destination interface{}, srcWhere, dstWhere string) *RedisResponse
	LMPop(count int64, where string, key ...interface{}) *RedisResponse
	LPop(key interface{}) *RedisResponse
	LPos(key, element interface{}) *RedisResponse
	LPush(key interface{}, val ...interface{}) *RedisResponse
	LPushX(key interface{}, val ...interface{}) *RedisResponse
	LRange(key interface{}, start, stop int64) *RedisResponse
	LRem(key interface{}, count int64, element interface{}) *RedisResponse
	LSet(key interface{}, index int64, element interface{}) *RedisResponse
	LTrim(key interface{}, start, stop int64) *RedisResponse
	RPop(key interface{}) *RedisResponse
	RPopLPush(source, destination interface{}) *RedisResponse
	RPush(key interface{}, val ...interface{}) *RedisResponse
	RPushX(key interface{}, val ...interface{}) *RedisResponse

	// Set operations
	SAdd(key interface{}, member ...interface{}) *RedisResponse
	SCard(key interface{}) *RedisResponse
	SDiff(key ...interface{}) *RedisResponse
	SDiffStore(destination interface{}, key ...interface{}) *RedisResponse
	SInter(key ...interface{}) *RedisResponse
	SInterCard(key ...interface{}) *RedisResponse
	SInterStore(destination interface{}, key ...interface{}) *RedisResponse
	SIsMember(key, member interface{}) *RedisResponse
	SMembers(key interface{}) *RedisResponse
	SMIsMember(key interface{}, member ...interface{}) *RedisResponse
	SMove(source, destination, member interface{}) *RedisResponse
	SPop(key interface{}) *RedisResponse
	SRandMember(key interface{}) *RedisResponse
	SRem(key interface{}, member ...interface{}) *RedisResponse
	SScan(key interface{}, cursor int64, match string, count int64) *RedisResponse
	SUnion(key ...interface{}) *RedisResponse
	SUnionStore(destination interface{}, key ...interface{}) *RedisResponse

	// Sorted Set operations
	ZAdd(key interface{}, score float64, member interface{}, pairs ...interface{}) *RedisResponse
	ZCard(key interface{}) *RedisResponse
	ZCount(key interface{}, min, max string) *RedisResponse
	ZDiff(key ...interface{}) *RedisResponse
	ZDiffStore(destination interface{}, key ...interface{}) *RedisResponse
	ZIncrBy(key interface{}, increment float64, member interface{}) *RedisResponse
	ZInter(key ...interface{}) *RedisResponse
	ZInterCard(key ...interface{}) *RedisResponse
	ZInterStore(destination interface{}, key ...interface{}) *RedisResponse
	ZLexCount(key interface{}, min, max string) *RedisResponse
	ZMPop(count int64, where string, key ...interface{}) *RedisResponse
	ZMScore(key interface{}, member ...interface{}) *RedisResponse
	ZPopMax(key interface{}) *RedisResponse
	ZPopMin(key interface{}) *RedisResponse
	ZRandMember(key interface{}) *RedisResponse
	ZRange(key interface{}, start, stop int64) *RedisResponse
	ZRangeByLex(key interface{}, min, max string) *RedisResponse
	ZRangeByScore(key interface{}, min, max string) *RedisResponse
	ZRangeStore(dst interface{}, src interface{}, min, max int64) *RedisResponse
	ZRevRange(key interface{}, start, stop int64) *RedisResponse
	ZRevRangeByLex(key interface{}, max, min string) *RedisResponse
	ZRevRangeByScore(key interface{}, max, min string) *RedisResponse
	ZRank(key, member interface{}) *RedisResponse
	ZRem(key interface{}, member ...interface{}) *RedisResponse
	ZRemRangeByLex(key interface{}, min, max string) *RedisResponse
	ZRemRangeByRank(key interface{}, start, stop int64) *RedisResponse
	ZRemRangeByScore(key interface{}, min, max string) *RedisResponse
	ZRevRank(key, member interface{}) *RedisResponse
	ZScan(key interface{}, cursor int64, match string, count int64) *RedisResponse
	ZScore(key, member interface{}) *RedisResponse
	ZUnion(key ...interface{}) *RedisResponse
	ZUnionStore(destination interface{}, key ...interface{}) *RedisResponse

	// Admin operations
	FlushDB() *RedisResponse
	FlushAll() *RedisResponse
	Scan(cursor int64, match string, count int64) *RedisResponse
	Ping() *RedisResponse
	Publish(key interface{}, val interface{}) *RedisResponse

	// Script operations
	Eval(script string, keys []interface{}, args []interface{}) *RedisResponse
}
