package datastore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	secret "github.com/yetiz-org/goth-datastore/secrets"
	kklogger "github.com/yetiz-org/goth-kklogger"

	redis "github.com/redis/go-redis/v9"
)

// DefaultRedisDialTimeout is the dial timeout in milliseconds used when creating new Redis connections.
var DefaultRedisDialTimeout = 1000

// DefaultRedisMaxIdle is the maximum number of idle connections kept in the pool.
var DefaultRedisMaxIdle = 20

// DefaultRedisIdleTimeout is the idle connection timeout in milliseconds before an idle connection is closed.
var DefaultRedisIdleTimeout = 60000

// DefaultRedisMaxConnLifetime is the maximum lifetime in milliseconds for a connection before it is closed.
var DefaultRedisMaxConnLifetime = 0

// DefaultRedisMaxActive is the maximum number of active connections allowed in the pool (0 means unlimited).
var DefaultRedisMaxActive = 0

// DefaultRedisWait controls whether Get() waits for a connection when the pool is exhausted.
var DefaultRedisWait = false

const (
	redisModeSingle      = secret.RedisModeSingle
	redisModeReplication = secret.RedisModeReplication
	redisModeCluster     = secret.RedisModeCluster
)

// Redis provides convenient accessors to master and slave Redis operations using a connection pool.
// It is constructed via NewRedis and exposes Master() and Slave() for executing commands.
// This type holds no business logic; it wires secret-loaded endpoints to pools.
type Redis struct {
	name   string
	master RedisOperator
	slave  RedisOperator
}

func redisMetaFromAddrs(addrs []string) secret.RedisMeta {
	if len(addrs) == 0 {
		return secret.RedisMeta{}
	}

	host, port := splitRedisAddr(addrs[0])
	return secret.RedisMeta{
		Host: host,
		Port: port,
	}
}

func splitRedisAddr(addr string) (string, uint) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, 0
	}

	portValue, err := strconv.ParseUint(portText, 10, 64)
	if err != nil {
		return host, 0
	}

	return host, uint(portValue)
}

// Master returns the master RedisOperator for primary/write operations.
func (r *Redis) Master() RedisOperator {
	return r.master
}

// Slave returns the slave RedisOperator for read operations.
func (r *Redis) Slave() RedisOperator {
	return r.slave
}

// RedisOp wraps a redis.Pool and exposes typed Redis command helpers.
// Obtain instances via Redis.Master() and Redis.Slave().
// Each method executes a single Redis command and returns a RedisResponse.
type RedisOp struct {
	meta   secret.RedisMeta
	client redis.UniversalClient
}

// Meta returns the Redis connection metadata (host and port) loaded from secret.
func (o *RedisOp) Meta() secret.RedisMeta {
	return o.meta
}

// ActiveCount returns the number of active connections in the pool.
func (o *RedisOp) ActiveCount() int {
	if o.client == nil {
		return 0
	}

	switch client := o.client.(type) {
	case *redis.Client:
		return int(client.PoolStats().TotalConns)
	case *redis.ClusterClient:
		return int(client.PoolStats().TotalConns)
	default:
		return 0
	}
}

// IdleCount returns the number of idle connections in the pool.
func (o *RedisOp) IdleCount() int {
	if o.client == nil {
		return 0
	}

	switch client := o.client.(type) {
	case *redis.Client:
		return int(client.PoolStats().IdleConns)
	case *redis.ClusterClient:
		return int(client.PoolStats().IdleConns)
	default:
		return 0
	}
}

// RedisNotFound is returned when a key or record does not exist (nil reply).
var RedisNotFound = fmt.Errorf("not_found")

// RedisPipelineCmd describes a single command and its arguments in a pipeline batch.
type RedisPipelineCmd struct {
	Cmd  string
	Args []interface{}
}

// Usage guarantees 1:1 mapping between cmds[i] and responses[i].
// Pipeline sends multiple commands in a single batch and returns responses in the same order.
func (o *RedisOp) Pipeline(cmds ...RedisPipelineCmd) []*RedisResponse {
	if len(cmds) == 0 {
		return nil
	}

	ctx := context.Background()
	pipe := o.client.Pipeline()

	n := len(cmds)
	responses := make([]*RedisResponse, n)
	redisCmds := make([]*redis.Cmd, n)

	for i, c := range cmds {
		args := append([]interface{}{c.Cmd}, c.Args...)
		redisCmds[i] = pipe.Do(ctx, args...)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		kklogger.ErrorJ("datastore:RedisOp.Pipeline#exec!io", err.Error())
	}

	for i := 0; i < n; i++ {
		err := redisCmds[i].Err()
		if errors.Is(err, redis.Nil) {
			responses[i] = &RedisResponse{Error: RedisNotFound}
			continue
		}
		if err != nil {
			responses[i] = &RedisResponse{Error: err}
			continue
		}

		r := redisCmds[i].Val()
		if r == nil {
			responses[i] = &RedisResponse{Error: RedisNotFound}
		} else {
			responses[i] = &RedisResponse{
				RedisResponseEntity: RedisResponseEntity{data: r},
				Error:               nil,
			}
		}
	}

	return responses
}

func (o *RedisOp) Do(cmd string, args ...interface{}) *RedisResponse {
	return o._Do(cmd, args...)
}

func (o *RedisOp) _Do(cmd string, args ...interface{}) *RedisResponse {
	cmdArgs := append([]interface{}{cmd}, args...)
	r, err := o.client.Do(context.Background(), cmdArgs...).Result()
	if errors.Is(err, redis.Nil) {
		return &RedisResponse{
			Error: RedisNotFound,
		}
	}
	if err != nil {
		return &RedisResponse{
			Error: err,
		}
	}
	if r == nil {
		return &RedisResponse{
			Error: RedisNotFound,
		}
	}

	return &RedisResponse{
		RedisResponseEntity: RedisResponseEntity{data: r},
		Error:               nil,
	}
}

// Get retrieves the string value of a key.
func (o *RedisOp) Get(key interface{}) *RedisResponse {
	return o._Do("GET", key)
}

// Set sets the string value of a key.
func (o *RedisOp) Set(key interface{}, val interface{}) *RedisResponse {
	return o._Do("SET", key, val)
}

// SetOptions defines options for the SetWithOptions command.
type SetOptions struct {
	// NX - Only set the key if it does not already exist
	NX bool
	// XX - Only set the key if it already exists
	XX bool
	// GET - Return the old value stored at key
	GET bool
	// EX - Set expire time in seconds
	EX int64
	// PX - Set expire time in milliseconds
	PX int64
	// EXAT - Set expire time as Unix timestamp in seconds
	EXAT int64
	// PXAT - Set expire time as Unix timestamp in milliseconds
	PXAT int64
	// KEEPTTL - Retain the TTL associated with the key
	KEEPTTL bool
}

// SetWithOptions sets the string value of a key with additional options.
func (o *RedisOp) SetWithOptions(key interface{}, val interface{}, opts SetOptions) *RedisResponse {
	args := []interface{}{key, val}

	// Add condition options (mutually exclusive)
	if opts.NX {
		args = append(args, "NX")
	} else if opts.XX {
		args = append(args, "XX")
	}

	// Add GET option
	if opts.GET {
		args = append(args, "GET")
	}

	// Add expiration options (mutually exclusive)
	if opts.EX > 0 {
		args = append(args, "EX", opts.EX)
	} else if opts.PX > 0 {
		args = append(args, "PX", opts.PX)
	} else if opts.EXAT > 0 {
		args = append(args, "EXAT", opts.EXAT)
	} else if opts.PXAT > 0 {
		args = append(args, "PXAT", opts.PXAT)
	} else if opts.KEEPTTL {
		args = append(args, "KEEPTTL")
	}

	return o._Do("SET", args...)
}

// Expire sets a timeout on key. After the TTL expires, the key is deleted.
func (o *RedisOp) Expire(key interface{}, ttl int64) *RedisResponse {
	return o._Do("EXPIRE", key, ttl)
}

// Delete removes one or more keys.
func (o *RedisOp) Delete(key ...interface{}) *RedisResponse {
	return o._Do("DEL", key...)
}

// Keys returns all keys matching the given pattern.
func (o *RedisOp) Keys(key interface{}) *RedisResponse {
	return o._Do("KEYS", key)
}

// Exists checks if one or more keys exist.
func (o *RedisOp) Exists(key ...interface{}) *RedisResponse {
	return o._Do("EXISTS", key...)
}

// SetExpire sets value and expiration in one command.
func (o *RedisOp) SetExpire(key interface{}, val interface{}, ttl int64) *RedisResponse {
	return o._Do("SETEX", key, ttl, val)
}

// SetNX sets the value of a key, only if the key does not exist.
func (o *RedisOp) SetNX(key interface{}, val interface{}) *RedisResponse {
	return o._Do("SETNX", key, val)
}

// MSetNX sets multiple keys to multiple values, only if none of the keys exist.
func (o *RedisOp) MSetNX(keyvals ...interface{}) *RedisResponse {
	return o._Do("MSETNX", keyvals...)
}

// HMSet sets multiple hash fields to multiple values.
func (o *RedisOp) HMSet(key interface{}, val map[interface{}]interface{}) *RedisResponse {
	vals := []interface{}{key}
	for mk, mv := range val {
		vals = append(vals, mk, mv)
	}

	return o._Do("HMSET", vals...)
}

// HMGet gets the values of all specified fields in a hash.
func (o *RedisOp) HMGet(key interface{}, field ...interface{}) *RedisResponse {
	vals := []interface{}{key}
	for _, f := range field {
		vals = append(vals, f)
	}

	return o._Do("HMGET", vals...)
}

// HSet sets field in the hash stored at key to value.
func (o *RedisOp) HSet(key, field, val interface{}) *RedisResponse {
	return o._Do("HSET", key, field, val)
}

// HSetNX sets field in the hash stored at key to value, only if field does not exist.
func (o *RedisOp) HSetNX(key, field, val interface{}) *RedisResponse {
	return o._Do("HSETNX", key, field, val)
}

// HGet gets the value of a field in a hash.
func (o *RedisOp) HGet(key, field interface{}) *RedisResponse {
	return o._Do("HGET", key, field)
}

// HExists determines if a hash field exists.
func (o *RedisOp) HExists(key, field interface{}) *RedisResponse {
	return o._Do("HEXISTS", key, field)
}

// HDel deletes one or more hash fields.
func (o *RedisOp) HDel(key interface{}, field ...interface{}) *RedisResponse {
	vals := []interface{}{key}
	for _, f := range field {
		vals = append(vals, f)
	}

	return o._Do("HDEL", vals...)
}

// HGetAll gets all fields and values in a hash.
func (o *RedisOp) HGetAll(key interface{}) *RedisResponse {
	return o._Do("HGETALL", key)
}

// HLen returns the number of fields contained in the hash.
func (o *RedisOp) HLen(key interface{}) *RedisResponse {
	return o._Do("HLEN", key)
}

// HKeys returns all field names in a hash.
func (o *RedisOp) HKeys(key interface{}) *RedisResponse {
	return o._Do("HKEYS", key)
}

// HIncrBy increments the integer value of a hash field by the given number.
func (o *RedisOp) HIncrBy(key interface{}, field interface{}, val int64) *RedisResponse {
	return o._Do("HINCRBY", key, field, val)
}

// HVals returns all values in a hash.
func (o *RedisOp) HVals(key interface{}) *RedisResponse {
	return o._Do("HVALS", key)
}

// HScan iterates over hash fields and values using a cursor.
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

// Incr increments the integer value of a key by one.
func (o *RedisOp) Incr(key interface{}) *RedisResponse {
	return o._Do("INCR", key)
}

// IncrBy increments the integer value of a key by the given amount.
func (o *RedisOp) IncrBy(key interface{}, val int64) *RedisResponse {
	return o._Do("INCRBY", key, val)
}

// Publish posts a message to the given channel.
func (o *RedisOp) Publish(key interface{}, val interface{}) *RedisResponse {
	return o._Do("PUBLISH", key, val)
}

// String commands (supplementary)
// Append appends a value to a key's string value.
func (o *RedisOp) Append(key interface{}, val interface{}) *RedisResponse {
	return o._Do("APPEND", key, val)
}

// StrLen returns the length of the string value stored at key.
func (o *RedisOp) StrLen(key interface{}) *RedisResponse {
	return o._Do("STRLEN", key)
}

// GetRange gets a substring of the string stored at key.
func (o *RedisOp) GetRange(key interface{}, start, end int64) *RedisResponse {
	return o._Do("GETRANGE", key, start, end)
}

// SetRange overwrites part of the string stored at key starting at the specified offset.
func (o *RedisOp) SetRange(key interface{}, offset int64, val interface{}) *RedisResponse {
	return o._Do("SETRANGE", key, offset, val)
}

// Key commands (supplementary)
// Copy copies the value stored at the source key to the destination key.
func (o *RedisOp) Copy(src, dst interface{}) *RedisResponse {
	return o._Do("COPY", src, dst)
}

// Decr decrements the integer value of a key by one.
func (o *RedisOp) Decr(key interface{}) *RedisResponse {
	return o._Do("DECR", key)
}

// DecrBy decrements the integer value of a key by the given number.
func (o *RedisOp) DecrBy(key interface{}, val int64) *RedisResponse {
	return o._Do("DECRBY", key, val)
}

// Dump serializes the value stored at key in a Redis-specific format.
func (o *RedisOp) Dump(key interface{}) *RedisResponse {
	return o._Do("DUMP", key)
}

// TTL returns the remaining time to live of a key in seconds.
func (o *RedisOp) TTL(key interface{}) *RedisResponse {
	return o._Do("TTL", key)
}

// PTTL returns the remaining time to live of a key in milliseconds.
func (o *RedisOp) PTTL(key interface{}) *RedisResponse {
	return o._Do("PTTL", key)
}

// Type returns the string representation of the key's data type.
func (o *RedisOp) Type(key interface{}) *RedisResponse {
	return o._Do("TYPE", key)
}

// RandomKey returns a random key from the current database.
func (o *RedisOp) RandomKey() *RedisResponse {
	return o._Do("RANDOMKEY")
}

// Rename renames a key.
func (o *RedisOp) Rename(oldKey, newKey interface{}) *RedisResponse {
	return o._Do("RENAME", oldKey, newKey)
}

// RenameNX renames a key, only if the new key does not exist.
func (o *RedisOp) RenameNX(oldKey, newKey interface{}) *RedisResponse {
	return o._Do("RENAMENX", oldKey, newKey)
}

// Touch updates the last access time of one or more keys.
func (o *RedisOp) Touch(key ...interface{}) *RedisResponse {
	return o._Do("TOUCH", key...)
}

// Unlink removes one or more keys asynchronously.
func (o *RedisOp) Unlink(key ...interface{}) *RedisResponse {
	return o._Do("UNLINK", key...)
}

// Persist removes the existing timeout on a key.
func (o *RedisOp) Persist(key interface{}) *RedisResponse {
	return o._Do("PERSIST", key)
}

// List commands
// LIndex returns the element at index in the list stored at key.
func (o *RedisOp) LIndex(key interface{}, index int64) *RedisResponse {
	return o._Do("LINDEX", key, index)
}

// LInsert inserts element before or after the pivot element in the list stored at key.
func (o *RedisOp) LInsert(key interface{}, where string, pivot, element interface{}) *RedisResponse {
	return o._Do("LINSERT", key, where, pivot, element)
}

// LLen returns the length of the list stored at key.
func (o *RedisOp) LLen(key interface{}) *RedisResponse {
	return o._Do("LLEN", key)
}

// LMove atomically returns and removes the element from the source list and pushes it to the destination list.
func (o *RedisOp) LMove(source, destination interface{}, srcWhere, dstWhere string) *RedisResponse {
	return o._Do("LMOVE", source, destination, srcWhere, dstWhere)
}

// LMPop pops one or multiple elements from the first non-empty list key among the given keys.
func (o *RedisOp) LMPop(count int64, where string, key ...interface{}) *RedisResponse {
	// LMPOP requires: numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
	// Here, count is treated as numkeys; if zero or negative, it is derived from len(key).
	numkeys := count
	if numkeys <= 0 {
		numkeys = int64(len(key))
	}
	args := []interface{}{numkeys}
	args = append(args, key...)
	args = append(args, where)
	return o._Do("LMPOP", args...)
}

// LPop removes and returns the first element of the list stored at key.
func (o *RedisOp) LPop(key interface{}) *RedisResponse {
	return o._Do("LPOP", key)
}

// LPos returns the index of the first occurrence of element in the list stored at key.
func (o *RedisOp) LPos(key, element interface{}) *RedisResponse {
	return o._Do("LPOS", key, element)
}

// LPush inserts all the specified values at the head of the list stored at key.
func (o *RedisOp) LPush(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return o._Do("LPUSH", args...)
}

// LPushX inserts values at the head of the list stored at key, only if the list exists.
func (o *RedisOp) LPushX(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return o._Do("LPUSHX", args...)
}

// LRange returns the specified elements of the list stored at key.
func (o *RedisOp) LRange(key interface{}, start, stop int64) *RedisResponse {
	return o._Do("LRANGE", key, start, stop)
}

// LRem removes the first count occurrences of element from the list stored at key.
func (o *RedisOp) LRem(key interface{}, count int64, element interface{}) *RedisResponse {
	return o._Do("LREM", key, count, element)
}

// LSet sets the list element at index to element.
func (o *RedisOp) LSet(key interface{}, index int64, element interface{}) *RedisResponse {
	return o._Do("LSET", key, index, element)
}

// LTrim trims an existing list so that it will contain only the specified range of elements.
func (o *RedisOp) LTrim(key interface{}, start, stop int64) *RedisResponse {
	return o._Do("LTRIM", key, start, stop)
}

// RPop removes and returns the last element of the list stored at key.
func (o *RedisOp) RPop(key interface{}) *RedisResponse {
	return o._Do("RPOP", key)
}

// RPopLPush removes the last element in the source list and pushes it to the head of the destination list.
func (o *RedisOp) RPopLPush(source, destination interface{}) *RedisResponse {
	return o._Do("RPOPLPUSH", source, destination)
}

// RPush inserts all the specified values at the tail of the list stored at key.
func (o *RedisOp) RPush(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return o._Do("RPUSH", args...)
}

// RPushX inserts values at the tail of the list stored at key, only if the list exists.
func (o *RedisOp) RPushX(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return o._Do("RPUSHX", args...)
}

// Set commands
// SAdd adds one or more members to a set.
func (o *RedisOp) SAdd(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return o._Do("SADD", args...)
}

// SCard returns the set cardinality (number of elements) of the set stored at key.
func (o *RedisOp) SCard(key interface{}) *RedisResponse {
	return o._Do("SCARD", key)
}

// SDiff returns the members of the set resulting from the difference between the first set and all the successive sets.
func (o *RedisOp) SDiff(key ...interface{}) *RedisResponse {
	return o._Do("SDIFF", key...)
}

// SDiffStore stores the result of SDIFF in the destination key.
func (o *RedisOp) SDiffStore(destination interface{}, key ...interface{}) *RedisResponse {
	args := []interface{}{destination}
	args = append(args, key...)
	return o._Do("SDIFFSTORE", args...)
}

// SInter returns the members of the set resulting from the intersection of all the given sets.
func (o *RedisOp) SInter(key ...interface{}) *RedisResponse {
	return o._Do("SINTER", key...)
}

// SInterCard returns the number of elements in the intersection of all the given sets.
func (o *RedisOp) SInterCard(key ...interface{}) *RedisResponse {
	// SINTERCARD requires: numkeys key [key ...] [LIMIT num]
	numkeys := int64(len(key))
	args := []interface{}{numkeys}
	args = append(args, key...)
	return o._Do("SINTERCARD", args...)
}

// SInterStore stores the result of SINTER in the destination key.
func (o *RedisOp) SInterStore(destination interface{}, key ...interface{}) *RedisResponse {
	args := []interface{}{destination}
	args = append(args, key...)
	return o._Do("SINTERSTORE", args...)
}

// SIsMember returns if member is a member of the set stored at key.
func (o *RedisOp) SIsMember(key, member interface{}) *RedisResponse {
	return o._Do("SISMEMBER", key, member)
}

// SMembers returns all the members of the set value stored at key.
func (o *RedisOp) SMembers(key interface{}) *RedisResponse {
	return o._Do("SMEMBERS", key)
}

// SMIsMember returns whether each member is a member of the set stored at key.
func (o *RedisOp) SMIsMember(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return o._Do("SMISMEMBER", args...)
}

// SMove moves member from the source set to the destination set.
func (o *RedisOp) SMove(source, destination, member interface{}) *RedisResponse {
	return o._Do("SMOVE", source, destination, member)
}

// SPop removes and returns one or more random members from the set stored at key.
func (o *RedisOp) SPop(key interface{}) *RedisResponse {
	return o._Do("SPOP", key)
}

// SRandMember returns one or more random members from the set value stored at key without removing them.
func (o *RedisOp) SRandMember(key interface{}) *RedisResponse {
	return o._Do("SRANDMEMBER", key)
}

// SRem removes one or more members from a set.
func (o *RedisOp) SRem(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return o._Do("SREM", args...)
}

// SScan iterates elements of the set stored at key.
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

// SUnion returns the members of the set resulting from the union of all the given sets.
func (o *RedisOp) SUnion(key ...interface{}) *RedisResponse {
	return o._Do("SUNION", key...)
}

// SUnionStore stores the result of SUNION in the destination key.
func (o *RedisOp) SUnionStore(destination interface{}, key ...interface{}) *RedisResponse {
	args := []interface{}{destination}
	args = append(args, key...)
	return o._Do("SUNIONSTORE", args...)
}

// FlushDB removes all keys from the current database.
func (o *RedisOp) FlushDB() *RedisResponse {
	return o._Do("FLUSHDB")
}

// FlushAll removes all keys from all databases.
func (o *RedisOp) FlushAll() *RedisResponse {
	return o._Do("FLUSHALL")
}

// Scan iterates the set of keys in the current database.
func (o *RedisOp) Scan(cursor int64, match string, count int64) *RedisResponse {
	nextCursor := cursor
	keys := make([]interface{}, 0)

	for {
		args := []interface{}{nextCursor}
		if match != "" {
			args = append(args, "MATCH", match)
		}
		if count > 0 {
			args = append(args, "COUNT", count)
		}

		resp := o._Do("SCAN", args...)
		if resp.Error != nil {
			return resp
		}

		parts := resp.GetSlice()
		if len(parts) != 2 {
			return &RedisResponse{Error: fmt.Errorf("invalid scan response")}
		}

		nextCursor = parts[0].GetInt64()
		for _, key := range parts[1].GetSlice() {
			keys = append(keys, key.data)
		}

		if nextCursor == 0 {
			return &RedisResponse{
				RedisResponseEntity: RedisResponseEntity{data: []interface{}{int64(0), keys}},
				Error:               nil,
			}
		}
	}
}

// Ping checks if the server is alive and responding.
func (o *RedisOp) Ping() *RedisResponse {
	return o._Do("PING")
}

// Close closes the underlying connection pool if present.
// This is not a Redis command; it releases local resources.
// Safe to call multiple times.
func (o *RedisOp) Close() error {
	if o.client != nil {
		return o.client.Close()
	}

	return nil
}

// Script commands
// Eval executes a Lua script on the server.
func (o *RedisOp) Eval(script string, keys []interface{}, args []interface{}) *RedisResponse {
	numkeys := int64(len(keys))
	cmdArgs := []interface{}{script, numkeys}
	cmdArgs = append(cmdArgs, keys...)
	cmdArgs = append(cmdArgs, args...)
	return o._Do("EVAL", cmdArgs...)
}

// RedisResponseEntity holds a single Redis reply value and provides typed accessors.
// It wraps the raw reply so callers can convert to int64/string/bytes safely.
type RedisResponseEntity struct {
	data interface{}
}

// GetInt64 converts the underlying reply to int64 when possible.
// Returns 0 if the value is not numeric or cannot be parsed.
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

// GetString returns the underlying reply coerced to string when possible.
// For []byte, it converts bytes to string; for numeric values, it formats as decimal string.
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

// GetBytes returns the underlying reply as a byte slice when available.
// Returns nil if the reply is not a []byte.
func (k *RedisResponseEntity) GetBytes() []byte {
	switch v := k.data.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	}

	return nil
}

// GetFloat64 converts the underlying reply to float64 when possible.
// Returns 0.0 if the value is not numeric or cannot be parsed.
func (k *RedisResponseEntity) GetFloat64() float64 {
	switch v := k.data.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int64:
		return float64(v)
	case int:
		return float64(v)
	case []byte:
		if p, err := strconv.ParseFloat(string(v), 64); err == nil {
			return p
		}

		return 0.0
	case string:
		if p, err := strconv.ParseFloat(v, 64); err == nil {
			return p
		}

		return 0.0
	}

	return 0.0
}

// GetSlice converts an array reply into a slice of RedisResponseEntity for typed access.
// Returns an empty slice if the reply is not an array.
func (k *RedisResponseEntity) GetSlice() []RedisResponseEntity {
	var entities []RedisResponseEntity
	switch v := k.data.(type) {
	case []interface{}:
		for _, entity := range v {
			entities = append(entities, RedisResponseEntity{data: entity})
		}
	case []string:
		for _, entity := range v {
			entities = append(entities, RedisResponseEntity{data: entity})
		}
	case map[interface{}]interface{}:
		for mk, mv := range v {
			entities = append(entities, RedisResponseEntity{data: mk}, RedisResponseEntity{data: mv})
		}
	case map[string]string:
		for mk, mv := range v {
			entities = append(entities, RedisResponseEntity{data: mk}, RedisResponseEntity{data: mv})
		}
	}

	return entities
}

// RedisResponse wraps a Redis reply and an optional error.
// It embeds RedisResponseEntity to provide typed accessors for the reply payload.
type RedisResponse struct {
	RedisResponseEntity
	Error error
}

func (k *RedisResponse) RecordNotFound() bool {
	return errors.Is(k.Error, RedisNotFound)
}

// NewRedis constructs a Redis client by loading the secret profile with the given name.
// The secret must contain master/slave endpoints defined by RedisMeta (host and port only).
func NewRedis(profileName string) *Redis {
	profile, err := secret.LoadRedisProfile(profileName)
	if err != nil {
		kklogger.ErrorJ("datastore.redis#Load", err.Error())
		return nil
	}

	return NewRedisWithProfile(profileName, profile)
}

func NewRedisWithProfile(profileName string, profile *secret.RedisProfile) *Redis {
	if profile == nil {
		return nil
	}

	profile.Normalize()

	r := &Redis{
		name: profileName,
	}

	r.master = &RedisOp{
		meta:   redisMetaFromAddrs(profile.MasterAddrs()),
		client: newRedisClient(profile, profile.MasterAddrs(), false),
	}

	r.slave = &RedisOp{
		meta:   redisMetaFromAddrs(profile.SlaveAddrs()),
		client: newRedisClient(profile, profile.SlaveAddrs(), profile.Mode == redisModeCluster),
	}

	return r
}

func newRedisClient(profile *secret.RedisProfile, addrs []string, readOnly bool) redis.UniversalClient {
	if len(addrs) == 0 {
		return nil
	}

	options := &redis.UniversalOptions{
		Addrs:           addrs,
		Username:        profile.Username,
		Password:        profile.Password,
		DB:              profile.DB,
		DialTimeout:     time.Duration(DefaultRedisDialTimeout) * time.Millisecond,
		MaxIdleConns:    DefaultRedisMaxIdle,
		MaxActiveConns:  DefaultRedisMaxActive,
		ConnMaxIdleTime: time.Duration(DefaultRedisIdleTimeout) * time.Millisecond,
		ConnMaxLifetime: time.Duration(DefaultRedisMaxConnLifetime) * time.Millisecond,
		ReadOnly:        readOnly,
		RouteByLatency:  profile.Cluster.RouteByLatency,
		RouteRandomly:   profile.Cluster.RouteRandomly,
	}

	if DefaultRedisWait {
		options.PoolTimeout = time.Duration(DefaultRedisDialTimeout) * time.Millisecond
	}

	return redis.NewUniversalClient(options)
}

// Sorted Set commands
// ZAdd adds all the specified members with the specified scores to the sorted set stored at key.
func (o *RedisOp) ZAdd(key interface{}, score float64, member interface{}, pairs ...interface{}) *RedisResponse {
	args := []interface{}{key, score, member}
	args = append(args, pairs...)
	return o._Do("ZADD", args...)
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func (o *RedisOp) ZCard(key interface{}) *RedisResponse {
	return o._Do("ZCARD", key)
}

// ZCount returns the number of elements in the sorted set with a score between min and max.
func (o *RedisOp) ZCount(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZCOUNT", key, min, max)
}

// ZDiff returns the members of the sorted set resulting from the difference between the first set and all the successive sets.
func (o *RedisOp) ZDiff(key ...interface{}) *RedisResponse {
	// ZDIFF requires: numkeys key [key ...] [WITHSCORES]
	numkeys := int64(len(key))
	args := []interface{}{numkeys}
	args = append(args, key...)
	return o._Do("ZDIFF", args...)
}

// ZDiffStore stores the result of ZDIFF in the destination key.
func (o *RedisOp) ZDiffStore(destination interface{}, key ...interface{}) *RedisResponse {
	// ZDIFFSTORE requires: destination numkeys key [key ...]
	numkeys := int64(len(key))
	args := []interface{}{destination, numkeys}
	args = append(args, key...)
	return o._Do("ZDIFFSTORE", args...)
}

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
func (o *RedisOp) ZIncrBy(key interface{}, increment float64, member interface{}) *RedisResponse {
	return o._Do("ZINCRBY", key, increment, member)
}

// ZInter returns the members of the sorted set resulting from the intersection of all the given sets.
func (o *RedisOp) ZInter(key ...interface{}) *RedisResponse {
	// ZINTER requires: numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES]
	numkeys := int64(len(key))
	args := []interface{}{numkeys}
	args = append(args, key...)
	return o._Do("ZINTER", args...)
}

// ZInterCard returns the number of elements in the intersection of all the given sorted sets.
func (o *RedisOp) ZInterCard(key ...interface{}) *RedisResponse {
	// ZINTERCARD requires: numkeys key [key ...] [LIMIT num]
	numkeys := int64(len(key))
	args := []interface{}{numkeys}
	args = append(args, key...)
	return o._Do("ZINTERCARD", args...)
}

// ZInterStore stores the result of ZINTER in the destination key.
func (o *RedisOp) ZInterStore(destination interface{}, key ...interface{}) *RedisResponse {
	// ZINTERSTORE requires: destination numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]
	numkeys := int64(len(key))
	args := []interface{}{destination, numkeys}
	args = append(args, key...)
	return o._Do("ZINTERSTORE", args...)
}

// ZLexCount returns the number of elements in the sorted set with a value between min and max, lexicographically.
func (o *RedisOp) ZLexCount(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZLEXCOUNT", key, min, max)
}

// ZMPop pops one or more elements from the first non-empty sorted set among the given keys.
func (o *RedisOp) ZMPop(count int64, where string, key ...interface{}) *RedisResponse {
	// ZMPOP requires: numkeys key [key ...] <MIN | MAX> [COUNT count]
	// Here, count is treated as numkeys; if zero or negative, it is derived from len(key).
	numkeys := count
	if numkeys <= 0 {
		numkeys = int64(len(key))
	}
	args := []interface{}{numkeys}
	args = append(args, key...)
	args = append(args, where)
	return o._Do("ZMPOP", args...)
}

// ZMScore returns the scores associated with the specified members in the sorted set stored at key.
func (o *RedisOp) ZMScore(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return o._Do("ZMSCORE", args...)
}

// ZPopMax removes and returns the member with the highest score from the sorted set.
func (o *RedisOp) ZPopMax(key interface{}) *RedisResponse {
	return o._Do("ZPOPMAX", key)
}

// ZPopMin removes and returns the member with the lowest score from the sorted set.
func (o *RedisOp) ZPopMin(key interface{}) *RedisResponse {
	return o._Do("ZPOPMIN", key)
}

// ZRandMember returns a random member from the sorted set without removing it.
func (o *RedisOp) ZRandMember(key interface{}) *RedisResponse {
	return o._Do("ZRANDMEMBER", key)
}

// ZRange returns the specified range of members in the sorted set stored at key by index.
func (o *RedisOp) ZRange(key interface{}, start, stop int64) *RedisResponse {
	return o._Do("ZRANGE", key, start, stop)
}

// ZRangeByLex returns all the elements in the sorted set with a value between min and max, lexicographically.
func (o *RedisOp) ZRangeByLex(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZRANGEBYLEX", key, min, max)
}

// ZRangeByScore returns all the elements in the sorted set with a score between min and max.
func (o *RedisOp) ZRangeByScore(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZRANGEBYSCORE", key, min, max)
}

// ZRangeStore stores a range of members selected by index from the source sorted set to the destination key.
func (o *RedisOp) ZRangeStore(dst interface{}, src interface{}, min, max int64) *RedisResponse {
	return o._Do("ZRANGESTORE", dst, src, min, max)
}

// ZRevRange returns the specified range of members in the sorted set stored at key by index, with scores ordered from high to low.
func (o *RedisOp) ZRevRange(key interface{}, start, stop int64) *RedisResponse {
	return o._Do("ZREVRANGE", key, start, stop)
}

// ZRevRangeByLex returns all the elements in the sorted set with a value between max and min, lexicographically, in reverse order.
func (o *RedisOp) ZRevRangeByLex(key interface{}, max, min string) *RedisResponse {
	return o._Do("ZREVRANGEBYLEX", key, max, min)
}

// ZRevRangeByScore returns all the elements in the sorted set with a score between max and min in reverse order.
func (o *RedisOp) ZRevRangeByScore(key interface{}, max, min string) *RedisResponse {
	return o._Do("ZREVRANGEBYSCORE", key, max, min)
}

// ZRank returns the rank of member in the sorted set stored at key, with scores ordered from low to high.
func (o *RedisOp) ZRank(key, member interface{}) *RedisResponse {
	return o._Do("ZRANK", key, member)
}

// ZRem removes one or more members from the sorted set stored at key.
func (o *RedisOp) ZRem(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return o._Do("ZREM", args...)
}

// ZRemRangeByLex removes all members in the sorted set between the lexicographical range specified by min and max.
func (o *RedisOp) ZRemRangeByLex(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZREMRANGEBYLEX", key, min, max)
}

// ZRemRangeByRank removes all members in the sorted set stored at key within the given index range.
func (o *RedisOp) ZRemRangeByRank(key interface{}, start, stop int64) *RedisResponse {
	return o._Do("ZREMRANGEBYRANK", key, start, stop)
}

// ZRemRangeByScore removes all members in the sorted set stored at key with scores between min and max.
func (o *RedisOp) ZRemRangeByScore(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZREMRANGEBYSCORE", key, min, max)
}

// ZRevRank returns the rank of member in the sorted set, with scores ordered from high to low.
func (o *RedisOp) ZRevRank(key, member interface{}) *RedisResponse {
	return o._Do("ZREVRANK", key, member)
}

// ZScan incrementally iterates members of the sorted set stored at key.
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

// ZScore returns the score associated with the specified member in the sorted set.
func (o *RedisOp) ZScore(key, member interface{}) *RedisResponse {
	return o._Do("ZSCORE", key, member)
}

// ZUnion returns the members of the sorted set resulting from the union of all the given sorted sets.
func (o *RedisOp) ZUnion(key ...interface{}) *RedisResponse {
	// ZUNION requires: numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES]
	numkeys := int64(len(key))
	args := []interface{}{numkeys}
	args = append(args, key...)
	return o._Do("ZUNION", args...)
}

func (o *RedisOp) ZUnionStore(destination interface{}, key ...interface{}) *RedisResponse {
	// ZUNIONSTORE requires: destination numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]
	numkeys := int64(len(key))
	args := []interface{}{destination, numkeys}
	args = append(args, key...)
	return o._Do("ZUNIONSTORE", args...)
}
