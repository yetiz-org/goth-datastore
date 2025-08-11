package datastore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	secret "github.com/yetiz-org/goth-secret"

	"github.com/gomodule/redigo/redis"
	"github.com/yetiz-org/goth-kklogger"
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

// Redis provides convenient accessors to master and slave Redis operations using a connection pool.
// It is constructed via NewRedis and exposes Master() and Slave() for executing commands.
// This type holds no business logic; it wires secret-loaded endpoints to pools.
type Redis struct {
	name   string
	master *RedisOp
	slave  *RedisOp
}

// Master returns the master RedisOp for primary/write operations.
// Params: none
// Usage: op := redis.Master()
// Redis: N/A (client-side helper)
func (r *Redis) Master() *RedisOp {
	return r.master
}

// Slave returns the slave RedisOp for read operations.
// Params: none
// Usage: op := redis.Slave()
// Redis: N/A (client-side helper)
func (r *Redis) Slave() *RedisOp {
	return r.slave
}

// RedisOp wraps a redis.Pool and exposes typed Redis command helpers.
// Obtain instances via Redis.Master() and Redis.Slave().
// Each method executes a single Redis command and returns a RedisResponse.
type RedisOp struct {
	meta   secret.RedisMeta
	pool   *redis.Pool
	opLock sync.Mutex
}

// Meta returns the Redis connection metadata (host and port) loaded from secret.
// Params: none
// Usage: meta := redis.Master().Meta()
// Redis: N/A (client-side metadata)
func (o *RedisOp) Meta() secret.RedisMeta {
	return o.meta
}

// Pool returns the underlying redis.Pool used by this RedisOp.
// Params: none
// Usage: p := redis.Master().Pool()
// Redis: N/A (client-side pool accessor)
func (o *RedisOp) Pool() *redis.Pool {
	return o.pool
}

// Conn returns a raw redis.Conn from the pool. Caller must Close() when done.
// Params: none
// Usage: c := redis.Master().Conn(); defer c.Close()
// Redis: N/A (low-level connection)
func (o *RedisOp) Conn() redis.Conn {
	return o.pool.Get()
}

// ActiveCount returns the number of active connections in the pool.
// Params: none
// Usage: n := redis.Master().ActiveCount()
// Redis: N/A (pool metric)
func (o *RedisOp) ActiveCount() int {
	if o.pool == nil {
		return 0
	}

	return o.pool.ActiveCount()
}

// IdleCount returns the number of idle connections in the pool.
// Params: none
// Usage: n := redis.Master().IdleCount()
// Redis: N/A (pool metric)
func (o *RedisOp) IdleCount() int {
	if o.pool == nil {
		return 0
	}

	return o.pool.IdleCount()
}

// RedisNotFound is returned when a key or record does not exist (nil reply).
var RedisNotFound = fmt.Errorf("not_found")

// RedisPipelineCmd describes a single command and its arguments in a pipeline batch.
type RedisPipelineCmd struct {
	Cmd  string
	Args []interface{}
}

// Exec obtains a connection from the pool and executes the given function.
// Params: f - a callback that receives a redis.Conn
// Usage: err := redis.Master().Exec(func(c redis.Conn){ /* use c */ })
// Redis: N/A (connection helper)
func (o *RedisOp) Exec(f func(conn redis.Conn)) error {
	if conn, err := o.pool.GetContext(context.Background()); err != nil {
		return err
	} else {
		f(conn)
		return conn.Close()
	}
}

// Note: The responses preserve the same order as the input cmds.
// Usage guarantees 1:1 mapping between cmds[i] and responses[i].
// Pipeline sends multiple commands in a single batch and returns responses in the same order.
// Params: cmds - a variadic list of RedisPipelineCmd {Cmd, Args}
// Usage: resps := redis.Master().Pipeline(RedisPipelineCmd{Cmd:"SET", Args:[]interface{}{key, val}}, ...)
// Redis: Pipeline (multiple commands)
func (o *RedisOp) Pipeline(cmds ...RedisPipelineCmd) []*RedisResponse {
	if len(cmds) == 0 {
		return nil
	}

	conn := o.Conn()
	defer conn.Close()

	n := len(cmds)
	responses := make([]*RedisResponse, n)
	sent := make([]bool, n)

	// Send each command one by one
	for i, c := range cmds {
		if err := conn.Send(c.Cmd, c.Args...); err != nil {
			kklogger.ErrorJ("datastore:RedisOp.Pipeline#send!io", err.Error())
			// If Send fails, do not send subsequent commands; mark current and remaining as errors
			for j := i; j < n; j++ {
				responses[j] = &RedisResponse{Error: fmt.Errorf("pipeline send failed at cmd %d: %w", j, err)}
			}
			return responses
		}
		sent[i] = true
	}

	// Flush all buffered commands to Redis
	if err := conn.Flush(); err != nil {
		kklogger.ErrorJ("datastore:RedisOp.Pipeline#flush!io", err.Error())
		for i := 0; i < n; i++ {
			if responses[i] == nil {
				responses[i] = &RedisResponse{Error: fmt.Errorf("pipeline flush failed: %w", err)}
			}
		}
		return responses
	}

	// Receive responses in the same order as commands were sent
	for i := 0; i < n; i++ {
		if !sent[i] {
			// Normally unreachable: previous Send failure returns earlier
			continue
		}

		r, err := conn.Receive()
		if err != nil {
			kklogger.ErrorJ("datastore:RedisOp.Pipeline#receive!io", err.Error())
			responses[i] = &RedisResponse{Error: err}
			continue
		}

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

// Get retrieves the string value of a key.
// Params: key - the key to fetch
// Usage: resp := redis.Master().Get("my_key"); val := resp.GetString()
// Redis: GET
func (o *RedisOp) Get(key interface{}) *RedisResponse {
	return o._Do("GET", key)
}

// Set sets the string value of a key.
// Params: key - target key; val - value to set
// Usage: resp := redis.Master().Set("k", "v")
// Redis: SET
func (o *RedisOp) Set(key interface{}, val interface{}) *RedisResponse {
	return o._Do("SET", key, val)
}

// Expire sets a timeout on key. After the TTL expires, the key is deleted.
// Params: key - target key; ttl - time to live in seconds
// Usage: resp := redis.Master().Expire("k", 60)
// Redis: EXPIRE
func (o *RedisOp) Expire(key interface{}, ttl int64) *RedisResponse {
	return o._Do("EXPIRE", key, ttl)
}

// Delete removes one or more keys.
// Params: key - one or more keys to delete
// Usage: resp := redis.Master().Delete("k1", "k2")
// Redis: DEL
func (o *RedisOp) Delete(key ...interface{}) *RedisResponse {
	return o._Do("DEL", key...)
}

// Keys returns all keys matching the given pattern.
// Params: key - pattern (e.g., "user:*")
// Usage: resp := redis.Master().Keys("prefix:*")
// Redis: KEYS
func (o *RedisOp) Keys(key interface{}) *RedisResponse {
	return o._Do("KEYS", key)
}

// Exists checks if one or more keys exist.
// Params: key - one or more keys to check
// Usage: resp := redis.Master().Exists("k1", "k2"); count := resp.GetInt64()
// Redis: EXISTS
func (o *RedisOp) Exists(key ...interface{}) *RedisResponse {
	return o._Do("EXISTS", key...)
}

// SetExpire sets value and expiration in one command.
// Params: key - target key; val - value to set; ttl - seconds to live
// Usage: resp := redis.Master().SetExpire("k", "v", 30)
// Redis: SETEX
func (o *RedisOp) SetExpire(key interface{}, val interface{}, ttl int64) *RedisResponse {
	return o._Do("SETEX", key, ttl, val)
}

// HMSet sets multiple hash fields to multiple values.
// Params: key - hash key; val - map of field->value pairs
// Usage: resp := redis.Master().HMSet("h", map[interface{}]interface{}{"f":"v"})
// Redis: HMSET
func (o *RedisOp) HMSet(key interface{}, val map[interface{}]interface{}) *RedisResponse {
	vals := []interface{}{key}
	for mk, mv := range val {
		vals = append(vals, mk, mv)
	}

	return o._Do("HMSET", vals...)
}

// HMGet gets the values of all specified fields in a hash.
// Params: key - hash key; field - one or more field names
// Usage: resp := redis.Master().HMGet("h", "f1", "f2")
// Redis: HMGET
func (o *RedisOp) HMGet(key interface{}, field ...interface{}) *RedisResponse {
	vals := []interface{}{key}
	for _, f := range field {
		vals = append(vals, f)
	}

	return o._Do("HMGET", vals...)
}

// HSet sets field in the hash stored at key to value.
// Params: key - hash key; field - field name; val - value to set
// Usage: resp := redis.Master().HSet("h", "f", "v")
// Redis: HSET
func (o *RedisOp) HSet(key, field, val interface{}) *RedisResponse {
	return o._Do("HSET", key, field, val)
}

// HGet gets the value of a field in a hash.
// Params: key - hash key; field - field name
// Usage: resp := redis.Master().HGet("h", "f")
// Redis: HGET
func (o *RedisOp) HGet(key, field interface{}) *RedisResponse {
	return o._Do("HGET", key, field)
}

// HExists determines if a hash field exists.
// Params: key - hash key; field - field name
// Usage: resp := redis.Master().HExists("h", "f"); exists := resp.GetInt64()
// Redis: HEXISTS
func (o *RedisOp) HExists(key, field interface{}) *RedisResponse {
	return o._Do("HEXISTS", key, field)
}

// HDel deletes one or more hash fields.
// Params: key - hash key; field - one or more field names
// Usage: resp := redis.Master().HDel("h", "f1", "f2")
// Redis: HDEL
func (o *RedisOp) HDel(key interface{}, field ...interface{}) *RedisResponse {
	vals := []interface{}{key}
	for _, f := range field {
		vals = append(vals, f)
	}

	return o._Do("HDEL", vals...)
}

// HGetAll gets all fields and values in a hash.
// Params: key - hash key
// Usage: resp := redis.Master().HGetAll("h")
// Redis: HGETALL
func (o *RedisOp) HGetAll(key interface{}) *RedisResponse {
	return o._Do("HGETALL", key)
}

// HLen returns the number of fields contained in the hash.
// Params: key - hash key
// Usage: n := redis.Master().HLen("h").GetInt64()
// Redis: HLEN
func (o *RedisOp) HLen(key interface{}) *RedisResponse {
	return o._Do("HLEN", key)
}

// HKeys returns all field names in a hash.
// Params: key - hash key
// Usage: resp := redis.Master().HKeys("h")
// Redis: HKEYS
func (o *RedisOp) HKeys(key interface{}) *RedisResponse {
	return o._Do("HKEYS", key)
}

// HIncrBy increments the integer value of a hash field by the given number.
// Params: key - hash key; field - field name; val - increment amount
// Usage: resp := redis.Master().HIncrBy("h", "f", 1)
// Redis: HINCRBY
func (o *RedisOp) HIncrBy(key interface{}, field interface{}, val int64) *RedisResponse {
	return o._Do("HINCRBY", key, field, val)
}

// HVals returns all values in a hash.
// Params: key - hash key
// Usage: resp := redis.Master().HVals("h")
// Redis: HVALS
func (o *RedisOp) HVals(key interface{}) *RedisResponse {
	return o._Do("HVALS", key)
}

// HScan iterates over hash fields and values using a cursor.
// Params: key - hash key; cursor - scan cursor; match - pattern filter; count - batch size hint (>0)
// Usage: resp := redis.Master().HScan("h", 0, "f*", 10)
// Redis: HSCAN
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
// Params: key - target key
// Usage: resp := redis.Master().Incr("k")
// Redis: INCR
func (o *RedisOp) Incr(key interface{}) *RedisResponse {
	return o._Do("INCR", key)
}

// IncrBy increments the integer value of a key by the given amount.
// Params: key - target key; val - increment amount
// Usage: resp := redis.Master().IncrBy("k", 5)
// Redis: INCRBY
func (o *RedisOp) IncrBy(key interface{}, val int64) *RedisResponse {
	return o._Do("INCRBY", key, val)
}

// Publish posts a message to the given channel.
// Params: key - channel name; val - message payload
// Usage: resp := redis.Master().Publish("channel", "hello")
// Redis: PUBLISH
func (o *RedisOp) Publish(key interface{}, val interface{}) *RedisResponse {
	return o._Do("PUBLISH", key, val)
}

// String commands (supplementary)
// Append appends a value to a key's string value.
// Params: key - target key; val - value to append
// Usage: resp := redis.Master().Append("k", "tail")
// Redis: APPEND
func (o *RedisOp) Append(key interface{}, val interface{}) *RedisResponse {
	return o._Do("APPEND", key, val)
}

// StrLen returns the length of the string value stored at key.
// Params: key - target key
// Usage: n := redis.Master().StrLen("k").GetInt64()
// Redis: STRLEN
func (o *RedisOp) StrLen(key interface{}) *RedisResponse {
	return o._Do("STRLEN", key)
}

// GetRange gets a substring of the string stored at key.
// Params: key - target key; start - start index; end - end index (inclusive)
// Usage: resp := redis.Master().GetRange("k", 0, 10)
// Redis: GETRANGE
func (o *RedisOp) GetRange(key interface{}, start, end int64) *RedisResponse {
	return o._Do("GETRANGE", key, start, end)
}

// SetRange overwrites part of the string stored at key starting at the specified offset.
// Params: key - target key; offset - byte offset; val - string to overwrite
// Usage: resp := redis.Master().SetRange("k", 3, "abc")
// Redis: SETRANGE
func (o *RedisOp) SetRange(key interface{}, offset int64, val interface{}) *RedisResponse {
	return o._Do("SETRANGE", key, offset, val)
}

// Key commands (supplementary)
// Copy copies the value stored at the source key to the destination key.
// Params: src - source key; dst - destination key
// Usage: resp := redis.Master().Copy("a", "b")
// Redis: COPY
func (o *RedisOp) Copy(src, dst interface{}) *RedisResponse {
	return o._Do("COPY", src, dst)
}

// Decr decrements the integer value of a key by one.
// Params: key - target key
// Usage: resp := redis.Master().Decr("k")
// Redis: DECR
func (o *RedisOp) Decr(key interface{}) *RedisResponse {
	return o._Do("DECR", key)
}

// DecrBy decrements the integer value of a key by the given number.
// Params: key - target key; val - decrement amount
// Usage: resp := redis.Master().DecrBy("k", 2)
// Redis: DECRBY
func (o *RedisOp) DecrBy(key interface{}, val int64) *RedisResponse {
	return o._Do("DECRBY", key, val)
}

// Dump serializes the value stored at key in a Redis-specific format.
// Params: key - target key
// Usage: resp := redis.Master().Dump("k")
// Redis: DUMP
func (o *RedisOp) Dump(key interface{}) *RedisResponse {
	return o._Do("DUMP", key)
}

// TTL returns the remaining time to live of a key in seconds.
// Params: key - target key
// Usage: secs := redis.Master().TTL("k").GetInt64()
// Redis: TTL
func (o *RedisOp) TTL(key interface{}) *RedisResponse {
	return o._Do("TTL", key)
}

// PTTL returns the remaining time to live of a key in milliseconds.
// Params: key - target key
// Usage: ms := redis.Master().PTTL("k").GetInt64()
// Redis: PTTL
func (o *RedisOp) PTTL(key interface{}) *RedisResponse {
	return o._Do("PTTL", key)
}

// Type returns the string representation of the key's data type.
// Params: key - target key
// Usage: resp := redis.Master().Type("k")
// Redis: TYPE
func (o *RedisOp) Type(key interface{}) *RedisResponse {
	return o._Do("TYPE", key)
}

// RandomKey returns a random key from the current database.
// Params: none
// Usage: resp := redis.Master().RandomKey()
// Redis: RANDOMKEY
func (o *RedisOp) RandomKey() *RedisResponse {
	return o._Do("RANDOMKEY")
}

// Rename renames a key.
// Params: oldKey - current key name; newKey - new key name
// Usage: resp := redis.Master().Rename("old", "new")
// Redis: RENAME
func (o *RedisOp) Rename(oldKey, newKey interface{}) *RedisResponse {
	return o._Do("RENAME", oldKey, newKey)
}

// RenameNX renames a key, only if the new key does not exist.
// Params: oldKey - current key; newKey - new key
// Usage: resp := redis.Master().RenameNX("old", "new")
// Redis: RENAMENX
func (o *RedisOp) RenameNX(oldKey, newKey interface{}) *RedisResponse {
	return o._Do("RENAMENX", oldKey, newKey)
}

// Touch updates the last access time of one or more keys.
// Params: key - one or more keys
// Usage: resp := redis.Master().Touch("k1", "k2")
// Redis: TOUCH
func (o *RedisOp) Touch(key ...interface{}) *RedisResponse {
	return o._Do("TOUCH", key...)
}

// Unlink removes one or more keys asynchronously.
// Params: key - one or more keys
// Usage: resp := redis.Master().Unlink("k1", "k2")
// Redis: UNLINK
func (o *RedisOp) Unlink(key ...interface{}) *RedisResponse {
	return o._Do("UNLINK", key...)
}

// Persist removes the existing timeout on a key.
// Params: key - target key
// Usage: resp := redis.Master().Persist("k")
// Redis: PERSIST
func (o *RedisOp) Persist(key interface{}) *RedisResponse {
	return o._Do("PERSIST", key)
}

// List commands
// LIndex returns the element at index in the list stored at key.
// Params: key - list key; index - zero-based index
// Usage: resp := redis.Master().LIndex("k", 0)
// Redis: LINDEX
func (o *RedisOp) LIndex(key interface{}, index int64) *RedisResponse {
	return o._Do("LINDEX", key, index)
}

// LInsert inserts element before or after the pivot element in the list stored at key.
// Params: key - list key; where - BEFORE or AFTER; pivot - pivot element; element - element to insert
// Usage: resp := redis.Master().LInsert("k", "BEFORE", "p", "x")
// Redis: LINSERT
func (o *RedisOp) LInsert(key interface{}, where string, pivot, element interface{}) *RedisResponse {
	return o._Do("LINSERT", key, where, pivot, element)
}

// LLen returns the length of the list stored at key.
// Params: key - list key
// Usage: n := redis.Master().LLen("k").GetInt64()
// Redis: LLEN
func (o *RedisOp) LLen(key interface{}) *RedisResponse {
	return o._Do("LLEN", key)
}

// LMove atomically returns and removes the element from the source list and pushes it to the destination list.
// Params: source - source key; destination - destination key; srcWhere - LEFT or RIGHT; dstWhere - LEFT or RIGHT
// Usage: resp := redis.Master().LMove("src", "dst", "LEFT", "RIGHT")
// Redis: LMOVE
func (o *RedisOp) LMove(source, destination interface{}, srcWhere, dstWhere string) *RedisResponse {
	return o._Do("LMOVE", source, destination, srcWhere, dstWhere)
}

// LMPop pops one or multiple elements from the first non-empty list key among the given keys.
// Params: count - number of keys provided (numkeys), not the COUNT option; where - LEFT or RIGHT; key - one or more list keys
// Usage: resp := redis.Master().LMPop(2, "LEFT", "k1", "k2")
// Redis: LMPOP
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
// Params: key - list key
// Usage: resp := redis.Master().LPop("k")
// Redis: LPOP
func (o *RedisOp) LPop(key interface{}) *RedisResponse {
	return o._Do("LPOP", key)
}

// LPos returns the index of the first occurrence of element in the list stored at key.
// Params: key - list key; element - element to search
// Usage: idx := redis.Master().LPos("k", "x").GetInt64()
// Redis: LPOS
func (o *RedisOp) LPos(key, element interface{}) *RedisResponse {
	return o._Do("LPOS", key, element)
}

// LPush inserts all the specified values at the head of the list stored at key.
// Params: key - list key; val - one or more values
// Usage: resp := redis.Master().LPush("k", "a", "b")
// Redis: LPUSH
func (o *RedisOp) LPush(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return o._Do("LPUSH", args...)
}

// LPushX inserts values at the head of the list stored at key, only if the list exists.
// Params: key - list key; val - one or more values
// Usage: resp := redis.Master().LPushX("k", "a")
// Redis: LPUSHX
func (o *RedisOp) LPushX(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return o._Do("LPUSHX", args...)
}

// LRange returns the specified elements of the list stored at key.
// Params: key - list key; start - start index; stop - end index (inclusive)
// Usage: resp := redis.Master().LRange("k", 0, -1)
// Redis: LRANGE
func (o *RedisOp) LRange(key interface{}, start, stop int64) *RedisResponse {
	return o._Do("LRANGE", key, start, stop)
}

// LRem removes the first count occurrences of element from the list stored at key.
// Params: key - list key; count - number of occurrences to remove; element - value to remove
// Usage: resp := redis.Master().LRem("k", 1, "x")
// Redis: LREM
func (o *RedisOp) LRem(key interface{}, count int64, element interface{}) *RedisResponse {
	return o._Do("LREM", key, count, element)
}

// LSet sets the list element at index to element.
// Params: key - list key; index - element index; element - new value
// Usage: resp := redis.Master().LSet("k", 0, "x")
// Redis: LSET
func (o *RedisOp) LSet(key interface{}, index int64, element interface{}) *RedisResponse {
	return o._Do("LSET", key, index, element)
}

// LTrim trims an existing list so that it will contain only the specified range of elements.
// Params: key - list key; start - start index; stop - end index (inclusive)
// Usage: resp := redis.Master().LTrim("k", 0, 99)
// Redis: LTRIM
func (o *RedisOp) LTrim(key interface{}, start, stop int64) *RedisResponse {
	return o._Do("LTRIM", key, start, stop)
}

// RPop removes and returns the last element of the list stored at key.
// Params: key - list key
// Usage: resp := redis.Master().RPop("k")
// Redis: RPOP
func (o *RedisOp) RPop(key interface{}) *RedisResponse {
	return o._Do("RPOP", key)
}

// RPopLPush removes the last element in the source list and pushes it to the head of the destination list.
// Params: source - source key; destination - destination key
// Usage: resp := redis.Master().RPopLPush("src", "dst")
// Redis: RPOPLPUSH
func (o *RedisOp) RPopLPush(source, destination interface{}) *RedisResponse {
	return o._Do("RPOPLPUSH", source, destination)
}

// RPush inserts all the specified values at the tail of the list stored at key.
// Params: key - list key; val - one or more values
// Usage: resp := redis.Master().RPush("k", "a", "b")
// Redis: RPUSH
func (o *RedisOp) RPush(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return o._Do("RPUSH", args...)
}

// RPushX inserts values at the tail of the list stored at key, only if the list exists.
// Params: key - list key; val - one or more values
// Usage: resp := redis.Master().RPushX("k", "a")
// Redis: RPUSHX
func (o *RedisOp) RPushX(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return o._Do("RPUSHX", args...)
}

// Set commands
// SAdd adds one or more members to a set.
// Params: key - set key; member - one or more members
// Usage: resp := redis.Master().SAdd("k", "a", "b")
// Redis: SADD
func (o *RedisOp) SAdd(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return o._Do("SADD", args...)
}

// SCard returns the set cardinality (number of elements) of the set stored at key.
// Params: key - set key
// Usage: n := redis.Master().SCard("k").GetInt64()
// Redis: SCARD
func (o *RedisOp) SCard(key interface{}) *RedisResponse {
	return o._Do("SCARD", key)
}

// SDiff returns the members of the set resulting from the difference between the first set and all the successive sets.
// Params: key - one or more set keys
// Usage: resp := redis.Master().SDiff("k1", "k2")
// Redis: SDIFF
func (o *RedisOp) SDiff(key ...interface{}) *RedisResponse {
	return o._Do("SDIFF", key...)
}

// SDiffStore stores the result of SDIFF in the destination key.
// Params: destination - destination key; key - one or more set keys
// Usage: resp := redis.Master().SDiffStore("dest", "k1", "k2")
// Redis: SDIFFSTORE
func (o *RedisOp) SDiffStore(destination interface{}, key ...interface{}) *RedisResponse {
	args := []interface{}{destination}
	args = append(args, key...)
	return o._Do("SDIFFSTORE", args...)
}

// SInter returns the members of the set resulting from the intersection of all the given sets.
// Params: key - one or more set keys
// Usage: resp := redis.Master().SInter("k1", "k2")
// Redis: SINTER
func (o *RedisOp) SInter(key ...interface{}) *RedisResponse {
	return o._Do("SINTER", key...)
}

// SInterCard returns the number of elements in the intersection of all the given sets.
// Params: key - one or more set keys
// Usage: n := redis.Master().SInterCard("k1", "k2").GetInt64()
// Redis: SINTERCARD
func (o *RedisOp) SInterCard(key ...interface{}) *RedisResponse {
	// SINTERCARD requires: numkeys key [key ...] [LIMIT num]
	numkeys := int64(len(key))
	args := []interface{}{numkeys}
	args = append(args, key...)
	return o._Do("SINTERCARD", args...)
}

// SInterStore stores the result of SINTER in the destination key.
// Params: destination - destination key; key - one or more set keys
// Usage: resp := redis.Master().SInterStore("dest", "k1", "k2")
// Redis: SINTERSTORE
func (o *RedisOp) SInterStore(destination interface{}, key ...interface{}) *RedisResponse {
	args := []interface{}{destination}
	args = append(args, key...)
	return o._Do("SINTERSTORE", args...)
}

// SIsMember returns if member is a member of the set stored at key.
// Params: key - set key; member - value to check
// Usage: ok := redis.Master().SIsMember("k", "a").GetInt64() == 1
// Redis: SISMEMBER
func (o *RedisOp) SIsMember(key, member interface{}) *RedisResponse {
	return o._Do("SISMEMBER", key, member)
}

// SMembers returns all the members of the set value stored at key.
// Params: key - set key
// Usage: resp := redis.Master().SMembers("k")
// Redis: SMEMBERS
func (o *RedisOp) SMembers(key interface{}) *RedisResponse {
	return o._Do("SMEMBERS", key)
}

// SMIsMember returns whether each member is a member of the set stored at key.
// Params: key - set key; member - one or more members to check
// Usage: resp := redis.Master().SMIsMember("k", "a", "b")
// Redis: SMISMEMBER
func (o *RedisOp) SMIsMember(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return o._Do("SMISMEMBER", args...)
}

// SMove moves member from the source set to the destination set.
// Params: source - source key; destination - destination key; member - value to move
// Usage: resp := redis.Master().SMove("s1", "s2", "a")
// Redis: SMOVE
func (o *RedisOp) SMove(source, destination, member interface{}) *RedisResponse {
	return o._Do("SMOVE", source, destination, member)
}

// SPop removes and returns one or more random members from the set stored at key.
// Params: key - set key
// Usage: resp := redis.Master().SPop("k")
// Redis: SPOP
func (o *RedisOp) SPop(key interface{}) *RedisResponse {
	return o._Do("SPOP", key)
}

// SRandMember returns one or more random members from the set value stored at key without removing them.
// Params: key - set key
// Usage: resp := redis.Master().SRandMember("k")
// Redis: SRANDMEMBER
func (o *RedisOp) SRandMember(key interface{}) *RedisResponse {
	return o._Do("SRANDMEMBER", key)
}

// SRem removes one or more members from a set.
// Params: key - set key; member - one or more members to remove
// Usage: resp := redis.Master().SRem("k", "a", "b")
// Redis: SREM
func (o *RedisOp) SRem(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return o._Do("SREM", args...)
}

// SScan iterates elements of the set stored at key.
// Params: key - set key; cursor - iteration cursor; match - pattern to filter members; count - hint for number of returned elements
// Usage: resp := redis.Master().SScan("k", 0, "user:*", 100)
// Redis: SSCAN
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
// Params: key - one or more set keys
// Usage: resp := redis.Master().SUnion("k1", "k2")
// Redis: SUNION
func (o *RedisOp) SUnion(key ...interface{}) *RedisResponse {
	return o._Do("SUNION", key...)
}

// SUnionStore stores the result of SUNION in the destination key.
// Params: destination - destination key; key - one or more set keys
// Usage: resp := redis.Master().SUnionStore("dest", "k1", "k2")
// Redis: SUNIONSTORE
func (o *RedisOp) SUnionStore(destination interface{}, key ...interface{}) *RedisResponse {
	args := []interface{}{destination}
	args = append(args, key...)
	return o._Do("SUNIONSTORE", args...)
}

// FlushDB removes all keys from the current database.
// Params: none
// Usage: resp := redis.Master().FlushDB()
// Redis: FLUSHDB
func (o *RedisOp) FlushDB() *RedisResponse {
	return o._Do("FLUSHDB")
}

// FlushAll removes all keys from all databases.
// Params: none
// Usage: resp := redis.Master().FlushAll()
// Redis: FLUSHALL
func (o *RedisOp) FlushAll() *RedisResponse {
	return o._Do("FLUSHALL")
}

// Scan iterates the set of keys in the current database.
// Params: cursor - iteration cursor; match - pattern to filter keys; count - hint for the number of returned elements
// Usage: resp := redis.Master().Scan(0, "user:*", 100)
// Redis: SCAN
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

// Ping checks if the server is alive and responding.
// Params: none
// Usage: resp := redis.Master().Ping()
// Redis: PING
func (o *RedisOp) Ping() *RedisResponse {
	return o._Do("PING")
}

// Close closes the underlying connection pool if present.
// This is not a Redis command; it releases local resources.
// Safe to call multiple times.
func (o *RedisOp) Close() error {
	if o.pool != nil {
		return o.pool.Close()
	}

	return nil
}

// Script commands
// Eval executes a Lua script on the server.
// Params: script - the Lua script to execute; keys - slice of keys used by script; args - slice of arguments for script
// Usage: resp := redis.Master().Eval("return KEYS[1]", []interface{}{"mykey"}, []interface{}{"arg1", "arg2"})
// Redis: EVAL
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
	}

	return nil
}

// GetSlice converts an array reply into a slice of RedisResponseEntity for typed access.
// Returns an empty slice if the reply is not an array.
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
// Params: profileName - the name of the redis secret profile to load
// Usage: r := datastore.NewRedis("default"); op := r.Master(); op.Ping()
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

// Sorted Set commands
// ZAdd adds all the specified members with the specified scores to the sorted set stored at key.
// Params: key - sorted set key; score - score for member; member - member value; pairs - optional additional (score, member) pairs
// Usage: resp := redis.Master().ZAdd("k", 1.0, "m1", 2.0, "m2")
// Redis: ZADD
func (o *RedisOp) ZAdd(key interface{}, score float64, member interface{}, pairs ...interface{}) *RedisResponse {
	args := []interface{}{key, score, member}
	args = append(args, pairs...)
	return o._Do("ZADD", args...)
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
// Params: key - sorted set key
// Usage: n := redis.Master().ZCard("k").GetInt64()
// Redis: ZCARD
func (o *RedisOp) ZCard(key interface{}) *RedisResponse {
	return o._Do("ZCARD", key)
}

// ZCount returns the number of elements in the sorted set with a score between min and max.
// Params: key - sorted set key; min - min score (inclusive); max - max score (inclusive)
// Usage: n := redis.Master().ZCount("k", "-inf", "+inf").GetInt64()
// Redis: ZCOUNT
func (o *RedisOp) ZCount(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZCOUNT", key, min, max)
}

// ZDiff returns the members of the sorted set resulting from the difference between the first set and all the successive sets.
// Params: key - one or more sorted set keys
// Usage: resp := redis.Master().ZDiff("k1", "k2")
// Redis: ZDIFF
func (o *RedisOp) ZDiff(key ...interface{}) *RedisResponse {
	// ZDIFF requires: numkeys key [key ...] [WITHSCORES]
	numkeys := int64(len(key))
	args := []interface{}{numkeys}
	args = append(args, key...)
	return o._Do("ZDIFF", args...)
}

// ZDiffStore stores the result of ZDIFF in the destination key.
// Params: destination - destination key; key - one or more sorted set keys
// Usage: resp := redis.Master().ZDiffStore("dest", "k1", "k2")
// Redis: ZDIFFSTORE
func (o *RedisOp) ZDiffStore(destination interface{}, key ...interface{}) *RedisResponse {
	// ZDIFFSTORE requires: destination numkeys key [key ...]
	numkeys := int64(len(key))
	args := []interface{}{destination, numkeys}
	args = append(args, key...)
	return o._Do("ZDIFFSTORE", args...)
}

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
// Params: key - sorted set key; increment - score delta; member - member value
// Usage: resp := redis.Master().ZIncrBy("k", 1.5, "m1")
// Redis: ZINCRBY
func (o *RedisOp) ZIncrBy(key interface{}, increment float64, member interface{}) *RedisResponse {
	return o._Do("ZINCRBY", key, increment, member)
}

// ZInter returns the members of the sorted set resulting from the intersection of all the given sets.
// Params: key - one or more sorted set keys
// Usage: resp := redis.Master().ZInter("k1", "k2")
// Redis: ZINTER
func (o *RedisOp) ZInter(key ...interface{}) *RedisResponse {
	// ZINTER requires: numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES]
	numkeys := int64(len(key))
	args := []interface{}{numkeys}
	args = append(args, key...)
	return o._Do("ZINTER", args...)
}

// ZInterCard returns the number of elements in the intersection of all the given sorted sets.
// Params: key - one or more sorted set keys
// Usage: n := redis.Master().ZInterCard("k1", "k2").GetInt64()
// Redis: ZINTERCARD
func (o *RedisOp) ZInterCard(key ...interface{}) *RedisResponse {
	// ZINTERCARD requires: numkeys key [key ...] [LIMIT num]
	numkeys := int64(len(key))
	args := []interface{}{numkeys}
	args = append(args, key...)
	return o._Do("ZINTERCARD", args...)
}

// ZInterStore stores the result of ZINTER in the destination key.
// Params: destination - destination key; key - one or more sorted set keys
// Usage: resp := redis.Master().ZInterStore("dest", "k1", "k2")
// Redis: ZINTERSTORE
func (o *RedisOp) ZInterStore(destination interface{}, key ...interface{}) *RedisResponse {
	// ZINTERSTORE requires: destination numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]
	numkeys := int64(len(key))
	args := []interface{}{destination, numkeys}
	args = append(args, key...)
	return o._Do("ZINTERSTORE", args...)
}

// ZLexCount returns the number of elements in the sorted set with a value between min and max, lexicographically.
// Params: key - sorted set key; min - min lex bound; max - max lex bound
// Usage: n := redis.Master().ZLexCount("k", "[a", "[z").GetInt64()
// Redis: ZLEXCOUNT
func (o *RedisOp) ZLexCount(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZLEXCOUNT", key, min, max)
}

// ZMPop pops one or more elements from the first non-empty sorted set among the given keys.
// Params: count - number of keys to check; where - MIN or MAX; key - one or more sorted set keys
// Usage: resp := redis.Master().ZMPop(2, "MIN", "k1", "k2")
// Redis: ZMPOP
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
// Params: key - sorted set key; member - one or more member values
// Usage: resp := redis.Master().ZMScore("k", "m1", "m2")
// Redis: ZMSCORE
func (o *RedisOp) ZMScore(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return o._Do("ZMSCORE", args...)
}

// ZPopMax removes and returns the member with the highest score from the sorted set.
// Params: key - sorted set key
// Usage: resp := redis.Master().ZPopMax("k")
// Redis: ZPOPMAX
func (o *RedisOp) ZPopMax(key interface{}) *RedisResponse {
	return o._Do("ZPOPMAX", key)
}

// ZPopMin removes and returns the member with the lowest score from the sorted set.
// Params: key - sorted set key
// Usage: resp := redis.Master().ZPopMin("k")
// Redis: ZPOPMIN
func (o *RedisOp) ZPopMin(key interface{}) *RedisResponse {
	return o._Do("ZPOPMIN", key)
}

// ZRandMember returns a random member from the sorted set without removing it.
// Params: key - sorted set key
// Usage: resp := redis.Master().ZRandMember("k")
// Redis: ZRANDMEMBER
func (o *RedisOp) ZRandMember(key interface{}) *RedisResponse {
	return o._Do("ZRANDMEMBER", key)
}

// ZRange returns the specified range of members in the sorted set stored at key by index.
// Params: key - sorted set key; start - start index; stop - end index (inclusive)
// Usage: resp := redis.Master().ZRange("k", 0, -1)
// Redis: ZRANGE
func (o *RedisOp) ZRange(key interface{}, start, stop int64) *RedisResponse {
	return o._Do("ZRANGE", key, start, stop)
}

// ZRangeByLex returns all the elements in the sorted set with a value between min and max, lexicographically.
// Params: key - sorted set key; min - min lex bound; max - max lex bound
// Usage: resp := redis.Master().ZRangeByLex("k", "[a", "[z")
// Redis: ZRANGEBYLEX
func (o *RedisOp) ZRangeByLex(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZRANGEBYLEX", key, min, max)
}

// ZRangeByScore returns all the elements in the sorted set with a score between min and max.
// Params: key - sorted set key; min - min score; max - max score
// Usage: resp := redis.Master().ZRangeByScore("k", "-inf", "+inf")
// Redis: ZRANGEBYSCORE
func (o *RedisOp) ZRangeByScore(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZRANGEBYSCORE", key, min, max)
}

// ZRangeStore stores a range of members selected by index from the source sorted set to the destination key.
// Params: dst - destination key; src - source key; min - start index; max - end index (inclusive)
// Usage: resp := redis.Master().ZRangeStore("dst", "src", 0, -1)
// Redis: ZRANGESTORE
func (o *RedisOp) ZRangeStore(dst interface{}, src interface{}, min, max int64) *RedisResponse {
	return o._Do("ZRANGESTORE", dst, src, min, max)
}

// ZRevRange returns the specified range of members in the sorted set stored at key by index, with scores ordered from high to low.
// Params: key - sorted set key; start - start index; stop - end index (inclusive)
// Usage: resp := redis.Master().ZRevRange("k", 0, -1)
// Redis: ZREVRANGE
func (o *RedisOp) ZRevRange(key interface{}, start, stop int64) *RedisResponse {
	return o._Do("ZREVRANGE", key, start, stop)
}

// ZRevRangeByLex returns all the elements in the sorted set with a value between max and min, lexicographically, in reverse order.
// Params: key - sorted set key; max - max lex bound; min - min lex bound
// Usage: resp := redis.Master().ZRevRangeByLex("k", "[z", "[a")
// Redis: ZREVRANGEBYLEX
func (o *RedisOp) ZRevRangeByLex(key interface{}, max, min string) *RedisResponse {
	return o._Do("ZREVRANGEBYLEX", key, max, min)
}

// ZRevRangeByScore returns all the elements in the sorted set with a score between max and min in reverse order.
// Params: key - sorted set key; max - max score; min - min score
// Usage: resp := redis.Master().ZRevRangeByScore("k", "+inf", "-inf")
// Redis: ZREVRANGEBYSCORE
func (o *RedisOp) ZRevRangeByScore(key interface{}, max, min string) *RedisResponse {
	return o._Do("ZREVRANGEBYSCORE", key, max, min)
}

// ZRank returns the rank of member in the sorted set stored at key, with scores ordered from low to high.
// Params: key - sorted set key; member - member value
// Usage: idx := redis.Master().ZRank("k", "m").GetInt64()
// Redis: ZRANK
func (o *RedisOp) ZRank(key, member interface{}) *RedisResponse {
	return o._Do("ZRANK", key, member)
}

// ZRem removes one or more members from the sorted set stored at key.
// Params: key - sorted set key; member - one or more members to remove
// Usage: resp := redis.Master().ZRem("k", "m1", "m2")
// Redis: ZREM
func (o *RedisOp) ZRem(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return o._Do("ZREM", args...)
}

// ZRemRangeByLex removes all members in the sorted set between the lexicographical range specified by min and max.
// Params: key - sorted set key; min - min lex bound; max - max lex bound
// Usage: resp := redis.Master().ZRemRangeByLex("k", "[a", "[z")
// Redis: ZREMRANGEBYLEX
func (o *RedisOp) ZRemRangeByLex(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZREMRANGEBYLEX", key, min, max)
}

// ZRemRangeByRank removes all members in the sorted set stored at key within the given index range.
// Params: key - sorted set key; start - start index; stop - end index (inclusive)
// Usage: resp := redis.Master().ZRemRangeByRank("k", 0, 10)
// Redis: ZREMRANGEBYRANK
func (o *RedisOp) ZRemRangeByRank(key interface{}, start, stop int64) *RedisResponse {
	return o._Do("ZREMRANGEBYRANK", key, start, stop)
}

// ZRemRangeByScore removes all members in the sorted set stored at key with scores between min and max.
// Params: key - sorted set key; min - min score; max - max score
// Usage: resp := redis.Master().ZRemRangeByScore("k", "-inf", "+inf")
// Redis: ZREMRANGEBYSCORE
func (o *RedisOp) ZRemRangeByScore(key interface{}, min, max string) *RedisResponse {
	return o._Do("ZREMRANGEBYSCORE", key, min, max)
}

// ZRevRank returns the rank of member in the sorted set, with scores ordered from high to low.
// Params: key - sorted set key; member - member value
// Usage: idx := redis.Master().ZRevRank("k", "m").GetInt64()
// Redis: ZREVRANK
func (o *RedisOp) ZRevRank(key, member interface{}) *RedisResponse {
	return o._Do("ZREVRANK", key, member)
}

// ZScan incrementally iterates members of the sorted set stored at key.
// Params: key - sorted set key; cursor - iteration cursor; match - pattern to filter members; count - hint for number of returned elements
// Usage: resp := redis.Master().ZScan("k", 0, "user:*", 100)
// Redis: ZSCAN
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
// Params: key - sorted set key; member - member value
// Usage: resp := redis.Master().ZScore("k", "m")
// Redis: ZSCORE
func (o *RedisOp) ZScore(key, member interface{}) *RedisResponse {
	return o._Do("ZSCORE", key, member)
}

// ZUnion returns the members of the sorted set resulting from the union of all the given sorted sets.
// Params: key - one or more sorted set keys
// Usage: resp := redis.Master().ZUnion("k1", "k2")
// Redis: ZUNION
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
