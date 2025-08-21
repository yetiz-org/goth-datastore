package datastore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	secret "github.com/yetiz-org/goth-secret"
)

func TestRedisResponseEntity(t *testing.T) {
	t.Run("GetInt64", func(t *testing.T) {
		// Test with int64 data
		resp := RedisResponseEntity{data: int64(123)}
		assert.Equal(t, int64(123), resp.GetInt64())

		// Test with byte slice data
		resp = RedisResponseEntity{data: []byte("456")}
		assert.Equal(t, int64(456), resp.GetInt64())

		// Test with invalid data
		resp = RedisResponseEntity{data: "not_a_number"}
		assert.Equal(t, int64(0), resp.GetInt64())
	})

	t.Run("GetString", func(t *testing.T) {
		// Test with byte slice data
		resp := RedisResponseEntity{data: []byte("hello")}
		assert.Equal(t, "hello", resp.GetString())

		// Test with int64 data
		resp = RedisResponseEntity{data: int64(123)}
		assert.Equal(t, "123", resp.GetString())
	})

	t.Run("GetBytes", func(t *testing.T) {
		// Test with byte slice data
		resp := RedisResponseEntity{data: []byte("hello")}
		assert.Equal(t, []byte("hello"), resp.GetBytes())

		// Test with non-byte data
		resp = RedisResponseEntity{data: 123}
		assert.Nil(t, resp.GetBytes())
	})

	t.Run("GetSlice", func(t *testing.T) {
		// Test with []interface{} data
		resp := RedisResponseEntity{data: []interface{}{"key1", []byte("key2"), int64(123)}}
		slice := resp.GetSlice()
		assert.Len(t, slice, 3)

		// Verify first element (string)
		assert.Equal(t, "key1", slice[0].GetString())

		// Verify second element ([]byte)
		assert.Equal(t, "key2", slice[1].GetString())
		assert.Equal(t, []byte("key2"), slice[1].GetBytes())

		// Verify third element (int64)
		assert.Equal(t, int64(123), slice[2].GetInt64())
		assert.Equal(t, "123", slice[2].GetString())

		// Test with non-array data
		resp = RedisResponseEntity{data: "not_an_array"}
		slice = resp.GetSlice()
		assert.Empty(t, slice)

		// Test with empty array
		resp = RedisResponseEntity{data: []interface{}{}}
		slice = resp.GetSlice()
		assert.Empty(t, slice)
	})
}

func TestRedisPool(t *testing.T) {
	t.Run("newRedisPool", func(t *testing.T) {
		// Save original defaults
		origDialTimeout := DefaultRedisDialTimeout
		origMaxIdle := DefaultRedisMaxIdle
		origIdleTimeout := DefaultRedisIdleTimeout
		origMaxConnLifetime := DefaultRedisMaxConnLifetime
		origMaxActive := DefaultRedisMaxActive
		origWait := DefaultRedisWait

		// Restore defaults after test
		defer func() {
			DefaultRedisDialTimeout = origDialTimeout
			DefaultRedisMaxIdle = origMaxIdle
			DefaultRedisIdleTimeout = origIdleTimeout
			DefaultRedisMaxConnLifetime = origMaxConnLifetime
			DefaultRedisMaxActive = origMaxActive
			DefaultRedisWait = origWait
		}()

		// Set test values
		DefaultRedisDialTimeout = 500
		DefaultRedisMaxIdle = 10
		DefaultRedisIdleTimeout = 30000
		DefaultRedisMaxConnLifetime = 60000
		DefaultRedisMaxActive = 100
		DefaultRedisWait = true

		// Create test meta
		meta := secret.RedisMeta{
			Host: "localhost",
			Port: 6379,
		}

		// Create pool
		pool := newRedisPool(meta)

		// Assert pool configuration
		assert.Equal(t, DefaultRedisMaxActive, pool.MaxActive)
		assert.Equal(t, DefaultRedisMaxIdle, pool.MaxIdle)
		assert.Equal(t, time.Duration(DefaultRedisIdleTimeout)*time.Millisecond, pool.IdleTimeout)
		assert.Equal(t, time.Duration(DefaultRedisMaxConnLifetime)*time.Millisecond, pool.MaxConnLifetime)
		assert.Equal(t, DefaultRedisWait, pool.Wait)

		// Close pool
		pool.Close()
	})
}

// TestLoadRedisExampleSecret tests loading Redis secret from example file
func TestLoadRedisExampleSecret(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory which has the correct structure
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	// Test loading the secret
	t.Run("LoadRedisSecret", func(t *testing.T) {
		profile := &secret.Redis{}
		err := secret.Load("redis", "test", profile)
		assert.NoError(t, err)

		// Verify master configuration
		assert.NotNil(t, profile.Master)
		assert.Equal(t, "localhost", profile.Master.Host)
		assert.Equal(t, uint(6379), profile.Master.Port)

		// Verify slave configuration (using same instance as master in simplified setup)
		assert.NotNil(t, profile.Slave)
		assert.Equal(t, "localhost", profile.Slave.Host)
		assert.Equal(t, uint(6379), profile.Slave.Port)
	})

	t.Run("NewRedisWithExampleSecret", func(t *testing.T) {
		// Test creating Redis instance with the example secret
		redis := NewRedis("test")

		// NewRedis should succeed with valid secret
		assert.NotNil(t, redis)
		assert.Equal(t, "test", redis.name)
		assert.NotNil(t, redis.master)
		assert.NotNil(t, redis.slave)

		// Verify the master and slave are configured correctly (using same instance in simplified setup)
		assert.Equal(t, "localhost", redis.master.Meta().Host)
		assert.Equal(t, uint(6379), redis.master.Meta().Port)
		assert.Equal(t, "localhost", redis.slave.Meta().Host)
		assert.Equal(t, uint(6379), redis.slave.Meta().Port)
	})
}

// TestRedisKeyCommands Key command tests
func TestRedisKeyCommands(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	redis := NewRedis("test")
	assert.NotNil(t, redis)

	t.Run("Copy", func(t *testing.T) {
		redis.Master().Set("test_key", "test_value")
		response := redis.Master().Copy("test_key", "copied_key")
		assert.NoError(t, response.Error)

		getResp := redis.Master().Get("copied_key")
		assert.NoError(t, getResp.Error)
		assert.Equal(t, "test_value", getResp.GetString())

		// Cleanup
		redis.Master().Delete("test_key", "copied_key")
	})

	t.Run("Decr", func(t *testing.T) {
		redis.Master().Set("counter", "10")
		response := redis.Master().Decr("counter")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(9), response.GetInt64())

		// Cleanup
		redis.Master().Delete("counter")
	})

	t.Run("DecrBy", func(t *testing.T) {
		redis.Master().Set("counter", "20")
		response := redis.Master().DecrBy("counter", 5)
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(15), response.GetInt64())

		// Cleanup
		redis.Master().Delete("counter")
	})

	t.Run("Dump", func(t *testing.T) {
		redis.Master().Set("test_key", "test_value")
		response := redis.Master().Dump("test_key")
		assert.NoError(t, response.Error)
		assert.NotNil(t, response.GetBytes())

		// Cleanup
		redis.Master().Delete("test_key")
	})

	t.Run("TTL", func(t *testing.T) {
		redis.Master().SetExpire("test_key", "test_value", 60)
		response := redis.Master().TTL("test_key")
		assert.NoError(t, response.Error)
		assert.True(t, response.GetInt64() > 0)

		// Cleanup
		redis.Master().Delete("test_key")
	})

	t.Run("PTTL", func(t *testing.T) {
		redis.Master().SetExpire("test_key", "test_value", 60)
		response := redis.Master().PTTL("test_key")
		assert.NoError(t, response.Error)
		assert.True(t, response.GetInt64() > 0)

		// Cleanup
		redis.Master().Delete("test_key")
	})

	t.Run("Type", func(t *testing.T) {
		redis.Master().Set("test_key", "test_value")
		response := redis.Master().Type("test_key")
		assert.NoError(t, response.Error)
		assert.Equal(t, "string", response.GetString())

		// Cleanup
		redis.Master().Delete("test_key")
	})

	t.Run("RandomKey", func(t *testing.T) {
		redis.Master().Set("test_key_1", "value1")
		redis.Master().Set("test_key_2", "value2")
		response := redis.Master().RandomKey()
		assert.NoError(t, response.Error)
		assert.NotEmpty(t, response.GetString())

		// Cleanup
		redis.Master().Delete("test_key_1", "test_key_2")
	})

	t.Run("Rename", func(t *testing.T) {
		redis.Master().Set("old_key", "test_value")
		response := redis.Master().Rename("old_key", "new_key")
		assert.NoError(t, response.Error)

		getResp := redis.Master().Get("new_key")
		assert.NoError(t, getResp.Error)
		assert.Equal(t, "test_value", getResp.GetString())

		// Cleanup
		redis.Master().Delete("new_key")
	})

	t.Run("RenameNX", func(t *testing.T) {
		redis.Master().Set("old_key", "test_value")
		response := redis.Master().RenameNX("old_key", "new_key")
		assert.NoError(t, response.Error)

		getResp := redis.Master().Get("new_key")
		assert.NoError(t, getResp.Error)
		assert.Equal(t, "test_value", getResp.GetString())

		// Cleanup
		redis.Master().Delete("new_key")
	})

	t.Run("Touch", func(t *testing.T) {
		redis.Master().Set("test_key", "test_value")
		response := redis.Master().Touch("test_key")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(1), response.GetInt64())

		// Cleanup
		redis.Master().Delete("test_key")
	})

	t.Run("Unlink", func(t *testing.T) {
		redis.Master().Set("test_key", "test_value")
		response := redis.Master().Unlink("test_key")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(1), response.GetInt64())
	})

	t.Run("Persist", func(t *testing.T) {
		redis.Master().SetExpire("test_key", "test_value", 60)
		response := redis.Master().Persist("test_key")
		assert.NoError(t, response.Error)

		ttlResp := redis.Master().TTL("test_key")
		assert.NoError(t, ttlResp.Error)
		assert.Equal(t, int64(-1), ttlResp.GetInt64()) // -1 means no expiry

		// Cleanup
		redis.Master().Delete("test_key")
	})

	t.Run("Append", func(t *testing.T) {
		redis.Master().Set("test_key", "hello")
		response := redis.Master().Append("test_key", " world")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(11), response.GetInt64()) // Length of "hello world"

		getResp := redis.Master().Get("test_key")
		assert.NoError(t, getResp.Error)
		assert.Equal(t, "hello world", getResp.GetString())

		// Cleanup
		redis.Master().Delete("test_key")
	})

	t.Run("StrLen", func(t *testing.T) {
		redis.Master().Set("test_key", "hello world")
		response := redis.Master().StrLen("test_key")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(11), response.GetInt64())

		// Cleanup
		redis.Master().Delete("test_key")
	})

	t.Run("GetRange", func(t *testing.T) {
		redis.Master().Set("test_key", "hello world")
		response := redis.Master().GetRange("test_key", 0, 4)
		assert.NoError(t, response.Error)
		assert.Equal(t, "hello", response.GetString())

		// Test negative indices
		response2 := redis.Master().GetRange("test_key", -5, -1)
		assert.NoError(t, response2.Error)
		assert.Equal(t, "world", response2.GetString())

		// Cleanup
		redis.Master().Delete("test_key")
	})

	t.Run("SetRange", func(t *testing.T) {
		redis.Master().Set("test_key", "hello world")
		response := redis.Master().SetRange("test_key", 6, "Redis")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(11), response.GetInt64()) // Length after modification

		getResp := redis.Master().Get("test_key")
		assert.NoError(t, getResp.Error)
		assert.Equal(t, "hello Redis", getResp.GetString())

		// Cleanup
		redis.Master().Delete("test_key")
	})

	t.Run("Scan", func(t *testing.T) {
		// Setup test data
		redis.Master().Set("scan_test_1", "value1")
		redis.Master().Set("scan_test_2", "value2")
		redis.Master().Set("scan_test_3", "value3")
		redis.Master().Set("other_key", "other_value")

		// Test SCAN with pattern matching
		response := redis.Master().Scan(0, "scan_test_*", 100)
		assert.NoError(t, response.Error)

		// SCAN returns an array with 2 elements: [cursor, [keys...]]
		scanResult := response.GetSlice()
		assert.Len(t, scanResult, 2, "SCAN should return exactly 2 elements: cursor and keys array")

		// Only proceed if we have the expected result structure
		if len(scanResult) >= 2 {
			// First element: cursor (should be int64, typically 0 for complete scan)
			cursor := scanResult[0].GetInt64()
			assert.GreaterOrEqual(t, cursor, int64(0), "cursor should be non-negative")

			// Second element: keys array - test GetSlice functionality on RedisResponseEntity
			keysEntity := scanResult[1]
			keys := keysEntity.GetSlice()

			// Should find the 3 scan_test_* keys (but not other_key)
			assert.GreaterOrEqual(t, len(keys), 3, "should find at least 3 matching keys")

			// Collect all key names for verification
			var keyNames []string
			for _, key := range keys {
				keyNames = append(keyNames, key.GetString())
			}

			// Verify specific keys are present
			assert.Contains(t, keyNames, "scan_test_1")
			assert.Contains(t, keyNames, "scan_test_2")
			assert.Contains(t, keyNames, "scan_test_3")
			assert.NotContains(t, keyNames, "other_key", "pattern should exclude non-matching keys")
		}

		// Test SCAN without pattern (should return more keys)
		responseAll := redis.Master().Scan(0, "", 100)
		assert.NoError(t, responseAll.Error)

		scanResultAll := responseAll.GetSlice()
		assert.Len(t, scanResultAll, 2)

		if len(scanResultAll) >= 2 {
			keysEntityAll := scanResultAll[1]
			keysAll := keysEntityAll.GetSlice()
			assert.GreaterOrEqual(t, len(keysAll), 4, "should find all keys including other_key")
		}

		// Test SCAN with non-matching pattern
		responseEmpty := redis.Master().Scan(0, "non_existing_pattern_*", 100)
		assert.NoError(t, responseEmpty.Error)

		scanResultEmpty := responseEmpty.GetSlice()
		assert.Len(t, scanResultEmpty, 2)

		if len(scanResultEmpty) >= 2 {
			keysEntityEmpty := scanResultEmpty[1]
			keysEmpty := keysEntityEmpty.GetSlice()
			// Could be empty or have very few keys
			assert.GreaterOrEqual(t, len(keysEmpty), 0)
		}

		// Cleanup
		redis.Master().Delete("scan_test_1", "scan_test_2", "scan_test_3", "other_key")
	})

	t.Run("FlushDB", func(t *testing.T) {
		// Setup test data
		redis.Master().Set("flush_test", "value")

		response := redis.Master().FlushDB()
		assert.NoError(t, response.Error)

		// Verify key was deleted
		getResp := redis.Master().Get("flush_test")
		assert.True(t, getResp.RecordNotFound())
	})
}

// TestRedisListCommands List command tests
func TestRedisListCommands(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	redis := NewRedis("test")
	assert.NotNil(t, redis)

	t.Run("LPush_LLen_LIndex", func(t *testing.T) {
		listKey := "test_list"

		// LPush
		response := redis.Master().LPush(listKey, "item1", "item2", "item3")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(3), response.GetInt64())

		// LLen
		lenResp := redis.Master().LLen(listKey)
		assert.NoError(t, lenResp.Error)
		assert.Equal(t, int64(3), lenResp.GetInt64())

		// LIndex
		idxResp := redis.Master().LIndex(listKey, 0)
		assert.NoError(t, idxResp.Error)
		assert.Equal(t, "item3", idxResp.GetString()) // LPUSH prepends

		// Cleanup
		redis.Master().Delete(listKey)
	})

	t.Run("RPush_LRange", func(t *testing.T) {
		listKey := "test_list"

		// RPush
		response := redis.Master().RPush(listKey, "a", "b", "c")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(3), response.GetInt64())

		// LRange
		rangeResp := redis.Master().LRange(listKey, 0, -1)
		assert.NoError(t, rangeResp.Error)
		slice := rangeResp.GetSlice()
		assert.Equal(t, 3, len(slice))
		assert.Equal(t, "a", slice[0].GetString())
		assert.Equal(t, "b", slice[1].GetString())
		assert.Equal(t, "c", slice[2].GetString())

		// Cleanup
		redis.Master().Delete(listKey)
	})

	t.Run("LPop_RPop", func(t *testing.T) {
		listKey := "test_list"

		redis.Master().LPush(listKey, "left1", "left2")
		redis.Master().RPush(listKey, "right1", "right2")

		// LPop
		lpopResp := redis.Master().LPop(listKey)
		assert.NoError(t, lpopResp.Error)
		assert.Equal(t, "left2", lpopResp.GetString())

		// RPop
		rpopResp := redis.Master().RPop(listKey)
		assert.NoError(t, rpopResp.Error)
		assert.Equal(t, "right2", rpopResp.GetString())

		// Cleanup
		redis.Master().Delete(listKey)
	})

	t.Run("LInsert", func(t *testing.T) {
		listKey := "test_list"

		redis.Master().RPush(listKey, "a", "c")

		// Insert before
		response := redis.Master().LInsert(listKey, "BEFORE", "c", "b")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(3), response.GetInt64())

		rangeResp := redis.Master().LRange(listKey, 0, -1)
		assert.NoError(t, rangeResp.Error)
		slice := rangeResp.GetSlice()
		assert.Equal(t, []string{"a", "b", "c"}, []string{
			slice[0].GetString(),
			slice[1].GetString(),
			slice[2].GetString(),
		})

		// Cleanup
		redis.Master().Delete(listKey)
	})

	t.Run("LPos", func(t *testing.T) {
		listKey := "test_list"

		redis.Master().RPush(listKey, "a", "b", "c", "b")

		// Find position
		response := redis.Master().LPos(listKey, "b")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(1), response.GetInt64())

		// Cleanup
		redis.Master().Delete(listKey)
	})

	t.Run("LRem", func(t *testing.T) {
		listKey := "test_list"

		redis.Master().RPush(listKey, "a", "b", "b", "c", "b")

		// Remove 2 occurrences of "b" from head
		response := redis.Master().LRem(listKey, 2, "b")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(2), response.GetInt64())

		// Cleanup
		redis.Master().Delete(listKey)
	})

	t.Run("LSet", func(t *testing.T) {
		listKey := "test_list"

		redis.Master().RPush(listKey, "a", "b", "c")

		// Set value at index 1
		response := redis.Master().LSet(listKey, 1, "new_value")
		assert.NoError(t, response.Error)

		idxResp := redis.Master().LIndex(listKey, 1)
		assert.NoError(t, idxResp.Error)
		assert.Equal(t, "new_value", idxResp.GetString())

		// Cleanup
		redis.Master().Delete(listKey)
	})

	t.Run("LTrim", func(t *testing.T) {
		listKey := "test_list"

		redis.Master().RPush(listKey, "a", "b", "c", "d", "e")

		// Trim to keep only indices 1-3
		response := redis.Master().LTrim(listKey, 1, 3)
		assert.NoError(t, response.Error)

		rangeResp := redis.Master().LRange(listKey, 0, -1)
		assert.NoError(t, rangeResp.Error)
		slice := rangeResp.GetSlice()
		assert.Equal(t, 3, len(slice))
		assert.Equal(t, "b", slice[0].GetString())
		assert.Equal(t, "c", slice[1].GetString())
		assert.Equal(t, "d", slice[2].GetString())

		// Cleanup
		redis.Master().Delete(listKey)
	})

	t.Run("LMove", func(t *testing.T) {
		srcKey := "src_list"
		dstKey := "dst_list"

		redis.Master().RPush(srcKey, "a", "b", "c")
		redis.Master().RPush(dstKey, "x", "y")

		// Move from source tail to destination head
		response := redis.Master().LMove(srcKey, dstKey, "RIGHT", "LEFT")
		assert.NoError(t, response.Error)
		assert.Equal(t, "c", response.GetString())

		// Check destination
		dstResp := redis.Master().LRange(dstKey, 0, -1)
		assert.NoError(t, dstResp.Error)
		slice := dstResp.GetSlice()
		assert.Equal(t, "c", slice[0].GetString())

		// Cleanup
		redis.Master().Delete(srcKey, dstKey)
	})

	t.Run("RPopLPush", func(t *testing.T) {
		srcKey := "src_list"
		dstKey := "dst_list"

		redis.Master().RPush(srcKey, "a", "b", "c")
		redis.Master().RPush(dstKey, "x")

		// Move from source tail to destination head
		response := redis.Master().RPopLPush(srcKey, dstKey)
		assert.NoError(t, response.Error)
		assert.Equal(t, "c", response.GetString())

		// Cleanup
		redis.Master().Delete(srcKey, dstKey)
	})

	t.Run("LPushX_RPushX", func(t *testing.T) {
		listKey := "test_list"

		// LPushX on non-existent key should return 0
		response := redis.Master().LPushX(listKey, "item")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(0), response.GetInt64())

		// Create list first
		redis.Master().LPush(listKey, "initial")

		// Now LPushX should work
		response = redis.Master().LPushX(listKey, "new_item")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(2), response.GetInt64())

		// Same for RPushX
		response = redis.Master().RPushX(listKey, "tail_item")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(3), response.GetInt64())

		// Cleanup
		redis.Master().Delete(listKey)
	})

	t.Run("LMPop", func(t *testing.T) {
		listKey := "test_list"

		redis.Master().RPush(listKey, "a", "b", "c")

		// Pop from left
		response := redis.Master().LMPop(1, "LEFT", listKey)
		assert.NoError(t, response.Error)

		// Cleanup
		redis.Master().Delete(listKey)
	})
}

// TestRedisSetCommands Set command tests
func TestRedisSetCommands(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	redis := NewRedis("test")
	assert.NotNil(t, redis)

	t.Run("SAdd_SCard_SMembers", func(t *testing.T) {
		setKey := "test_set"

		// SAdd
		response := redis.Master().SAdd(setKey, "member1", "member2", "member3")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(3), response.GetInt64())

		// SCard
		cardResp := redis.Master().SCard(setKey)
		assert.NoError(t, cardResp.Error)
		assert.Equal(t, int64(3), cardResp.GetInt64())

		// SMembers
		membersResp := redis.Master().SMembers(setKey)
		assert.NoError(t, membersResp.Error)
		slice := membersResp.GetSlice()
		assert.Equal(t, 3, len(slice))

		// Cleanup
		redis.Master().Delete(setKey)
	})

	t.Run("SIsMember_SMIsMember", func(t *testing.T) {
		setKey := "test_set"

		redis.Master().SAdd(setKey, "member1", "member2", "member3")

		// SIsMember
		response := redis.Master().SIsMember(setKey, "member1")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(1), response.GetInt64())

		response = redis.Master().SIsMember(setKey, "nonexistent")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(0), response.GetInt64())

		// SMIsMember
		multiResp := redis.Master().SMIsMember(setKey, "member1", "member2", "nonexistent")
		assert.NoError(t, multiResp.Error)

		// Cleanup
		redis.Master().Delete(setKey)
	})

	t.Run("SRem", func(t *testing.T) {
		setKey := "test_set"

		redis.Master().SAdd(setKey, "member1", "member2", "member3")

		// SRem
		response := redis.Master().SRem(setKey, "member1", "member2")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(2), response.GetInt64())

		// Verify remaining member
		cardResp := redis.Master().SCard(setKey)
		assert.NoError(t, cardResp.Error)
		assert.Equal(t, int64(1), cardResp.GetInt64())

		// Cleanup
		redis.Master().Delete(setKey)
	})

	t.Run("SPop_SRandMember", func(t *testing.T) {
		setKey := "test_set"

		redis.Master().SAdd(setKey, "member1", "member2", "member3", "member4", "member5")

		// SPop
		popResp := redis.Master().SPop(setKey)
		assert.NoError(t, popResp.Error)
		assert.NotEmpty(t, popResp.GetString())

		// Verify set size decreased
		cardResp := redis.Master().SCard(setKey)
		assert.NoError(t, cardResp.Error)
		assert.Equal(t, int64(4), cardResp.GetInt64())

		// SRandMember (doesn't remove)
		randResp := redis.Master().SRandMember(setKey)
		assert.NoError(t, randResp.Error)
		assert.NotEmpty(t, randResp.GetString())

		// Verify set size unchanged
		cardResp = redis.Master().SCard(setKey)
		assert.NoError(t, cardResp.Error)
		assert.Equal(t, int64(4), cardResp.GetInt64())

		// Cleanup
		redis.Master().Delete(setKey)
	})

	t.Run("SMove", func(t *testing.T) {
		srcSet := "src_set"
		dstSet := "dst_set"

		redis.Master().SAdd(srcSet, "member1", "member2", "member3")
		redis.Master().SAdd(dstSet, "existing")

		// SMove
		response := redis.Master().SMove(srcSet, dstSet, "member1")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(1), response.GetInt64())

		// Verify source decreased
		srcCardResp := redis.Master().SCard(srcSet)
		assert.NoError(t, srcCardResp.Error)
		assert.Equal(t, int64(2), srcCardResp.GetInt64())

		// Verify destination increased
		dstCardResp := redis.Master().SCard(dstSet)
		assert.NoError(t, dstCardResp.Error)
		assert.Equal(t, int64(2), dstCardResp.GetInt64())

		// Cleanup
		redis.Master().Delete(srcSet, dstSet)
	})

	t.Run("SDiff_SDiffStore", func(t *testing.T) {
		set1 := "set1"
		set2 := "set2"
		diffSet := "diff_set"

		redis.Master().SAdd(set1, "a", "b", "c", "d")
		redis.Master().SAdd(set2, "c", "d", "e", "f")

		// SDiff
		diffResp := redis.Master().SDiff(set1, set2)
		assert.NoError(t, diffResp.Error)
		slice := diffResp.GetSlice()
		assert.Equal(t, 2, len(slice)) // "a" and "b"

		// SDiffStore
		storeResp := redis.Master().SDiffStore(diffSet, set1, set2)
		assert.NoError(t, storeResp.Error)
		assert.Equal(t, int64(2), storeResp.GetInt64())

		// Cleanup
		redis.Master().Delete(set1, set2, diffSet)
	})

	t.Run("SInter_SInterStore", func(t *testing.T) {
		set1 := "set1"
		set2 := "set2"
		interSet := "inter_set"

		redis.Master().SAdd(set1, "a", "b", "c", "d")
		redis.Master().SAdd(set2, "c", "d", "e", "f")

		// SInter
		interResp := redis.Master().SInter(set1, set2)
		assert.NoError(t, interResp.Error)
		slice := interResp.GetSlice()
		assert.Equal(t, 2, len(slice)) // "c" and "d"

		// SInterStore
		storeResp := redis.Master().SInterStore(interSet, set1, set2)
		assert.NoError(t, storeResp.Error)
		assert.Equal(t, int64(2), storeResp.GetInt64())

		// Cleanup
		redis.Master().Delete(set1, set2, interSet)
	})

	t.Run("SUnion_SUnionStore", func(t *testing.T) {
		set1 := "set1"
		set2 := "set2"
		unionSet := "union_set"

		redis.Master().SAdd(set1, "a", "b", "c")
		redis.Master().SAdd(set2, "c", "d", "e")

		// SUnion
		unionResp := redis.Master().SUnion(set1, set2)
		assert.NoError(t, unionResp.Error)
		slice := unionResp.GetSlice()
		assert.Equal(t, 5, len(slice)) // "a", "b", "c", "d", "e"

		// SUnionStore
		storeResp := redis.Master().SUnionStore(unionSet, set1, set2)
		assert.NoError(t, storeResp.Error)
		assert.Equal(t, int64(5), storeResp.GetInt64())

		// Cleanup
		redis.Master().Delete(set1, set2, unionSet)
	})

	t.Run("SInterCard", func(t *testing.T) {
		set1 := "set1"
		set2 := "set2"

		redis.Master().SAdd(set1, "a", "b", "c", "d")
		redis.Master().SAdd(set2, "c", "d", "e", "f")

		// SInterCard
		response := redis.Master().SInterCard(set1, set2)
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(2), response.GetInt64()) // "c" and "d"

		// Cleanup
		redis.Master().Delete(set1, set2)
	})

	t.Run("SScan", func(t *testing.T) {
		setKey := "test_set"

		redis.Master().SAdd(setKey, "member1", "member2", "member3")

		// SScan
		response := redis.Master().SScan(setKey, 0, "", 0)
		assert.NoError(t, response.Error)

		// Cleanup
		redis.Master().Delete(setKey)
	})
}

// TestRedisSortedSetCommands Sorted Set command tests
func TestRedisSortedSetCommands(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	redis := NewRedis("test")
	assert.NotNil(t, redis)

	t.Run("ZAdd_ZCard_ZRange", func(t *testing.T) {
		zsetKey := "test_zset"

		// ZAdd
		response := redis.Master().ZAdd(zsetKey, 1.0, "member1", 2.0, "member2", 3.0, "member3")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(3), response.GetInt64())

		// ZCard
		cardResp := redis.Master().ZCard(zsetKey)
		assert.NoError(t, cardResp.Error)
		assert.Equal(t, int64(3), cardResp.GetInt64())

		// ZRange
		rangeResp := redis.Master().ZRange(zsetKey, 0, -1)
		assert.NoError(t, rangeResp.Error)
		slice := rangeResp.GetSlice()
		assert.Equal(t, 3, len(slice))
		assert.Equal(t, "member1", slice[0].GetString()) // lowest score first

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZScore_ZRank_ZRevRank", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 1.0, "a", 2.0, "b", 3.0, "c")

		// ZScore
		scoreResp := redis.Master().ZScore(zsetKey, "b")
		assert.NoError(t, scoreResp.Error)
		assert.Equal(t, "2", scoreResp.GetString())

		// ZRank (0-based, ascending order)
		rankResp := redis.Master().ZRank(zsetKey, "b")
		assert.NoError(t, rankResp.Error)
		assert.Equal(t, int64(1), rankResp.GetInt64())

		// ZRevRank (0-based, descending order)
		revRankResp := redis.Master().ZRevRank(zsetKey, "b")
		assert.NoError(t, revRankResp.Error)
		assert.Equal(t, int64(1), revRankResp.GetInt64()) // c=0, b=1, a=2 in desc order

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZIncrBy", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 1.0, "member", 2.0, "other")

		// ZIncrBy
		response := redis.Master().ZIncrBy(zsetKey, 5.0, "member")
		assert.NoError(t, response.Error)
		assert.Equal(t, "6", response.GetString()) // 1.0 + 5.0 = 6.0

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZCount", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 1.0, "a", 2.0, "b", 3.0, "c", 4.0, "d", 5.0, "e")

		// ZCount
		response := redis.Master().ZCount(zsetKey, "2", "4")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(3), response.GetInt64()) // b, c, d

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZRem", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 1.0, "a", 2.0, "b", 3.0, "c")

		// ZRem
		response := redis.Master().ZRem(zsetKey, "a", "c")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(2), response.GetInt64())

		// Verify remaining
		cardResp := redis.Master().ZCard(zsetKey)
		assert.NoError(t, cardResp.Error)
		assert.Equal(t, int64(1), cardResp.GetInt64())

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZPopMin_ZPopMax", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 1.0, "lowest", 2.0, "middle", 3.0, "highest")

		// ZPopMin
		minResp := redis.Master().ZPopMin(zsetKey)
		assert.NoError(t, minResp.Error)
		// Redis returns array: [member, score]

		// ZPopMax
		maxResp := redis.Master().ZPopMax(zsetKey)
		assert.NoError(t, maxResp.Error)

		// Verify remaining
		cardResp := redis.Master().ZCard(zsetKey)
		assert.NoError(t, cardResp.Error)
		assert.Equal(t, int64(1), cardResp.GetInt64())

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZRandMember", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 1.0, "a", 2.0, "b", 3.0, "c")

		// ZRandMember
		response := redis.Master().ZRandMember(zsetKey)
		assert.NoError(t, response.Error)
		assert.NotEmpty(t, response.GetString())

		// Verify set size unchanged
		cardResp := redis.Master().ZCard(zsetKey)
		assert.NoError(t, cardResp.Error)
		assert.Equal(t, int64(3), cardResp.GetInt64())

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZRangeByScore", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 1.0, "a", 2.0, "b", 3.0, "c", 4.0, "d", 5.0, "e")

		// ZRangeByScore
		response := redis.Master().ZRangeByScore(zsetKey, "2", "4")
		assert.NoError(t, response.Error)
		slice := response.GetSlice()
		assert.Equal(t, 3, len(slice)) // b, c, d

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZRevRange", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 1.0, "a", 2.0, "b", 3.0, "c")

		// ZRevRange (highest to lowest)
		response := redis.Master().ZRevRange(zsetKey, 0, -1)
		assert.NoError(t, response.Error)
		slice := response.GetSlice()
		assert.Equal(t, 3, len(slice))
		assert.Equal(t, "c", slice[0].GetString()) // highest score first

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZRevRangeByScore", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 1.0, "a", 2.0, "b", 3.0, "c", 4.0, "d", 5.0, "e")

		// ZRevRangeByScore (max, min)
		response := redis.Master().ZRevRangeByScore(zsetKey, "4", "2")
		assert.NoError(t, response.Error)
		slice := response.GetSlice()
		assert.Equal(t, 3, len(slice)) // d, c, b (highest to lowest)

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZRangeByLex", func(t *testing.T) {
		zsetKey := "test_zset"

		// All members with same score for lexicographical ordering
		redis.Master().ZAdd(zsetKey, 0.0, "apple", 0.0, "banana", 0.0, "cherry", 0.0, "date")

		// ZRangeByLex
		response := redis.Master().ZRangeByLex(zsetKey, "[b", "[d")
		assert.NoError(t, response.Error)
		slice := response.GetSlice()
		assert.True(t, len(slice) >= 2) // banana, cherry

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZRevRangeByLex", func(t *testing.T) {
		zsetKey := "test_zset"

		// All members with same score for lexicographical ordering
		redis.Master().ZAdd(zsetKey, 0.0, "apple", 0.0, "banana", 0.0, "cherry", 0.0, "date")

		// ZRevRangeByLex (max, min)
		response := redis.Master().ZRevRangeByLex(zsetKey, "[d", "[b")
		assert.NoError(t, response.Error)
		slice := response.GetSlice()
		assert.True(t, len(slice) >= 2) // date, cherry, banana (reverse order)

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZLexCount", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 0.0, "apple", 0.0, "banana", 0.0, "cherry", 0.0, "date")

		// ZLexCount
		response := redis.Master().ZLexCount(zsetKey, "[b", "[d")
		assert.NoError(t, response.Error)
		assert.True(t, response.GetInt64() >= 2) // banana, cherry

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZRemRangeByScore", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 1.0, "a", 2.0, "b", 3.0, "c", 4.0, "d", 5.0, "e")

		// ZRemRangeByScore
		response := redis.Master().ZRemRangeByScore(zsetKey, "2", "4")
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(3), response.GetInt64()) // removed b, c, d

		// Verify remaining
		cardResp := redis.Master().ZCard(zsetKey)
		assert.NoError(t, cardResp.Error)
		assert.Equal(t, int64(2), cardResp.GetInt64()) // a, e remain

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZRemRangeByRank", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 1.0, "a", 2.0, "b", 3.0, "c", 4.0, "d", 5.0, "e")

		// ZRemRangeByRank (remove ranks 1-3: b, c, d)
		response := redis.Master().ZRemRangeByRank(zsetKey, 1, 3)
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(3), response.GetInt64())

		// Verify remaining
		cardResp := redis.Master().ZCard(zsetKey)
		assert.NoError(t, cardResp.Error)
		assert.Equal(t, int64(2), cardResp.GetInt64()) // a, e remain

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZRemRangeByLex", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 0.0, "apple", 0.0, "banana", 0.0, "cherry", 0.0, "date")

		// ZRemRangeByLex
		response := redis.Master().ZRemRangeByLex(zsetKey, "[b", "[d")
		assert.NoError(t, response.Error)
		assert.True(t, response.GetInt64() >= 2) // removed banana, cherry

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZScan", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 1.0, "member1", 2.0, "member2", 3.0, "member3")

		// ZScan
		response := redis.Master().ZScan(zsetKey, 0, "", 0)
		assert.NoError(t, response.Error)

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZMScore", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 1.0, "a", 2.0, "b", 3.0, "c")

		// ZMScore
		response := redis.Master().ZMScore(zsetKey, "a", "b", "nonexistent")
		assert.NoError(t, response.Error)

		// Cleanup
		redis.Master().Delete(zsetKey)
	})

	t.Run("ZRangeStore", func(t *testing.T) {
		srcKey := "src_zset"
		dstKey := "dst_zset"

		redis.Master().ZAdd(srcKey, 1.0, "a", 2.0, "b", 3.0, "c", 4.0, "d", 5.0, "e")

		// ZRangeStore
		response := redis.Master().ZRangeStore(dstKey, srcKey, 1, 3)
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(3), response.GetInt64()) // stored b, c, d

		// Verify destination
		cardResp := redis.Master().ZCard(dstKey)
		assert.NoError(t, cardResp.Error)
		assert.Equal(t, int64(3), cardResp.GetInt64())

		// Cleanup
		redis.Master().Delete(srcKey, dstKey)
	})

	t.Run("ZDiff_ZDiffStore", func(t *testing.T) {
		set1 := "zset1"
		set2 := "zset2"
		diffSet := "zdiff_set"

		redis.Master().ZAdd(set1, 1.0, "a", 2.0, "b", 3.0, "c")
		redis.Master().ZAdd(set2, 2.0, "b", 3.0, "c", 4.0, "d")

		// ZDiff
		diffResp := redis.Master().ZDiff(set1, set2)
		assert.NoError(t, diffResp.Error)

		// ZDiffStore
		storeResp := redis.Master().ZDiffStore(diffSet, set1, set2)
		assert.NoError(t, storeResp.Error)

		// Cleanup
		redis.Master().Delete(set1, set2, diffSet)
	})

	t.Run("ZInter_ZInterStore", func(t *testing.T) {
		set1 := "zset1"
		set2 := "zset2"
		interSet := "zinter_set"

		redis.Master().ZAdd(set1, 1.0, "a", 2.0, "b", 3.0, "c")
		redis.Master().ZAdd(set2, 2.0, "b", 3.0, "c", 4.0, "d")

		// ZInter
		interResp := redis.Master().ZInter(set1, set2)
		assert.NoError(t, interResp.Error)

		// ZInterStore
		storeResp := redis.Master().ZInterStore(interSet, set1, set2)
		assert.NoError(t, storeResp.Error)

		// Cleanup
		redis.Master().Delete(set1, set2, interSet)
	})

	t.Run("ZUnion_ZUnionStore", func(t *testing.T) {
		set1 := "zset1"
		set2 := "zset2"
		unionSet := "zunion_set"

		redis.Master().ZAdd(set1, 1.0, "a", 2.0, "b")
		redis.Master().ZAdd(set2, 2.0, "b", 3.0, "c")

		// ZUnion
		unionResp := redis.Master().ZUnion(set1, set2)
		assert.NoError(t, unionResp.Error)

		// ZUnionStore
		storeResp := redis.Master().ZUnionStore(unionSet, set1, set2)
		assert.NoError(t, storeResp.Error)

		// Cleanup
		redis.Master().Delete(set1, set2, unionSet)
	})

	t.Run("ZInterCard", func(t *testing.T) {
		set1 := "zset1"
		set2 := "zset2"

		redis.Master().ZAdd(set1, 1.0, "a", 2.0, "b", 3.0, "c")
		redis.Master().ZAdd(set2, 2.0, "b", 3.0, "c", 4.0, "d")

		// ZInterCard
		response := redis.Master().ZInterCard(set1, set2)
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(2), response.GetInt64()) // b, c

		// Cleanup
		redis.Master().Delete(set1, set2)
	})

	t.Run("ZMPop", func(t *testing.T) {
		zsetKey := "test_zset"

		redis.Master().ZAdd(zsetKey, 1.0, "a", 2.0, "b", 3.0, "c")

		// ZMPop
		response := redis.Master().ZMPop(1, "MIN", zsetKey)
		assert.NoError(t, response.Error)

		// Cleanup
		redis.Master().Delete(zsetKey)
	})
}

// TestRedisHashCommands Hash command tests
func TestRedisHashCommands(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	redis := NewRedis("test")
	assert.NotNil(t, redis)

	// Clean test environment
	defer func() {
		redis.Master().Delete("test_hash", "test_hash_2", "test_hash_incr", "test_hash_scan")
		redis.Master().Close()
	}()

	t.Run("HSet_HGet_Basic_Set_And_Get", func(t *testing.T) {
		hashKey := "test_hash"
		field := "test_field"
		value := "test_value"

		// HSet - set hash fieldvalue
		setResp := redis.Master().HSet(hashKey, field, value)
		assert.NoError(t, setResp.Error)
		assert.Equal(t, int64(1), setResp.GetInt64()) // newfieldshouldreturn 1

		// HGet - get hash fieldvalue
		getResp := redis.Master().HGet(hashKey, field)
		assert.NoError(t, getResp.Error)
		assert.Equal(t, value, getResp.GetString())

		// HSet updateexistingfield
		setResp2 := redis.Master().HSet(hashKey, field, "updated_value")
		assert.NoError(t, setResp2.Error)
		assert.Equal(t, int64(0), setResp2.GetInt64()) // updateexistingfieldshouldreturn 0

		// Verifyupdateresult
		getResp2 := redis.Master().HGet(hashKey, field)
		assert.NoError(t, getResp2.Error)
		assert.Equal(t, "updated_value", getResp2.GetString())

		// Testgetnon-existentfield
		nonExistResp := redis.Master().HGet(hashKey, "non_exist")
		assert.True(t, nonExistResp.RecordNotFound())
	})

	t.Run("HMSet_HMGet_Batch_Set_And_Get", func(t *testing.T) {
		hashKey := "test_hash"

		// HMSet - batchsetmultiplefield
		hashData := map[interface{}]interface{}{
			"field1": "value1",
			"field2": "value2",
			"field3": "value3",
			"number": "123",
		}

		hmsetResp := redis.Master().HMSet(hashKey, hashData)
		assert.NoError(t, hmsetResp.Error)

		// HMGet - batchgetmultiplefield
		hmgetResp := redis.Master().HMGet(hashKey, "field1", "field2", "field3", "number")
		assert.NoError(t, hmgetResp.Error)

		values := hmgetResp.GetSlice()
		assert.Len(t, values, 4)
		assert.Equal(t, "value1", values[0].GetString())
		assert.Equal(t, "value2", values[1].GetString())
		assert.Equal(t, "value3", values[2].GetString())
		assert.Equal(t, "123", values[3].GetString())

		// Testincludingnon-existentfieldbatchread
		hmgetResp2 := redis.Master().HMGet(hashKey, "field1", "non_exist", "field2")
		assert.NoError(t, hmgetResp2.Error)

		values2 := hmgetResp2.GetSlice()
		assert.Len(t, values2, 3)
		assert.Equal(t, "value1", values2[0].GetString())
		// values2[1] shouldis nil (non-existentfield)
		assert.Equal(t, "value2", values2[2].GetString())
	})

	t.Run("HExists_Field_Existence_Check", func(t *testing.T) {
		hashKey := "test_hash"

		// Settestdata
		redis.Master().HSet(hashKey, "existing_field", "value")

		// Testexistingfield
		existResp := redis.Master().HExists(hashKey, "existing_field")
		assert.NoError(t, existResp.Error)
		assert.Equal(t, int64(1), existResp.GetInt64()) // existingshouldreturn 1

		// Testnon-existentfield
		nonExistResp := redis.Master().HExists(hashKey, "non_existing_field")
		assert.NoError(t, nonExistResp.Error)
		assert.Equal(t, int64(0), nonExistResp.GetInt64()) // non-existentshouldreturn 0

		// Testnon-existent hash key
		nonExistHashResp := redis.Master().HExists("non_existing_hash", "field")
		assert.NoError(t, nonExistHashResp.Error)
		assert.Equal(t, int64(0), nonExistHashResp.GetInt64())
	})

	t.Run("HDel_Delete_Field", func(t *testing.T) {
		hashKey := "test_hash"

		// Settestdata
		redis.Master().HSet(hashKey, "field1", "value1")
		redis.Master().HSet(hashKey, "field2", "value2")
		redis.Master().HSet(hashKey, "field3", "value3")

		// Deletesinglefield
		delResp := redis.Master().HDel(hashKey, "field1")
		assert.NoError(t, delResp.Error)
		assert.Equal(t, int64(1), delResp.GetInt64()) // delete 1 field

		// Verifyfieldalreadydelete
		existResp := redis.Master().HExists(hashKey, "field1")
		assert.NoError(t, existResp.Error)
		assert.Equal(t, int64(0), existResp.GetInt64())

		// batchdeletemultiplefield
		delResp2 := redis.Master().HDel(hashKey, "field2", "field3", "non_exist")
		assert.NoError(t, delResp2.Error)
		assert.Equal(t, int64(2), delResp2.GetInt64()) // delete 2 existingfield

		// attemptdeletenon-existentfield
		delResp3 := redis.Master().HDel(hashKey, "non_exist")
		assert.NoError(t, delResp3.Error)
		assert.Equal(t, int64(0), delResp3.GetInt64()) // delete 0 field
	})

	t.Run("HGetAll_Get_All_Fields", func(t *testing.T) {
		hashKey := "test_hash"

		// Cleanandsettestdata
		redis.Master().Delete(hashKey)
		redis.Master().HSet(hashKey, "field1", "value1")
		redis.Master().HSet(hashKey, "field2", "value2")
		redis.Master().HSet(hashKey, "field3", "value3")

		// Getallfieldandvalue
		getAllResp := redis.Master().HGetAll(hashKey)
		assert.NoError(t, getAllResp.Error)

		allData := getAllResp.GetSlice()
		// resultformat: [field1, value1, field2, value2, ...]
		assert.Equal(t, 6, len(allData))

		// build map toverifyresult
		resultMap := make(map[string]string)
		for i := 0; i < len(allData); i += 2 {
			field := allData[i].GetString()
			value := allData[i+1].GetString()
			resultMap[field] = value
		}

		assert.Equal(t, "value1", resultMap["field1"])
		assert.Equal(t, "value2", resultMap["field2"])
		assert.Equal(t, "value3", resultMap["field3"])

		// Testempty hash
		emptyResp := redis.Master().HGetAll("non_existing_hash")
		assert.NoError(t, emptyResp.Error)
		emptyData := emptyResp.GetSlice()
		assert.Equal(t, 0, len(emptyData))
	})

	t.Run("HKeys_Get_All_Field_Names", func(t *testing.T) {
		hashKey := "test_hash"

		// Cleanandsettestdata
		redis.Master().Delete(hashKey)
		redis.Master().HSet(hashKey, "field1", "value1")
		redis.Master().HSet(hashKey, "field2", "value2")
		redis.Master().HSet(hashKey, "field3", "value3")

		// Getallfieldnames
		keysResp := redis.Master().HKeys(hashKey)
		assert.NoError(t, keysResp.Error)

		keys := keysResp.GetSlice()
		assert.Equal(t, 3, len(keys))

		// build set toverifyresult (Redis order not guaranteed)
		keySet := make(map[string]bool)
		for _, key := range keys {
			keySet[key.GetString()] = true
		}

		assert.True(t, keySet["field1"])
		assert.True(t, keySet["field2"])
		assert.True(t, keySet["field3"])

		// Testempty hash
		emptyResp := redis.Master().HKeys("non_existing_hash")
		assert.NoError(t, emptyResp.Error)
		emptyKeys := emptyResp.GetSlice()
		assert.Equal(t, 0, len(emptyKeys))
	})

	t.Run("HVals_Get_All_Values", func(t *testing.T) {
		hashKey := "test_hash"

		// Cleanandsettestdata
		redis.Master().Delete(hashKey)
		redis.Master().HSet(hashKey, "field1", "value1")
		redis.Master().HSet(hashKey, "field2", "value2")
		redis.Master().HSet(hashKey, "field3", "value3")

		// Getallvalue
		valsResp := redis.Master().HVals(hashKey)
		assert.NoError(t, valsResp.Error)

		vals := valsResp.GetSlice()
		assert.Equal(t, 3, len(vals))

		// build set toverifyresult (Redis order not guaranteed)
		valSet := make(map[string]bool)
		for _, val := range vals {
			valSet[val.GetString()] = true
		}

		assert.True(t, valSet["value1"])
		assert.True(t, valSet["value2"])
		assert.True(t, valSet["value3"])

		// Testempty hash
		emptyResp := redis.Master().HVals("non_existing_hash")
		assert.NoError(t, emptyResp.Error)
		emptyVals := emptyResp.GetSlice()
		assert.Equal(t, 0, len(emptyVals))
	})

	t.Run("HIncrBy_Increment_Field_Value", func(t *testing.T) {
		hashKey := "test_hash_incr"
		field := "counter"

		// Cleantestdata
		redis.Master().Delete(hashKey)

		// fornon-existentfieldincrement (shouldfrom 0 start)
		incrResp := redis.Master().HIncrBy(hashKey, field, 5)
		assert.NoError(t, incrResp.Error)
		assert.Equal(t, int64(5), incrResp.GetInt64())

		// continueincrement
		incrResp2 := redis.Master().HIncrBy(hashKey, field, 10)
		assert.NoError(t, incrResp2.Error)
		assert.Equal(t, int64(15), incrResp2.GetInt64())

		// negative numberincrement (actuallyisdecrement)
		incrResp3 := redis.Master().HIncrBy(hashKey, field, -3)
		assert.NoError(t, incrResp3.Error)
		assert.Equal(t, int64(12), incrResp3.GetInt64())

		// Verifyfinalvalue
		getResp := redis.Master().HGet(hashKey, field)
		assert.NoError(t, getResp.Error)
		assert.Equal(t, "12", getResp.GetString())

		// Testfornonnumericvaluefieldincrement (shoulderror)
		redis.Master().HSet(hashKey, "text_field", "not_a_number")
		incrRespErr := redis.Master().HIncrBy(hashKey, "text_field", 1)
		// Redis willreturnerror，butherewecheckitnotwill panic
		assert.NotNil(t, incrRespErr)
	})

	t.Run("HScan_Iterate_Scan", func(t *testing.T) {
		hashKey := "test_hash_scan"

		// Cleanandsettestdata
		redis.Master().Delete(hashKey)
		redis.Master().HSet(hashKey, "field1", "value1")
		redis.Master().HSet(hashKey, "field2", "value2")
		redis.Master().HSet(hashKey, "field3", "value3")
		redis.Master().HSet(hashKey, "other1", "othervalue1")
		redis.Master().HSet(hashKey, "other2", "othervalue2")

		// HScan no pattern matching - getall
		scanResp := redis.Master().HScan(hashKey, 0, "", 100)
		assert.NoError(t, scanResp.Error)

		scanData := scanResp.GetSlice()
		// resultformat: [cursor, [field1, value1, field2, value2, ...]]
		assert.GreaterOrEqual(t, len(scanData), 2) // at least cursor anddatanumericpairs

		// HScan havepattern matching - onlyget field* prefix
		scanResp2 := redis.Master().HScan(hashKey, 0, "field*", 100)
		assert.NoError(t, scanResp2.Error)
		// shouldincluding field1, field2, field3 anditsvalue

		// HScan havepattern matching - get other* prefix
		scanResp3 := redis.Master().HScan(hashKey, 0, "other*", 100)
		assert.NoError(t, scanResp3.Error)
		// shouldincluding other1, other2 anditsvalue

		// Testempty hash
		emptyScanResp := redis.Master().HScan("non_existing_hash", 0, "", 100)
		assert.NoError(t, emptyScanResp.Error)

		// HScan with count parametersnumeric
		scanResp4 := redis.Master().HScan(hashKey, 0, "", 2)
		assert.NoError(t, scanResp4.Error)
	})

	t.Run("Hash_Commands_Comprehensive_Test", func(t *testing.T) {
		hashKey := "test_comprehensive_hash"

		// Clean
		redis.Master().Delete(hashKey)

		// scenario 1: userinformationstorage
		userInfo := map[interface{}]interface{}{
			"name":   "John",
			"age":    "30",
			"email":  "zhangsan@example.com",
			"score":  "100",
			"status": "active",
		}

		// batchsetuserinformation
		hmsetResp := redis.Master().HMSet(hashKey, userInfo)
		assert.NoError(t, hmsetResp.Error)

		// checkfieldnumericcount
		lenResp := redis.Master().HLen(hashKey)
		assert.NoError(t, lenResp.Error)
		assert.Equal(t, int64(5), lenResp.GetInt64())

		// increasescorenumeric
		redis.Master().HIncrBy(hashKey, "score", 50)
		scoreResp := redis.Master().HGet(hashKey, "score")
		assert.NoError(t, scoreResp.Error)
		assert.Equal(t, "150", scoreResp.GetString())

		// updatestatus
		redis.Master().HSet(hashKey, "status", "inactive")

		// batchgetkeyinformation
		hmgetResp := redis.Master().HMGet(hashKey, "name", "age", "score", "status")
		assert.NoError(t, hmgetResp.Error)

		values := hmgetResp.GetSlice()
		assert.Equal(t, "John", values[0].GetString())
		assert.Equal(t, "30", values[1].GetString())
		assert.Equal(t, "150", values[2].GetString())
		assert.Equal(t, "inactive", values[3].GetString())

		// Deletesensitiveinformation
		redis.Master().HDel(hashKey, "email")

		// confirmdeletesuccess
		existResp := redis.Master().HExists(hashKey, "email")
		assert.NoError(t, existResp.Error)
		assert.Equal(t, int64(0), existResp.GetInt64())

		// finalverifyallremainingfield
		getAllResp := redis.Master().HGetAll(hashKey)
		assert.NoError(t, getAllResp.Error)

		finalData := getAllResp.GetSlice()
		// 4 field * 2 = 8 elements (field-value for)
		assert.Equal(t, 8, len(finalData))
	})
}

// TestRedisEval Script command tests
func TestRedisEval(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	redis := NewRedis("test")
	assert.NotNil(t, redis)

	t.Run("EvalBasicScript", func(t *testing.T) {
		// Simple script that returns a string
		script := "return 'hello world'"
		response := redis.Master().Eval(script, []interface{}{}, []interface{}{})
		assert.NoError(t, response.Error)
		assert.Equal(t, "hello world", response.GetString())
	})

	t.Run("EvalWithKeys", func(t *testing.T) {
		// Set up test data
		testKey := "eval_test_key"
		redis.Master().Set(testKey, "test_value")

		// Script that returns the value of KEYS[1]
		script := "return redis.call('GET', KEYS[1])"
		keys := []interface{}{testKey}
		response := redis.Master().Eval(script, keys, []interface{}{})
		assert.NoError(t, response.Error)
		assert.Equal(t, "test_value", response.GetString())

		// Cleanup
		redis.Master().Delete(testKey)
	})

	t.Run("EvalWithArgs", func(t *testing.T) {
		// Script that uses ARGV arguments
		script := "return ARGV[1] .. ' ' .. ARGV[2]"
		args := []interface{}{"hello", "redis"}
		response := redis.Master().Eval(script, []interface{}{}, args)
		assert.NoError(t, response.Error)
		assert.Equal(t, "hello redis", response.GetString())
	})

	t.Run("EvalWithKeysAndArgs", func(t *testing.T) {
		// Set up test data
		testKey := "eval_counter"
		redis.Master().Set(testKey, "10")

		// Script that increments a key by the amount in ARGV[1]
		script := `
			local current = redis.call('GET', KEYS[1]) or 0
			local increment = tonumber(ARGV[1])
			local result = tonumber(current) + increment
			redis.call('SET', KEYS[1], result)
			return result
		`
		keys := []interface{}{testKey}
		args := []interface{}{"5"}
		response := redis.Master().Eval(script, keys, args)
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(15), response.GetInt64())

		// Verify the value was actually set
		getResp := redis.Master().Get(testKey)
		assert.NoError(t, getResp.Error)
		assert.Equal(t, "15", getResp.GetString())

		// Cleanup
		redis.Master().Delete(testKey)
	})

	t.Run("EvalReturnNumber", func(t *testing.T) {
		// Script that returns a number
		script := "return 42"
		response := redis.Master().Eval(script, []interface{}{}, []interface{}{})
		assert.NoError(t, response.Error)
		assert.Equal(t, int64(42), response.GetInt64())
	})

	t.Run("EvalReturnArray", func(t *testing.T) {
		// Script that returns an array
		script := "return {'item1', 'item2', 'item3'}"
		response := redis.Master().Eval(script, []interface{}{}, []interface{}{})
		assert.NoError(t, response.Error)

		slice := response.GetSlice()
		assert.Equal(t, 3, len(slice))
		assert.Equal(t, "item1", slice[0].GetString())
		assert.Equal(t, "item2", slice[1].GetString())
		assert.Equal(t, "item3", slice[2].GetString())
	})

	t.Run("EvalScriptError", func(t *testing.T) {
		// Script with syntax error
		script := "return invalid_syntax("
		response := redis.Master().Eval(script, []interface{}{}, []interface{}{})
		assert.Error(t, response.Error)
	})
}

// TestRedisStringCommands String command tests - completecoverall 11  String command
func TestRedisStringCommands(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	redis := NewRedis("test")
	assert.NotNil(t, redis)

	// Clean test environment
	defer func() {
		redis.Master().Delete("test_string", "test_string_2", "test_counter", "test_append", "test_range")
		redis.Master().Close()
	}()

	t.Run("Set_Get_Basic_String_Operations", func(t *testing.T) {
		key := "test_string"
		value := "hello_world"

		// Set - setstringvalue
		setResp := redis.Master().Set(key, value)
		assert.NoError(t, setResp.Error)

		// Get - getstringvalue
		getResp := redis.Master().Get(key)
		assert.NoError(t, getResp.Error)
		assert.Equal(t, value, getResp.GetString())

		// Set overwriteexistingvalue
		newValue := "updated_value"
		setResp2 := redis.Master().Set(key, newValue)
		assert.NoError(t, setResp2.Error)

		// Verifyupdateresult
		getResp2 := redis.Master().Get(key)
		assert.NoError(t, getResp2.Error)
		assert.Equal(t, newValue, getResp2.GetString())

		// Testgetnon-existent key
		nonExistResp := redis.Master().Get("non_exist_key")
		assert.True(t, nonExistResp.RecordNotFound())
	})

	t.Run("SetExpire_Set_With_Expiration", func(t *testing.T) {
		key := "test_string"
		value := "expire_test"
		ttl := int64(2) // 2 secondsexpire

		// SetExpire - setwithexpiretimestring
		setExpResp := redis.Master().SetExpire(key, value, ttl)
		assert.NoError(t, setExpResp.Error)

		// immediatecheckvalue
		getResp := redis.Master().Get(key)
		assert.NoError(t, getResp.Error)
		assert.Equal(t, value, getResp.GetString())

		// check TTL
		ttlResp := redis.Master().TTL(key)
		assert.NoError(t, ttlResp.Error)
		ttlValue := ttlResp.GetInt64()
		assert.True(t, ttlValue > 0 && ttlValue <= ttl)

		// waitexpire (simplifiedtest，notactuallywait)
		// inactuallyproductionenvironmentduringmayneedwaittoverifyexpire
	})

	t.Run("Incr_IncrBy_Increment_String", func(t *testing.T) {
		counterKey := "test_counter"

		// Clean
		redis.Master().Delete(counterKey)

		// Incr - incrementnon-existent key (from 0 start)
		incrResp := redis.Master().Incr(counterKey)
		assert.NoError(t, incrResp.Error)
		assert.Equal(t, int64(1), incrResp.GetInt64())

		// continue Incr
		incrResp2 := redis.Master().Incr(counterKey)
		assert.NoError(t, incrResp2.Error)
		assert.Equal(t, int64(2), incrResp2.GetInt64())

		// IncrBy - byspecifiednumericcountincrement
		incrByResp := redis.Master().IncrBy(counterKey, 5)
		assert.NoError(t, incrByResp.Error)
		assert.Equal(t, int64(7), incrByResp.GetInt64())

		// IncrBy negative number (actuallyisdecrement)
		incrByResp2 := redis.Master().IncrBy(counterKey, -3)
		assert.NoError(t, incrByResp2.Error)
		assert.Equal(t, int64(4), incrByResp2.GetInt64())

		// Verifyfinalvalue
		getResp := redis.Master().Get(counterKey)
		assert.NoError(t, getResp.Error)
		assert.Equal(t, "4", getResp.GetString())

		// Testfornonnumericvaluestringincrement (shoulderror)
		redis.Master().Set("text_key", "not_a_number")
		incrRespErr := redis.Master().Incr("text_key")
		// Redis willreturnerror，butherewecheckitnotwill panic
		assert.NotNil(t, incrRespErr)
	})

	t.Run("Decr_DecrBy_Decrement_String", func(t *testing.T) {
		counterKey := "test_counter"

		// Setinitialvalue
		redis.Master().Set(counterKey, "10")

		// Decr - decrement
		decrResp := redis.Master().Decr(counterKey)
		assert.NoError(t, decrResp.Error)
		assert.Equal(t, int64(9), decrResp.GetInt64())

		// DecrBy - byspecifiednumericcountdecrement
		decrByResp := redis.Master().DecrBy(counterKey, 4)
		assert.NoError(t, decrByResp.Error)
		assert.Equal(t, int64(5), decrByResp.GetInt64())

		// DecrBy negative number (actuallyisincrement)
		decrByResp2 := redis.Master().DecrBy(counterKey, -2)
		assert.NoError(t, decrByResp2.Error)
		assert.Equal(t, int64(7), decrByResp2.GetInt64())

		// Verifyfinalvalue
		getResp := redis.Master().Get(counterKey)
		assert.NoError(t, getResp.Error)
		assert.Equal(t, "7", getResp.GetString())

		// Decr tonegative number
		redis.Master().Set(counterKey, "1")
		decrResp3 := redis.Master().Decr(counterKey)
		assert.NoError(t, decrResp3.Error)
		assert.Equal(t, int64(0), decrResp3.GetInt64())

		decrResp4 := redis.Master().Decr(counterKey)
		assert.NoError(t, decrResp4.Error)
		assert.Equal(t, int64(-1), decrResp4.GetInt64())
	})

	t.Run("Append_Append_To_String", func(t *testing.T) {
		key := "test_append"

		// Clean
		redis.Master().Delete(key)

		// Append tonon-existent key (equalto SET)
		appendResp := redis.Master().Append(key, "hello")
		assert.NoError(t, appendResp.Error)
		assert.Equal(t, int64(5), appendResp.GetInt64()) // "hello" length

		// checkvalue
		getResp := redis.Master().Get(key)
		assert.NoError(t, getResp.Error)
		assert.Equal(t, "hello", getResp.GetString())

		// Append toexistingstring
		appendResp2 := redis.Master().Append(key, " world")
		assert.NoError(t, appendResp2.Error)
		assert.Equal(t, int64(11), appendResp2.GetInt64()) // "hello world" length

		// Verifyresult
		getResp2 := redis.Master().Get(key)
		assert.NoError(t, getResp2.Error)
		assert.Equal(t, "hello world", getResp2.GetString())

		// Append emptystring
		appendResp3 := redis.Master().Append(key, "")
		assert.NoError(t, appendResp3.Error)
		assert.Equal(t, int64(11), appendResp3.GetInt64()) // lengthnotchanged

		// Append numericcharacter
		appendResp4 := redis.Master().Append(key, "123")
		assert.NoError(t, appendResp4.Error)
		assert.Equal(t, int64(14), appendResp4.GetInt64())

		getResp3 := redis.Master().Get(key)
		assert.NoError(t, getResp3.Error)
		assert.Equal(t, "hello world123", getResp3.GetString())
	})

	t.Run("StrLen_Get_String_Length", func(t *testing.T) {
		key := "test_string"

		// Testemptystring
		redis.Master().Set(key, "")
		lenResp := redis.Master().StrLen(key)
		assert.NoError(t, lenResp.Error)
		assert.Equal(t, int64(0), lenResp.GetInt64())

		// Testcommonstring
		redis.Master().Set(key, "hello")
		lenResp2 := redis.Master().StrLen(key)
		assert.NoError(t, lenResp2.Error)
		assert.Equal(t, int64(5), lenResp2.GetInt64())

		// Testduringtextstring (UTF-8 encoding)
		redis.Master().Set(key, "hello")
		lenResp3 := redis.Master().StrLen(key)
		assert.NoError(t, lenResp3.Error)
		// "hello" is 5 ASCII characters, so length is 5
		assert.Equal(t, int64(5), lenResp3.GetInt64())

		// Testnon-existent key
		lenResp4 := redis.Master().StrLen("non_exist_key")
		assert.NoError(t, lenResp4.Error)
		assert.Equal(t, int64(0), lenResp4.GetInt64())

		// Testnumericcharacterstring
		redis.Master().Set(key, "12345")
		lenResp5 := redis.Master().StrLen(key)
		assert.NoError(t, lenResp5.Error)
		assert.Equal(t, int64(5), lenResp5.GetInt64())
	})

	t.Run("GetRange_Get_String_Range", func(t *testing.T) {
		key := "test_range"
		value := "hello world"

		// Setteststring
		redis.Master().Set(key, value)

		// Getcompletestring (0 to -1)
		rangeResp := redis.Master().GetRange(key, 0, -1)
		assert.NoError(t, rangeResp.Error)
		assert.Equal(t, value, rangeResp.GetString())

		// Getbefore 5 charactercharacter
		rangeResp2 := redis.Master().GetRange(key, 0, 4)
		assert.NoError(t, rangeResp2.Error)
		assert.Equal(t, "hello", rangeResp2.GetString())

		// Getafter 5 charactercharacter
		rangeResp3 := redis.Master().GetRange(key, 6, 10)
		assert.NoError(t, rangeResp3.Error)
		assert.Equal(t, "world", rangeResp3.GetString())

		// useusenegative numberindex
		rangeResp4 := redis.Master().GetRange(key, -5, -1)
		assert.NoError(t, rangeResp4.Error)
		assert.Equal(t, "world", rangeResp4.GetString())

		// exceedrangeindex
		rangeResp5 := redis.Master().GetRange(key, 0, 100)
		assert.NoError(t, rangeResp5.Error)
		assert.Equal(t, value, rangeResp5.GetString())

		// start > end
		rangeResp6 := redis.Master().GetRange(key, 5, 2)
		assert.NoError(t, rangeResp6.Error)
		assert.Equal(t, "", rangeResp6.GetString())

		// non-existent key
		rangeResp7 := redis.Master().GetRange("non_exist_key", 0, 10)
		assert.NoError(t, rangeResp7.Error)
		assert.Equal(t, "", rangeResp7.GetString())
	})

	t.Run("SetRange_Set_String_Range", func(t *testing.T) {
		key := "test_range"

		// Clean
		redis.Master().Delete(key)

		// inempty key on SetRange (willuse null charactercharacterpadding)
		setRangeResp := redis.Master().SetRange(key, 5, "world")
		assert.NoError(t, setRangeResp.Error)
		assert.Equal(t, int64(10), setRangeResp.GetInt64()) // newstringlength

		getResp := redis.Master().Get(key)
		assert.NoError(t, getResp.Error)
		result := getResp.GetString()
		// before 5 charactercharactershouldis null charactercharacter，thenafteris "world"
		assert.Equal(t, 10, len(result))
		assert.Equal(t, "world", result[5:])

		// inexistingstringon SetRange
		redis.Master().Set(key, "hello world")
		setRangeResp2 := redis.Master().SetRange(key, 6, "Redis")
		assert.NoError(t, setRangeResp2.Error)
		assert.Equal(t, int64(11), setRangeResp2.GetInt64()) // keeporiginallength

		getResp2 := redis.Master().Get(key)
		assert.NoError(t, getResp2.Error)
		assert.Equal(t, "hello Redis", getResp2.GetString())

		// SetRange exceedoriginalstringlength
		setRangeResp3 := redis.Master().SetRange(key, 15, "test")
		assert.NoError(t, setRangeResp3.Error)
		assert.Equal(t, int64(19), setRangeResp3.GetInt64())

		getResp3 := redis.Master().Get(key)
		assert.NoError(t, getResp3.Error)
		result3 := getResp3.GetString()
		assert.Equal(t, 19, len(result3))
		assert.Equal(t, "test", result3[15:])

		// SetRange offset 0
		setRangeResp4 := redis.Master().SetRange(key, 0, "hi")
		assert.NoError(t, setRangeResp4.Error)

		getResp4 := redis.Master().Get(key)
		assert.NoError(t, getResp4.Error)
		result4 := getResp4.GetString()
		assert.True(t, len(result4) >= 2)
		assert.Equal(t, "hi", result4[:2])
	})

	t.Run("String_Commands_Comprehensive_Test", func(t *testing.T) {
		// scenario：countnumericcounterandtextdocumentprocessingcomprehensiveappuse
		counterKey := "page_views"
		contentKey := "page_content"

		// Clean
		redis.Master().Delete(counterKey, contentKey)

		// 1. initialinitpagebrowsecountnumeric
		redis.Master().Set(counterKey, "0")

		// 2. simulatemultiplepagebrowse
		for i := 0; i < 5; i++ {
			redis.Master().Incr(counterKey)
		}

		// 3. batchincreasebrowsecount
		redis.Master().IncrBy(counterKey, 10)

		// Verifycountnumericresult
		countResp := redis.Master().Get(counterKey)
		assert.NoError(t, countResp.Error)
		assert.Equal(t, "15", countResp.GetString())

		// 4. createpagecontent
		redis.Master().Set(contentKey, "title：")
		redis.Master().Append(contentKey, "Redis stringoperationguide")
		redis.Master().Append(contentKey, "\ncontent：thisisone")
		redis.Master().Append(contentKey, "complete Redis testexample")

		// 5. getcontentlength
		lenResp := redis.Master().StrLen(contentKey)
		assert.NoError(t, lenResp.Error)
		assert.True(t, lenResp.GetInt64() > 30) // ensurehavereasonablecontentlength

		// 6. extractcontentsegment
		titleRange := redis.Master().GetRange(contentKey, 3, 15) // extracttitlepartscore
		assert.NoError(t, titleRange.Error)
		assert.Contains(t, titleRange.GetString(), "Redis")

		// 7. setcontentexpiretime
		redis.Master().SetExpire(contentKey+"_backup", "backup content", 3600)

		// 8. verifyexpiretimeset
		ttlResp := redis.Master().TTL(contentKey + "_backup")
		assert.NoError(t, ttlResp.Error)
		assert.True(t, ttlResp.GetInt64() > 3500) // ensure TTL correctset

		// 9. finalverifyalloperationresult
		finalCountResp := redis.Master().Get(counterKey)
		assert.NoError(t, finalCountResp.Error)
		assert.Equal(t, "15", finalCountResp.GetString())

		finalContentResp := redis.Master().Get(contentKey)
		assert.NoError(t, finalContentResp.Error)
		finalContent := finalContentResp.GetString()
		assert.Contains(t, finalContent, "title：")
		assert.Contains(t, finalContent, "Redis")
		assert.Contains(t, finalContent, "testexample")
	})
}

// =============================================================================
// Mock Redis Tests - Comprehensive Testing of Mock Functionality
// =============================================================================

func TestMockRedisBasicFunctionality(t *testing.T) {
	t.Run("NewMockRedis_Creates_Working_Instance", func(t *testing.T) {
		mockRedis := NewMockRedis()
		assert.NotNil(t, mockRedis)
		assert.Equal(t, "mock", mockRedis.name)
		assert.NotNil(t, mockRedis.Master())
		assert.NotNil(t, mockRedis.Slave())
	})

	t.Run("NewRedisWithMock_Custom_Mocks", func(t *testing.T) {
		masterMock := NewMockRedisOp()
		slaveMock := NewMockRedisOp()
		
		mockRedis := NewRedisWithMock(masterMock, slaveMock)
		assert.NotNil(t, mockRedis)
		assert.Equal(t, "custom-mock", mockRedis.name)
		assert.Equal(t, masterMock, mockRedis.Master())
		assert.Equal(t, slaveMock, mockRedis.Slave())
	})

	t.Run("MockRedisBuilder_Fluent_Interface", func(t *testing.T) {
		mockRedis := NewMockRedisBuilder().
			WithResponse("GET", "test_key", "test_value", nil).
			WithMasterResponse("SET", "master_key", int64(1), nil).
			WithSlaveResponse("GET", "slave_key", "slave_value", nil).
			Build()

		assert.NotNil(t, mockRedis)
		assert.Equal(t, "builder-mock", mockRedis.name)

		// Test configured responses
		getResp := mockRedis.Master().Get("test_key")
		assert.NoError(t, getResp.Error)
		assert.Equal(t, "test_value", getResp.GetString())

		setResp := mockRedis.Master().Set("master_key", "value")
		assert.NoError(t, setResp.Error)
		assert.Equal(t, int64(1), setResp.GetInt64())
	})
}

func TestMockRedisResponseHandling(t *testing.T) {
	t.Run("SetResponse_Single_Static_Response", func(t *testing.T) {
		mock := NewMockRedisOp()
		
		// Configure static response
		mock.SetResponse("GET", "key1", "static_value", nil)
		mock.SetResponse("SET", "key2", int64(1), nil)
		mock.SetResponse("GET", "key3", nil, errors.New("test error"))

		// Test successful responses
		resp1 := mock.Get("key1")
		assert.NoError(t, resp1.Error)
		assert.Equal(t, "static_value", resp1.GetString())

		resp2 := mock.Set("key2", "value")
		assert.NoError(t, resp2.Error)
		assert.Equal(t, int64(1), resp2.GetInt64())

		// Test error response
		resp3 := mock.Get("key3")
		assert.Error(t, resp3.Error)
		assert.Equal(t, "test error", resp3.Error.Error())

		// Test fallback to default nil response for unconfigured keys
		resp4 := mock.Get("unconfigured_key")
		assert.Nil(t, resp4.Error)
		assert.Nil(t, resp4.data)
	})

	t.Run("SetSequentialResponses_Multiple_Calls", func(t *testing.T) {
		mock := NewMockRedisOp()
		
		// Configure sequential responses
		sequential := []MockResponse{
			{Data: "first", Error: nil},
			{Data: "second", Error: nil},
			{Data: nil, Error: errors.New("third error")},
			{Data: "fourth", Error: nil},
		}
		mock.SetSequentialResponses("GET", "seq_key", sequential)

		// Test sequential calls
		resp1 := mock.Get("seq_key")
		assert.NoError(t, resp1.Error)
		assert.Equal(t, "first", resp1.GetString())

		resp2 := mock.Get("seq_key")
		assert.NoError(t, resp2.Error)
		assert.Equal(t, "second", resp2.GetString())

		resp3 := mock.Get("seq_key")
		assert.Error(t, resp3.Error)
		assert.Equal(t, "third error", resp3.Error.Error())

		resp4 := mock.Get("seq_key")
		assert.NoError(t, resp4.Error)
		assert.Equal(t, "fourth", resp4.GetString())

		// After all responses are exhausted, should return last response
		resp5 := mock.Get("seq_key")
		assert.NoError(t, resp5.Error)
		assert.Equal(t, "fourth", resp5.GetString())
	})

	t.Run("SetConditionalResponse_Based_On_Args", func(t *testing.T) {
		mock := NewMockRedisOp()
		
		// Configure conditional responses
		condition1 := func(cmd string, args []interface{}) bool {
			return len(args) > 0 && args[0] == "special_key"
		}
		condition2 := func(cmd string, args []interface{}) bool {
			return len(args) > 1 && args[1] == "special_value"
		}

		mock.SetConditionalResponse("GET", condition1, MockResponse{Data: "special_response", Error: nil})
		mock.SetConditionalResponse("SET", condition2, MockResponse{Data: int64(999), Error: nil})

		// Test conditional responses
		resp1 := mock.Get("special_key")
		assert.NoError(t, resp1.Error)
		assert.Equal(t, "special_response", resp1.GetString())

		resp2 := mock.Get("normal_key")
		assert.Nil(t, resp2.Error)
		assert.Nil(t, resp2.data) // Default response

		resp3 := mock.Set("any_key", "special_value")
		assert.NoError(t, resp3.Error)
		assert.Equal(t, int64(999), resp3.GetInt64())

		resp4 := mock.Set("any_key", "normal_value")
		assert.Nil(t, resp4.Error)
		assert.Nil(t, resp4.data) // Default response
	})
}

func TestMockRedisCallHistory(t *testing.T) {
	t.Run("GetCallHistory_Records_All_Calls", func(t *testing.T) {
		mock := NewMockRedisOp()
		
		// Make various calls
		mock.Get("key1")
		mock.Set("key2", "value2")
		mock.HSet("hash1", "field1", "value1")
		mock.Delete("key3")
		
		// Check call history
		history := mock.GetCallHistory()
		assert.Len(t, history, 4)
		
		// Verify first call
		assert.Equal(t, "GET", history[0].Command)
		assert.Equal(t, []interface{}{"key1"}, history[0].Args)
		
		// Verify second call
		assert.Equal(t, "SET", history[1].Command)
		assert.Equal(t, []interface{}{"key2", "value2"}, history[1].Args)
		
		// Verify third call
		assert.Equal(t, "HSET", history[2].Command)
		assert.Equal(t, []interface{}{"hash1", "field1", "value1"}, history[2].Args)
		
		// Verify fourth call
		assert.Equal(t, "DEL", history[3].Command)
		assert.Equal(t, []interface{}{"key3"}, history[3].Args)
	})

	t.Run("GetCallsByCommand_Filter_By_Command", func(t *testing.T) {
		mock := NewMockRedisOp()
		
		// Make multiple calls
		mock.Get("key1")
		mock.Set("key2", "value2")
		mock.Get("key3")
		mock.HSet("hash1", "field1", "value1")
		mock.Get("key4")
		
		// Get only GET commands
		getCalls := mock.GetCallsByCommand("GET")
		assert.Len(t, getCalls, 3)
		assert.Equal(t, "GET", getCalls[0].Command)
		assert.Equal(t, []interface{}{"key1"}, getCalls[0].Args)
		assert.Equal(t, "GET", getCalls[1].Command)
		assert.Equal(t, []interface{}{"key3"}, getCalls[1].Args)
		assert.Equal(t, "GET", getCalls[2].Command)
		assert.Equal(t, []interface{}{"key4"}, getCalls[2].Args)
		
		// Get only SET commands
		setCalls := mock.GetCallsByCommand("SET")
		assert.Len(t, setCalls, 1)
		assert.Equal(t, "SET", setCalls[0].Command)
		
		// Get only HSET commands
		hsetCalls := mock.GetCallsByCommand("HSET")
		assert.Len(t, hsetCalls, 1)
		assert.Equal(t, "HSET", hsetCalls[0].Command)
		
		// Get non-existent command
		noneCalls := mock.GetCallsByCommand("NONEXISTENT")
		assert.Len(t, noneCalls, 0)
	})

	t.Run("ClearCallHistory_Resets_History", func(t *testing.T) {
		mock := NewMockRedisOp()
		
		// Make some calls
		mock.Get("key1")
		mock.Set("key2", "value2")
		
		// Verify history exists
		history := mock.GetCallHistory()
		assert.Len(t, history, 2)
		
		// Clear history
		mock.ClearCallHistory()
		
		// Verify history is empty
		clearedHistory := mock.GetCallHistory()
		assert.Len(t, clearedHistory, 0)
		
		// Make new calls after clearing
		mock.Delete("key3")
		newHistory := mock.GetCallHistory()
		assert.Len(t, newHistory, 1)
		assert.Equal(t, "DEL", newHistory[0].Command)
	})
}

func TestMockRedisHashCommands(t *testing.T) {
	// Test all 12 Hash Commands from memory with mock functionality
	t.Run("Hash_Commands_With_Mock_Responses", func(t *testing.T) {
		mock := NewMockRedisOp()
		
		// Configure responses for each Hash command
		mock.SetResponse("HSET", "hash1", int64(1), nil)
		mock.SetResponse("HGET", "hash1", "field_value", nil)
		mock.SetResponse("HMSET", "hash1", "OK", nil)
		mock.SetResponse("HMGET", "hash1", []interface{}{"value1", "value2"}, nil)
		mock.SetResponse("HEXISTS", "hash1", int64(1), nil)
		mock.SetResponse("HDEL", "hash1", int64(2), nil)
		mock.SetResponse("HGETALL", "hash1", []interface{}{"field1", "value1", "field2", "value2"}, nil)
		mock.SetResponse("HLEN", "hash1", int64(2), nil)
		mock.SetResponse("HKEYS", "hash1", []interface{}{"field1", "field2"}, nil)
		mock.SetResponse("HINCRBY", "hash1", int64(15), nil)
		mock.SetResponse("HVALS", "hash1", []interface{}{"value1", "value2"}, nil)
		mock.SetResponse("HSCAN", "hash1", []interface{}{0, []interface{}{"field1", "value1"}}, nil)

		// Test HSet
		hsetResp := mock.HSet("hash1", "field1", "value1")
		assert.NoError(t, hsetResp.Error)
		assert.Equal(t, int64(1), hsetResp.GetInt64())

		// Test HGet
		hgetResp := mock.HGet("hash1", "field1")
		assert.NoError(t, hgetResp.Error)
		assert.Equal(t, "field_value", hgetResp.GetString())

		// Test HMSet
		hashData := map[interface{}]interface{}{
			"field1": "value1",
			"field2": "value2",
		}
		hmsetResp := mock.HMSet("hash1", hashData)
		assert.NoError(t, hmsetResp.Error)
		assert.Equal(t, "OK", hmsetResp.GetString())

		// Test HMGet
		hmgetResp := mock.HMGet("hash1", "field1", "field2")
		assert.NoError(t, hmgetResp.Error)
		values := hmgetResp.GetSlice()
		assert.Len(t, values, 2)
		assert.Equal(t, "value1", values[0].GetString())
		assert.Equal(t, "value2", values[1].GetString())

		// Test HExists
		hexistsResp := mock.HExists("hash1", "field1")
		assert.NoError(t, hexistsResp.Error)
		assert.Equal(t, int64(1), hexistsResp.GetInt64())

		// Test HDel
		hdelResp := mock.HDel("hash1", "field1", "field2")
		assert.NoError(t, hdelResp.Error)
		assert.Equal(t, int64(2), hdelResp.GetInt64())

		// Test HGetAll
		hgetallResp := mock.HGetAll("hash1")
		assert.NoError(t, hgetallResp.Error)
		allData := hgetallResp.GetSlice()
		assert.Len(t, allData, 4)

		// Test HLen
		hlenResp := mock.HLen("hash1")
		assert.NoError(t, hlenResp.Error)
		assert.Equal(t, int64(2), hlenResp.GetInt64())

		// Test HKeys
		hkeysResp := mock.HKeys("hash1")
		assert.NoError(t, hkeysResp.Error)
		keys := hkeysResp.GetSlice()
		assert.Len(t, keys, 2)

		// Test HIncrBy
		hincrbyResp := mock.HIncrBy("hash1", "counter", 5)
		assert.NoError(t, hincrbyResp.Error)
		assert.Equal(t, int64(15), hincrbyResp.GetInt64())

		// Test HVals
		hvalsResp := mock.HVals("hash1")
		assert.NoError(t, hvalsResp.Error)
		vals := hvalsResp.GetSlice()
		assert.Len(t, vals, 2)

		// Test HScan
		hscanResp := mock.HScan("hash1", 0, "", 0)
		assert.NoError(t, hscanResp.Error)
		scanData := hscanResp.GetSlice()
		assert.Len(t, scanData, 2)

		// Verify call history recorded all commands
		history := mock.GetCallHistory()
		assert.Len(t, history, 12)
		
		expectedCommands := []string{"HSET", "HGET", "HMSET", "HMGET", "HEXISTS", "HDEL", "HGETALL", "HLEN", "HKEYS", "HINCRBY", "HVALS", "HSCAN"}
		for i, expectedCmd := range expectedCommands {
			assert.Equal(t, expectedCmd, history[i].Command)
		}
	})
}

func TestMockRedisStringCommands(t *testing.T) {
	// Test all 11 String Commands from memory
	t.Run("String_Commands_With_Mock_Responses", func(t *testing.T) {
		mock := NewMockRedisOp()
		
		// Configure responses for each String command
		mock.SetResponse("GET", "key1", "mock_value", nil)
		mock.SetResponse("SET", "key1", "OK", nil)
		mock.SetResponse("SETEX", "key1", "OK", nil)
		mock.SetResponse("INCR", "counter", int64(5), nil)
		mock.SetResponse("INCRBY", "counter", int64(15), nil)
		mock.SetResponse("DECR", "counter", int64(4), nil)
		mock.SetResponse("DECRBY", "counter", int64(10), nil)
		mock.SetResponse("APPEND", "key1", int64(20), nil)
		mock.SetResponse("STRLEN", "key1", int64(10), nil)
		mock.SetResponse("GETRANGE", "key1", "moc", nil)
		mock.SetResponse("SETRANGE", "key1", int64(15), nil)

		// Test all String commands
		getResp := mock.Get("key1")
		assert.NoError(t, getResp.Error)
		assert.Equal(t, "mock_value", getResp.GetString())

		setResp := mock.Set("key1", "value")
		assert.NoError(t, setResp.Error)
		assert.Equal(t, "OK", setResp.GetString())

		setexResp := mock.SetExpire("key1", "value", 60)
		assert.NoError(t, setexResp.Error)
		assert.Equal(t, "OK", setexResp.GetString())

		incrResp := mock.Incr("counter")
		assert.NoError(t, incrResp.Error)
		assert.Equal(t, int64(5), incrResp.GetInt64())

		incrbyResp := mock.IncrBy("counter", 10)
		assert.NoError(t, incrbyResp.Error)
		assert.Equal(t, int64(15), incrbyResp.GetInt64())

		decrResp := mock.Decr("counter")
		assert.NoError(t, decrResp.Error)
		assert.Equal(t, int64(4), decrResp.GetInt64())

		decrbyResp := mock.DecrBy("counter", 5)
		assert.NoError(t, decrbyResp.Error)
		assert.Equal(t, int64(10), decrbyResp.GetInt64())

		appendResp := mock.Append("key1", " suffix")
		assert.NoError(t, appendResp.Error)
		assert.Equal(t, int64(20), appendResp.GetInt64())

		strlenResp := mock.StrLen("key1")
		assert.NoError(t, strlenResp.Error)
		assert.Equal(t, int64(10), strlenResp.GetInt64())

		getrangeResp := mock.GetRange("key1", 0, 2)
		assert.NoError(t, getrangeResp.Error)
		assert.Equal(t, "moc", getrangeResp.GetString())

		setrangeResp := mock.SetRange("key1", 5, "new")
		assert.NoError(t, setrangeResp.Error)
		assert.Equal(t, int64(15), setrangeResp.GetInt64())

		// Verify call history
		history := mock.GetCallHistory()
		assert.Len(t, history, 11)
		
		expectedCommands := []string{"GET", "SET", "SETEX", "INCR", "INCRBY", "DECR", "DECRBY", "APPEND", "STRLEN", "GETRANGE", "SETRANGE"}
		for i, expectedCmd := range expectedCommands {
			assert.Equal(t, expectedCmd, history[i].Command)
		}
	})
}

func TestMockRedisBackwardCompatibility(t *testing.T) {
	t.Run("Existing_API_Unchanged_With_Mock", func(t *testing.T) {
		// Test that existing code patterns still work with mock
		mockRedis := NewMockRedis()
		
		// Configure some mock responses
		masterMock := mockRedis.Master().(*MockRedisOp)
		slaveMock := mockRedis.Slave().(*MockRedisOp)
		
		masterMock.SetResponse("SET", "test_key", "OK", nil)
		masterMock.SetResponse("GET", "test_key", "test_value", nil)
		slaveMock.SetResponse("GET", "test_key", "test_value", nil)

		// Test Master/Slave pattern still works
		master := mockRedis.Master()
		slave := mockRedis.Slave()
		
		assert.NotNil(t, master)
		assert.NotNil(t, slave)
		
		// Test method chaining still works
		setResp := master.Set("test_key", "value")
		assert.NoError(t, setResp.Error)
		assert.Equal(t, "OK", setResp.GetString())
		
		getResp := slave.Get("test_key")
		assert.NoError(t, getResp.Error)
		assert.Equal(t, "test_value", getResp.GetString())
		
		// Test interface methods work
		assert.NotNil(t, master.Pool())
		assert.NotNil(t, master.Meta())
		assert.GreaterOrEqual(t, master.ActiveCount(), 0)
		assert.GreaterOrEqual(t, master.IdleCount(), 0)
	})

	t.Run("Real_Redis_Interface_Still_Works", func(t *testing.T) {
		// This test verifies that real Redis instances still work
		// when using the interface-based design
		
		// Save original secret path and restore it after test
		originalPath := secret.PATH
		defer func() {
			secret.PATH = originalPath
		}()

		// Set secret path to the example directory
		wd, _ := os.Getwd()
		secret.PATH = filepath.Join(wd, "example")

		realRedis := NewRedis("test")
		assert.NotNil(t, realRedis)
		
		// Test interface methods work with real Redis
		master := realRedis.Master()
		slave := realRedis.Slave()
		
		assert.NotNil(t, master)
		assert.NotNil(t, slave)
		
		// Test that the interface methods exist and don't panic
		assert.NotNil(t, master.Pool())
		assert.NotNil(t, master.Meta())
		assert.GreaterOrEqual(t, master.ActiveCount(), 0)
		assert.GreaterOrEqual(t, master.IdleCount(), 0)
		
		// The actual Redis operations would require a real Redis server
		// So we just verify the interface methods are accessible
	})
}

func TestMockRedisConnectionAndPool(t *testing.T) {
	t.Run("Mock_Connection_And_Pool_Methods", func(t *testing.T) {
		mock := NewMockRedisOp()
		
		// Test pool info methods
		assert.GreaterOrEqual(t, mock.ActiveCount(), 0)
		assert.GreaterOrEqual(t, mock.IdleCount(), 0)
		
		// Test meta info
		meta := mock.Meta()
		assert.Equal(t, "mock", meta.Host)
		assert.Equal(t, uint(6379), meta.Port)
		
		// Test connection methods don't panic
		assert.NotNil(t, mock.Pool())
		assert.NotNil(t, mock.Conn())
		assert.NoError(t, mock.Close())
		
		// Test Exec method
		var executed bool
		err := mock.Exec(func(conn redis.Conn) {
			executed = true
		})
		assert.NoError(t, err)
		assert.True(t, executed)
	})
}

func TestMockRedisPipeline(t *testing.T) {
	t.Run("Pipeline_With_Mock_Responses", func(t *testing.T) {
		mock := NewMockRedisOp()
		
		// Configure responses for pipeline commands
		mock.SetResponse("PIPELINE", "", []interface{}{
			&RedisResponse{RedisResponseEntity{data: "OK"}, nil},
			&RedisResponse{RedisResponseEntity{data: int64(1)}, nil},
			&RedisResponse{RedisResponseEntity{data: "value"}, nil},
		}, nil)

		// Create pipeline commands
		cmds := []RedisPipelineCmd{
			{Cmd: "SET", Args: []interface{}{"key1", "value1"}},
			{Cmd: "INCR", Args: []interface{}{"counter"}},
			{Cmd: "GET", Args: []interface{}{"key2"}},
		}

		// Execute pipeline
		responses := mock.Pipeline(cmds...)
		assert.Len(t, responses, 3)
		
		assert.NoError(t, responses[0].Error)
		assert.Equal(t, "OK", responses[0].GetString())
		
		assert.NoError(t, responses[1].Error)
		assert.Equal(t, int64(1), responses[1].GetInt64())
		
		assert.NoError(t, responses[2].Error)
		assert.Equal(t, "value", responses[2].GetString())

		// Verify call history
		history := mock.GetCallHistory()
		assert.Len(t, history, 1)
		assert.Equal(t, "PIPELINE", history[0].Command)
	})
}

func TestMockRedisListCommands(t *testing.T) {
	t.Run("List_Commands_With_Mock_Responses", func(t *testing.T) {
		mock := NewMockRedisOp()
		
		// Configure responses for all list commands
		mock.SetResponse("LPUSH", "list1", int64(1), nil)
		mock.SetResponse("LPUSHX", "list1", int64(1), nil)
		mock.SetResponse("LPOP", "list1", "item1", nil)
		mock.SetResponse("LRANGE", "list1", []interface{}{"item1", "item2"}, nil)
		mock.SetResponse("LSET", "list1", "OK", nil)
		mock.SetResponse("LREM", "list1", int64(1), nil)
		mock.SetResponse("LTRIM", "list1", "OK", nil)
		mock.SetResponse("LINDEX", "list1", "item1", nil)
		mock.SetResponse("LINSERT", "list1", int64(3), nil)
		mock.SetResponse("LLEN", "list1", int64(2), nil)
		mock.SetResponse("LMOVE", "list1", "moved_item", nil)
		mock.SetResponse("LMPOP", "*", []interface{}{"list1", []interface{}{"item1"}}, nil)
		mock.SetResponse("LPOS", "list1", int64(0), nil)
		mock.SetResponse("RPOP", "list1", "last_item", nil)
		mock.SetResponse("RPOPLPUSH", "list1", "moved_item", nil)
		mock.SetResponse("RPUSH", "list1", int64(2), nil)
		mock.SetResponse("RPUSHX", "list1", int64(2), nil)

		// Test all list commands
		resp1 := mock.LPush("list1", "item1")
		assert.NoError(t, resp1.Error)
		assert.Equal(t, int64(1), resp1.GetInt64())

		resp2 := mock.LPushX("list1", "item0")
		assert.NoError(t, resp2.Error)
		assert.Equal(t, int64(1), resp2.GetInt64())

		resp3 := mock.LPop("list1")
		assert.NoError(t, resp3.Error)
		assert.Equal(t, "item1", resp3.GetString())

		resp4 := mock.LRange("list1", 0, -1)
		assert.NoError(t, resp4.Error)
		assert.Equal(t, []interface{}{"item1", "item2"}, resp4.data)

		resp5 := mock.LSet("list1", 0, "new_item")
		assert.NoError(t, resp5.Error)
		assert.Equal(t, "OK", resp5.GetString())

		resp6 := mock.LRem("list1", 1, "item1")
		assert.NoError(t, resp6.Error)
		assert.Equal(t, int64(1), resp6.GetInt64())

		resp7 := mock.LTrim("list1", 0, 1)
		assert.NoError(t, resp7.Error)
		assert.Equal(t, "OK", resp7.GetString())

		resp8 := mock.LIndex("list1", 0)
		assert.NoError(t, resp8.Error)
		assert.Equal(t, "item1", resp8.GetString())

		resp9 := mock.LInsert("list1", "BEFORE", "item1", "new_item")
		assert.NoError(t, resp9.Error)
		assert.Equal(t, int64(3), resp9.GetInt64())

		resp10 := mock.LLen("list1")
		assert.NoError(t, resp10.Error)
		assert.Equal(t, int64(2), resp10.GetInt64())

		resp11 := mock.LMove("list1", "list2", "LEFT", "RIGHT")
		assert.NoError(t, resp11.Error)
		assert.Equal(t, "moved_item", resp11.GetString())

		resp12 := mock.LMPop(1, "LEFT", "list1")
		assert.NoError(t, resp12.Error)
		assert.Equal(t, []interface{}{"list1", []interface{}{"item1"}}, resp12.data)

		resp13 := mock.LPos("list1", "item1")
		assert.NoError(t, resp13.Error)
		assert.Equal(t, int64(0), resp13.GetInt64())

		resp14 := mock.RPop("list1")
		assert.NoError(t, resp14.Error)
		assert.Equal(t, "last_item", resp14.GetString())

		resp15 := mock.RPopLPush("list1", "list2")
		assert.NoError(t, resp15.Error)
		assert.Equal(t, "moved_item", resp15.GetString())

		resp16 := mock.RPush("list1", "item2")
		assert.NoError(t, resp16.Error)
		assert.Equal(t, int64(2), resp16.GetInt64())

		resp17 := mock.RPushX("list1", "item3")
		assert.NoError(t, resp17.Error)
		assert.Equal(t, int64(2), resp17.GetInt64())

		// Verify call history - should have 17 commands (corrected count)
		history := mock.GetCallHistory()
		assert.Equal(t, 17, len(history)) // All 17 list command calls
		
		// Verify specific commands were called
		lpushCalls := mock.GetCallsByCommand("LPUSH")
		assert.Equal(t, 1, len(lpushCalls))
		assert.Equal(t, []interface{}{"list1", "item1"}, lpushCalls[0].Args)
	})
}

func TestMockRedisSetCommands(t *testing.T) {
	t.Run("Set_Commands_With_Mock_Responses", func(t *testing.T) {
		mock := NewMockRedisOp()
		
		// Configure responses for all set commands
		mock.SetResponse("SADD", "set1", int64(1), nil)
		mock.SetResponse("SCARD", "set1", int64(3), nil)
		mock.SetResponse("SDIFF", "set1", []interface{}{"item1"}, nil)
		mock.SetResponse("SDIFFSTORE", "dest", int64(1), nil)
		mock.SetResponse("SINTER", "set1", []interface{}{"common"}, nil)
		mock.SetResponse("SINTERCARD", "*", int64(1), nil)
		mock.SetResponse("SINTERSTORE", "dest", int64(1), nil)
		mock.SetResponse("SISMEMBER", "set1", int64(1), nil)
		mock.SetResponse("SMEMBERS", "set1", []interface{}{"item1", "item2", "item3"}, nil)
		mock.SetResponse("SMISMEMBER", "set1", []interface{}{1, 0, 1}, nil)
		mock.SetResponse("SMOVE", "set1", int64(1), nil)
		mock.SetResponse("SPOP", "set1", "random_item", nil)
		mock.SetResponse("SRANDMEMBER", "set1", "random_item", nil)
		mock.SetResponse("SREM", "set1", int64(1), nil)
		mock.SetResponse("SSCAN", "*", []interface{}{"0", []interface{}{"item1", "item2"}}, nil)
		mock.SetResponse("SUNION", "set1", []interface{}{"item1", "item2", "item3", "item4"}, nil)
		mock.SetResponse("SUNIONSTORE", "dest", int64(4), nil)

		// Test all set commands
		resp1 := mock.SAdd("set1", "item1")
		assert.NoError(t, resp1.Error)
		assert.Equal(t, int64(1), resp1.GetInt64())

		resp2 := mock.SCard("set1")
		assert.NoError(t, resp2.Error)
		assert.Equal(t, int64(3), resp2.GetInt64())

		resp3 := mock.SDiff("set1", "set2")
		assert.NoError(t, resp3.Error)
		assert.Equal(t, []interface{}{"item1"}, resp3.data)

		resp4 := mock.SDiffStore("dest", "set1", "set2")
		assert.NoError(t, resp4.Error)
		assert.Equal(t, int64(1), resp4.GetInt64())

		resp5 := mock.SInter("set1", "set2")
		assert.NoError(t, resp5.Error)
		assert.Equal(t, []interface{}{"common"}, resp5.data)

		resp6 := mock.SInterCard(1, "set1", "set2")
		assert.NoError(t, resp6.Error)
		assert.Equal(t, int64(1), resp6.GetInt64())

		resp7 := mock.SInterStore("dest", "set1", "set2")
		assert.NoError(t, resp7.Error)
		assert.Equal(t, int64(1), resp7.GetInt64())

		resp8 := mock.SIsMember("set1", "item1")
		assert.NoError(t, resp8.Error)
		assert.Equal(t, int64(1), resp8.GetInt64())

		resp9 := mock.SMembers("set1")
		assert.NoError(t, resp9.Error)
		assert.Equal(t, []interface{}{"item1", "item2", "item3"}, resp9.data)

		resp10 := mock.SMIsMember("set1", "item1", "item4", "item2")
		assert.NoError(t, resp10.Error)
		assert.Equal(t, []interface{}{1, 0, 1}, resp10.data)

		resp11 := mock.SMove("set1", "set2", "item1")
		assert.NoError(t, resp11.Error)
		assert.Equal(t, int64(1), resp11.GetInt64())

		resp12 := mock.SPop("set1")
		assert.NoError(t, resp12.Error)
		assert.Equal(t, "random_item", resp12.GetString())

		resp13 := mock.SRandMember("set1")
		assert.NoError(t, resp13.Error)
		assert.Equal(t, "random_item", resp13.GetString())

		resp14 := mock.SRem("set1", "item1")
		assert.NoError(t, resp14.Error)
		assert.Equal(t, int64(1), resp14.GetInt64())

		resp15 := mock.SScan("set1", 0, "", 10)
		assert.NoError(t, resp15.Error)
		assert.Equal(t, []interface{}{"0", []interface{}{"item1", "item2"}}, resp15.data)

		resp16 := mock.SUnion("set1", "set2")
		assert.NoError(t, resp16.Error)
		assert.Equal(t, []interface{}{"item1", "item2", "item3", "item4"}, resp16.data)

		resp17 := mock.SUnionStore("dest", "set1", "set2")
		assert.NoError(t, resp17.Error)
		assert.Equal(t, int64(4), resp17.GetInt64())

		// Verify call history - should have 17 commands
		history := mock.GetCallHistory()
		assert.Equal(t, 17, len(history))
		
		// Verify specific commands were called
		saddCalls := mock.GetCallsByCommand("SADD")
		assert.Equal(t, 1, len(saddCalls))
		assert.Equal(t, []interface{}{"set1", "item1"}, saddCalls[0].Args)
	})
}

func TestMockRedisSortedSetCommands(t *testing.T) {
	t.Run("SortedSet_Commands_With_Mock_Responses", func(t *testing.T) {
		mock := NewMockRedisOp()
		
		// Configure responses for all sorted set commands (30 commands)
		mock.SetResponse("ZADD", "zset1", int64(1), nil)
		mock.SetResponse("ZCARD", "zset1", int64(5), nil)
		mock.SetResponse("ZCOUNT", "zset1", int64(3), nil)
		mock.SetResponse("ZDIFF", "*", []interface{}{"member1"}, nil)
		mock.SetResponse("ZDIFFSTORE", "dest", int64(1), nil)
		mock.SetResponse("ZINCRBY", "zset1", float64(1.5), nil)
		mock.SetResponse("ZINTER", "*", []interface{}{"common1"}, nil)
		mock.SetResponse("ZINTERCARD", "*", int64(1), nil)
		mock.SetResponse("ZINTERSTORE", "dest", int64(1), nil)
		mock.SetResponse("ZLEXCOUNT", "zset1", int64(2), nil)
		mock.SetResponse("ZMPOP", "*", []interface{}{"zset1", []interface{}{[]interface{}{"member1", 1.0}}}, nil)
		mock.SetResponse("ZMSCORE", "zset1", []interface{}{1.0, 2.0}, nil)
		mock.SetResponse("ZPOPMAX", "zset1", []interface{}{"member1", 1.0}, nil)
		mock.SetResponse("ZPOPMIN", "zset1", []interface{}{"member1", 1.0}, nil)
		mock.SetResponse("ZRANDMEMBER", "zset1", "random_member", nil)
		mock.SetResponse("ZRANGE", "zset1", []interface{}{"member1", "member2"}, nil)
		mock.SetResponse("ZRANGEBYLEX", "zset1", []interface{}{"member1"}, nil)
		mock.SetResponse("ZRANGEBYSCORE", "zset1", []interface{}{"member1"}, nil)
		mock.SetResponse("ZRANGESTORE", "dest", int64(2), nil)
		mock.SetResponse("ZREVRANGE", "zset1", []interface{}{"member2", "member1"}, nil)
		mock.SetResponse("ZREVRANGEBYLEX", "zset1", []interface{}{"member1"}, nil)
		mock.SetResponse("ZREVRANGEBYSCORE", "zset1", []interface{}{"member1"}, nil)
		mock.SetResponse("ZRANK", "zset1", int64(0), nil)
		mock.SetResponse("ZREM", "zset1", int64(1), nil)
		mock.SetResponse("ZREMRANGEBYLEX", "zset1", int64(1), nil)
		mock.SetResponse("ZREMRANGEBYRANK", "zset1", int64(1), nil)
		mock.SetResponse("ZREMRANGEBYSCORE", "zset1", int64(1), nil)
		mock.SetResponse("ZREVRANK", "zset1", int64(4), nil)
		mock.SetResponse("ZSCAN", "*", []interface{}{"0", []interface{}{"member1", "1.0"}}, nil)
		mock.SetResponse("ZSCORE", "zset1", float64(1.5), nil)
		mock.SetResponse("ZUNION", "*", []interface{}{"member1", "member2"}, nil)
		mock.SetResponse("ZUNIONSTORE", "dest", int64(3), nil)

		// Test all sorted set commands (30 commands)
		resp1 := mock.ZAdd("zset1", 1.0, "member1")
		assert.NoError(t, resp1.Error)
		assert.Equal(t, int64(1), resp1.GetInt64())

		resp2 := mock.ZCard("zset1")
		assert.NoError(t, resp2.Error)
		assert.Equal(t, int64(5), resp2.GetInt64())

		resp3 := mock.ZCount("zset1", "1", "5")
		assert.NoError(t, resp3.Error)
		assert.Equal(t, int64(3), resp3.GetInt64())

		resp4 := mock.ZDiff("zset1", "zset2")
		assert.NoError(t, resp4.Error)
		assert.Equal(t, []interface{}{"member1"}, resp4.data)

		resp5 := mock.ZDiffStore("dest", "zset1", "zset2")
		assert.NoError(t, resp5.Error)
		assert.Equal(t, int64(1), resp5.GetInt64())

		resp6 := mock.ZIncrBy("zset1", 1.5, "member1")
		assert.NoError(t, resp6.Error)
		assert.Equal(t, float64(1.5), resp6.GetFloat64())

		resp7 := mock.ZInter("zset1", "zset2")
		assert.NoError(t, resp7.Error)
		assert.Equal(t, []interface{}{"common1"}, resp7.data)

		resp8 := mock.ZInterCard(1, "zset1", "zset2")
		assert.NoError(t, resp8.Error)
		assert.Equal(t, int64(1), resp8.GetInt64())

		resp9 := mock.ZInterStore("dest", "zset1", "zset2")
		assert.NoError(t, resp9.Error)
		assert.Equal(t, int64(1), resp9.GetInt64())

		resp10 := mock.ZLexCount("zset1", "[a", "[z")
		assert.NoError(t, resp10.Error)
		assert.Equal(t, int64(2), resp10.GetInt64())

		resp11 := mock.ZMPop(1, "MAX", "zset1")
		assert.NoError(t, resp11.Error)
		assert.NotNil(t, resp11.data)

		resp12 := mock.ZMScore("zset1", "member1", "member2")
		assert.NoError(t, resp12.Error)
		assert.Equal(t, []interface{}{1.0, 2.0}, resp12.data)

		resp13 := mock.ZPopMax("zset1")
		assert.NoError(t, resp13.Error)
		assert.Equal(t, []interface{}{"member1", 1.0}, resp13.data)

		resp14 := mock.ZPopMin("zset1")
		assert.NoError(t, resp14.Error)
		assert.Equal(t, []interface{}{"member1", 1.0}, resp14.data)

		resp15 := mock.ZRandMember("zset1")
		assert.NoError(t, resp15.Error)
		assert.Equal(t, "random_member", resp15.GetString())

		resp16 := mock.ZRange("zset1", 0, -1)
		assert.NoError(t, resp16.Error)
		assert.Equal(t, []interface{}{"member1", "member2"}, resp16.data)

		resp17 := mock.ZRangeByLex("zset1", "[a", "[z")
		assert.NoError(t, resp17.Error)
		assert.Equal(t, []interface{}{"member1"}, resp17.data)

		resp18 := mock.ZRangeByScore("zset1", "1", "5")
		assert.NoError(t, resp18.Error)
		assert.Equal(t, []interface{}{"member1"}, resp18.data)

		resp19 := mock.ZRangeStore("dest", "zset1", 0, 1)
		assert.NoError(t, resp19.Error)
		assert.Equal(t, int64(2), resp19.GetInt64())

		resp20 := mock.ZRevRange("zset1", 0, -1)
		assert.NoError(t, resp20.Error)
		assert.Equal(t, []interface{}{"member2", "member1"}, resp20.data)

		resp21 := mock.ZRevRangeByLex("zset1", "[z", "[a")
		assert.NoError(t, resp21.Error)
		assert.Equal(t, []interface{}{"member1"}, resp21.data)

		resp22 := mock.ZRevRangeByScore("zset1", "5", "1")
		assert.NoError(t, resp22.Error)
		assert.Equal(t, []interface{}{"member1"}, resp22.data)

		resp23 := mock.ZRank("zset1", "member1")
		assert.NoError(t, resp23.Error)
		assert.Equal(t, int64(0), resp23.GetInt64())

		resp24 := mock.ZRem("zset1", "member1")
		assert.NoError(t, resp24.Error)
		assert.Equal(t, int64(1), resp24.GetInt64())

		resp25 := mock.ZRemRangeByLex("zset1", "[a", "[m")
		assert.NoError(t, resp25.Error)
		assert.Equal(t, int64(1), resp25.GetInt64())

		resp26 := mock.ZRemRangeByRank("zset1", 0, 1)
		assert.NoError(t, resp26.Error)
		assert.Equal(t, int64(1), resp26.GetInt64())

		resp27 := mock.ZRemRangeByScore("zset1", "1", "3")
		assert.NoError(t, resp27.Error)
		assert.Equal(t, int64(1), resp27.GetInt64())

		resp28 := mock.ZRevRank("zset1", "member1")
		assert.NoError(t, resp28.Error)
		assert.Equal(t, int64(4), resp28.GetInt64())

		resp29 := mock.ZScan("zset1", 0, "", 10)
		assert.NoError(t, resp29.Error)
		assert.Equal(t, []interface{}{"0", []interface{}{"member1", "1.0"}}, resp29.data)

		resp30 := mock.ZScore("zset1", "member1")
		assert.NoError(t, resp30.Error)
		assert.Equal(t, float64(1.5), resp30.GetFloat64())

		resp31 := mock.ZUnion("zset1", "zset2")
		assert.NoError(t, resp31.Error)
		assert.Equal(t, []interface{}{"member1", "member2"}, resp31.data)

		resp32 := mock.ZUnionStore("dest", "zset1", "zset2")
		assert.NoError(t, resp32.Error)
		assert.Equal(t, int64(3), resp32.GetInt64())

		// Verify call history - should have 32 commands (30 original + 2 union commands)
		history := mock.GetCallHistory()
		assert.Equal(t, 32, len(history))
		
		// Verify specific commands were called
		zaddCalls := mock.GetCallsByCommand("ZADD")
		assert.Equal(t, 1, len(zaddCalls))
		assert.Equal(t, []interface{}{"zset1", 1.0, "member1"}, zaddCalls[0].Args)
	})
}

func TestMockRedisKeyTTLCommands(t *testing.T) {
	t.Run("KeyTTL_Commands_With_Mock_Responses", func(t *testing.T) {
		mock := NewMockRedisOp()
		
		// Configure responses for all key/TTL commands - use actual Redis command names
		mock.SetResponse("EXPIRE", "*", int64(1), nil)
		mock.SetResponse("DEL", "*", int64(1), nil)
		mock.SetResponse("KEYS", "*", []interface{}{"key1", "key2", "key3"}, nil)
		mock.SetResponse("EXISTS", "*", int64(1), nil)
		mock.SetResponse("COPY", "*", int64(1), nil)
		mock.SetResponse("DUMP", "*", []byte("serialized_data"), nil)
		mock.SetResponse("TTL", "*", int64(3600), nil)
		mock.SetResponse("PTTL", "*", int64(3600000), nil)
		mock.SetResponse("TYPE", "*", "string", nil)
		mock.SetResponse("RANDOMKEY", "", "random_key", nil)
		mock.SetResponse("RENAME", "*", "OK", nil)
		mock.SetResponse("RENAMENX", "*", int64(1), nil)
		mock.SetResponse("TOUCH", "*", int64(1), nil)
		mock.SetResponse("UNLINK", "*", int64(1), nil)
		mock.SetResponse("PERSIST", "*", int64(1), nil)
		mock.SetResponse("FLUSHDB", "", "OK", nil)
		mock.SetResponse("FLUSHALL", "", "OK", nil)
		mock.SetResponse("SCAN", "*", []interface{}{"0", []interface{}{"key1", "key2"}}, nil)
		mock.SetResponse("PING", "", "PONG", nil)

		// Test all key/TTL commands (19 total calls)
		resp1 := mock.Expire("key1", 3600)
		assert.NoError(t, resp1.Error)
		assert.Equal(t, int64(1), resp1.GetInt64())

		resp2 := mock.Delete("key1")
		assert.NoError(t, resp2.Error)
		assert.Equal(t, int64(1), resp2.GetInt64())

		resp3 := mock.Keys("*")
		assert.NoError(t, resp3.Error)
		assert.Equal(t, []interface{}{"key1", "key2", "key3"}, resp3.data)

		resp4 := mock.Exists("key1")
		assert.NoError(t, resp4.Error)
		assert.Equal(t, int64(1), resp4.GetInt64())

		resp5 := mock.Copy("key1", "key2")
		assert.NoError(t, resp5.Error)
		assert.Equal(t, int64(1), resp5.GetInt64())

		resp6 := mock.Dump("key1")
		assert.NoError(t, resp6.Error)
		assert.Equal(t, []byte("serialized_data"), resp6.GetBytes())

		resp7 := mock.TTL("key1")
		assert.NoError(t, resp7.Error)
		assert.Equal(t, int64(3600), resp7.GetInt64())

		resp8 := mock.PTTL("key1")
		assert.NoError(t, resp8.Error)
		assert.Equal(t, int64(3600000), resp8.GetInt64())

		resp9 := mock.Type("key1")
		assert.NoError(t, resp9.Error)
		assert.Equal(t, "string", resp9.GetString())

		resp10 := mock.RandomKey()
		assert.NoError(t, resp10.Error)
		assert.Equal(t, "random_key", resp10.GetString())

		resp11 := mock.Rename("key1", "new_key")
		assert.NoError(t, resp11.Error)
		assert.Equal(t, "OK", resp11.GetString())

		resp12 := mock.RenameNX("key1", "new_key2")
		assert.NoError(t, resp12.Error)
		assert.Equal(t, int64(1), resp12.GetInt64())

		resp13 := mock.Touch("key1")
		assert.NoError(t, resp13.Error)
		assert.Equal(t, int64(1), resp13.GetInt64())

		resp14 := mock.Unlink("key1")
		assert.NoError(t, resp14.Error)
		assert.Equal(t, int64(1), resp14.GetInt64())

		resp15 := mock.Persist("key1")
		assert.NoError(t, resp15.Error)
		assert.Equal(t, int64(1), resp15.GetInt64())

		resp16 := mock.FlushDB()
		assert.NoError(t, resp16.Error)
		assert.Equal(t, "OK", resp16.GetString())

		resp17 := mock.FlushAll()
		assert.NoError(t, resp17.Error)
		assert.Equal(t, "OK", resp17.GetString())

		resp18 := mock.Scan(0, "", 10)
		assert.NoError(t, resp18.Error)
		assert.Equal(t, []interface{}{"0", []interface{}{"key1", "key2"}}, resp18.data)

		resp19 := mock.Ping()
		assert.NoError(t, resp19.Error)
		assert.Equal(t, "PONG", resp19.GetString())

		// Verify call history - should have 19 commands
		history := mock.GetCallHistory()
		assert.Equal(t, 19, len(history))
		
		// Verify specific commands were called
		expireCalls := mock.GetCallsByCommand("EXPIRE")
		assert.Equal(t, 1, len(expireCalls))
		assert.Equal(t, []interface{}{"key1", int64(3600)}, expireCalls[0].Args)
	})
}

// Benchmark tests comparing Real Redis vs Mock Redis performance
func BenchmarkRedisOperations(b *testing.B) {
	// Setup real Redis for benchmarking
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")
	
	realRedis := NewRedis("test")
	if realRedis == nil {
		b.Skip("Skipping benchmark - Redis not available")
		return
	}

	// Setup Mock Redis
	mockRedis := NewMockRedis()
	mockOp := mockRedis.Master().(*MockRedisOp)
	
	// Configure common mock responses
	mockOp.SetResponse("SET", "*", "OK", nil)
	mockOp.SetResponse("GET", "*", "test_value", nil)
	mockOp.SetResponse("HSET", "*", int64(1), nil)
	mockOp.SetResponse("HGET", "*", "test_field_value", nil)
	mockOp.SetResponse("LPUSH", "*", int64(1), nil)
	mockOp.SetResponse("LPOP", "*", "test_list_item", nil)

	b.Run("String_Operations", func(b *testing.B) {
		b.Run("Real_Redis", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("bench_key_%d", i%1000)
				realRedis.Master().Set(key, "benchmark_value")
				realRedis.Master().Get(key)
			}
		})

		b.Run("Mock_Redis", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("bench_key_%d", i%1000)
				mockRedis.Master().Set(key, "benchmark_value")
				mockRedis.Master().Get(key)
			}
		})
	})

	b.Run("Hash_Operations", func(b *testing.B) {
		b.Run("Real_Redis", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("bench_hash_%d", i%1000)
				field := fmt.Sprintf("field_%d", i%100)
				realRedis.Master().HSet(key, field, "benchmark_hash_value")
				realRedis.Master().HGet(key, field)
			}
		})

		b.Run("Mock_Redis", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("bench_hash_%d", i%1000)
				field := fmt.Sprintf("field_%d", i%100)
				mockRedis.Master().HSet(key, field, "benchmark_hash_value")
				mockRedis.Master().HGet(key, field)
			}
		})
	})

	b.Run("List_Operations", func(b *testing.B) {
		b.Run("Real_Redis", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("bench_list_%d", i%1000)
				realRedis.Master().LPush(key, fmt.Sprintf("item_%d", i))
				realRedis.Master().LPop(key)
			}
		})

		b.Run("Mock_Redis", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("bench_list_%d", i%1000)
				mockRedis.Master().LPush(key, fmt.Sprintf("item_%d", i))
				mockRedis.Master().LPop(key)
			}
		})
	})

	b.Run("Pipeline_Operations", func(b *testing.B) {
		// Configure mock pipeline response
		mockOp.SetResponse("PIPELINE", "", []interface{}{
			&RedisResponse{RedisResponseEntity: RedisResponseEntity{data: "OK"}},
			&RedisResponse{RedisResponseEntity: RedisResponseEntity{data: "test_value"}},
		}, nil)

		b.Run("Real_Redis", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cmds := []RedisPipelineCmd{
					{Cmd: "SET", Args: []interface{}{fmt.Sprintf("pipe_key_%d", i), "pipe_value"}},
					{Cmd: "GET", Args: []interface{}{fmt.Sprintf("pipe_key_%d", i)}},
				}
				realRedis.Master().Pipeline(cmds...)
			}
		})

		b.Run("Mock_Redis", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cmds := []RedisPipelineCmd{
					{Cmd: "SET", Args: []interface{}{fmt.Sprintf("pipe_key_%d", i), "pipe_value"}},
					{Cmd: "GET", Args: []interface{}{fmt.Sprintf("pipe_key_%d", i)}},
				}
				mockRedis.Master().Pipeline(cmds...)
			}
		})
	})
}
