package datastore

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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

		// Verify slave configuration
		assert.NotNil(t, profile.Slave)
		assert.Equal(t, "localhost", profile.Slave.Host)
		assert.Equal(t, uint(6380), profile.Slave.Port)
	})

	t.Run("NewRedisWithExampleSecret", func(t *testing.T) {
		// Test creating Redis instance with the example secret
		redis := NewRedis("test")

		// NewRedis should succeed with valid secret
		assert.NotNil(t, redis)
		assert.Equal(t, "test", redis.name)
		assert.NotNil(t, redis.master)
		assert.NotNil(t, redis.slave)

		// Verify the master and slave are configured correctly
		assert.Equal(t, "localhost", redis.master.meta.Host)
		assert.Equal(t, uint(6379), redis.master.meta.Port)
		assert.Equal(t, "localhost", redis.slave.meta.Host)
		assert.Equal(t, uint(6380), redis.slave.meta.Port)
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
		
		response := redis.Master().Scan(0, "scan_test_*", 100)
		assert.NoError(t, response.Error)
		
		// Cleanup
		redis.Master().Delete("scan_test_1", "scan_test_2", "scan_test_3")
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

	t.Run("HScan", func(t *testing.T) {
		hashKey := "test_hash"
		
		// Setup test data
		redis.Master().HSet(hashKey, "field1", "value1")
		redis.Master().HSet(hashKey, "field2", "value2")
		redis.Master().HSet(hashKey, "field3", "value3")
		redis.Master().HSet(hashKey, "other1", "othervalue1")
		
		// HScan with pattern
		response := redis.Master().HScan(hashKey, 0, "field*", 100)
		assert.NoError(t, response.Error)
		
		// HScan without pattern
		response2 := redis.Master().HScan(hashKey, 0, "", 100)
		assert.NoError(t, response2.Error)
		
		// Cleanup
		redis.Master().Delete(hashKey)
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
