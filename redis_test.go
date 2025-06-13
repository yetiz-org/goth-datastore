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
