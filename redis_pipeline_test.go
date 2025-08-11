package datastore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	secret "github.com/yetiz-org/goth-secret"
)

// TestRedisPipeline Basic batch pipeline flow
func TestRedisPipeline(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	r := NewRedis("test")

	t.Run("BasicPipelineOperations", func(t *testing.T) {
		// Ensure a clean test environment
		r.Master().Delete("p_key1", "p_key2", "p_key3", "p_counter")

		cmds := []RedisPipelineCmd{
			{Cmd: "SET", Args: []interface{}{"p_key1", "value1"}},
			{Cmd: "SET", Args: []interface{}{"p_key2", "value2"}},
			{Cmd: "SET", Args: []interface{}{"p_counter", "10"}},
			{Cmd: "GET", Args: []interface{}{"p_key1"}},
			{Cmd: "GET", Args: []interface{}{"p_key2"}},
			{Cmd: "GET", Args: []interface{}{"p_key3"}}, // non-existing key
			{Cmd: "INCR", Args: []interface{}{"p_counter"}},
		}

		resps := r.Master().Pipeline(cmds...)
		assert.Equal(t, 7, len(resps))

		// Verify each response is independent and correct
		assert.NoError(t, resps[0].Error)
		assert.Equal(t, "OK", resps[0].GetString())

		assert.NoError(t, resps[1].Error)
		assert.Equal(t, "OK", resps[1].GetString())

		assert.NoError(t, resps[2].Error)
		assert.Equal(t, "OK", resps[2].GetString())

		assert.NoError(t, resps[3].Error)
		assert.Equal(t, "value1", resps[3].GetString())

		assert.NoError(t, resps[4].Error)
		assert.Equal(t, "value2", resps[4].GetString())

		assert.True(t, resps[5].RecordNotFound())

		assert.NoError(t, resps[6].Error)
		assert.Equal(t, int64(11), resps[6].GetInt64())

		// Cleanup
		r.Master().Delete("p_key1", "p_key2", "p_key3", "p_counter")
	})

	t.Run("PipelineWithDifferentDataTypes", func(t *testing.T) {
		// Ensure responses differ across data types
		r.Master().Delete("p_string", "p_list", "p_hash", "p_set")

		cmds := []RedisPipelineCmd{
			{Cmd: "SET", Args: []interface{}{"p_string", "hello"}},
			{Cmd: "LPUSH", Args: []interface{}{"p_list", "item1", "item2"}},
			{Cmd: "HSET", Args: []interface{}{"p_hash", "field", "value"}},
			{Cmd: "SADD", Args: []interface{}{"p_set", "member1", "member2"}},
			{Cmd: "GET", Args: []interface{}{"p_string"}},
			{Cmd: "LLEN", Args: []interface{}{"p_list"}},
			{Cmd: "HGET", Args: []interface{}{"p_hash", "field"}},
			{Cmd: "SCARD", Args: []interface{}{"p_set"}},
		}

		resps := r.Master().Pipeline(cmds...)
		assert.Equal(t, 8, len(resps))

		// Verify set operations
		assert.NoError(t, resps[0].Error)
		assert.Equal(t, "OK", resps[0].GetString())

		assert.NoError(t, resps[1].Error)
		assert.Equal(t, int64(2), resps[1].GetInt64())

		assert.NoError(t, resps[2].Error)
		assert.Equal(t, int64(1), resps[2].GetInt64())

		assert.NoError(t, resps[3].Error)
		assert.Equal(t, int64(2), resps[3].GetInt64())

		// Verify query operations - each response data type differs
		assert.NoError(t, resps[4].Error)
		assert.Equal(t, "hello", resps[4].GetString())

		assert.NoError(t, resps[5].Error)
		assert.Equal(t, int64(2), resps[5].GetInt64())

		assert.NoError(t, resps[6].Error)
		assert.Equal(t, "value", resps[6].GetString())

		assert.NoError(t, resps[7].Error)
		assert.Equal(t, int64(2), resps[7].GetInt64())

		// Cleanup
		r.Master().Delete("p_string", "p_list", "p_hash", "p_set")
	})
}

// TestRedisPipelineEmpty Empty command set behavior
func TestRedisPipelineEmpty(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	r := NewRedis("test")

	var cmds []RedisPipelineCmd
	resps := r.Master().Pipeline(cmds...)
	assert.Nil(t, resps)
}

// TestRedisPipelineWithServerError Server-side error responses still preserve order and subsequent responses
func TestRedisPipelineWithServerError(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	r := NewRedis("test")

	// Prepare data
	r.Master().Delete("p_key_err")
	setOK := r.Master().Set("p_key_err", "ok")
	assert.NoError(t, setOK.Error)

	cmds := []RedisPipelineCmd{
		{Cmd: "SET", Args: []interface{}{"p_key_err", "ok2"}},
		{Cmd: "WRONGCMD", Args: []interface{}{"x"}}, // Invalid command that will cause a server error
		{Cmd: "GET", Args: []interface{}{"p_key_err"}},
		{Cmd: "INCR", Args: []interface{}{"p_key_err"}}, // This will fail because the value is not a number
		{Cmd: "SET", Args: []interface{}{"p_key_err", "123"}},
		{Cmd: "GET", Args: []interface{}{"p_key_err"}},
	}

	resps := r.Master().Pipeline(cmds...)
	assert.Equal(t, 6, len(resps))

	// The first SET should succeed
	assert.NoError(t, resps[0].Error)
	assert.Equal(t, "OK", resps[0].GetString())

	// The second is an invalid command; should return an error
	assert.Error(t, resps[1].Error)

	// The third GET should still get the result, order preserved
	assert.NoError(t, resps[2].Error)
	assert.Equal(t, "ok2", resps[2].GetString())

	// The fourth INCR should fail (value is not a number)
	assert.Error(t, resps[3].Error)

	// The fifth SET should succeed
	assert.NoError(t, resps[4].Error)
	assert.Equal(t, "OK", resps[4].GetString())

	// The sixth GET should retrieve the last set value
	assert.NoError(t, resps[5].Error)
	assert.Equal(t, "123", resps[5].GetString())

	// Cleanup
	r.Master().Delete("p_key_err")
}
