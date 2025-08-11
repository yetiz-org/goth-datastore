package datastore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	secret "github.com/yetiz-org/goth-secret"
)

// TestRedisPipeline 基本批次操作流程
func TestRedisPipeline(t *testing.T) {
	if os.Getenv("REDIS_TEST") != "1" {
		t.Skip("Skipping Redis tests - set REDIS_TEST=1 to run")
	}

	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	r := NewRedis("test")
	if r == nil {
		t.Skip("Redis not available for testing")
	}

	t.Run("BasicPipelineOperations", func(t *testing.T) {
		// 確保乾淨的測試環境
		r.Master().Delete("p_key1", "p_key2", "p_key3", "p_counter")

		cmds := []RedisPipelineCmd{
			{Cmd: "SET", Args: []interface{}{"p_key1", "value1"}},
			{Cmd: "SET", Args: []interface{}{"p_key2", "value2"}},
			{Cmd: "SET", Args: []interface{}{"p_counter", "10"}},
			{Cmd: "GET", Args: []interface{}{"p_key1"}},
			{Cmd: "GET", Args: []interface{}{"p_key2"}},
			{Cmd: "GET", Args: []interface{}{"p_key3"}}, // 不存在的 key
			{Cmd: "INCR", Args: []interface{}{"p_counter"}},
		}

		resps := r.Master().Pipeline(cmds...)
		assert.Equal(t, 7, len(resps))

		// 驗證每個回應都是獨立且正確的
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

		// 清理
		r.Master().Delete("p_key1", "p_key2", "p_key3", "p_counter")
	})

	t.Run("PipelineWithDifferentDataTypes", func(t *testing.T) {
		// 測試不同資料類型的回應確實不同
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

		// 驗證設置操作
		assert.NoError(t, resps[0].Error)
		assert.Equal(t, "OK", resps[0].GetString())

		assert.NoError(t, resps[1].Error)
		assert.Equal(t, int64(2), resps[1].GetInt64())

		assert.NoError(t, resps[2].Error)
		assert.Equal(t, int64(1), resps[2].GetInt64())

		assert.NoError(t, resps[3].Error)
		assert.Equal(t, int64(2), resps[3].GetInt64())

		// 驗證查詢操作 - 每個回應的資料都不同
		assert.NoError(t, resps[4].Error)
		assert.Equal(t, "hello", resps[4].GetString())

		assert.NoError(t, resps[5].Error)
		assert.Equal(t, int64(2), resps[5].GetInt64())

		assert.NoError(t, resps[6].Error)
		assert.Equal(t, "value", resps[6].GetString())

		assert.NoError(t, resps[7].Error)
		assert.Equal(t, int64(2), resps[7].GetInt64())

		// 清理
		r.Master().Delete("p_string", "p_list", "p_hash", "p_set")
	})
}

// TestRedisPipelineEmpty 空指令集行為
func TestRedisPipelineEmpty(t *testing.T) {
	if os.Getenv("REDIS_TEST") != "1" {
		t.Skip("Skipping Redis tests - set REDIS_TEST=1 to run")
	}

	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	r := NewRedis("test")
	if r == nil {
		t.Skip("Redis not available for testing")
	}

	var cmds []RedisPipelineCmd
	resps := r.Master().Pipeline(cmds...)
	assert.Nil(t, resps)
}

// TestRedisPipelineWithServerError 服務端錯誤回應仍可保持順序與後續回應
func TestRedisPipelineWithServerError(t *testing.T) {
	if os.Getenv("REDIS_TEST") != "1" {
		t.Skip("Skipping Redis tests - set REDIS_TEST=1 to run")
	}

	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	r := NewRedis("test")
	if r == nil {
		t.Skip("Redis not available for testing")
	}

	// 準備資料
	r.Master().Delete("p_key_err")
	setOK := r.Master().Set("p_key_err", "ok")
	if setOK.Error != nil {
		t.Skip("Redis not available for testing")
	}

	cmds := []RedisPipelineCmd{
		{Cmd: "SET", Args: []interface{}{"p_key_err", "ok2"}},
		{Cmd: "WRONGCMD", Args: []interface{}{"x"}}, // 無效指令，將導致伺服器錯誤
		{Cmd: "GET", Args: []interface{}{"p_key_err"}},
		{Cmd: "INCR", Args: []interface{}{"p_key_err"}}, // 這會失敗，因為 value 不是數字
		{Cmd: "SET", Args: []interface{}{"p_key_err", "123"}},
		{Cmd: "GET", Args: []interface{}{"p_key_err"}},
	}

	resps := r.Master().Pipeline(cmds...)
	assert.Equal(t, 6, len(resps))

	// 第一個 SET 應成功
	assert.NoError(t, resps[0].Error)
	assert.Equal(t, "OK", resps[0].GetString())

	// 第二個為錯誤指令，應回傳錯誤
	assert.Error(t, resps[1].Error)

	// 第三個 GET 仍應取得結果，且順序正確
	assert.NoError(t, resps[2].Error)
	assert.Equal(t, "ok2", resps[2].GetString())

	// 第四個 INCR 會失敗（因為值不是數字）
	assert.Error(t, resps[3].Error)

	// 第五個 SET 應成功
	assert.NoError(t, resps[4].Error)
	assert.Equal(t, "OK", resps[4].GetString())

	// 第六個 GET 應取得最後設置的值
	assert.NoError(t, resps[5].Error)
	assert.Equal(t, "123", resps[5].GetString())

	// 清理
	r.Master().Delete("p_key_err")
}
