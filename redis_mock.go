package datastore

import (
	"fmt"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	secret "github.com/yetiz-org/goth-secret"
)

// MockCallRecord represents a single Redis command call record for testing verification.
type MockCallRecord struct {
	Timestamp time.Time
	Command   string
	Args      []interface{}
	Response  interface{}
	Error     error
}

// MockResponse contains the response data and optional error for mock operations.
type MockResponse struct {
	Data  interface{}
	Error error
	Delay time.Duration // Optional: simulate network delay
}

// MockConditionFunc defines a function that determines if a condition matches for conditional responses.
type MockConditionFunc func(cmd string, args []interface{}) bool

// MockConditionRule represents a conditional response rule.
type MockConditionRule struct {
	Command   string
	Condition MockConditionFunc
	Response  MockResponse
}

// MockRedisOp implements RedisOperator interface for testing purposes.
// It provides a full mock implementation that can simulate Redis behavior,
// record call history, and return configured responses.
type MockRedisOp struct {
	mutex           sync.RWMutex
	responses       map[string]MockResponse     // Static responses by command:key pattern
	sequences       map[string][]MockResponse   // Sequential responses
	conditions      []MockConditionRule         // Conditional responses
	callHistory     []MockCallRecord            // All call records
	sequenceIndexes map[string]int              // Current index for sequence responses
	defaultError    error                       // Default error for unmatched calls
	
	// Simulated connection pool info
	activeCount int
	idleCount   int
	meta        secret.RedisMeta
}

// NewMockRedisOp creates a new MockRedisOp instance.
func NewMockRedisOp() *MockRedisOp {
	return &MockRedisOp{
		responses:       make(map[string]MockResponse),
		sequences:       make(map[string][]MockResponse),
		conditions:      make([]MockConditionRule, 0),
		callHistory:     make([]MockCallRecord, 0),
		sequenceIndexes: make(map[string]int),
		activeCount:     0,
		idleCount:       1,
		meta: secret.RedisMeta{
			Host: "mock",
			Port: 6379,
		},
	}
}

// SetResponse sets a static response for a specific command and key pattern.
// Pattern supports "*" as wildcard for any key.
func (m *MockRedisOp) SetResponse(cmd string, keyPattern string, data interface{}, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	key := fmt.Sprintf("%s:%s", cmd, keyPattern)
	m.responses[key] = MockResponse{Data: data, Error: err}
}

// SetSequentialResponses sets a sequence of responses for a command and key pattern.
// Each call will return the next response in sequence, cycling back to start when exhausted.
func (m *MockRedisOp) SetSequentialResponses(cmd string, keyPattern string, responses []MockResponse) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	key := fmt.Sprintf("%s:%s", cmd, keyPattern)
	m.sequences[key] = responses
	m.sequenceIndexes[key] = 0
}

// SetConditionalResponse adds a conditional response rule.
func (m *MockRedisOp) SetConditionalResponse(cmd string, condition MockConditionFunc, response MockResponse) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.conditions = append(m.conditions, MockConditionRule{
		Command:   cmd,
		Condition: condition,
		Response:  response,
	})
}

// SetDefaultError sets a default error returned when no specific response is configured.
func (m *MockRedisOp) SetDefaultError(err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.defaultError = err
}

// GetCallHistory returns all recorded call history.
func (m *MockRedisOp) GetCallHistory() []MockCallRecord {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	// Return a copy to avoid race conditions
	history := make([]MockCallRecord, len(m.callHistory))
	copy(history, m.callHistory)
	return history
}

// GetCallsByCommand returns all recorded calls for a specific command.
func (m *MockRedisOp) GetCallsByCommand(command string) []MockCallRecord {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	var filteredCalls []MockCallRecord
	for _, call := range m.callHistory {
		if call.Command == command {
			filteredCalls = append(filteredCalls, call)
		}
	}
	return filteredCalls
}

// ClearCallHistory clears all recorded call history.
func (m *MockRedisOp) ClearCallHistory() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	m.callHistory = m.callHistory[:0] // Clear slice but keep capacity
}

// GetCallCount returns the number of times a specific command was called.
func (m *MockRedisOp) GetCallCount(cmd string) int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	count := 0
	for _, record := range m.callHistory {
		if record.Command == cmd {
			count++
		}
	}
	return count
}

// GetLastCall returns the most recent call record, or nil if no calls made.
func (m *MockRedisOp) GetLastCall() *MockCallRecord {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if len(m.callHistory) == 0 {
		return nil
	}
	// Return a copy
	last := m.callHistory[len(m.callHistory)-1]
	return &last
}

// Reset clears all mock data including responses, history, and sequences.
func (m *MockRedisOp) Reset() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.responses = make(map[string]MockResponse)
	m.sequences = make(map[string][]MockResponse)
	m.conditions = make([]MockConditionRule, 0)
	m.callHistory = make([]MockCallRecord, 0)
	m.sequenceIndexes = make(map[string]int)
	m.defaultError = nil
}

// SetActiveCount sets the simulated active connection count.
func (m *MockRedisOp) SetActiveCount(count int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.activeCount = count
}

// SetIdleCount sets the simulated idle connection count.
func (m *MockRedisOp) SetIdleCount(count int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.idleCount = count
}

// SetMeta sets the simulated Redis connection metadata.
func (m *MockRedisOp) SetMeta(meta secret.RedisMeta) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.meta = meta
}

// mockDo handles the core mock logic for Redis commands.
func (m *MockRedisOp) mockDo(cmd string, args ...interface{}) *RedisResponse {
	timestamp := time.Now()
	
	// Try to find a matching response
	response := m.findResponse(cmd, args)
	
	// Record the call
	record := MockCallRecord{
		Timestamp: timestamp,
		Command:   cmd,
		Args:      args,
		Response:  response.Data,
		Error:     response.Error,
	}
	
	m.mutex.Lock()
	m.callHistory = append(m.callHistory, record)
	m.mutex.Unlock()
	
	// Simulate delay if configured
	if response.Delay > 0 {
		time.Sleep(response.Delay)
	}
	
	// Return mock response
	if response.Error != nil {
		return &RedisResponse{Error: response.Error}
	}
	
	return &RedisResponse{
		RedisResponseEntity: RedisResponseEntity{data: response.Data},
		Error:               nil,
	}
}

// findResponse finds the appropriate mock response for a command.
func (m *MockRedisOp) findResponse(cmd string, args []interface{}) MockResponse {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	// 1. Try conditional responses first
	for _, rule := range m.conditions {
		if rule.Command == cmd && rule.Condition(cmd, args) {
			return rule.Response
		}
	}
	
	// 2. Try sequence responses
	if len(args) > 0 {
		key := fmt.Sprintf("%s:%v", cmd, args[0])
		if sequence, exists := m.sequences[key]; exists && len(sequence) > 0 {
			index := m.sequenceIndexes[key]
			response := sequence[index]
			// Update index for next call, but don't exceed sequence length
			if index < len(sequence)-1 {
				m.sequenceIndexes[key] = index + 1
			}
			// Stay at last response once exhausted
			return response
		}
		
		// Try wildcard sequence
		wildcardKey := fmt.Sprintf("%s:*", cmd)
		if sequence, exists := m.sequences[wildcardKey]; exists && len(sequence) > 0 {
			index := m.sequenceIndexes[wildcardKey]
			response := sequence[index]
			m.sequenceIndexes[wildcardKey] = (index + 1) % len(sequence)
			return response
		}
	}
	
	// 3. Try static responses
	if len(args) > 0 {
		key := fmt.Sprintf("%s:%v", cmd, args[0])
		if response, exists := m.responses[key]; exists {
			return response
		}
		
		// Try wildcard static response
		wildcardKey := fmt.Sprintf("%s:*", cmd)
		if response, exists := m.responses[wildcardKey]; exists {
			return response
		}
	}
	
	// 4. Command without key (like PING)
	noKeyResponse := fmt.Sprintf("%s:", cmd)
	if response, exists := m.responses[noKeyResponse]; exists {
		return response
	}
	
	// 5. Return default error or not found
	if m.defaultError != nil {
		return MockResponse{Error: m.defaultError}
	}
	
	// Default: return nil for unconfigured responses (allows test flexibility)
	return MockResponse{Data: nil, Error: nil}
}

// Connection and pool management methods
func (m *MockRedisOp) Meta() secret.RedisMeta {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.meta
}

func (m *MockRedisOp) Pool() *redis.Pool {
	// Mock implementation: create a basic mock pool
	return &redis.Pool{
		MaxIdle:         m.idleCount,
		MaxActive:       m.activeCount,
		IdleTimeout:     240 * time.Second,
		MaxConnLifetime: 1 * time.Hour,
		Wait:            true,
		Dial: func() (redis.Conn, error) {
			return m.Conn(), nil
		},
	}
}

func (m *MockRedisOp) Conn() redis.Conn {
	// Mock implementation: return a mock connection that implements redis.Conn interface
	return &MockRedisConn{}
}

// MockRedisConn implements redis.Conn interface for testing
type MockRedisConn struct{}

func (c *MockRedisConn) Close() error                                          { return nil }
func (c *MockRedisConn) Err() error                                            { return nil }
func (c *MockRedisConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	return "OK", nil
}
func (c *MockRedisConn) Send(commandName string, args ...interface{}) error    { return nil }
func (c *MockRedisConn) Flush() error                                          { return nil }
func (c *MockRedisConn) Receive() (reply interface{}, err error)               { return "OK", nil }

func (m *MockRedisOp) ActiveCount() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.activeCount
}

func (m *MockRedisOp) IdleCount() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.idleCount
}

func (m *MockRedisOp) Close() error {
	// Mock implementation: nothing to close
	return nil
}

func (m *MockRedisOp) Exec(f func(conn redis.Conn)) error {
	// Mock implementation: call function with nil connection
	f(nil)
	return nil
}

// Pipeline operations
func (m *MockRedisOp) Pipeline(cmds ...RedisPipelineCmd) []*RedisResponse {
	timestamp := time.Now()
	
	// Try to find a configured PIPELINE response first
	pipelineResponse := m.findResponse("PIPELINE", []interface{}{})
	
	var responses []*RedisResponse
	
	if pipelineResponse.Data != nil {
		// Use the configured pipeline response if available
		if responseArray, ok := pipelineResponse.Data.([]interface{}); ok {
			responses = make([]*RedisResponse, len(responseArray))
			for i, respInterface := range responseArray {
				if redisResp, ok := respInterface.(*RedisResponse); ok {
					responses[i] = redisResp
				} else {
					responses[i] = &RedisResponse{
						RedisResponseEntity: RedisResponseEntity{data: respInterface},
						Error:               nil,
					}
				}
			}
		} else {
			// If not an array, create default responses
			responses = make([]*RedisResponse, len(cmds))
			for i := range responses {
				responses[i] = &RedisResponse{
					RedisResponseEntity: RedisResponseEntity{data: nil},
					Error:               nil,
				}
			}
		}
	} else {
		// Fallback: create responses for each individual command
		responses = make([]*RedisResponse, len(cmds))
		for i, cmd := range cmds {
			response := m.findResponse(cmd.Cmd, cmd.Args)
			
			if response.Error != nil {
				responses[i] = &RedisResponse{Error: response.Error}
			} else {
				responses[i] = &RedisResponse{
					RedisResponseEntity: RedisResponseEntity{data: response.Data},
					Error:               nil,
				}
			}
		}
	}
	
	// Record a single PIPELINE call in history
	record := MockCallRecord{
		Timestamp: timestamp,
		Command:   "PIPELINE",
		Args:      []interface{}{cmds},
		Response:  responses,
		Error:     pipelineResponse.Error,
	}
	
	m.mutex.Lock()
	m.callHistory = append(m.callHistory, record)
	m.mutex.Unlock()
	
	return responses
}

// String operations
func (m *MockRedisOp) Get(key interface{}) *RedisResponse {
	return m.mockDo("GET", key)
}

func (m *MockRedisOp) Set(key interface{}, val interface{}) *RedisResponse {
	return m.mockDo("SET", key, val)
}

func (m *MockRedisOp) SetExpire(key interface{}, val interface{}, ttl int64) *RedisResponse {
	return m.mockDo("SETEX", key, ttl, val)
}

func (m *MockRedisOp) Incr(key interface{}) *RedisResponse {
	return m.mockDo("INCR", key)
}

func (m *MockRedisOp) IncrBy(key interface{}, val int64) *RedisResponse {
	return m.mockDo("INCRBY", key, val)
}

func (m *MockRedisOp) Decr(key interface{}) *RedisResponse {
	return m.mockDo("DECR", key)
}

func (m *MockRedisOp) DecrBy(key interface{}, val int64) *RedisResponse {
	return m.mockDo("DECRBY", key, val)
}

func (m *MockRedisOp) Append(key interface{}, val interface{}) *RedisResponse {
	return m.mockDo("APPEND", key, val)
}

func (m *MockRedisOp) StrLen(key interface{}) *RedisResponse {
	return m.mockDo("STRLEN", key)
}

func (m *MockRedisOp) GetRange(key interface{}, start, end int64) *RedisResponse {
	return m.mockDo("GETRANGE", key, start, end)
}

func (m *MockRedisOp) SetRange(key interface{}, offset int64, val interface{}) *RedisResponse {
	return m.mockDo("SETRANGE", key, offset, val)
}

// Hash operations
func (m *MockRedisOp) HMSet(key interface{}, val map[interface{}]interface{}) *RedisResponse {
	args := []interface{}{key}
	for mk, mv := range val {
		args = append(args, mk, mv)
	}
	return m.mockDo("HMSET", args...)
}

func (m *MockRedisOp) HMGet(key interface{}, field ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, field...)
	return m.mockDo("HMGET", args...)
}

func (m *MockRedisOp) HSet(key, field, val interface{}) *RedisResponse {
	return m.mockDo("HSET", key, field, val)
}

func (m *MockRedisOp) HGet(key, field interface{}) *RedisResponse {
	return m.mockDo("HGET", key, field)
}

func (m *MockRedisOp) HExists(key, field interface{}) *RedisResponse {
	return m.mockDo("HEXISTS", key, field)
}

func (m *MockRedisOp) HDel(key interface{}, field ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, field...)
	return m.mockDo("HDEL", args...)
}

func (m *MockRedisOp) HGetAll(key interface{}) *RedisResponse {
	return m.mockDo("HGETALL", key)
}

func (m *MockRedisOp) HLen(key interface{}) *RedisResponse {
	return m.mockDo("HLEN", key)
}

func (m *MockRedisOp) HKeys(key interface{}) *RedisResponse {
	return m.mockDo("HKEYS", key)
}

func (m *MockRedisOp) HIncrBy(key interface{}, field interface{}, val int64) *RedisResponse {
	return m.mockDo("HINCRBY", key, field, val)
}

func (m *MockRedisOp) HVals(key interface{}) *RedisResponse {
	return m.mockDo("HVALS", key)
}

func (m *MockRedisOp) HScan(key interface{}, cursor int64, match string, count int64) *RedisResponse {
	args := []interface{}{key, cursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", count)
	}
	return m.mockDo("HSCAN", args...)
}

// Key operations
func (m *MockRedisOp) Expire(key interface{}, ttl int64) *RedisResponse {
	return m.mockDo("EXPIRE", key, ttl)
}

func (m *MockRedisOp) Delete(key ...interface{}) *RedisResponse {
	return m.mockDo("DEL", key...)
}

func (m *MockRedisOp) Keys(key interface{}) *RedisResponse {
	return m.mockDo("KEYS", key)
}

func (m *MockRedisOp) Exists(key ...interface{}) *RedisResponse {
	return m.mockDo("EXISTS", key...)
}

func (m *MockRedisOp) Copy(src, dst interface{}) *RedisResponse {
	return m.mockDo("COPY", src, dst)
}

func (m *MockRedisOp) Dump(key interface{}) *RedisResponse {
	return m.mockDo("DUMP", key)
}

func (m *MockRedisOp) TTL(key interface{}) *RedisResponse {
	return m.mockDo("TTL", key)
}

func (m *MockRedisOp) PTTL(key interface{}) *RedisResponse {
	return m.mockDo("PTTL", key)
}

func (m *MockRedisOp) Type(key interface{}) *RedisResponse {
	return m.mockDo("TYPE", key)
}

func (m *MockRedisOp) RandomKey() *RedisResponse {
	return m.mockDo("RANDOMKEY")
}

func (m *MockRedisOp) Rename(oldKey, newKey interface{}) *RedisResponse {
	return m.mockDo("RENAME", oldKey, newKey)
}

func (m *MockRedisOp) RenameNX(oldKey, newKey interface{}) *RedisResponse {
	return m.mockDo("RENAMENX", oldKey, newKey)
}

func (m *MockRedisOp) Touch(key ...interface{}) *RedisResponse {
	return m.mockDo("TOUCH", key...)
}

func (m *MockRedisOp) Unlink(key ...interface{}) *RedisResponse {
	return m.mockDo("UNLINK", key...)
}

func (m *MockRedisOp) Persist(key interface{}) *RedisResponse {
	return m.mockDo("PERSIST", key)
}

// List operations
func (m *MockRedisOp) LIndex(key interface{}, index int64) *RedisResponse {
	return m.mockDo("LINDEX", key, index)
}

func (m *MockRedisOp) LInsert(key interface{}, where string, pivot, element interface{}) *RedisResponse {
	return m.mockDo("LINSERT", key, where, pivot, element)
}

func (m *MockRedisOp) LLen(key interface{}) *RedisResponse {
	return m.mockDo("LLEN", key)
}

func (m *MockRedisOp) LMove(source, destination interface{}, srcWhere, dstWhere string) *RedisResponse {
	return m.mockDo("LMOVE", source, destination, srcWhere, dstWhere)
}

func (m *MockRedisOp) LMPop(count int64, where string, key ...interface{}) *RedisResponse {
	numkeys := count
	if numkeys <= 0 {
		numkeys = int64(len(key))
	}
	args := []interface{}{numkeys}
	args = append(args, key...)
	args = append(args, where)
	return m.mockDo("LMPOP", args...)
}

func (m *MockRedisOp) LPop(key interface{}) *RedisResponse {
	return m.mockDo("LPOP", key)
}

func (m *MockRedisOp) LPos(key, element interface{}) *RedisResponse {
	return m.mockDo("LPOS", key, element)
}

func (m *MockRedisOp) LPush(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return m.mockDo("LPUSH", args...)
}

func (m *MockRedisOp) LPushX(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return m.mockDo("LPUSHX", args...)
}

func (m *MockRedisOp) LRange(key interface{}, start, stop int64) *RedisResponse {
	return m.mockDo("LRANGE", key, start, stop)
}

func (m *MockRedisOp) LRem(key interface{}, count int64, element interface{}) *RedisResponse {
	return m.mockDo("LREM", key, count, element)
}

func (m *MockRedisOp) LSet(key interface{}, index int64, element interface{}) *RedisResponse {
	return m.mockDo("LSET", key, index, element)
}

func (m *MockRedisOp) LTrim(key interface{}, start, stop int64) *RedisResponse {
	return m.mockDo("LTRIM", key, start, stop)
}

func (m *MockRedisOp) RPop(key interface{}) *RedisResponse {
	return m.mockDo("RPOP", key)
}

func (m *MockRedisOp) RPopLPush(source, destination interface{}) *RedisResponse {
	return m.mockDo("RPOPLPUSH", source, destination)
}

func (m *MockRedisOp) RPush(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return m.mockDo("RPUSH", args...)
}

func (m *MockRedisOp) RPushX(key interface{}, val ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, val...)
	return m.mockDo("RPUSHX", args...)
}

// Set operations
func (m *MockRedisOp) SAdd(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return m.mockDo("SADD", args...)
}

func (m *MockRedisOp) SCard(key interface{}) *RedisResponse {
	return m.mockDo("SCARD", key)
}

func (m *MockRedisOp) SDiff(key ...interface{}) *RedisResponse {
	return m.mockDo("SDIFF", key...)
}

func (m *MockRedisOp) SDiffStore(destination interface{}, key ...interface{}) *RedisResponse {
	args := []interface{}{destination}
	args = append(args, key...)
	return m.mockDo("SDIFFSTORE", args...)
}

func (m *MockRedisOp) SInter(key ...interface{}) *RedisResponse {
	return m.mockDo("SINTER", key...)
}

func (m *MockRedisOp) SInterCard(key ...interface{}) *RedisResponse {
	numkeys := int64(len(key))
	args := []interface{}{numkeys}
	args = append(args, key...)
	return m.mockDo("SINTERCARD", args...)
}

func (m *MockRedisOp) SInterStore(destination interface{}, key ...interface{}) *RedisResponse {
	args := []interface{}{destination}
	args = append(args, key...)
	return m.mockDo("SINTERSTORE", args...)
}

func (m *MockRedisOp) SIsMember(key, member interface{}) *RedisResponse {
	return m.mockDo("SISMEMBER", key, member)
}

func (m *MockRedisOp) SMembers(key interface{}) *RedisResponse {
	return m.mockDo("SMEMBERS", key)
}

func (m *MockRedisOp) SMIsMember(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return m.mockDo("SMISMEMBER", args...)
}

func (m *MockRedisOp) SMove(source, destination, member interface{}) *RedisResponse {
	return m.mockDo("SMOVE", source, destination, member)
}

func (m *MockRedisOp) SPop(key interface{}) *RedisResponse {
	return m.mockDo("SPOP", key)
}

func (m *MockRedisOp) SRandMember(key interface{}) *RedisResponse {
	return m.mockDo("SRANDMEMBER", key)
}

func (m *MockRedisOp) SRem(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return m.mockDo("SREM", args...)
}

func (m *MockRedisOp) SScan(key interface{}, cursor int64, match string, count int64) *RedisResponse {
	args := []interface{}{key, cursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", count)
	}
	return m.mockDo("SSCAN", args...)
}

func (m *MockRedisOp) SUnion(key ...interface{}) *RedisResponse {
	return m.mockDo("SUNION", key...)
}

func (m *MockRedisOp) SUnionStore(destination interface{}, key ...interface{}) *RedisResponse {
	args := []interface{}{destination}
	args = append(args, key...)
	return m.mockDo("SUNIONSTORE", args...)
}

// Sorted Set operations
func (m *MockRedisOp) ZAdd(key interface{}, score float64, member interface{}, pairs ...interface{}) *RedisResponse {
	args := []interface{}{key, score, member}
	args = append(args, pairs...)
	return m.mockDo("ZADD", args...)
}

func (m *MockRedisOp) ZCard(key interface{}) *RedisResponse {
	return m.mockDo("ZCARD", key)
}

func (m *MockRedisOp) ZCount(key interface{}, min, max string) *RedisResponse {
	return m.mockDo("ZCOUNT", key, min, max)
}

func (m *MockRedisOp) ZDiff(key ...interface{}) *RedisResponse {
	numkeys := int64(len(key))
	args := []interface{}{numkeys}
	args = append(args, key...)
	return m.mockDo("ZDIFF", args...)
}

func (m *MockRedisOp) ZDiffStore(destination interface{}, key ...interface{}) *RedisResponse {
	numkeys := int64(len(key))
	args := []interface{}{destination, numkeys}
	args = append(args, key...)
	return m.mockDo("ZDIFFSTORE", args...)
}

func (m *MockRedisOp) ZIncrBy(key interface{}, increment float64, member interface{}) *RedisResponse {
	return m.mockDo("ZINCRBY", key, increment, member)
}

func (m *MockRedisOp) ZInter(key ...interface{}) *RedisResponse {
	numkeys := int64(len(key))
	args := []interface{}{numkeys}
	args = append(args, key...)
	return m.mockDo("ZINTER", args...)
}

func (m *MockRedisOp) ZInterCard(key ...interface{}) *RedisResponse {
	numkeys := int64(len(key))
	args := []interface{}{numkeys}
	args = append(args, key...)
	return m.mockDo("ZINTERCARD", args...)
}

func (m *MockRedisOp) ZInterStore(destination interface{}, key ...interface{}) *RedisResponse {
	numkeys := int64(len(key))
	args := []interface{}{destination, numkeys}
	args = append(args, key...)
	return m.mockDo("ZINTERSTORE", args...)
}

func (m *MockRedisOp) ZLexCount(key interface{}, min, max string) *RedisResponse {
	return m.mockDo("ZLEXCOUNT", key, min, max)
}

func (m *MockRedisOp) ZMPop(count int64, where string, key ...interface{}) *RedisResponse {
	numkeys := count
	if numkeys <= 0 {
		numkeys = int64(len(key))
	}
	args := []interface{}{numkeys}
	args = append(args, key...)
	args = append(args, where)
	return m.mockDo("ZMPOP", args...)
}

func (m *MockRedisOp) ZMScore(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return m.mockDo("ZMSCORE", args...)
}

func (m *MockRedisOp) ZPopMax(key interface{}) *RedisResponse {
	return m.mockDo("ZPOPMAX", key)
}

func (m *MockRedisOp) ZPopMin(key interface{}) *RedisResponse {
	return m.mockDo("ZPOPMIN", key)
}

func (m *MockRedisOp) ZRandMember(key interface{}) *RedisResponse {
	return m.mockDo("ZRANDMEMBER", key)
}

func (m *MockRedisOp) ZRange(key interface{}, start, stop int64) *RedisResponse {
	return m.mockDo("ZRANGE", key, start, stop)
}

func (m *MockRedisOp) ZRangeByLex(key interface{}, min, max string) *RedisResponse {
	return m.mockDo("ZRANGEBYLEX", key, min, max)
}

func (m *MockRedisOp) ZRangeByScore(key interface{}, min, max string) *RedisResponse {
	return m.mockDo("ZRANGEBYSCORE", key, min, max)
}

func (m *MockRedisOp) ZRangeStore(dst interface{}, src interface{}, min, max int64) *RedisResponse {
	return m.mockDo("ZRANGESTORE", dst, src, min, max)
}

func (m *MockRedisOp) ZRevRange(key interface{}, start, stop int64) *RedisResponse {
	return m.mockDo("ZREVRANGE", key, start, stop)
}

func (m *MockRedisOp) ZRevRangeByLex(key interface{}, max, min string) *RedisResponse {
	return m.mockDo("ZREVRANGEBYLEX", key, max, min)
}

func (m *MockRedisOp) ZRevRangeByScore(key interface{}, max, min string) *RedisResponse {
	return m.mockDo("ZREVRANGEBYSCORE", key, max, min)
}

func (m *MockRedisOp) ZRank(key, member interface{}) *RedisResponse {
	return m.mockDo("ZRANK", key, member)
}

func (m *MockRedisOp) ZRem(key interface{}, member ...interface{}) *RedisResponse {
	args := []interface{}{key}
	args = append(args, member...)
	return m.mockDo("ZREM", args...)
}

func (m *MockRedisOp) ZRemRangeByLex(key interface{}, min, max string) *RedisResponse {
	return m.mockDo("ZREMRANGEBYLEX", key, min, max)
}

func (m *MockRedisOp) ZRemRangeByRank(key interface{}, start, stop int64) *RedisResponse {
	return m.mockDo("ZREMRANGEBYRANK", key, start, stop)
}

func (m *MockRedisOp) ZRemRangeByScore(key interface{}, min, max string) *RedisResponse {
	return m.mockDo("ZREMRANGEBYSCORE", key, min, max)
}

func (m *MockRedisOp) ZRevRank(key, member interface{}) *RedisResponse {
	return m.mockDo("ZREVRANK", key, member)
}

func (m *MockRedisOp) ZScan(key interface{}, cursor int64, match string, count int64) *RedisResponse {
	args := []interface{}{key, cursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", count)
	}
	return m.mockDo("ZSCAN", args...)
}

func (m *MockRedisOp) ZScore(key, member interface{}) *RedisResponse {
	return m.mockDo("ZSCORE", key, member)
}

func (m *MockRedisOp) ZUnion(key ...interface{}) *RedisResponse {
	numkeys := int64(len(key))
	args := []interface{}{numkeys}
	args = append(args, key...)
	return m.mockDo("ZUNION", args...)
}

func (m *MockRedisOp) ZUnionStore(destination interface{}, key ...interface{}) *RedisResponse {
	numkeys := int64(len(key))
	args := []interface{}{destination, numkeys}
	args = append(args, key...)
	return m.mockDo("ZUNIONSTORE", args...)
}

// Admin operations
func (m *MockRedisOp) FlushDB() *RedisResponse {
	return m.mockDo("FLUSHDB")
}

func (m *MockRedisOp) FlushAll() *RedisResponse {
	return m.mockDo("FLUSHALL")
}

func (m *MockRedisOp) Scan(cursor int64, match string, count int64) *RedisResponse {
	args := []interface{}{cursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", count)
	}
	return m.mockDo("SCAN", args...)
}

func (m *MockRedisOp) Ping() *RedisResponse {
	return m.mockDo("PING")
}

func (m *MockRedisOp) Publish(key interface{}, val interface{}) *RedisResponse {
	return m.mockDo("PUBLISH", key, val)
}

// Script operations
func (m *MockRedisOp) Eval(script string, keys []interface{}, args []interface{}) *RedisResponse {
	numkeys := int64(len(keys))
	cmdArgs := []interface{}{script, numkeys}
	cmdArgs = append(cmdArgs, keys...)
	cmdArgs = append(cmdArgs, args...)
	return m.mockDo("EVAL", cmdArgs...)
}

// NewMockRedis creates a Redis instance with mock operators for testing.
// This allows full testing of Redis operations without requiring a real Redis server.
func NewMockRedis() *Redis {
	mockMaster := NewMockRedisOp()
	mockSlave := NewMockRedisOp()
	
	return &Redis{
		name:   "mock",
		master: mockMaster,
		slave:  mockSlave,
	}
}

// NewRedisWithMock creates a Redis instance with custom mock operators.
// This allows fine-grained control over mock behavior for advanced testing scenarios.
func NewRedisWithMock(master, slave *MockRedisOp) *Redis {
	return &Redis{
		name:   "custom-mock",
		master: master,
		slave:  slave,
	}
}

// MockRedisBuilder provides a fluent interface for configuring mock Redis instances.
type MockRedisBuilder struct {
	masterMock *MockRedisOp
	slaveMock  *MockRedisOp
}

// NewMockRedisBuilder creates a new builder for configuring mock Redis instances.
func NewMockRedisBuilder() *MockRedisBuilder {
	return &MockRedisBuilder{
		masterMock: NewMockRedisOp(),
		slaveMock:  NewMockRedisOp(),
	}
}

// WithMasterResponse configures a response for master operations.
func (b *MockRedisBuilder) WithMasterResponse(cmd, key string, data interface{}, err error) *MockRedisBuilder {
	b.masterMock.SetResponse(cmd, key, data, err)
	return b
}

// WithSlaveResponse configures a response for slave operations.
func (b *MockRedisBuilder) WithSlaveResponse(cmd, key string, data interface{}, err error) *MockRedisBuilder {
	b.slaveMock.SetResponse(cmd, key, data, err)
	return b
}

// WithResponse configures a response for both master and slave operations.
func (b *MockRedisBuilder) WithResponse(cmd, key string, data interface{}, err error) *MockRedisBuilder {
	b.masterMock.SetResponse(cmd, key, data, err)
	b.slaveMock.SetResponse(cmd, key, data, err)
	return b
}

// WithSequentialResponses configures sequential responses for both master and slave.
func (b *MockRedisBuilder) WithSequentialResponses(cmd, key string, responses []MockResponse) *MockRedisBuilder {
	b.masterMock.SetSequentialResponses(cmd, key, responses)
	b.slaveMock.SetSequentialResponses(cmd, key, responses)
	return b
}

// WithConditionalResponse configures conditional responses for both master and slave.
func (b *MockRedisBuilder) WithConditionalResponse(cmd string, condition MockConditionFunc, response MockResponse) *MockRedisBuilder {
	b.masterMock.SetConditionalResponse(cmd, condition, response)
	b.slaveMock.SetConditionalResponse(cmd, condition, response)
	return b
}

// Build creates the configured Redis instance.
func (b *MockRedisBuilder) Build() *Redis {
	return &Redis{
		name:   "builder-mock",
		master: b.masterMock,
		slave:  b.slaveMock,
	}
}

// GetMasterMock returns the master mock for advanced configuration.
func (b *MockRedisBuilder) GetMasterMock() *MockRedisOp {
	return b.masterMock
}

// GetSlaveMock returns the slave mock for advanced configuration.
func (b *MockRedisBuilder) GetSlaveMock() *MockRedisOp {
	return b.slaveMock
}
