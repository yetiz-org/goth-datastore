package datastore

import (
	"sync"
	"time"

	"github.com/yetiz-org/goth-secret"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// MockDatabaseOp is a mock implementation of DatabaseOperator for testing.
// It provides configurable responses and tracks all operations for test verification.
type MockDatabaseOp struct {
	mutex sync.RWMutex

	// Mock configuration
	mockDB          *gorm.DB
	mockAdapter     string
	mockMeta        secret.DatabaseMeta
	mockConnParams  ConnParams
	mockMysqlParams MysqlParams
	mockGORMParams  gorm.Config
	mockLogger      logger.Interface

	// Call tracking
	callHistory []MockDatabaseCall
	dbCallCount int

	// Response configuration
	dbResponse          *gorm.DB
	dbError             error
	adapterResponse     string
	returnNilDB         bool
	simulateDBFailure   bool
	simulateConnFailure bool
}

// MockDatabaseCall represents a recorded database operation call.
type MockDatabaseCall struct {
	Timestamp time.Time
	Method    string
	Args      []interface{}
	Result    interface{}
	Error     error
}

// NewMockDatabaseOp creates a new mock database operator with default settings.
func NewMockDatabaseOp() *MockDatabaseOp {
	return &MockDatabaseOp{
		mockAdapter: "mysql",
		mockMeta: secret.DatabaseMeta{
			Adapter: "mysql",
		},
		mockConnParams: ConnParams{
			Charset:          "utf8mb4",
			Timeout:          "3s",
			ReadTimeout:      "30s",
			WriteTimeout:     "30s",
			Collation:        "utf8mb4_general_ci",
			Loc:              "Local",
			ClientFoundRows:  false,
			ParseTime:        true,
			MaxAllowedPacket: 25165824,
			MaxOpenConn:      4,
			MaxIdleConn:      2,
			ConnMaxLifetime:  20000,
			ConnMaxIdleTime:  0,
		},
		callHistory: make([]MockDatabaseCall, 0),
	}
}

// DB returns the configured mock database instance.
func (m *MockDatabaseOp) DB() *gorm.DB {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.dbCallCount++
	call := MockDatabaseCall{
		Timestamp: time.Now(),
		Method:    "DB",
		Args:      []interface{}{},
		Result:    m.dbResponse,
		Error:     m.dbError,
	}
	m.callHistory = append(m.callHistory, call)

	if m.returnNilDB {
		return nil
	}

	if m.simulateDBFailure {
		return nil
	}

	if m.dbResponse != nil {
		return m.dbResponse
	}

	return m.mockDB
}

// Adapter returns the configured adapter name.
func (m *MockDatabaseOp) Adapter() string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if m.adapterResponse != "" {
		return m.adapterResponse
	}
	return m.mockAdapter
}

// GetConnParams returns the connection parameters.
func (m *MockDatabaseOp) GetConnParams() ConnParams {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.mockConnParams
}

// GetMysqlParams returns the MySQL-specific parameters.
func (m *MockDatabaseOp) GetMysqlParams() MysqlParams {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.mockMysqlParams
}

// GetGORMParams returns the GORM configuration.
func (m *MockDatabaseOp) GetGORMParams() gorm.Config {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.mockGORMParams
}

// GetLogger returns the logger interface.
func (m *MockDatabaseOp) GetLogger() logger.Interface {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.mockLogger
}

// Meta returns the database metadata.
func (m *MockDatabaseOp) Meta() secret.DatabaseMeta {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.mockMeta
}

// SetConnParams sets the connection parameters.
func (m *MockDatabaseOp) SetConnParams(params ConnParams) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mockConnParams = params
}

// SetMysqlParams sets the MySQL-specific parameters.
func (m *MockDatabaseOp) SetMysqlParams(params MysqlParams) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mockMysqlParams = params
}

// SetGORMParams sets the GORM configuration.
func (m *MockDatabaseOp) SetGORMParams(config gorm.Config) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mockGORMParams = config
}

// SetLogger sets the logger interface.
func (m *MockDatabaseOp) SetLogger(logger logger.Interface) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mockLogger = logger
}

// Mock configuration methods for testing

// SetMockDB sets the mock database instance to return.
func (m *MockDatabaseOp) SetMockDB(db *gorm.DB) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mockDB = db
}

// SetDBResponse configures the DB() method to return specific response.
func (m *MockDatabaseOp) SetDBResponse(db *gorm.DB, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.dbResponse = db
	m.dbError = err
}

// SetAdapterResponse sets the adapter name to return.
func (m *MockDatabaseOp) SetAdapterResponse(adapter string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.adapterResponse = adapter
}

// SetMeta sets the database metadata.
func (m *MockDatabaseOp) SetMeta(meta secret.DatabaseMeta) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mockMeta = meta
}

// SimulateDBFailure configures DB() to return nil (simulating connection failure).
func (m *MockDatabaseOp) SimulateDBFailure(fail bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.simulateDBFailure = fail
}

// SetReturnNilDB configures DB() to always return nil.
func (m *MockDatabaseOp) SetReturnNilDB(returnNil bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.returnNilDB = returnNil
}

// Test helper methods

// GetCallHistory returns all recorded method calls.
func (m *MockDatabaseOp) GetCallHistory() []MockDatabaseCall {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return append([]MockDatabaseCall{}, m.callHistory...)
}

// GetDBCallCount returns the number of times DB() was called.
func (m *MockDatabaseOp) GetDBCallCount() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.dbCallCount
}

// ClearCallHistory resets all recorded calls.
func (m *MockDatabaseOp) ClearCallHistory() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.callHistory = make([]MockDatabaseCall, 0)
	m.dbCallCount = 0
}

// GetCallsByMethod returns calls filtered by method name.
func (m *MockDatabaseOp) GetCallsByMethod(method string) []MockDatabaseCall {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var filtered []MockDatabaseCall
	for _, call := range m.callHistory {
		if call.Method == method {
			filtered = append(filtered, call)
		}
	}
	return filtered
}

// NewMockDatabase creates a Database instance with mock operators.
func NewMockDatabase() *Database {
	return &Database{
		writer: NewMockDatabaseOp(),
		reader: NewMockDatabaseOp(),
	}
}

// NewMockDatabaseWithOps creates a Database instance with custom mock operators.
func NewMockDatabaseWithOps(writer, reader *MockDatabaseOp) *Database {
	return &Database{
		writer: writer,
		reader: reader,
	}
}

// MockDatabaseBuilder provides a fluent interface for building mock Database instances.
type MockDatabaseBuilder struct {
	writerMock *MockDatabaseOp
	readerMock *MockDatabaseOp
}

// NewMockDatabaseBuilder creates a new builder for mock Database instances.
func NewMockDatabaseBuilder() *MockDatabaseBuilder {
	return &MockDatabaseBuilder{
		writerMock: NewMockDatabaseOp(),
		readerMock: NewMockDatabaseOp(),
	}
}

// WithWriterDB sets the writer's mock database.
func (b *MockDatabaseBuilder) WithWriterDB(db *gorm.DB) *MockDatabaseBuilder {
	b.writerMock.SetMockDB(db)
	return b
}

// WithReaderDB sets the reader's mock database.
func (b *MockDatabaseBuilder) WithReaderDB(db *gorm.DB) *MockDatabaseBuilder {
	b.readerMock.SetMockDB(db)
	return b
}

// WithWriterAdapter sets the writer's adapter.
func (b *MockDatabaseBuilder) WithWriterAdapter(adapter string) *MockDatabaseBuilder {
	b.writerMock.SetAdapterResponse(adapter)
	return b
}

// WithReaderAdapter sets the reader's adapter.
func (b *MockDatabaseBuilder) WithReaderAdapter(adapter string) *MockDatabaseBuilder {
	b.readerMock.SetAdapterResponse(adapter)
	return b
}

// WithWriterMeta sets the writer's metadata.
func (b *MockDatabaseBuilder) WithWriterMeta(meta secret.DatabaseMeta) *MockDatabaseBuilder {
	b.writerMock.SetMeta(meta)
	return b
}

// WithReaderMeta sets the reader's metadata.
func (b *MockDatabaseBuilder) WithReaderMeta(meta secret.DatabaseMeta) *MockDatabaseBuilder {
	b.readerMock.SetMeta(meta)
	return b
}

// Build creates the mock Database instance.
func (b *MockDatabaseBuilder) Build() *Database {
	return NewMockDatabaseWithOps(b.writerMock, b.readerMock)
}
