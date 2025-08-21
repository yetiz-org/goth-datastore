package datastore

import (
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/yetiz-org/goth-secret"
)

// MockCassandraOp is a mock implementation of CassandraOperator for testing.
// It provides configurable responses and tracks all operations for test verification.
type MockCassandraOp struct {
	mutex sync.RWMutex

	// Mock configuration
	mockKeyspace        string
	mockConfig          *gocql.ClusterConfig
	mockSession         *gocql.Session
	mockColumnsMetadata map[string]CassandraColumnMetadata
	mockMaxRetryAttempt int

	// Call tracking
	callHistory []MockCassandraCall

	// Response configuration
	sessionResponse     *gocql.Session
	sessionError        error
	newSessionResponse  *gocql.Session
	newSessionError     error
	execError           error
	simulateFailure     bool
	returnNilSession    bool
	sessionClosed       bool
}

// MockCassandraCall represents a recorded Cassandra operation call.
type MockCassandraCall struct {
	Timestamp time.Time
	Method    string
	Args      []interface{}
	Result    interface{}
	Error     error
}

// NewMockCassandraOp creates a new mock Cassandra operator with default settings.
func NewMockCassandraOp() *MockCassandraOp {
	return &MockCassandraOp{
		mockKeyspace:        "test_keyspace",
		mockMaxRetryAttempt: 3,
		mockColumnsMetadata: make(map[string]CassandraColumnMetadata),
		callHistory:         make([]MockCassandraCall, 0),
		mockConfig:          gocql.NewCluster("127.0.0.1"),
	}
}

// Session returns the configured mock session.
func (m *MockCassandraOp) Session() *gocql.Session {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	call := MockCassandraCall{
		Timestamp: time.Now(),
		Method:    "Session",
		Args:      []interface{}{},
		Result:    m.sessionResponse,
		Error:     m.sessionError,
	}
	m.callHistory = append(m.callHistory, call)

	if m.returnNilSession || m.simulateFailure {
		return nil
	}

	if m.sessionResponse != nil {
		return m.sessionResponse
	}

	return m.mockSession
}

// NewSession creates a new mock session.
func (m *MockCassandraOp) NewSession() (*gocql.Session, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	call := MockCassandraCall{
		Timestamp: time.Now(),
		Method:    "NewSession",
		Args:      []interface{}{},
		Result:    m.newSessionResponse,
		Error:     m.newSessionError,
	}
	m.callHistory = append(m.callHistory, call)

	if m.simulateFailure {
		return nil, m.newSessionError
	}

	if m.newSessionError != nil {
		return nil, m.newSessionError
	}

	if m.newSessionResponse != nil {
		return m.newSessionResponse, nil
	}

	return m.mockSession, nil
}

// Close simulates closing the session.
func (m *MockCassandraOp) Close() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	call := MockCassandraCall{
		Timestamp: time.Now(),
		Method:    "Close",
		Args:      []interface{}{},
	}
	m.callHistory = append(m.callHistory, call)

	m.sessionClosed = true
}

// Exec executes a function with the mock session.
func (m *MockCassandraOp) Exec(f func(session *gocql.Session)) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	call := MockCassandraCall{
		Timestamp: time.Now(),
		Method:    "Exec",
		Args:      []interface{}{},
		Error:     m.execError,
	}
	m.callHistory = append(m.callHistory, call)

	if m.execError != nil {
		return m.execError
	}

	if m.simulateFailure {
		return m.execError
	}

	// Execute the function with mock session
	session := m.mockSession
	if m.newSessionResponse != nil {
		session = m.newSessionResponse
	}

	// Always execute the function, even with nil session for testing purposes
	f(session)

	return nil
}

// Keyspace returns the configured keyspace name.
func (m *MockCassandraOp) Keyspace() string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.mockKeyspace
}

// Config returns the mock cluster configuration.
func (m *MockCassandraOp) Config() *gocql.ClusterConfig {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.mockConfig
}

// ColumnsMetadata returns the configured column metadata.
func (m *MockCassandraOp) ColumnsMetadata() map[string]CassandraColumnMetadata {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.mockColumnsMetadata
}

// SetMaxRetryAttempt sets the maximum retry attempts.
func (m *MockCassandraOp) SetMaxRetryAttempt(maxRetry int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mockMaxRetryAttempt = maxRetry
}

// Mock configuration methods for testing

// SetMockSession sets the mock session to return.
func (m *MockCassandraOp) SetMockSession(session *gocql.Session) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mockSession = session
}

// SetSessionResponse configures the Session() method response.
func (m *MockCassandraOp) SetSessionResponse(session *gocql.Session, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.sessionResponse = session
	m.sessionError = err
}

// SetNewSessionResponse configures the NewSession() method response.
func (m *MockCassandraOp) SetNewSessionResponse(session *gocql.Session, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.newSessionResponse = session
	m.newSessionError = err
}

// SetExecError configures the Exec() method to return an error.
func (m *MockCassandraOp) SetExecError(err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.execError = err
}

// SetKeyspace sets the keyspace name.
func (m *MockCassandraOp) SetKeyspace(keyspace string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mockKeyspace = keyspace
}

// SetConfig sets the cluster configuration.
func (m *MockCassandraOp) SetConfig(config *gocql.ClusterConfig) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mockConfig = config
}

// SetColumnsMetadata sets the column metadata.
func (m *MockCassandraOp) SetColumnsMetadata(metadata map[string]CassandraColumnMetadata) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mockColumnsMetadata = metadata
}

// SimulateFailure configures methods to simulate failures.
func (m *MockCassandraOp) SimulateFailure(fail bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.simulateFailure = fail
}

// SetReturnNilSession configures Session() to always return nil.
func (m *MockCassandraOp) SetReturnNilSession(returnNil bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.returnNilSession = returnNil
}

// Test helper methods

// GetCallHistory returns all recorded method calls.
func (m *MockCassandraOp) GetCallHistory() []MockCassandraCall {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return append([]MockCassandraCall{}, m.callHistory...)
}

// ClearCallHistory resets all recorded calls.
func (m *MockCassandraOp) ClearCallHistory() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.callHistory = make([]MockCassandraCall, 0)
}

// GetCallsByMethod returns calls filtered by method name.
func (m *MockCassandraOp) GetCallsByMethod(method string) []MockCassandraCall {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var filtered []MockCassandraCall
	for _, call := range m.callHistory {
		if call.Method == method {
			filtered = append(filtered, call)
		}
	}
	return filtered
}

// IsSessionClosed returns whether the session was closed.
func (m *MockCassandraOp) IsSessionClosed() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.sessionClosed
}

// NewMockCassandra creates a Cassandra instance with mock operators.
func NewMockCassandra() *Cassandra {
	return &Cassandra{
		name:    "mock-cassandra",
		profile: secret.Cassandra{},
		writer:  NewMockCassandraOp(),
		reader:  NewMockCassandraOp(),
	}
}

// NewMockCassandraWithOps creates a Cassandra instance with custom mock operators.
func NewMockCassandraWithOps(writer, reader *MockCassandraOp) *Cassandra {
	return &Cassandra{
		name:    "custom-mock-cassandra",
		profile: secret.Cassandra{},
		writer:  writer,
		reader:  reader,
	}
}

// MockCassandraBuilder provides a fluent interface for building mock Cassandra instances.
type MockCassandraBuilder struct {
	writerMock *MockCassandraOp
	readerMock *MockCassandraOp
	profile    secret.Cassandra
	name       string
}

// NewMockCassandraBuilder creates a new builder for mock Cassandra instances.
func NewMockCassandraBuilder() *MockCassandraBuilder {
	return &MockCassandraBuilder{
		writerMock: NewMockCassandraOp(),
		readerMock: NewMockCassandraOp(),
		name:       "mock-cassandra",
	}
}

// WithWriterSession sets the writer's mock session.
func (b *MockCassandraBuilder) WithWriterSession(session *gocql.Session) *MockCassandraBuilder {
	b.writerMock.SetMockSession(session)
	return b
}

// WithReaderSession sets the reader's mock session.
func (b *MockCassandraBuilder) WithReaderSession(session *gocql.Session) *MockCassandraBuilder {
	b.readerMock.SetMockSession(session)
	return b
}

// WithWriterKeyspace sets the writer's keyspace.
func (b *MockCassandraBuilder) WithWriterKeyspace(keyspace string) *MockCassandraBuilder {
	b.writerMock.SetKeyspace(keyspace)
	return b
}

// WithReaderKeyspace sets the reader's keyspace.
func (b *MockCassandraBuilder) WithReaderKeyspace(keyspace string) *MockCassandraBuilder {
	b.readerMock.SetKeyspace(keyspace)
	return b
}

// WithProfile sets the Cassandra profile.
func (b *MockCassandraBuilder) WithProfile(profile secret.Cassandra) *MockCassandraBuilder {
	b.profile = profile
	return b
}

// WithName sets the instance name.
func (b *MockCassandraBuilder) WithName(name string) *MockCassandraBuilder {
	b.name = name
	return b
}

// Build creates the mock Cassandra instance.
func (b *MockCassandraBuilder) Build() *Cassandra {
	return &Cassandra{
		name:    b.name,
		profile: b.profile,
		writer:  b.writerMock,
		reader:  b.readerMock,
	}
}
