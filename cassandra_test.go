package datastore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	secret "github.com/yetiz-org/goth-secret"
)

// TestCassandraBasic tests the basic methods of Cassandra
func TestCassandraBasic(t *testing.T) {
	t.Run("Writer and Reader methods", func(t *testing.T) {
		csd := &Cassandra{
			name:   "test",
			writer: &CassandraOp{keyspace: "test_write"},
			reader: &CassandraOp{keyspace: "test_read"},
		}

		// Test Writer
		writer := csd.Writer()
		assert.NotNil(t, writer)
		assert.Equal(t, "test_write", writer.Keyspace())

		// Test Reader
		reader := csd.Reader()
		assert.NotNil(t, reader)
		assert.Equal(t, "test_read", reader.Keyspace())
	})

	t.Run("Close method", func(t *testing.T) {
		// Simply test that close doesn't panic when sessions are nil
		csd := &Cassandra{
			name:   "test",
			writer: &CassandraOp{},
			reader: &CassandraOp{},
		}

		// Should not panic
		csd.Close()
	})
}

// TestCassandraOpBasic tests the basic methods of CassandraOp
// testQuery is a simple struct that implements the RetryableQuery interface for testing purposes
type testQuery struct {
	attempts    int
	consistency gocql.Consistency
}

func (q *testQuery) Attempts() int {
	return q.attempts
}

// Context implements the RetryableQuery interface
func (q *testQuery) Context() context.Context {
	return context.Background()
}

// GetConsistency implements the RetryableQuery interface
func (q *testQuery) GetConsistency() gocql.Consistency {
	if q.consistency == 0 {
		return gocql.LocalQuorum
	}
	return q.consistency
}

// SetConsistency implements the RetryableQuery interface
func (q *testQuery) SetConsistency(consistency gocql.Consistency) {
	q.consistency = consistency
}

func TestCassandraOpBasic(t *testing.T) {
	t.Run("Keyspace method", func(t *testing.T) {
		op := &CassandraOp{keyspace: "test_keyspace"}
		assert.Equal(t, "test_keyspace", op.Keyspace())
	})

	t.Run("Config method", func(t *testing.T) {
		clusterCfg := gocql.NewCluster("127.0.0.1")
		op := &CassandraOp{
			cluster: clusterCfg,
		}
		assert.Equal(t, clusterCfg, op.Config())
	})

	t.Run("configureCluster method", func(t *testing.T) {
		meta := secret.CassandraMeta{
			Endpoints: []string{"127.0.0.1:9042"},
			Username:  "testuser",
			Password:  "testpass",
			CaPath:    "/path/to/ca",
			Keyspace:  "testkeyspace",
		}

		op := &CassandraOp{
			meta: meta,
		}

		// Call configureCluster
		op.configureCluster()

		// Verify cluster configuration
		assert.NotNil(t, op.cluster)
		assert.Equal(t, []string{"127.0.0.1"}, op.cluster.Hosts)
		assert.Equal(t, 9042, op.cluster.Port)
		assert.Equal(t, "testkeyspace", op.cluster.Keyspace)

		// Verify authenticator
		auth, ok := op.cluster.Authenticator.(gocql.PasswordAuthenticator)
		assert.True(t, ok)
		assert.Equal(t, "testuser", auth.Username)
		assert.Equal(t, "testpass", auth.Password)
	})

	t.Run("GetRetryType method", func(t *testing.T) {
		op := &CassandraOp{}
		retryType := op.GetRetryType(nil)
		assert.Equal(t, gocql.RetryNextHost, retryType)
	})

	t.Run("Attempt method", func(t *testing.T) {
		op := &CassandraOp{
			MaxRetryAttempt: 3,
		}

		// Test under max attempts
		q1 := &testQuery{attempts: 2}
		result := op.Attempt(q1)
		assert.True(t, result)

		// Test at max attempts
		q2 := &testQuery{attempts: 3}
		result = op.Attempt(q2)
		assert.False(t, result)
	})
}

// TestNewCassandra tests creating a new Cassandra instance
func TestNewCassandra(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	t.Run("Empty profile name", func(t *testing.T) {
		// Test with empty profile name
		csd := NewCassandra("")
		assert.Nil(t, csd)
	})

	// The following test would require setting up correct example files
	// We'll add a basic structure test but skip the actual file creation
	t.Run("configureCassandraOp function", func(t *testing.T) {
		meta := secret.CassandraMeta{
			Endpoints: []string{"127.0.0.1:9042"},
			Username:  "testuser",
			Password:  "testpass",
			CaPath:    "/path/to/ca",
			Keyspace:  "testkeyspace",
		}

		op := configureCassandraOp(meta)

		assert.NotNil(t, op)
		assert.Equal(t, "testkeyspace", op.keyspace)
		assert.Equal(t, meta, op.meta)
		assert.NotNil(t, op.cluster)
	})
}

// TestMockCassandraOp tests the mock Cassandra operator functionality
func TestMockCassandraOp(t *testing.T) {
	t.Run("Basic mock functionality", func(t *testing.T) {
		mock := NewMockCassandraOp()

		// Test initial state
		assert.Equal(t, "test_keyspace", mock.Keyspace())
		assert.NotNil(t, mock.Config())
		assert.NotNil(t, mock.ColumnsMetadata())

		// Test Session call tracking
		session := mock.Session()
		assert.Nil(t, session) // Should be nil without configuration

		// Test call history
		history := mock.GetCallHistory()
		assert.Len(t, history, 1)
		assert.Equal(t, "Session", history[0].Method)
	})

	t.Run("Configuration methods", func(t *testing.T) {
		mock := NewMockCassandraOp()

		// Test setting keyspace
		mock.SetKeyspace("production_keyspace")
		assert.Equal(t, "production_keyspace", mock.Keyspace())

		// Test setting cluster config
		newConfig := gocql.NewCluster("192.168.1.100")
		mock.SetConfig(newConfig)
		assert.Equal(t, newConfig, mock.Config())

		// Test setting column metadata
		metadata := map[string]CassandraColumnMetadata{
			"users": {
				keyspaceName: "test_keyspace",
				tableName:    "users",
				Columns: map[string]CassandraColumnMetadataColumn{
					"id":   {Name: "id", Kind: "partition_key", Type: "uuid"},
					"name": {Name: "name", Kind: "regular", Type: "text"},
				},
			},
		}
		mock.SetColumnsMetadata(metadata)
		assert.Equal(t, metadata, mock.ColumnsMetadata())

		// Test max retry attempts
		mock.SetMaxRetryAttempt(5)
		// Note: GetMaxRetryAttempt is not exposed in interface, but the value is set internally
	})

	t.Run("Session response simulation", func(t *testing.T) {
		mock := NewMockCassandraOp()

		// Test nil session response
		mock.SetReturnNilSession(true)
		assert.Nil(t, mock.Session())

		// Test session failure simulation
		mock.SetReturnNilSession(false)
		mock.SimulateFailure(true)
		assert.Nil(t, mock.Session())

		// Test NewSession with error
		expectedErr := errors.New("connection refused")
		mock.SetNewSessionResponse(nil, expectedErr)
		mock.SimulateFailure(false)
		session, err := mock.NewSession()
		assert.Nil(t, session)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("Exec functionality", func(t *testing.T) {
		mock := NewMockCassandraOp()

		// Test successful exec
		executed := false
		err := mock.Exec(func(session *gocql.Session) {
			executed = true
		})
		assert.NoError(t, err)
		assert.True(t, executed)

		// Test exec with error
		expectedErr := errors.New("exec failed")
		mock.SetExecError(expectedErr)
		err = mock.Exec(func(session *gocql.Session) {
			// Should not be called
		})
		assert.Equal(t, expectedErr, err)
	})

	t.Run("Close functionality", func(t *testing.T) {
		mock := NewMockCassandraOp()

		// Test close doesn't panic
		assert.NotPanics(t, func() {
			mock.Close()
		})

		// Verify close was recorded
		assert.True(t, mock.IsSessionClosed())

		closeHistory := mock.GetCallsByMethod("Close")
		assert.Len(t, closeHistory, 1)
	})

	t.Run("Call history tracking", func(t *testing.T) {
		mock := NewMockCassandraOp()

		// Make multiple calls
		mock.Session()
		mock.NewSession()
		mock.Close()
		mock.Exec(func(session *gocql.Session) {})

		// Verify call history
		history := mock.GetCallHistory()
		assert.Len(t, history, 4)

		sessionCalls := mock.GetCallsByMethod("Session")
		assert.Len(t, sessionCalls, 1)

		newSessionCalls := mock.GetCallsByMethod("NewSession")
		assert.Len(t, newSessionCalls, 1)

		closeCalls := mock.GetCallsByMethod("Close")
		assert.Len(t, closeCalls, 1)

		execCalls := mock.GetCallsByMethod("Exec")
		assert.Len(t, execCalls, 1)

		// Test clearing history
		mock.ClearCallHistory()
		history = mock.GetCallHistory()
		assert.Len(t, history, 0)
	})
}

// TestNewMockCassandra tests the mock Cassandra constructor
func TestNewMockCassandra(t *testing.T) {
	t.Run("Creates valid mock Cassandra instance", func(t *testing.T) {
		cass := NewMockCassandra()

		assert.NotNil(t, cass)
		assert.NotNil(t, cass.Writer())
		assert.NotNil(t, cass.Reader())

		// Verify both writer and reader are MockCassandraOp instances
		writer := cass.Writer()
		reader := cass.Reader()

		assert.Equal(t, "test_keyspace", writer.Keyspace())
		assert.Equal(t, "test_keyspace", reader.Keyspace())
	})

	t.Run("NewMockCassandraWithOps creates custom instance", func(t *testing.T) {
		writerMock := NewMockCassandraOp()
		readerMock := NewMockCassandraOp()

		writerMock.SetKeyspace("write_keyspace")
		readerMock.SetKeyspace("read_keyspace")

		cass := NewMockCassandraWithOps(writerMock, readerMock)

		assert.NotNil(t, cass)
		assert.Equal(t, "write_keyspace", cass.Writer().Keyspace())
		assert.Equal(t, "read_keyspace", cass.Reader().Keyspace())
	})
}

// TestMockCassandraBuilder tests the builder pattern for mock Cassandra instances
func TestMockCassandraBuilder(t *testing.T) {
	t.Run("Builder pattern configuration", func(t *testing.T) {
		builder := NewMockCassandraBuilder()

		profile := secret.Cassandra{}

		cass := builder.
			WithWriterKeyspace("writer_keyspace").
			WithReaderKeyspace("reader_keyspace").
			WithProfile(profile).
			WithName("test-builder-cassandra").
			Build()

		assert.NotNil(t, cass)
		assert.Equal(t, "writer_keyspace", cass.Writer().Keyspace())
		assert.Equal(t, "reader_keyspace", cass.Reader().Keyspace())
		assert.Equal(t, profile, cass.Profile())
	})
}
