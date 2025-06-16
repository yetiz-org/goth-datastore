package datastore

import (
	"context"
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
		assert.Equal(t, "test_write", writer.keyspace)

		// Test Reader
		reader := csd.Reader()
		assert.NotNil(t, reader)
		assert.Equal(t, "test_read", reader.keyspace)
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
	attempts int
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
