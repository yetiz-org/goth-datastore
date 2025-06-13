package datastore

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	secret "github.com/yetiz-org/goth-secret"
)

func TestCassandra(t *testing.T) {
	t.Run("Writer", func(t *testing.T) {
		writer := &CassandraOp{name: "test-writer"}
		reader := &CassandraOp{name: "test-reader"}
		
		cassandra := &Cassandra{
			name:   "test",
			writer: writer,
			reader: reader,
		}

		assert.Equal(t, writer, cassandra.Writer())
	})

	t.Run("Reader", func(t *testing.T) {
		writer := &CassandraOp{name: "test-writer"}
		reader := &CassandraOp{name: "test-reader"}
		
		cassandra := &Cassandra{
			name:   "test",
			writer: writer,
			reader: reader,
		}

		assert.Equal(t, reader, cassandra.Reader())
	})

	t.Run("Close", func(t *testing.T) {
		writer := &CassandraOp{name: "test-writer"}
		reader := &CassandraOp{name: "test-reader"}
		
		cassandra := &Cassandra{
			name:   "test",
			writer: writer,
			reader: reader,
		}

		// Should not panic even with nil sessions
		assert.NotPanics(t, func() {
			cassandra.Close()
		})
	})

	t.Run("Close with nil operations", func(t *testing.T) {
		cassandra := &Cassandra{
			name:   "test",
			writer: nil,
			reader: nil,
		}

		// Should not panic
		assert.NotPanics(t, func() {
			cassandra.Close()
		})
	})
}

func TestCassandraOp(t *testing.T) {
	t.Run("Config", func(t *testing.T) {
		cluster := gocql.NewCluster("127.0.0.1")
		op := &CassandraOp{
			name:    "test",
			cluster: cluster,
		}

		assert.Equal(t, cluster, op.Config())
	})

	t.Run("NewSession with invalid cluster", func(t *testing.T) {
		// Create a cluster with invalid hosts to test error handling
		cluster := gocql.NewCluster("invalid-host:9999")
		cluster.Timeout = 100 * 1000 // Very short timeout
		
		op := &CassandraOp{
			name:    "test",
			cluster: cluster,
		}

		session, err := op.NewSession()
		
		// Should return error for invalid connection
		assert.Error(t, err)
		assert.Nil(t, session)
	})

	t.Run("Session with nil cluster", func(t *testing.T) {
		op := &CassandraOp{
			name:    "test",
			cluster: nil,
		}

		// This test documents that the original code has a bug - it panics with nil cluster
		// The Session method should check for nil cluster before calling NewSession
		assert.Panics(t, func() {
			op.Session()
		})
	})

	t.Run("Thread safety test", func(t *testing.T) {
		// Test concurrent access to Session method
		// Create a valid cluster to avoid nil pointer issues
		cluster := gocql.NewCluster("invalid-host:9999")
		cluster.Timeout = 1 // Very short timeout for quick failure
		
		op := &CassandraOp{
			name:    "test",
			cluster: cluster,
		}

		// Run multiple goroutines trying to get session
		var wg sync.WaitGroup
		numGoroutines := 10
		
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// This will return nil due to connection failure but should not cause race conditions
				op.Session()
			}()
		}

		wg.Wait()
		// Test passes if no race condition occurs
	})

	t.Run("Close with nil session", func(t *testing.T) {
		op := &CassandraOp{
			name:    "test",
			session: nil,
		}

		// Should not panic
		assert.NotPanics(t, func() {
			op.Close()
		})
	})
}

func TestConfigureCassandraOp(t *testing.T) {
	t.Run("Basic configuration", func(t *testing.T) {
		meta := secret.CassandraMeta{
			Endpoints: []string{"127.0.0.1", "127.0.0.2"},
			Username:  "testuser",
			Password:  "testpass",
			CaPath:    "/path/to/ca.pem",
		}

		op := configureCassandraOp("test", meta)

		assert.Equal(t, "test", op.name)
		assert.Equal(t, meta, op.meta)
		assert.NotNil(t, op.cluster)
	})
}

func TestCassandraOpConfigureCluster(t *testing.T) {
	t.Run("Configure cluster with all options", func(t *testing.T) {
		meta := secret.CassandraMeta{
			Endpoints: []string{"127.0.0.1:9042", "127.0.0.2:9042"},
			Username:  "testuser",
			Password:  "testpass",
			CaPath:    "/path/to/ca.pem",
		}

		op := &CassandraOp{
			name: "test",
			meta: meta,
		}

		op.configureCluster()

		assert.NotNil(t, op.cluster)
		assert.Equal(t, meta.Endpoints, op.cluster.Hosts)
		assert.Equal(t, gocql.LocalQuorum, op.cluster.Consistency)
		
		// Check authenticator
		if auth, ok := op.cluster.Authenticator.(gocql.PasswordAuthenticator); ok {
			assert.Equal(t, meta.Username, auth.Username)
			assert.Equal(t, meta.Password, auth.Password)
		} else {
			t.Error("Expected PasswordAuthenticator")
		}

		// Check SSL options
		assert.NotNil(t, op.cluster.SslOpts)
		assert.Equal(t, meta.CaPath, op.cluster.SslOpts.CaPath)
	})

	t.Run("Configure cluster with single endpoint", func(t *testing.T) {
		meta := secret.CassandraMeta{
			Endpoints: []string{"localhost:9042"},
			Username:  "user",
			Password:  "pass",
		}

		op := &CassandraOp{
			name: "test",
			meta: meta,
		}

		op.configureCluster()

		assert.NotNil(t, op.cluster)
		assert.Equal(t, []string{"localhost:9042"}, op.cluster.Hosts)
	})

	t.Run("Configure cluster with empty endpoints", func(t *testing.T) {
		meta := secret.CassandraMeta{
			Endpoints: []string{},
			Username:  "user",
			Password:  "pass",
		}

		op := &CassandraOp{
			name: "test",
			meta: meta,
		}

		op.configureCluster()

		assert.NotNil(t, op.cluster)
		// Should create cluster even with empty endpoints
		assert.Equal(t, []string{}, op.cluster.Hosts)
	})
}

func TestNewCassandra(t *testing.T) {
	t.Run("Empty profile name", func(t *testing.T) {
		cassandra := NewCassandra("")
		assert.Nil(t, cassandra)
	})

	t.Run("Invalid profile name", func(t *testing.T) {
		// This should return nil due to configuration loading failure
		cassandra := NewCassandra("non-existent-profile")
		assert.Nil(t, cassandra)
	})

	t.Run("Valid profile name structure", func(t *testing.T) {
		// This test verifies the function doesn't panic with valid input
		profileName := "test-profile"
		
		// The function should return nil if configuration loading fails
		// but it shouldn't panic
		assert.NotPanics(t, func() {
			cassandra := NewCassandra(profileName)
			// Will be nil due to configuration loading failure in test environment
			assert.Nil(t, cassandra)
		})
	})
}

func TestCassandraOpExec(t *testing.T) {
	t.Run("Exec with invalid cluster", func(t *testing.T) {
		// Create an operation with invalid cluster to test error handling
		cluster := gocql.NewCluster("invalid-host:9999")
		cluster.Timeout = 100 * 1000 // Very short timeout
		
		op := &CassandraOp{
			name:    "test",
			cluster: cluster,
		}

		var functionCalled bool
		err := op.Exec(func(session *gocql.Session) {
			functionCalled = true
		})

		// Should return error and function should not be called
		assert.Error(t, err)
		assert.False(t, functionCalled)
	})

	t.Run("Exec with nil cluster", func(t *testing.T) {
		op := &CassandraOp{
			name:    "test",
			cluster: nil,
		}

		var functionCalled bool
		
		// This test documents that the original code has a bug - it panics with nil cluster
		assert.Panics(t, func() {
			op.Exec(func(session *gocql.Session) {
				functionCalled = true
			})
		})
		
		assert.False(t, functionCalled)
	})
}

// Integration-style tests that work with actual gocql types
func TestCassandraIntegration(t *testing.T) {
	t.Run("Full configuration workflow", func(t *testing.T) {
		meta := secret.CassandraMeta{
			Endpoints: []string{"127.0.0.1:9042"},
			Username:  "cassandra",
			Password:  "cassandra",
			CaPath:    "",
		}

		// Test the full configuration process
		op := configureCassandraOp("integration-test", meta)
		
		assert.Equal(t, "integration-test", op.name)
		assert.Equal(t, meta, op.meta)
		assert.NotNil(t, op.cluster)
		
		// Verify cluster configuration
		assert.Equal(t, meta.Endpoints, op.cluster.Hosts)
		assert.Equal(t, gocql.LocalQuorum, op.cluster.Consistency)
		
		// Test that methods don't panic
		assert.NotPanics(t, func() {
			config := op.Config()
			assert.NotNil(t, config)
		})
		
		assert.NotPanics(t, func() {
			op.Close()
		})
	})
}

// Benchmark tests
func BenchmarkCassandraOpConfig(b *testing.B) {
	cluster := gocql.NewCluster("127.0.0.1")
	op := &CassandraOp{
		name:    "benchmark",
		cluster: cluster,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op.Config()
	}
}

func BenchmarkConfigureCassandraOp(b *testing.B) {
	meta := secret.CassandraMeta{
		Endpoints: []string{"127.0.0.1:9042"},
		Username:  "user",
		Password:  "password",
		CaPath:    "/path/to/ca.pem",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := configureCassandraOp("benchmark", meta)
		_ = op
	}
}

// TestLoadCassandraExampleSecret tests loading Cassandra secret from example file
func TestLoadCassandraExampleSecret(t *testing.T) {
	// Save original secret path and restore it after test
	originalPath := secret.PATH
	defer func() {
		secret.PATH = originalPath
	}()

	// Set secret path to the example directory which has the correct structure
	wd, _ := os.Getwd()
	secret.PATH = filepath.Join(wd, "example")

	// Test loading the secret
	t.Run("LoadCassandraSecret", func(t *testing.T) {
		profile := &secret.Cassandra{}
		err := secret.Load("cassandra", "test", profile)
		assert.NoError(t, err)

		// Verify writer configuration
		assert.NotNil(t, profile.Writer)
		assert.Equal(t, []string{"localhost:9042"}, profile.Writer.Endpoints)
		assert.Equal(t, "cassandra", profile.Writer.Username)
		assert.Equal(t, "cassandra", profile.Writer.Password)
		assert.Equal(t, "/path/to/ca.pem", profile.Writer.CaPath)

		// Verify reader configuration
		assert.NotNil(t, profile.Reader)
		assert.Equal(t, []string{"localhost:9042"}, profile.Reader.Endpoints)
		assert.Equal(t, "cassandra", profile.Reader.Username)
		assert.Equal(t, "cassandra", profile.Reader.Password)
		assert.Equal(t, "", profile.Reader.CaPath)
	})

	t.Run("NewCassandraWithExampleSecret", func(t *testing.T) {
		// Test creating Cassandra instance with the example secret
		cassandra := NewCassandra("test")

		// NewCassandra should succeed with valid secret
		assert.NotNil(t, cassandra)
		assert.Equal(t, "test", cassandra.name)
		assert.NotNil(t, cassandra.writer)
		assert.NotNil(t, cassandra.reader)

		// Verify the writer and reader are configured correctly
		assert.Equal(t, "test", cassandra.writer.name)
		assert.Equal(t, "test", cassandra.reader.name)
	})
}
