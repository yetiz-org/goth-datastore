package datastore

import (
	"github.com/gocql/gocql"
	secret "github.com/yetiz-org/goth-datastore/secrets"
)

// CassandraOperator defines the interface for Cassandra operations.
// This interface allows for both real and mock implementations,
// enabling comprehensive unit testing while maintaining API compatibility.
type CassandraOperator interface {
	// Session management
	Session() *gocql.Session
	NewSession() (*gocql.Session, error)
	Close()
	Exec(f func(session *gocql.Session)) error

	// Configuration access
	Keyspace() string
	Config() *gocql.ClusterConfig
	ColumnsMetadata() map[string]CassandraColumnMetadata

	// Configuration setters for testing
	SetMaxRetryAttempt(maxRetry int)
}

// CassandraProvider defines the interface for Cassandra instances.
// This allows both real and mock Cassandra implementations.
type CassandraProvider interface {
	Writer() CassandraOperator
	Reader() CassandraOperator
	Profile() secret.Cassandra
	Close()
}
