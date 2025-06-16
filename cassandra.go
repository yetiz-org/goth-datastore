package datastore

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/yetiz-org/goth-kklogger"
	secret "github.com/yetiz-org/goth-secret"
)

// Cassandra represents a Cassandra database connection with separate read and write operations.
// It maintains separate connection pools for read and write operations to support different
// consistency requirements and potentially different endpoints.
type Cassandra struct {
	name   string       // Profile name used for configuration
	writer *CassandraOp // Write operations handler
	reader *CassandraOp // Read operations handler
}

// Writer returns the CassandraOp configured for write operations.
func (c *Cassandra) Writer() *CassandraOp {
	return c.writer
}

// Reader returns the CassandraOp configured for read operations.
func (c *Cassandra) Reader() *CassandraOp {
	return c.reader
}

// Close closes all active sessions (both reader and writer).
func (c *Cassandra) Close() {
	if c.writer != nil {
		c.writer.Close()
	}
	if c.reader != nil {
		c.reader.Close()
	}
}

// CassandraOp represents operations for a Cassandra database connection.
type CassandraOp struct {
	keyspace        string
	meta            secret.CassandraMeta // Connection metadata from configuration
	cluster         *gocql.ClusterConfig // Cassandra cluster configuration
	session         *gocql.Session       // Lazy-loaded session
	opLock          sync.Mutex           // Mutex to protect session initialization
	MaxRetryAttempt int
}

func (c *CassandraOp) Keyspace() string {
	return c.keyspace
}

// Config returns the underlying gocql cluster configuration.
func (c *CassandraOp) Config() *gocql.ClusterConfig {
	return c.cluster
}

func (c *CassandraOp) Exec(f func(session *gocql.Session)) error {
	if session, err := c.NewSession(); err == nil {
		defer session.Close()
		f(session)
		return nil
	} else {
		return err
	}
}

// NewSession creates and returns a new Cassandra session.
// Returns nil if session creation fails.
func (c *CassandraOp) NewSession() (*gocql.Session, error) {
	session, err := c.cluster.CreateSession()
	if err != nil {
		kklogger.ErrorJ("datastore:CassandraOp.NewSession", err.Error())
		return nil, err
	}

	return session, nil
}

// Session returns the current Cassandra session, creating it if it doesn't exist.
// Uses double-checked locking pattern for thread safety.
func (c *CassandraOp) Session() *gocql.Session {
	if c.session != nil && c.session.Closed() == false {
		return c.session
	}

	c.opLock.Lock()
	defer c.opLock.Unlock()
	var err error
	c.session, err = c.NewSession()
	if err != nil {
		return nil
	}

	return c.session
}

// Close safely closes the current session if it exists.
func (c *CassandraOp) Close() {
	if c.session != nil && c.session.Closed() == false {
		c.opLock.Lock()
		defer c.opLock.Unlock()
		c.session.Close()
		c.session = nil
	}
}

func (c *CassandraOp) ObserveConnect(connect gocql.ObservedConnect) {
	if connect.Err != nil {
		kklogger.WarnJ("datastore:CassandraOp.ObserveConnect", connect.Err.Error())
	} else {
		kklogger.DebugJ("datastore:CassandraOp.ObserveConnect", fmt.Sprintf("new connection to %s", connect.Host))
	}
}

func (c *CassandraOp) Attempt(query gocql.RetryableQuery) bool {
	eval := query.Attempts() < c.MaxRetryAttempt
	if eval {
		time.Sleep(time.Millisecond * 100)
	}

	return eval
}

func (c *CassandraOp) GetRetryType(err error) gocql.RetryType {
	return gocql.RetryNextHost
}

// configureCassandraOp creates and configures a CassandraOp with the provided metadata.
func configureCassandraOp(meta secret.CassandraMeta) *CassandraOp {
	op := &CassandraOp{
		keyspace: meta.Keyspace,
		meta:     meta,
	}

	// Configure the cluster
	op.configureCluster()

	return op
}

// configureCluster initializes and configures the gocql cluster based on the metadata.
func (c *CassandraOp) configureCluster() {
	c.cluster = gocql.NewCluster(strings.Split(c.meta.Endpoints[0], ":")[0])
	c.cluster.Port, _ = strconv.Atoi(strings.Split(c.meta.Endpoints[0], ":")[1])
	c.cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: c.meta.Username,
		Password: c.meta.Password,
	}

	c.cluster.SslOpts = &gocql.SslOptions{CaPath: c.meta.CaPath, EnableHostVerification: false}
	c.cluster.ProtoVersion = 3
	c.cluster.Consistency = gocql.LocalQuorum
	c.cluster.DisableInitialHostLookup = false
	c.cluster.DisableSkipMetadata = true
	c.cluster.NumConns = 2
	c.cluster.Compressor = gocql.SnappyCompressor{}
	c.cluster.Keyspace = c.meta.Keyspace
	c.cluster.ConnectObserver = c
	c.cluster.RetryPolicy = c
}

// NewCassandra creates a new Cassandra connection handler with the specified profile.
// Returns nil if the profile name is empty or if loading the profile fails.
func NewCassandra(profileName string) *Cassandra {
	if profileName == "" {
		kklogger.ErrorJ("datastore.NewCassandra#profileName", "profile name is empty")
		return nil
	}

	// Load the Cassandra profile
	profile := &secret.Cassandra{}
	if err := secret.Load("cassandra", profileName, profile); err != nil {
		kklogger.ErrorJ("datastore.NewCassandra#Load", err.Error())
		return nil
	}

	// Create Cassandra handler
	csd := &Cassandra{
		name: profileName,
	}

	// Configure writer and reader operations
	csd.writer = configureCassandraOp(profile.Writer)
	csd.reader = configureCassandraOp(profile.Reader)

	return csd
}
