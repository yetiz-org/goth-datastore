package datastore

import (
	"sync"

	"github.com/gocql/gocql"
	"github.com/kklab-com/goth-kklogger"
	kksecret "github.com/kklab-com/goth-kksecret"
)

var KKCassandraLocker = sync.Mutex{}
var KKCassandraProfiles = sync.Map{}
var KKCassandraDebug = false
var _KKCassandraDebug = sync.Once{}

type Cassandra struct {
	name   string
	writer *CassandraOp
	reader *CassandraOp
}

func (k *Cassandra) Writer() *CassandraOp {
	return k.writer
}

func (k *Cassandra) Reader() *CassandraOp {
	return k.reader
}

type CassandraOp struct {
	opType  OPType
	meta    kksecret.CassandraMeta
	cluster *gocql.ClusterConfig
	session *gocql.Session
	opLock  sync.Mutex
}

func (k *CassandraOp) Cluster() *gocql.ClusterConfig {
	return k.cluster
}

func (k *CassandraOp) Session() *gocql.Session {
	if k.session == nil {
		k.opLock.Lock()
		defer k.opLock.Unlock()
		if k.session == nil {
			session, err := k.cluster.CreateSession()
			if err != nil {
				kklogger.ErrorJ("goth-kkdatastore:CassandraOp.Session", err.Error())
				return nil
			}

			k.session = session
		}
	}

	return k.session
}

func KKCassandra(cassandraName string) *Cassandra {
	_KKCassandraDebug.Do(func() {
		if !KKCassandraDebug {
			KKCassandraDebug = _IsKKDatastoreDebug()
		}
	})

	if r, f := KKCassandraProfiles.Load(cassandraName); f && !KKCassandraDebug {
		return r.(*Cassandra)
	}

	profile := kksecret.CassandraProfile(cassandraName)
	if profile == nil {
		return nil
	}

	KKCassandraLocker.Lock()
	defer KKCassandraLocker.Unlock()
	if KKCassandraDebug {
		KKCassandraProfiles.Delete(cassandraName)
	}

	if r, f := KKCassandraProfiles.Load(cassandraName); !f {
		csd := &Cassandra{
			name: cassandraName,
		}

		wop := &CassandraOp{}
		wop.opType = TypeWriter
		wop.meta = profile.Writer
		wop.cluster = gocql.NewCluster(wop.meta.Hosts...)
		wop.cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: wop.meta.Username,
			Password: wop.meta.Password,
		}

		wop.cluster.SslOpts = &gocql.SslOptions{CaPath: wop.meta.CaPath}
		wop.cluster.Consistency = gocql.LocalQuorum
		csd.writer = wop

		rop := &CassandraOp{}
		rop.opType = TypeReader
		rop.meta = profile.Reader
		rop.cluster = gocql.NewCluster(rop.meta.Hosts...)
		rop.cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: rop.meta.Username,
			Password: rop.meta.Password,
		}

		rop.cluster.SslOpts = &gocql.SslOptions{CaPath: rop.meta.CaPath}
		rop.cluster.Consistency = gocql.LocalQuorum
		csd.reader = rop

		KKCassandraProfiles.Store(cassandraName, csd)
		return csd
	} else {
		return r.(*Cassandra)
	}
}
