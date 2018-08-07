package connection

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

func NewCassandra(cfg CassandraConfig) (sess *gocql.Session, err error) {
	cluster := gocql.NewCluster(cfg.ClusterDSN...)
	cluster.Port = cfg.Port
	if cfg.Port <= 0 {
		cfg.Port = 9042
	}
	if cfg.Keyspace != "" {
		cluster.Keyspace = cfg.Keyspace
	}
	cluster.Consistency = gocql.One

	if cfg.Environment == "development" || cfg.Environment == "" {
		cluster.DisableInitialHostLookup = true
		cluster.Timeout = time.Second * 100000
		cluster.ConnectTimeout = time.Second * 100000
	}

	sess, err = cluster.CreateSession()
	if err != nil {
		err = fmt.Errorf("[error][cassandra] Failed to initialize cassandra client %s  ", err)
	}

	return sess, err
}
