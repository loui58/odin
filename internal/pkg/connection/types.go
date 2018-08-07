package connection

import (
	"github.com/garyburd/redigo/redis"
	"github.com/tokopedia/r3/srcClean/datadog"
)

type (
	RedisConfig struct {
		Connection  string
		IdleTimeout int
		MaxActive   int
		MaxIdle     int
	}

	CassandraConfig struct {
		ClusterDSN  []string
		Keyspace    string
		Environment string
		Port        int
	}

	ElasticConfig struct {
		DSN            string
		SetSniff       bool
		SetHealthcheck bool
	}
)

type RedisOptionFunc func(*RedisInstance) error
type (
	RedisInstance struct {
		RedisPool *redis.Pool
		Config    RedisConfig
		datadog   *datadog.DatadogInstance
	}
)
