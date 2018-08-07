package connection

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/tokopedia/r3/srcClean/datadog"
)

// NewRedis create new redis instance
func NewRedis(cfg RedisConfig, dd *datadog.DatadogInstance) (instance *RedisInstance, err error) {
	instance = &RedisInstance{
		RedisPool: nil,
		Config:    cfg,
		datadog:   dd,
	}

	instance.RedisPool, err = InitializeRedis(instance.Config)
	return
}

func (i *RedisInstance) SetDatadog(dd *datadog.DatadogInstance) (err error) {
	i.datadog = dd
	return
}

// InitializeRedis create new redis connection. nb: Idle timeout is in second
func InitializeRedis(cfg RedisConfig) (*redis.Pool, error) {
	var err error
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 10
	}
	if cfg.MaxActive < 0 {
		cfg.MaxActive = 600
	}
	if cfg.MaxIdle <= 0 {
		cfg.MaxIdle = 3
	}

	redisPool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, errDial := redis.Dial("tcp", cfg.Connection)
			if errDial != nil {
				err = errDial
				return nil, errDial
			}
			return c, nil
		},
		IdleTimeout: time.Duration(cfg.IdleTimeout) * time.Second,
		MaxActive:   cfg.MaxActive,
		MaxIdle:     cfg.MaxIdle,
		Wait:        true,
	}
	return redisPool, err
}

/*H Command*/
func (i *RedisInstance) HGetAll(key string, datadogAdditionalInfo map[string]string) (result map[string]string, err error) {
	loggingStartTime := time.Now()

	rdsConn := i.RedisPool.Get()
	result, err = redis.StringMap(rdsConn.Do("HGETALL", key))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	tags := []string{fmt.Sprintf("type:%s", "hgetall")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) HLen(key string, datadogAdditionalInfo map[string]string) (result int, err error) {
	loggingStartTime := time.Now()

	resultInt64 := int64(0)
	rdsConn := i.RedisPool.Get()
	resultInt64, err = redis.Int64(rdsConn.Do("HLEN", key))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}
	result = int(resultInt64)

	tags := []string{fmt.Sprintf("type:%s", "hlen")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) HGet(key, field string, datadogAdditionalInfo map[string]string) (result string, err error) {
	loggingStartTime := time.Now()

	resultTmp := []byte{}
	rdsConn := i.RedisPool.Get()
	resultTmp, err = redis.Bytes(rdsConn.Do("HGET", key, field))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	if err == redis.ErrNil {
		return "", nil
	} else if err != nil {
		return "", err
	}

	if resultTmp != nil {
		result = string(resultTmp)
	}

	tags := []string{fmt.Sprintf("type:%s", "hget")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) HSet(key, field string, value string, datadogAdditionalInfo map[string]string) (err error) {
	loggingStartTime := time.Now()

	rdsConn := i.RedisPool.Get()
	_, err = rdsConn.Do("HSET", key, field, value)
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	if err != nil {
		return err
	}

	tags := []string{fmt.Sprintf("type:%s", "hset")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}
func (i *RedisInstance) HMGet(key string, fields []string, datadogAdditionalInfo map[string]string) (result map[string]string, err error) {
	loggingStartTime := time.Now()

	result = make(map[string]string)
	if len(fields) > 0 {
		var pairsValInterface []interface{}
		pairsValInterface = append(pairsValInterface, key)
		for _, f := range fields {
			pairsValInterface = append(pairsValInterface, f)
		}
		resultTmp := []string{}
		rdsConn := i.RedisPool.Get()
		resultTmp, err = redis.Strings(rdsConn.Do("HMGET", pairsValInterface...))
		errRdsConn := rdsConn.Close()
		if errRdsConn != nil {
			err = errRdsConn

			return
		}
		for i, f := range fields {
			if len(resultTmp) > i {
				result[f] = resultTmp[i]
			}
		}
	}

	tags := []string{fmt.Sprintf("type:%s", "hmget")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}
func (i *RedisInstance) HMSet(key string, pairs map[string]string, datadogAdditionalInfo map[string]string) (err error) {
	loggingStartTime := time.Now()

	if len(pairs) > 0 {
		var pairsValInterface []interface{}
		pairsValInterface = append(pairsValInterface, key)
		for k, v := range pairs {
			pairsValInterface = append(pairsValInterface, k, v)
		}
		rdsConn := i.RedisPool.Get()
		_, err = rdsConn.Do("HMSET", pairsValInterface...)
		errRdsConn := rdsConn.Close()
		if errRdsConn != nil {
			err = errRdsConn

			return
		}
		if err != nil {
			return err
		}
	}

	tags := []string{fmt.Sprintf("type:%s", "hmset")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) HDel(key string, members []string, datadogAdditionalInfo map[string]string) (err error) {
	loggingStartTime := time.Now()

	var datas []interface{}
	datas = append(datas, key)
	for _, m := range members {
		datas = append(datas, m)
	}
	rdsConn := i.RedisPool.Get()
	_, err = rdsConn.Do("HDEL", datas...)
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}
	if err == redis.ErrNil {
		err = nil
	}
	tags := []string{fmt.Sprintf("type:%s", "hdel")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

/*Z Command*/

func (i *RedisInstance) ZScore(key, member string, datadogAdditionalInfo map[string]string) (result float64, err error) {
	loggingStartTime := time.Now()

	rdsConn := i.RedisPool.Get()
	result, err = redis.Float64(rdsConn.Do("ZSCORE", key, member))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	if err == redis.ErrNil {
		err = nil
		result = 0
	}

	tags := []string{fmt.Sprintf("type:%s", "zscore")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) ZAdd(key string, pairs map[string]float64, datadogAdditionalInfo map[string]string) (result int, err error) {
	loggingStartTime := time.Now()

	if len(pairs) <= 0 {
		return
	}

	var pairsValInterface []interface{}
	pairsValInterface = append(pairsValInterface, key)
	for k, v := range pairs {
		pairsValInterface = append(pairsValInterface, v, k)
	}

	r := int64(0)
	rdsConn := i.RedisPool.Get()
	r, err = redis.Int64(rdsConn.Do("ZADD", pairsValInterface...))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	if err == nil {
		result = int(r)
	}

	tags := []string{fmt.Sprintf("type:%s", "zadd")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) ZIncrBy(key string, increment float64, member string, datadogAdditionalInfo map[string]string) (err error) {
	loggingStartTime := time.Now()

	rdsConn := i.RedisPool.Get()
	_, err = rdsConn.Do("ZINCRBY", key, increment, member)
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	tags := []string{fmt.Sprintf("type:%s", "zincrby")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) ZRevRangeByScore(key, max, min string, datadogAdditionalInfo map[string]string) (result []string, err error) {
	loggingStartTime := time.Now()

	rdsConn := i.RedisPool.Get()
	result, err = redis.Strings(rdsConn.Do("ZREVRANGEBYSCORE", key, max, min))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	tags := []string{fmt.Sprintf("type:%s", "zrevrangebyscore")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) ZRevRange(key string, start, stop int, datadogAdditionalInfo map[string]string) (result []string, err error) {
	loggingStartTime := time.Now()

	rdsConn := i.RedisPool.Get()
	result, err = redis.Strings(rdsConn.Do("ZREVRANGE", key, start, stop))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	tags := []string{fmt.Sprintf("type:%s", "zrevrange")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) ZRevRangeWithscores(key string, start, stop int, datadogAdditionalInfo map[string]string) (result map[string]float64, err error) {
	loggingStartTime := time.Now()
	result = make(map[string]float64)
	redisResult := []string{}
	rdsConn := i.RedisPool.Get()
	redisResult, err = redis.Strings(rdsConn.Do("ZREVRANGE", key, start, stop, "WITHSCORES"))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}
	currentKey := ""
	for i, r := range redisResult {
		if i%2 == 0 {
			currentKey = r
		} else {
			currentScore, errParse := strconv.ParseFloat(r, 64)
			if errParse != nil {
				log.Println("[warning]", errParse)
				continue
			}
			result[currentKey] = currentScore
		}
	}

	tags := []string{fmt.Sprintf("type:%s", "zrevrange_withscores")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) ZRange(key string, start, stop int, datadogAdditionalInfo map[string]string) (result []string, err error) {
	loggingStartTime := time.Now()

	rdsConn := i.RedisPool.Get()
	result, err = redis.Strings(rdsConn.Do("ZRANGE", key, start, stop))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	tags := []string{fmt.Sprintf("type:%s", "zrange")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) ZRangeWithscores(key string, start, stop int, datadogAdditionalInfo map[string]string) (result map[string]float64, err error) {
	loggingStartTime := time.Now()
	result = make(map[string]float64)
	redisResult := []string{}

	rdsConn := i.RedisPool.Get()
	redisResult, err = redis.Strings(rdsConn.Do("ZRANGE", key, start, stop, "WITHSCORES"))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	currentKey := ""
	for i, r := range redisResult {
		if i%2 == 0 {
			currentKey = r
		} else {
			currentScore, errParse := strconv.ParseFloat(r, 64)
			if errParse != nil {
				log.Println("[warning]", errParse)
				continue
			}
			result[currentKey] = currentScore
		}
	}

	tags := []string{fmt.Sprintf("type:%s", "zrange_withscores")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}
func (i *RedisInstance) ZRangeByScore(key, min, max string, datadogAdditionalInfo map[string]string) (result []string, err error) {
	loggingStartTime := time.Now()

	rdsConn := i.RedisPool.Get()
	result, err = redis.Strings(rdsConn.Do("ZRANGEBYSCORE", key, min, max))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	tags := []string{fmt.Sprintf("type:%s", "zrangebyscore")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) ZRem(key string, members []string, datadogAdditionalInfo map[string]string) (err error) {
	loggingStartTime := time.Now()

	var datas []interface{}
	datas = append(datas, key)
	for _, m := range members {
		datas = append(datas, m)
	}

	rdsConn := i.RedisPool.Get()
	_, err = rdsConn.Do("ZREM", datas...)
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	if err == redis.ErrNil {
		err = nil
	}

	tags := []string{fmt.Sprintf("type:%s", "zrem")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) ZCount(key, min, max string, datadogAdditionalInfo map[string]string) (result int, err error) {
	loggingStartTime := time.Now()

	var tmpResult int64
	rdsConn := i.RedisPool.Get()
	tmpResult, err = redis.Int64(rdsConn.Do("ZCOUNT", key, min, max))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}
	if err == nil {
		result = int(tmpResult)
	}

	tags := []string{fmt.Sprintf("type:%s", "zcount")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) SAdd(key string, members []string, expireSeconds int, datadogAdditionalInfo map[string]string) (err error) {
	loggingStartTime := time.Now()
	if len(members) > 0 {
		var pairsValInterface []interface{}
		pairsValInterface = append(pairsValInterface, key)
		for _, v := range members {
			pairsValInterface = append(pairsValInterface, v)
		}

		rdsConn := i.RedisPool.Get()
		_, err = rdsConn.Do("SADD", pairsValInterface...)
		errRdsConn := rdsConn.Close()
		if errRdsConn != nil {
			err = errRdsConn

			return
		}
		if err == nil {
			if expireSeconds > 0 {
				rdsConn = i.RedisPool.Get()
				_, err = rdsConn.Do("EXPIRE", key, expireSeconds)
				errRdsConn := rdsConn.Close()
				if errRdsConn != nil {
					err = errRdsConn

					return
				}

				if err != nil {
					return err
				}
			}
		}
		if err != nil {
			return err
		}
	}

	tags := []string{fmt.Sprintf("type:%s", "sadd")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) IsExist(key string, datadogAdditionalInfo map[string]string) (bool, error) {
	loggingStartTime := time.Now()

	rdsConn := i.RedisPool.Get()
	results, err := redis.Int64(rdsConn.Do("EXISTS", key))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		return false, errRdsConn
	}

	if err != nil {
		return false, err
	}

	tags := []string{fmt.Sprintf("type:%s", "exists")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return results > 0, err
}

func (i *RedisInstance) SMembers(key string, datadogAdditionalInfo map[string]string) ([]string, error) {
	loggingStartTime := time.Now()

	rdsConn := i.RedisPool.Get()
	results, err := redis.Strings(rdsConn.Do("SMEMBERS", key))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		return []string{}, errRdsConn
	}

	tags := []string{fmt.Sprintf("type:%s", "smembers")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return results, err
}

func (i *RedisInstance) RPush(key string, members []string, expireSeconds int, datadogAdditionalInfo map[string]string) (err error) {
	loggingStartTime := time.Now()
	if len(members) > 0 {
		var pairsValInterface []interface{}
		pairsValInterface = append(pairsValInterface, key)
		for _, v := range members {
			pairsValInterface = append(pairsValInterface, v)
		}

		rdsConn := i.RedisPool.Get()
		_, err = rdsConn.Do("RPUSH", pairsValInterface...)
		errRdsConn := rdsConn.Close()
		if errRdsConn != nil {
			err = errRdsConn

			return
		}
		if err == nil {
			if expireSeconds > 0 {
				rdsConn = i.RedisPool.Get()
				_, err = rdsConn.Do("EXPIRE", key, expireSeconds)
				errRdsConn := rdsConn.Close()
				if errRdsConn != nil {
					err = errRdsConn

					return
				}
				if err != nil {
					return err
				}
			}
		}
		if err != nil {
			return err
		}
	}

	tags := []string{fmt.Sprintf("type:%s", "rpush")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) LPush(key string, members []string, expireSeconds int, datadogAdditionalInfo map[string]string) (err error) {
	loggingStartTime := time.Now()
	if len(members) > 0 {
		var pairsValInterface []interface{}
		pairsValInterface = append(pairsValInterface, key)
		for _, v := range members {
			pairsValInterface = append(pairsValInterface, v)
		}

		rdsConn := i.RedisPool.Get()
		_, err = rdsConn.Do("LPUSH", pairsValInterface...)
		errRdsConn := rdsConn.Close()
		if errRdsConn != nil {
			err = errRdsConn

			return
		}
		if err == nil {
			if expireSeconds > 0 {
				rdsConn = i.RedisPool.Get()
				_, err = rdsConn.Do("EXPIRE", key, expireSeconds)
				errRdsConn := rdsConn.Close()
				if errRdsConn != nil {
					err = errRdsConn

					return
				}
				if err != nil {
					return err
				}
			}
		}
		if err != nil {
			return err
		}
	}

	tags := []string{fmt.Sprintf("type:%s", "lpush")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) LRem(key string, count int, value string, datadogAdditionalInfo map[string]string) (err error) {
	loggingStartTime := time.Now()

	rdsConn := i.RedisPool.Get()
	_, err = rdsConn.Do("LREM", key, count, value)
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	tags := []string{fmt.Sprintf("type:%s", "lrem")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}
func (i *RedisInstance) LTrim(key string, start, stop int, datadogAdditionalInfo map[string]string) (err error) {
	loggingStartTime := time.Now()

	rdsConn := i.RedisPool.Get()
	_, err = rdsConn.Do("LTRIM", key, start, stop)
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	tags := []string{fmt.Sprintf("type:%s", "ltrim")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}
func (i *RedisInstance) LRange(key string, startIndex int, endIndex int, datadogAdditionalInfo map[string]string) ([]string, error) {
	loggingStartTime := time.Now()

	rdsConn := i.RedisPool.Get()
	results, err := redis.Strings(rdsConn.Do("LRANGE", key, startIndex, endIndex))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return []string{}, err
	}

	tags := []string{fmt.Sprintf("type:%s", "lrange")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return results, err
}

func (i *RedisInstance) Expire(key string, seconds int, datadogAdditionalInfo map[string]string) (result int, err error) {
	loggingStartTime := time.Now()

	rdsConn := i.RedisPool.Get()
	_, err = rdsConn.Do("EXPIRE", key, seconds)
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	tags := []string{fmt.Sprintf("type:%s", "expire")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) Delete(key string, datadogAdditionalInfo map[string]string) (err error) {
	loggingStartTime := time.Now()

	rdsConn := i.RedisPool.Get()
	_, err = rdsConn.Do("del", key)
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	tags := []string{fmt.Sprintf("type:%s", "delete")}
	for k, v := range datadogAdditionalInfo {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}

	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) Set(key string, value string, expireSeconds int, datadogAdditionalInfo map[string]string) (err error) {
	loggingStartTime := time.Now()
	tags := []string{fmt.Sprintf("type:%s", "set")}
	if expireSeconds <= 0 {
		rdsConn := i.RedisPool.Get()
		_, err = rdsConn.Do("set", key, value)
		errRdsConn := rdsConn.Close()
		if errRdsConn != nil {
			err = errRdsConn

			return
		}
	} else {
		rdsConn := i.RedisPool.Get()
		_, err = rdsConn.Do("setex", key, expireSeconds, value)
		errRdsConn := rdsConn.Close()
		if errRdsConn != nil {
			err = errRdsConn

			return
		}
	}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	i.datadog.RedisHistogram(
		time.Since(loggingStartTime).Seconds()*1000,
		tags,
	)
	return
}

func (i *RedisInstance) Get(key string, datadogAdditionalInfo map[string]string) (level string, err error) {
	results := []byte{}
	rdsConn := i.RedisPool.Get()
	results, err = redis.Bytes(rdsConn.Do("get", key))
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	if err != nil {
		if err.Error() == "redigo: nil returned" {
			return "", nil
		} else {
			log.Printf("[Redis] Get [ %s ] failed: %s", datadogAdditionalInfo, err.Error())
			return
		}
	}
	level = string(results)

	return
}

func (i *RedisInstance) Rename(key string, newkey string) (err error) {
	rdsConn := i.RedisPool.Get()
	_, err = rdsConn.Do("RENAME", key, newkey)
	errRdsConn := rdsConn.Close()
	if errRdsConn != nil {
		err = errRdsConn

		return
	}

	tags := []string{fmt.Sprintf("type:%s", "rename")}
	tags = append(tags, "ipredis:"+i.Config.Connection)
	return
}
