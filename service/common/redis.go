package common

import (
	"context"
	"github.com/go-redis/redis/v8"
	"go-mysql-transfer/global"
	"go.uber.org/atomic"
	"strings"
)

type RedisCli struct {
	IsCluster bool
	Client    *redis.Client
	Cluster   *redis.ClusterClient
	closed    atomic.Bool
}

func GetRdsClient() *RedisCli {
	rdsCli := &RedisCli{}
	cfg := global.Cfg()
	list := strings.Split(cfg.RedisAddr, ",")
	if len(list) == 1 {
		rdsCli.Client = redis.NewClient(&redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPass,
			DB:       cfg.RedisDatabase,
		})
	} else {
		if cfg.RedisGroupType == global.RedisGroupTypeSentinel {
			rdsCli.Client = redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:    cfg.RedisMasterName,
				SentinelAddrs: list,
				Password:      cfg.RedisPass,
				DB:            cfg.RedisDatabase,
			})
		}
		if cfg.RedisGroupType == global.RedisGroupTypeCluster {
			rdsCli.IsCluster = true
			rdsCli.Cluster = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    list,
				Password: cfg.RedisPass,
			})
		}
	}

	return rdsCli
}

func (cli *RedisCli) Connect() error {
	return cli.Ping()
}

func (cli *RedisCli) Ping() error {
	var err error
	if cli.IsCluster {
		_, err = cli.Cluster.Ping(context.Background()).Result()
	} else {
		_, err = cli.Client.Ping(context.Background()).Result()
	}
	return err
}

func (cli *RedisCli) Close() {
	if !cli.closed.Load() {
		cli.closed.Store(true)
		if cli.Client != nil {
			cli.Client.Close()
		}

		if cli.Cluster != nil {
			cli.Cluster.Close()
		}
	}
}
