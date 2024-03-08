package storage

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"go-mysql-transfer/model"
	"time"
)

type redisPositionStorage struct {
}

func (s *redisPositionStorage) Initialize(pos model.PosRequest) error {
	p, err := s.Get()
	if err != nil {
		return err
	}

	if p.Gtid == "" {
		s.Save(pos)
	}

	return nil
}

func (s *redisPositionStorage) Save(pos model.PosRequest) error {
	bytes, _ := json.Marshal(pos) //msgpack.Marshal(pos)
	var (
		err error
		ctx = context.Background()
	)
	if _rdsCli.IsCluster {
		_, err = _rdsCli.Cluster.Set(ctx, s.getKey(), string(bytes), time.Hour*24*30*3).Result()
	} else {
		_, err = _rdsCli.Client.Set(ctx, s.getKey(), string(bytes), time.Hour*24*30*3).Result()
	}

	return err
}

func (s *redisPositionStorage) Get() (model.PosRequest, error) {
	ctx := context.Background()
	var (
		content string
		err     error
	)
	if _rdsCli.IsCluster {
		content, err = _rdsCli.Cluster.Get(ctx, s.getKey()).Result()
		if err != nil && err != redis.Nil {
			return model.PosRequest{}, err
		}
	} else {
		content, err = _rdsCli.Client.Get(ctx, s.getKey()).Result()
		if err != nil && err != redis.Nil {
			return model.PosRequest{}, err
		}
	}

	if len(content) == 0 {
		return model.PosRequest{}, nil
	}

	var ret model.PosRequest
	err = json.Unmarshal([]byte(content), &ret)
	return ret, err
}

func (s *redisPositionStorage) getKey() string {
	return "mysql.pos"
}
