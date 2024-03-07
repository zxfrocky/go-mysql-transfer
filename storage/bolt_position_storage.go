/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
package storage

import (
	"encoding/json"
	"github.com/juju/errors"
	"go-mysql-transfer/model"
	"go.etcd.io/bbolt"
)

type boltPositionStorage struct {
	//Name string
	//Pos  uint32
}

func (s *boltPositionStorage) Initialize(pos model.PosRequest) error {
	return _bolt.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_positionBucket)
		data := bt.Get(_fixPositionId)
		if data != nil {
			return nil
		}

		bytes, err := json.Marshal(pos) //msgpack.Marshal(pos)
		if err != nil {
			return err
		}
		return bt.Put(_fixPositionId, bytes)
	})
}

func (s *boltPositionStorage) Save(pos model.PosRequest) error {
	return _bolt.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_positionBucket)
		data, err := json.Marshal(pos)
		if err != nil {
			return err
		}
		return bt.Put(_fixPositionId, data)
	})
}

func (s *boltPositionStorage) Get() (model.PosRequest, error) {
	var entity model.PosRequest
	err := _bolt.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_positionBucket)
		data := bt.Get(_fixPositionId)
		if data == nil {
			return errors.NotFoundf("PositionStorage")
		}
		return json.Unmarshal(data, &entity)
	})

	return entity, err
}
