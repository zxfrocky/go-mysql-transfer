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
	"go-mysql-transfer/model"

	"go-mysql-transfer/global"
)

type PositionStorage interface {
	Initialize(model.PosRequest) error
	Save(model.PosRequest) error
	Get() (model.PosRequest, error)
}

func NewPositionStorage() PositionStorage {
	if global.Cfg().IsCluster() {
		if global.Cfg().IsZk() {
			return &zkPositionStorage{}
		}
		if global.Cfg().IsEtcd() {
			return &etcdPositionStorage{}
		}
	}

	if global.Cfg().IsFile() {
		return &filePositionStorage{}
	}

	if global.Cfg().IsRedisStorage() {
		return &redisPositionStorage{}
	}

	return &boltPositionStorage{}
}
