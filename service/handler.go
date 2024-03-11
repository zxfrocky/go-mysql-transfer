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
package service

import (
	"github.com/juju/errors"
	"go-mysql-transfer/metrics"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"go-mysql-transfer/global"
	"go-mysql-transfer/model"
	"go-mysql-transfer/util/logs"
)

type handler struct {
	queue chan interface{}
	stop  chan struct{}
}

func newHandler() *handler {
	return &handler{
		queue: make(chan interface{}, 4096),
		stop:  make(chan struct{}, 1),
	}
}

func (s *handler) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	if global.Cfg().SyncType == global.SyncTypePosition {
		logs.Infof("OnRotate  header:%+v rotateEvent:%+v", header, rotateEvent)
		s.queue <- model.PosRequest{
			Name:  string(rotateEvent.NextLogName),
			Pos:   uint32(rotateEvent.Position),
			Force: true,
		}
	}
	return nil
}

/*
func (s *handler) OnRotate(e *replication.RotateEvent) error {
	s.queue <- model.PosRequest{
		Name:  string(e.NextLogName),
		Pos:   uint32(e.Position),
		Force: true,
	}
	return nil
}
*/

func (s *handler) OnTableChanged(header *replication.EventHeader, schema string, table string) error {
	logs.Infof("OnTableChanged  header:%+v schema:%v table：%v", header, schema, table)
	err := _transferService.updateRule(schema, table)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *handler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, _ *replication.QueryEvent) error {
	if global.Cfg().SyncType == global.SyncTypePosition {
		logs.Infof("OnDDL  header:%+v nextPos:%v ", header, nextPos)
		s.queue <- model.PosRequest{
			Name:  nextPos.Name,
			Pos:   nextPos.Pos,
			Force: true,
		}
	}
	return nil
}

func (s *handler) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	if global.Cfg().SyncType == global.SyncTypePosition {
		logs.Infof("header  header:%+v nextPos:%v ", header, nextPos)
		s.queue <- model.PosRequest{
			Name:  nextPos.Name,
			Pos:   nextPos.Pos,
			Force: false,
		}
	}
	return nil
}

func (s *handler) OnRow(e *canal.RowsEvent) error {
	ruleKey := global.RuleKey(e.Table.Schema, e.Table.Name)
	if !global.RuleInsExist(ruleKey) {
		return nil
	}
	logs.Infof("header OnRow ruleKey:%v ", ruleKey)
	var requests []*model.RowRequest
	if e.Action != canal.UpdateAction {
		// 定长分配
		requests = make([]*model.RowRequest, 0, len(e.Rows))
	}

	if e.Action == canal.UpdateAction {
		for i := 0; i < len(e.Rows); i++ {
			if (i+1)%2 == 0 {
				v := new(model.RowRequest)
				v.RuleKey = ruleKey
				v.Action = e.Action
				v.Timestamp = e.Header.Timestamp
				if global.Cfg().IsReserveRawData() {
					v.Old = e.Rows[i-1]
				}
				v.Row = e.Rows[i]
				requests = append(requests, v)
			}
		}
	} else {
		for _, row := range e.Rows {
			v := new(model.RowRequest)
			v.RuleKey = ruleKey
			v.Action = e.Action
			v.Timestamp = e.Header.Timestamp
			v.Row = row
			requests = append(requests, v)
		}
	}
	s.queue <- requests

	return nil
}

func (s *handler) OnGTID(header *replication.EventHeader, gtid mysql.GTIDSet) error {
	if global.Cfg().SyncType == global.SyncTypeGtid {
		logs.Infof("OnGTID  header:%+v gtid:%v ", header, gtid)
		s.queue <- model.PosRequest{
			Gtid:  gtid.String(),
			Force: false,
		}
	}
	return nil
}

func (s *handler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	if global.Cfg().SyncType == global.SyncTypeGtid {
		logs.Infof("OnPosSynced  header:%+v pos:%v set:%v force:%v", header, pos, set, force)
		s.queue <- model.PosRequest{
			Name:  pos.Name,
			Pos:   pos.Pos,
			Gtid:  set.String(),
			Force: force,
		}
	}
	return nil
}

func (s *handler) String() string {
	return "TransferHandler"
}

func (s *handler) startListener() {
	go func() {
		interval := time.Duration(global.Cfg().FlushBulkInterval)
		bulkSize := global.Cfg().BulkSize
		ticker := time.NewTicker(time.Millisecond * interval)
		defer ticker.Stop()

		lastSavedTime := time.Now()
		requests := make([]*model.RowRequest, 0, bulkSize)
		var current model.PosRequest //mysql.Position
		from, _ := _transferService.positionDao.Get()
		for {
			needFlush := false
			needSavePos := false
			select {
			case v := <-s.queue:
				switch v := v.(type) {
				case model.PosRequest:
					now := time.Now()
					if v.Force || now.Sub(lastSavedTime) > 3*time.Second {
						lastSavedTime = now
						needFlush = true
						needSavePos = true
						/*
							current = mysql.Position{
								Name: v.Name,
								Pos:  v.Pos,
							}
						*/
						current = v
					}
				case []*model.RowRequest:
					requests = append(requests, v...)
					needFlush = int64(len(requests)) >= global.Cfg().BulkSize
				}
			case <-ticker.C:
				needFlush = true
			case <-s.stop:
				return
			}

			if needFlush && len(requests) > 0 && _transferService.endpointEnable.Load() {
				err := _transferService.endpoint.Consume(from, requests)
				if err != nil {
					_transferService.endpointEnable.Store(false)
					metrics.SetDestState(metrics.DestStateFail)
					logs.Error(err.Error())
					go _transferService.stopDump()
				}
				requests = requests[0:0]
			}
			if needSavePos && _transferService.endpointEnable.Load() {
				logs.Infof("save position %s %d %v", current.Name, current.Pos, current.Gtid)
				if err := _transferService.positionDao.Save(current); err != nil {
					logs.Errorf("save sync position %s err %v, close sync", current, err)
					_transferService.Close()
					return
				}
				from = current
			}
		}
	}()
}

func (s *handler) stopListener() {
	logs.Infof("transfer stop")
	s.stop <- struct{}{}
}
