package storage

import (
	"encoding/json"
	"go-mysql-transfer/model"
	"go-mysql-transfer/util/logs"
	"io/ioutil"
	"os"
)

type filePositionStorage struct {
}

func (s *filePositionStorage) Initialize(pos model.PosRequest) error {
	p, err := s.Get()
	if err != nil {
		return err
	}

	if p.Gtid == "" {
		s.Save(pos)
	}

	return nil
}

func (s *filePositionStorage) Save(pos model.PosRequest) error {
	_file.Seek(0, os.SEEK_SET)
	_file.Truncate(0)
	bytes, _ := json.Marshal(pos) //msgpack.Marshal(pos)
	_, err := _file.Write(bytes)

	return err
}

func (s *filePositionStorage) Get() (model.PosRequest, error) {
	_file.Seek(0, os.SEEK_SET)
	content, err := ioutil.ReadAll(_file)
	if err != nil {
		logs.Errorf("filePositionStorage get err:%v", err)
		return model.PosRequest{}, err
	}
	if len(content) == 0 {
		return model.PosRequest{}, nil
	}

	var ret model.PosRequest
	err = json.Unmarshal(content, &ret)
	return ret, err
}
