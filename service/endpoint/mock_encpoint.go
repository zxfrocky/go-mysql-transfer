package endpoint

import (
	"go-mysql-transfer/model"
	"go-mysql-transfer/util/logs"
)

type MockEndpoint struct {
}

func (m MockEndpoint) Connect() error {
	//TODO implement me
	//panic("implement me")
	return nil
}

func (m MockEndpoint) Ping() error {
	//TODO implement me
	//panic("implement me")
	return nil
}

func (m MockEndpoint) Consume(i interface{}, requests []*model.RowRequest) error {
	//TODO implement me
	//panic("implement me")
	logs.Infof("consume requests:%+v", requests)
	return nil
}

func (m MockEndpoint) Stock(requests []*model.RowRequest) int64 {
	//TODO implement me
	//panic("implement me")
	return 0
}

func (m MockEndpoint) Close() {
	//TODO implement me
	//panic("implement me")
}

func newMockEndpoint() *MockEndpoint {
	return &MockEndpoint{}
}
