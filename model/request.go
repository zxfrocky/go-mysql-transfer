package model

import "sync"

var RowRequestPool = sync.Pool{
	New: func() interface{} {
		return new(RowRequest)
	},
}

type RowRequest struct {
	RuleKey   string
	Action    string
	Timestamp uint32
	Old       []interface{}
	Row       []interface{}
}

type PosRequest struct {
	Name  string `json:"name,omitempty"`
	Pos   uint32 `json:"pos,omitempty"`
	Gtid  string `json:"gtid,omitempty"`
	Force bool   `json:"-"`
}

func BuildRowRequest() *RowRequest {
	return RowRequestPool.Get().(*RowRequest)
}

func ReleaseRowRequest(t *RowRequest) {
	RowRequestPool.Put(t)
}
