package model

import "github.com/go-mysql-org/go-mysql/schema"

type Padding struct {
	WrapName string

	ColumnName     string
	ColumnIndex    int
	ColumnType     int
	ColumnMetadata *schema.TableColumn
}
