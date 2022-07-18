// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odbc

import (
	"database/sql/driver"
	"io"

	"github.com/alexbrainman/odbc/api"
)

var _ driver.RowsColumnTypeDatabaseTypeName = (*Rows)(nil)

type Rows struct {
	os *ODBCStmt
}

func (r *Rows) Columns() []string {
	names := make([]string, len(r.os.Cols))
	for i := 0; i < len(names); i++ {
		names[i] = r.os.Cols[i].Name()
	}
	return names
}

func (r *Rows) Next(dest []driver.Value) error {
	ret := api.SQLFetch(r.os.h)
	if ret == api.SQL_NO_DATA {
		return io.EOF
	}
	if IsError(ret) {
		return NewError("SQLFetch", r.os.h)
	}
	for i := range dest {
		v, err := r.os.Cols[i].Value(r.os.h, i)
		if err != nil {
			return err
		}
		dest[i] = v
	}
	return nil
}

func (r *Rows) Close() error {
	return r.os.closeByRows()
}

func (r *Rows) HasNextResultSet() bool {
	return true
}

func (r *Rows) NextResultSet() error {
	ret := api.SQLMoreResults(r.os.h)
	if ret == api.SQL_NO_DATA {
		return io.EOF
	}
	if IsError(ret) {
		return NewError("SQLMoreResults", r.os.h)
	}

	err := r.os.BindColumns()
	if err != nil {
		return err
	}
	return nil
}

func (r *Rows) ColumnTypeDatabaseTypeName(i int) string {
	var base *BaseColumn
	switch col := r.os.Cols[i].(type) {
	case *BindableColumn:
		base = col.BaseColumn
	case *NonBindableColumn:
		base = col.BaseColumn
	default:
		panic("unknown column")
	}

	switch base.SQLType {
	case api.SQL_CHAR:
		return "CHAR"
	case api.SQL_NUMERIC:
		return "NUMERIC"
	case api.SQL_DECIMAL:
		return "DECIMAL"
	case api.SQL_INTEGER:
		return "INTEGER"
	case api.SQL_SMALLINT:
		return "SMALLINT"
	case api.SQL_FLOAT:
		return "FLOAT"
	case api.SQL_REAL:
		return "REAL"
	case api.SQL_DOUBLE:
		return "DOUBLE"
	case api.SQL_DATETIME:
		return "DATETIME"
	case api.SQL_TIME:
		return "TIME"
	case api.SQL_VARCHAR:
		return "VARCHAR"
	case api.SQL_TYPE_DATE:
		return "DATE"
	case api.SQL_TYPE_TIME:
		return "TIME"
	case api.SQL_TYPE_TIMESTAMP:
		return "TIMESTAMP"
	case api.SQL_TIMESTAMP:
		return "TIMESTAMP"
	case api.SQL_LONGVARCHAR:
		return "LONGVARCHAR"
	case api.SQL_BINARY:
		return "BINARY"
	case api.SQL_VARBINARY:
		return "VARBINARY"
	case api.SQL_LONGVARBINARY:
		return "LONGVARBINARY"
	case api.SQL_BIGINT:
		return "BIGINT"
	case api.SQL_TINYINT:
		return "TINYINT"
	case api.SQL_BIT:
		return "BIT"
	case api.SQL_WCHAR:
		return "WCHAR"
	case api.SQL_WVARCHAR:
		return "WVARCHAR"
	case api.SQL_WLONGVARCHAR:
		return "WLONGVARCHAR"
	case api.SQL_GUID:
		return "GUID"
	default:
		panic("unknown column type")
	}
}
