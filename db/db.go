package db

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

var Conn *sql.DB

func InitDb(host, user, password string, port uint) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema?charset=utf8", user, password, host, port)
	var err error
	Conn, err = sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	Conn.SetMaxOpenConns(4)
	Conn.SetMaxIdleConns(2)
	Conn.SetConnMaxLifetime(1000)
	Conn.SetConnMaxIdleTime(600)
	if err := Conn.Ping(); err != nil {
		return err
	}
	return nil
}

func GetColumns(schema, table string) (columns []string, err error) {
	if err := Conn.Ping(); err != nil {
		return nil, err
	}
	var rows *sql.Rows
	rows, err = Conn.Query(fmt.Sprintf(`select column_name from information_schema.columns where table_schema="%s" and table_name = "%s" order by ORDINAL_POSITION`, schema, table))
	if err != nil {
		return
	}
	for rows.Next() {
		var column string
		_ = rows.Scan(&column)
		columns = append(columns, column)
	}
	return
}

func GetPk(schema, table string) (pk []string, err error) {
	if err := Conn.Ping(); err != nil {
		return nil, err
	}
	var rows *sql.Rows
	rows, err = Conn.Query(fmt.Sprintf(`select column_name from information_schema.columns where table_schema="%s" and table_name = "%s" and column_key='PRI' order by ORDINAL_POSITION`, schema, table))
	if err != nil {
		return
	}
	for rows.Next() {
		var column string
		_ = rows.Scan(&column)
		pk = append(pk, column)
	}
	return
}

type Variables struct {
	ServerId                     int
	LogBin                       bool
	BinlogFormat, BinlogRowImage string
}

func GetVariables() (v Variables, err error) {
	queryRow := Conn.QueryRow("select @@server_id,@@log_bin, @@binlog_format,@@binlog_row_image;")
	if queryRow.Err() != nil {
		err = queryRow.Err()
		return
	}
	if err = queryRow.Scan(&v.ServerId, &v.LogBin, &v.BinlogFormat, &v.BinlogRowImage); err != nil {
		return
	}
	return
}
