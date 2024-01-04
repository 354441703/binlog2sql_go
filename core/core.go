package core

import (
	"binlog2sql_go/conf"
	"binlog2sql_go/db"
	"binlog2sql_go/utils"
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	"strings"
	"time"
)

func ConcatSqlFromRowsEvent(e *replication.BinlogEvent, cfg *conf.Config) (sql string, err error) {
	//if !IsDmlEvent(e) && e.Header.EventType != replication.QUERY_EVENT {
	//	err = fmt.Errorf("core event must be DML Event or Query Event")
	//	return "", nil
	//}
	//if IsDmlEvent(e) {
	rowsEvent, ok := e.Event.(*replication.RowsEvent)
	if !ok {
		err = fmt.Errorf("event is not a RowsEvent")
		return
	}
	if cfg.Databases.Len() != 0 && !cfg.Databases.In(string(rowsEvent.Table.Schema)) {
		//fmt.Println("schema not match")
		return
	}
	if cfg.Tables.Len() != 0 && !cfg.Tables.In(string(rowsEvent.Table.Table)) {
		//fmt.Println("table not match")
		return
	}
	sql, err = genSqlStatement(e.Header.EventType, rowsEvent, cfg)
	if err != nil {
		return
	}
	sql = fmt.Sprintf("#pos %v time %v\n%s", e.Header.LogPos, time.Unix(int64(e.Header.Timestamp), 0).Format("2006-01-02 15:04:05"), sql)
	return
	//}
	//else if !conf.flashback && e.Header.EventType == replication.QUERY_EVENT {
	//	qe, _ := e.Event.(*replication.QueryEvent)
	//	if string(qe.Query) != "BEGIN" && string(qe.Query) != "COMMIT" {
	//		if len(qe.Schema) != 0 {
	//			sql = fmt.Sprintf("USE %s;", string(qe.Schema))
	//		}
	//		sql = fmt.Sprintf("%s %s;", sql, string(qe.Query))
	//	}
	//}
}

func genSqlStatement(eventType replication.EventType, rowsEvent *replication.RowsEvent, conf *conf.Config) (sql string, err error) {
	var sqlList []string
	columns, err := db.GetColumns(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
	if err != nil {
		return "", err
	}
	if conf.Flashback {
		if eventType == replication.WRITE_ROWS_EVENTv0 || eventType == replication.WRITE_ROWS_EVENTv1 ||
			eventType == replication.WRITE_ROWS_EVENTv2 {
			for _, row := range rowsEvent.Rows {
				delSql := generateDeleteSql(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table), columns, row)
				sqlList = append(sqlList, delSql)
			}
		}
		if eventType == replication.UPDATE_ROWS_EVENTv0 || eventType == replication.UPDATE_ROWS_EVENTv1 ||
			eventType == replication.UPDATE_ROWS_EVENTv2 {
			for i := 0; i < len(rowsEvent.Rows); i = i + 2 {
				updateSql := ""
				if conf.Simple {
					updateSql = genSimpleUpdateSql(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table), columns, rowsEvent.Rows[i+1], rowsEvent.Rows[i])
				} else {
					updateSql = generateUpdateSql(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table), columns, rowsEvent.Rows[i], rowsEvent.Rows[i+1])
				}
				sqlList = append(sqlList, updateSql)
			}
		}
		if eventType == replication.DELETE_ROWS_EVENTv0 || eventType == replication.DELETE_ROWS_EVENTv1 ||
			eventType == replication.DELETE_ROWS_EVENTv2 {
			for _, row := range rowsEvent.Rows {
				insertSql := generateInsertSql(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table), columns, row)
				sqlList = append(sqlList, insertSql)
			}
		}
	} else {
		if eventType == replication.WRITE_ROWS_EVENTv0 || eventType == replication.WRITE_ROWS_EVENTv1 ||
			eventType == replication.WRITE_ROWS_EVENTv2 {
			for _, row := range rowsEvent.Rows {
				insertSql := generateInsertSql(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table), columns, row)
				sql = fmt.Sprintf("%s \n %s", sql, insertSql)
				sqlList = append(sqlList, insertSql)
			}
		}
		if eventType == replication.UPDATE_ROWS_EVENTv0 || eventType == replication.UPDATE_ROWS_EVENTv1 ||
			eventType == replication.UPDATE_ROWS_EVENTv2 {
			for i := 0; i < len(rowsEvent.Rows); i = i + 2 {
				updateSql := ""
				if conf.Simple {
					updateSql = genSimpleUpdateSql(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table), columns, rowsEvent.Rows[i], rowsEvent.Rows[i+1])
				} else {
					updateSql = generateUpdateSql(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table), columns, rowsEvent.Rows[i], rowsEvent.Rows[i+1])
				}
				sqlList = append(sqlList, updateSql)
			}
		}
		if eventType == replication.DELETE_ROWS_EVENTv0 || eventType == replication.DELETE_ROWS_EVENTv1 ||
			eventType == replication.DELETE_ROWS_EVENTv2 {
			for _, row := range rowsEvent.Rows {
				delSql := generateDeleteSql(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table), columns, row)
				sqlList = append(sqlList, delSql)
			}
		}
	}
	sql = strings.Join(sqlList, "\n")
	return
}

func generateInsertSql(schema, table string, columns []string, row []interface{}) string {
	var valueString []string
	for _, r := range row {
		switch val := r.(type) {
		case string:
			valueString = append(valueString, fmt.Sprintf("'%v'", val))
		case nil:
			valueString = append(valueString, "NULL")
		default:
			valueString = append(valueString, fmt.Sprintf("%v", val))
		}
	}
	// INSERT INTO test.t(id,a,b,c,d,e,f) VALUES(1,"hello",23.5,true,NULL,"")
	return fmt.Sprintf(`INSERT INTO %s.%s(%s) VALUES(%s);`, schema, table, strings.Join(columns, ","), strings.Join(valueString, ","))
}

func generateDeleteSql(schema, table string, columns []string, row []interface{}) string {
	var condition []string
	for i, col := range columns {
		switch val := row[i].(type) {
		case string:
			condition = append(condition, fmt.Sprintf("%s='%v'", col, val))
		case nil:
			condition = append(condition, fmt.Sprintf("%s IS NULL", col))
		default:
			condition = append(condition, fmt.Sprintf("%s=%v", col, val))
		}
	}
	return fmt.Sprintf("DELETE FROM %s.%s WHERE %s LIMIT 1;", schema, table, strings.Join(condition, " AND "))
}

func generateUpdateSql(schema, table string, columns []string, oldValue []interface{}, newValue []interface{}) string {
	// UPDATE test.t SET id=1,a="hello",b=true,c=23.4,d=NULL,f="" WHERE id=1 AND a = 'world' AND b=true AND c=23.4 AND d IS NULL ADN f=''
	var condition []string
	var setString []string
	for i, col := range columns {
		switch oval := oldValue[i].(type) {
		case string:
			condition = append(condition, fmt.Sprintf("%s='%v'", col, oval))
		case nil:
			condition = append(condition, fmt.Sprintf("%s IS NULL", col))
		default:
			condition = append(condition, fmt.Sprintf("%s=%v", col, oval))
		}
		switch nval := newValue[i].(type) {
		case string:
			setString = append(setString, fmt.Sprintf("%s='%v'", col, nval))
		case nil:
			setString = append(setString, fmt.Sprintf("%s=NULL", col))
		default:
			setString = append(setString, fmt.Sprintf("%s=%v", col, nval))
		}
	}
	return fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s LIMIT 1;", schema, table, strings.Join(setString, ","), strings.Join(condition, " AND "))
}

func genSimpleUpdateSql(schema, table string, columns []string, oldValue []interface{}, newValue []interface{}) string {
	// UPDATE test.t SET id=1,a="hello",b=true,c=23.4,d=NULL,f="" WHERE id=1 AND a = 'world' AND b=true AND c=23.4 AND d IS NULL ADN f=''
	var condition []string
	var setString []string
	pk, err := db.GetPk(schema, table)
	if err != nil {
		return ""
	}
	for i, col := range columns {
		if !utils.Contains(pk, col) && fmt.Sprintf("%v", oldValue[i]) == fmt.Sprintf("%v", newValue[i]) {
			continue
		}
		switch oval := oldValue[i].(type) {
		case string:
			condition = append(condition, fmt.Sprintf("%s='%v'", col, oval))
		case nil:
			condition = append(condition, fmt.Sprintf("%s IS NULL", col))
		default:
			condition = append(condition, fmt.Sprintf("%s=%v", col, oval))
		}
		switch nval := newValue[i].(type) {
		case string:
			setString = append(setString, fmt.Sprintf("%s='%v'", col, nval))
		case nil:
			setString = append(setString, fmt.Sprintf("%s=NULL", col))
		default:
			setString = append(setString, fmt.Sprintf("%s=%v", col, nval))
		}
	}
	return fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s LIMIT 1;", schema, table, strings.Join(setString, ","), strings.Join(condition, " AND "))
}
