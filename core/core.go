package core

import (
	"binlog2sql_go/conf"
	"binlog2sql_go/db"
	"binlog2sql_go/utils"
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	"strings"
)

var cachedPks, cachedCol *Cache

func init() {
	if cachedCol == nil {
		cachedCol = NewCache()
	}
	if cachedPks == nil {
		cachedPks = NewCache()
	}
}

func ConcatSqlFromQueryEvent(e *replication.BinlogEvent, cfg *conf.Config) (sql string, err error) {
	qe, ok := e.Event.(*replication.QueryEvent)
	if !ok {
		err = fmt.Errorf("event is not a Query Event")
		return
	}
	ignoreQuery := []string{"BEGIN", "COMMIT"}
	if utils.Contains(ignoreQuery, string(qe.Query)) {
		return
	}
	if len(qe.Schema) != 0 {
		sql = fmt.Sprintf("USE %s;", string(qe.Schema))
	}
	sql = fmt.Sprintf("%s\n%s;", sql, string(qe.Query))
	return
}

func ConcatSqlFromRowsEvent(e *replication.BinlogEvent, cfg *conf.Config) (sql string, err error) {
	rowsEvent, ok := e.Event.(*replication.RowsEvent)
	if !ok {
		err = fmt.Errorf("event is not a RowsEvent")
		return
	}
	if cfg.Databases.Len() != 0 && !cfg.Databases.In(string(rowsEvent.Table.Schema)) {
		return
	}
	if cfg.Tables.Len() != 0 && !cfg.Tables.In(string(rowsEvent.Table.Table)) {
		return
	}
	sql, err = genSqlStatement(e.Header.EventType, rowsEvent, cfg)
	return
}

type Table struct {
	Schema, Table string
	Columns, Pks  []string
	TableId       uint64
}

func NewTable(re *replication.RowsEvent) *Table {
	return &Table{TableId: re.TableID, Schema: string(re.Table.Schema), Table: string(re.Table.Table)}
}

func eventTypeToString(eventType replication.EventType) string {
	switch eventType {
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return "DELETE"
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return "INSERT"
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return "UPDATE"
	case replication.QUERY_EVENT:
		return "QUERY"
	default:
		return ""
	}
}

func genSqlStatement(eventType replication.EventType, rowsEvent *replication.RowsEvent, conf *conf.Config) (sql string, err error) {
	var sqlList []string
	t := NewTable(rowsEvent)
	if t.Columns, err = cachedCol.Get(rowsEvent, db.GetColumns); err != nil {
		return
	}
	if t.Pks, err = cachedPks.Get(rowsEvent, db.GetPk); err != nil {
		return
	}
	if conf.Flashback {
		switch eventTypeToString(eventType) {
		case "DELETE":
			if !conf.SqlType.In("DELETE") {
				return "", nil
			}
			for _, row := range rowsEvent.Rows {
				insertSql := generateInsertSql(t, row)
				sqlList = append(sqlList, insertSql)
			}
		case "INSERT":
			if !conf.SqlType.In("INSERT") {
				return "", nil
			}
			for _, row := range rowsEvent.Rows {
				delSql := generateDeleteSql(t, row)
				sqlList = append(sqlList, delSql)
			}
		case "UPDATE":
			if !conf.SqlType.In("UPDATE") {
				return "", nil
			}
			for i := 0; i < len(rowsEvent.Rows); i = i + 2 {
				updateSql := ""
				if conf.Simple {
					updateSql = genSimpleUpdateSql(t, rowsEvent.Rows[i+1], rowsEvent.Rows[i])
				} else {
					updateSql = generateUpdateSql(t, rowsEvent.Rows[i+1], rowsEvent.Rows[i])
				}
				sqlList = append(sqlList, updateSql)
			}
		}
	} else {
		switch eventTypeToString(eventType) {
		case "DELETE":
			if !conf.SqlType.In("DELETE") {
				return "", nil
			}
			for _, row := range rowsEvent.Rows {
				delSql := generateDeleteSql(t, row)
				sqlList = append(sqlList, delSql)
			}
		case "INSERT":
			if !conf.SqlType.In("INSERT") {
				return "", nil
			}
			for _, row := range rowsEvent.Rows {
				insertSql := generateInsertSql(t, row)
				sqlList = append(sqlList, insertSql)
			}
		case "UPDATE":
			if !conf.SqlType.In("UPDATE") {
				return "", nil
			}
			for i := 0; i < len(rowsEvent.Rows); i = i + 2 {
				updateSql := ""
				if conf.Simple {
					updateSql = genSimpleUpdateSql(t, rowsEvent.Rows[i], rowsEvent.Rows[i+1])
				} else {
					updateSql = generateUpdateSql(t, rowsEvent.Rows[i], rowsEvent.Rows[i+1])
				}
				sqlList = append(sqlList, updateSql)
			}
		}
	}
	sql = strings.Join(sqlList, "\n")
	return
}

func generateInsertSql(t *Table, row []interface{}) string {
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
	return fmt.Sprintf(`INSERT INTO %s.%s(%s) VALUES(%s);`, t.Schema, t.Table, strings.Join(t.Columns, ","), strings.Join(valueString, ","))
}

func generateDeleteSql(t *Table, row []interface{}) string {
	var condition []string
	for i, col := range t.Columns {
		switch val := row[i].(type) {
		case string:
			condition = append(condition, fmt.Sprintf("%s='%v'", col, val))
		case nil:
			condition = append(condition, fmt.Sprintf("%s IS NULL", col))
		default:
			condition = append(condition, fmt.Sprintf("%s=%v", col, val))
		}
	}
	return fmt.Sprintf("DELETE FROM %s.%s WHERE %s LIMIT 1;", t.Schema, t.Table, strings.Join(condition, " AND "))
}

func generateUpdateSql(t *Table, oldValue []interface{}, newValue []interface{}) string {
	// UPDATE test.t SET id=1,a="hello",b=true,c=23.4,d=NULL,f="" WHERE id=1 AND a = 'world' AND b=true AND c=23.4 AND d IS NULL ADN f=''
	var condition []string
	var setString []string
	for i, col := range t.Columns {
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
	return fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s LIMIT 1;", t.Schema, t.Table, strings.Join(setString, ","), strings.Join(condition, " AND "))
}

func genSimpleUpdateSql(t *Table, oldValue []interface{}, newValue []interface{}) string {
	// UPDATE test.t SET id=1,a="hello",b=true,c=23.4,d=NULL,f="" WHERE id=1 AND a = 'world' AND b=true AND c=23.4 AND d IS NULL ADN f=''
	var condition []string
	var setString []string

	for i, col := range t.Columns {
		if !utils.Contains(t.Pks, col) && fmt.Sprintf("%v", oldValue[i]) == fmt.Sprintf("%v", newValue[i]) {
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
	return fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s LIMIT 1;", t.Schema, t.Table, strings.Join(setString, ","), strings.Join(condition, " AND "))
}

func genNoPkInsertSql(t *Table, rows []interface{}) string {
	pkMap := make(map[string]bool)
	for _, pk := range t.Pks {
		pkMap[pk] = true
	}
	var columnsRes []string
	var valueString []string
	for i := 0; i < len(t.Columns); i++ {
		if !pkMap[t.Columns[i]] {
			columnsRes = append(columnsRes, t.Columns[i])
			switch val := rows[i].(type) {
			case string:
				valueString = append(valueString, fmt.Sprintf("'%v'", val))
			case nil:
				valueString = append(valueString, "NULL")
			default:
				valueString = append(valueString, fmt.Sprintf("%v", val))
			}
		}
	}
	return fmt.Sprintf(`INSERT INTO %s.%s(%s) VALUES(%s);`, t.Schema, t.Table, strings.Join(columnsRes, ","), strings.Join(valueString, ","))
}
