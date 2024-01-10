package core

import (
	"bytes"
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	"log"
	"os"
	"testing"
)

func Test_generateDeleteSql(t *testing.T) {
	tab := &Table{
		Schema:  "test",
		Table:   "t",
		Columns: []string{"id", "a", "b", "c", "d", "f", "ff"},
		Pks:     []string{"id"},
		TableId: 100,
	}
	value := []interface{}{1, "hello", "", "NULL", 22.35, true, nil}
	sql := generateDeleteSql(tab, value)
	t.Log(sql)
}

func Test_generateInsertSql(t *testing.T) {
	tab := &Table{
		Schema:  "test",
		Table:   "t",
		Columns: []string{"id", "a", "b", "c", "d", "f", "ff"},
		Pks:     []string{"id"},
		TableId: 100,
	}
	value := []interface{}{1, "hello", "", "NULL", 22.35, true, nil}
	sql := generateInsertSql(tab, value)
	t.Log(sql)
}

func Test_generateUpdateSql(t *testing.T) {
	tab := &Table{
		Schema:  "test",
		Table:   "t",
		Columns: []string{"id", "a", "b", "c", "d", "f", "ff"},
		Pks:     []string{"id"},
		TableId: 100,
	}
	old_value := []interface{}{1, "world", "", "NULL", 22.35, true, nil}
	new_value := []interface{}{1, "hello", "", "NULL", 22.35, true, nil}
	sql := generateUpdateSql(tab, old_value, new_value)
	t.Log(sql)
}
func Test_genSimpleUpdateSql(t *testing.T) {
	tab := &Table{
		Schema:  "test",
		Table:   "t",
		Columns: []string{"id", "a", "b", "c", "d", "f", "ff"},
		Pks:     []string{"id"},
		TableId: 100,
	}
	old_value := []interface{}{1, "world", "", "NULL", 22.35, true, nil}
	new_value := []interface{}{1, "hello", "", "NULL", 22.35, true, nil}
	sql := genSimpleUpdateSql(tab, old_value, new_value)
	t.Log(sql)
}

func Test_concatSqlFromBinlogEvent(t *testing.T) {
	f, err := os.Open("/usr/local/var/mysql/mysql-bin.000002")
	if err != nil {
		t.Log(err)
		return
	}
	if f != nil {
		defer f.Close()
	}
	binlogHeader := int64(4)
	buf := make([]byte, binlogHeader)
	_, err = f.Read(buf)
	if err != nil {
		t.Log(err)
		return
	}
	if !bytes.Equal(buf, replication.BinLogFileHeader) {
		t.Log(fmt.Errorf("core header is not match,file may be damaged "))
		return
	}
	if _, err := f.Seek(binlogHeader, os.SEEK_SET); err != nil {
		t.Log(err)
		return
	}
	//binlogParser := replication.NewBinlogParser()
	//err = binlogParser.ParseReader(f,onEvent )
	//if err != nil {
	//	t.Log(err)
	//	return
	//}
	return
}

func Test_genNoPkInsertSql(t *testing.T) {
	//old_value := []interface{}{1, "world", "", "NULL", 22.35, true, nil}
	val := []interface{}{1, "hello", "", "NULL", 22.35, true, nil}
	tab := &Table{
		Schema:  "test",
		Table:   "t",
		Columns: []string{"id", "a", "b", "c", "d", "f", "ff"},
		Pks:     []string{"id"},
		TableId: 100,
	}
	sql := genNoPkInsertSql(tab, val)
	if sql != "INSERT INTO test.t(a,b,c,d,f,ff) VALUES('hello','','NULL',22.35,true,NULL);" {
		log.Fatal(sql)
	}
	tab.Pks = []string{"id", "ff"}
	sql = genNoPkInsertSql(tab, val)
	if sql != "INSERT INTO test.t(a,b,c,d,f) VALUES('hello','','NULL',22.35,true);" {
		log.Fatal(sql)
	}
}
