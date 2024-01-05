package core

import (
	"log"
	"testing"
)

//func Test_startBinlogSyncer(t *testing.T) {
//	cfg := &conf.Config{Host: "127.0.0.1", Port: 3306, User: "root", Password: "123456"}
//	cfg.StartFile = "mysql-bin.000002"
//	streamer, err := BinlogStreamReader(cfg)
//	if err != nil {
//		t.Log(err)
//		return
//	}
//	event, err := streamer.GetEvent(context.Background())
//	if err != nil {
//		t.Log(err)
//	}
//	t.Logf("%+v", event.Header)
//	event, err = streamer.GetEventWithStartTime(context.Background(), time.Now())
//	if err != nil {
//		t.Log(err)
//	}
//	t.Logf("%+v", event.Header)
//}

func Test_generateDeleteSql(t *testing.T) {
	columns := []string{"id", "a", "b", "c", "d", "f", "ff"}
	value := []interface{}{1, "hello", "", "NULL", 22.35, true, nil}
	sql := generateDeleteSql("test", "t", columns, value)
	t.Log(sql)
}

func Test_generateInsertSql(t *testing.T) {
	columns := []string{"id", "a", "b", "c", "d", "f", "ff"}
	value := []interface{}{1, "hello", "", "NULL", 22.35, true, nil}
	sql := generateInsertSql("test", "t", columns, value)
	t.Log(sql)
}

func Test_generateUpdateSql(t *testing.T) {
	columns := []string{"id", "a", "b", "c", "d", "f", "ff"}
	old_value := []interface{}{1, "world", "", "NULL", 22.35, true, nil}
	new_value := []interface{}{1, "hello", "", "NULL", 22.35, true, nil}
	sql := generateUpdateSql("test", "t", columns, old_value, new_value)
	t.Log(sql)
}
func Test_genSimpleUpdateSql(t *testing.T) {
	columns := []string{"id", "a", "b", "c", "d", "f", "ff"}
	old_value := []interface{}{1, "world", "", "NULL", 22.35, true, nil}
	new_value := []interface{}{1, "hello", "", "NULL", 22.35, true, nil}
	sql := genSimpleUpdateSql("test", "t", columns, old_value, new_value)
	t.Log(sql)
}

//func Test_concatSqlFromBinlogEvent(t *testing.T) {
//	f, err := os.Open("/usr/local/var/mysql/mysql-bin.000002")
//	if err != nil {
//		t.Log(err)
//		return
//	}
//	if f != nil {
//		defer f.Close()
//	}
//	binlogHeader := int64(4)
//	buf := make([]byte, binlogHeader)
//	_, err = f.Read(buf)
//	if err != nil {
//		t.Log(err)
//		return
//	}
//	if !bytes.Equal(buf, replication.BinLogFileHeader) {
//		t.Log(fmt.Errorf("core header is not match,file may be damaged "))
//		return
//	}
//	if _, err := f.Seek(binlogHeader, os.SEEK_SET); err != nil {
//		t.Log(err)
//		return
//	}
//	binlogParser := replication.NewBinlogParser()
//	err = binlogParser.ParseReader(f, )
//	if err != nil {
//		t.Log(err)
//		return
//	}
//	return
//}

func Test_genNoPkInsertSql(t *testing.T) {
	columns := []string{"id", "a", "b", "c", "d", "f", "ff"}
	//old_value := []interface{}{1, "world", "", "NULL", 22.35, true, nil}
	val := []interface{}{1, "hello", "", "NULL", 22.35, true, nil}
	pks := []string{"id"}
	sql := genNoPkInsertSql("test", "t", columns, pks, val)
	if sql != "INSERT INTO test.t(a,b,c,d,f,ff) VALUES('hello','','NULL',22.35,true,NULL);" {
		log.Fatal(sql)
	}
	pks = []string{"id", "ff"}
	sql = genNoPkInsertSql("test", "t", columns, pks, val)
	if sql != "INSERT INTO test.t(a,b,c,d,f) VALUES('hello','','NULL',22.35,true);" {
		log.Fatal(sql)
	}
}
