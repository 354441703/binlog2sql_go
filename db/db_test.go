package db

import "testing"

func TestGetColumns(t *testing.T) {
	if err := InitDb("127.0.0.1", "root", "123456", 3306); err != nil {
		t.Errorf(err.Error())
	}
	columns, err := GetColumns("cmdb", "t")
	if err != nil {
		t.Log(err)
	}
	t.Log(columns)
	pk, err := GetPk("cmdb", "t")
	if err != nil {
		t.Log(err)
	}
	t.Log(pk)
}
