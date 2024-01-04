package db

import "testing"

func TestGetColumns(t *testing.T) {
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
