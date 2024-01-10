package core

import (
	"github.com/go-mysql-org/go-mysql/replication"
	"sync"
)

type Cache struct {
	cache map[uint64][]string
	rl    sync.RWMutex
}

func NewCache() *Cache {
	return &Cache{cache: make(map[uint64][]string)}
}

func (c *Cache) Get(re *replication.RowsEvent, fn func(string, string) ([]string, error)) (res []string, err error) {
	tableId, schema, table := re.TableID, string(re.Table.Schema), string(re.Table.Table)
	if c.cache == nil {
		c.cache = make(map[uint64][]string)
	}
	c.rl.Lock()
	defer c.rl.Unlock()
	var ok bool
	if res, ok = c.cache[tableId]; !ok {
		res, err = fn(schema, table)
		if err != nil {
			return
		}
		c.cache[tableId] = res
	}
	return
}
