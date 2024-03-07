package main

import (
	"binlog2sql_go/conf"
	"binlog2sql_go/core"
	"binlog2sql_go/db"
	"binlog2sql_go/utils"
	"bytes"
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var cfg *conf.Config
var pos []uint32
var currentBinlogFile string

var eventChan chan *binlogEvent
var wg sync.WaitGroup

func main() {
	var binlogList []string

	cfg = conf.NewConfig()
	conf.ParseConfig(cfg)
	if err := db.InitDb(cfg.Host, cfg.User, cfg.Password, cfg.Port); err != nil {
		fmt.Println(err)
		return
	}
	v, err := db.GetVariables()
	if err != nil {
		fmt.Println(err)
		return
	}
	if v.ServerId == 0 {
		fmt.Printf("Error: missing server_id in %s:%v\n", cfg.Host, cfg.Port)
		return
	}
	if !v.LogBin {
		fmt.Printf("Error: binlog is disabled in %s:%v\n", cfg.Host, cfg.Port)
		return
	}
	if strings.ToUpper(v.BinlogFormat) != "ROW" {
		fmt.Printf("Error: binlog format is not 'ROW' in %s:%v\n", cfg.Host, cfg.Port)
		return
	}
	if strings.ToUpper(v.BinlogRowImage) != "FULL" {
		fmt.Printf("Error: binlog format is not 'FULL' in %s:%v\n", cfg.Host, cfg.Port)
		return
	}
	eventChan = make(chan *binlogEvent, cfg.Threads)
	go processBinlogEvents()
	defer func() {
		wg.Wait()
		close(eventChan)
	}()
	if cfg.Local {
		BinlogLocalReader(cfg.LocalFile)
	} else {
		//var curLogFile string
		//var curLogPost string
		//row := db.Conn.QueryRow("show master status;")
		//if row.Err() != nil {
		//	fmt.Println(row.Err().Error())
		//	return
		//}
		//var _ignore string
		//if err := row.Scan(&curLogFile, &curLogPost, &_ignore, &_ignore, &_ignore); err != nil {
		//	fmt.Println(err.Error())
		//	return
		//}
		var _ignore string
		rows, err := db.Conn.Query("show master logs;")
		if err != nil {
			fmt.Println(err)
			return
		}
		var ok bool
		startId, _ := strconv.Atoi(strings.Split(cfg.StartFile, ".")[1])
		stopId, _ := strconv.Atoi(strings.Split(cfg.StopFile, ".")[1])
		for rows.Next() {
			var logName string
			err := rows.Scan(&logName, &_ignore)
			if err != nil {
				fmt.Println(err)
				return
			}
			if cfg.StartFile == logName {
				ok = true
			}
			logId, _ := strconv.Atoi(strings.Split(logName, ".")[1])
			if ok && startId <= logId && logId <= stopId {
				binlogList = append(binlogList, logName)
			}
		}
		currentBinlogFile = cfg.StartFile
		if !ok {
			fmt.Printf("Error: -start-file %s not in mysql server", cfg.StartFile)
			return
		}
		streamer, err := BinlogStreamReader(cfg)
		if err != nil {
			fmt.Println(err)
			return
		}
		for {
			ctx := context.Background()
			var timeout context.CancelFunc
			var e *replication.BinlogEvent
			var err error
			if !cfg.StopNever {
				ctx, timeout = context.WithTimeout(ctx, time.Second*3)
				e, err = streamer.GetEvent(ctx)
				go timeout()
			} else {
				e, err = streamer.GetEvent(ctx)
			}
			if err != nil {
				fmt.Println(err)
				break
			}
			if e.Header.EventType == replication.ROTATE_EVENT {
				rotateEvent := e.Event.(*replication.RotateEvent)
				if !cfg.StopNever && !utils.Contains(binlogList, string(rotateEvent.NextLogName)) {
					break
				}
				currentBinlogFile = string(rotateEvent.NextLogName)
				fmt.Printf("#Rotate to %s\n", currentBinlogFile)
			}
			if err = onEvent(e); err != nil {
				fmt.Println(err)
				break
			}
		}
	}
}

type binlogEvent struct {
	lastEventPos uint32
	e            *replication.BinlogEvent
}

func onEvent(e *replication.BinlogEvent) error {
	if pos = append(pos, e.Header.LogPos); len(pos) > 2 {
		pos = pos[len(pos)-2:]
	}
	eventChan <- &binlogEvent{
		lastEventPos: pos[0],
		e:            e,
	}
	return nil
}

func onEventWorker(b *binlogEvent) {
	defer wg.Done()
	e := b.e
	if !isDMLEvent(e) && e.Header.EventType != replication.QUERY_EVENT {
		return
	}
	if cfg.OnlyDML && !isDMLEvent(e) {
		return
	}
	eventTime := time.Unix(int64(e.Header.Timestamp), 0)
	//if (!cfg.StartDatetime.IsZero() && eventTime.Before(cfg.StartDatetime)) || (!cfg.StopDatetime.IsZero() && eventTime.After(cfg.StopDatetime)) {
	//	return nil
	//}
	if !cfg.StartDatetime.IsZero() && eventTime.Before(cfg.StartDatetime) {
		return
	}
	if !cfg.StopDatetime.IsZero() && eventTime.After(cfg.StopDatetime) {
		return
	}
	if currentBinlogFile == cfg.StopFile && cfg.StopPosition != 0 && e.Header.LogPos > uint32(cfg.StopPosition) {
		return
	}
	if currentBinlogFile == cfg.StartFile && e.Header.LogPos < uint32(cfg.StartPosition) {
		return
	}
	var err error
	var sql string
	if e.Header.EventType == replication.QUERY_EVENT && !cfg.Flashback {
		sql, err = core.ConcatSqlFromQueryEvent(e, cfg)
	} else if isDMLEvent(e) {
		sql, err = core.ConcatSqlFromRowsEvent(e, cfg)
	}
	if err != nil {
		return
	}
	if sql != "" {
		sql := strings.Join(strings.Split(sql, ";"), fmt.Sprintf("; #start %v end %v time %v", b.lastEventPos, e.Header.LogPos, time.Unix(int64(e.Header.Timestamp), 0).Format("2006-01-02 15:04:05")))
		//sql = fmt.Sprintf("%s #start %v end %v time %v", sql, b.lastEventPos, e.Header.LogPos, time.Unix(int64(e.Header.Timestamp), 0).Format("2006-01-02 15:04:05"))
		fmt.Println(sql)
	}
	return
}

func BinlogLocalReader(file string) {
	f, err := os.Open(file)
	if err != nil {
		fmt.Println("error: ", err)
		return
	}
	if f != nil {
		defer f.Close()
	}
	binlogHeader := int64(4)
	buf := make([]byte, binlogHeader)
	_, err = f.Read(buf)
	if err != nil {
		fmt.Println(err)
		return
	}
	if !bytes.Equal(buf, replication.BinLogFileHeader) {
		fmt.Println(fmt.Sprintf("file header is not match,file may be damaged "))
		return
	}
	if _, err := f.Seek(binlogHeader, os.SEEK_SET); err != nil {
		fmt.Println(err.Error())
		return
	}
	binlogParser := replication.NewBinlogParser()
	err = binlogParser.ParseReader(f, onEvent)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func BinlogStreamReader(conf *conf.Config) (*replication.BinlogStreamer, error) {
	rand.Seed(time.Now().UnixNano())
	handler, err := log.NewFileHandler("binlog2sql_go.log", os.O_CREATE|os.O_WRONLY)
	if err != nil {
		return nil, err
	}
	logger := log.NewDefault(handler)
	syncConf := replication.BinlogSyncerConfig{
		ServerID:        uint32(rand.Intn(2<<31) - 1),
		Host:            conf.Host,
		Port:            uint16(conf.Port),
		User:            conf.User,
		Password:        conf.Password,
		Charset:         "utf8",
		SemiSyncEnabled: false,
		UseDecimal:      false,
		Logger:          logger,
	}
	replSyncer := replication.NewBinlogSyncer(syncConf)
	position := mysql.Position{
		Name: conf.StartFile,
		Pos:  uint32(conf.StartPosition),
	}
	return replSyncer.StartSync(position)
}

func isDMLEvent(e *replication.BinlogEvent) bool {
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return true
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return true
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return true
	default:
		return false
	}
}

func processBinlogEvents() {
	//fmt.Printf("start process binlog event\n")
	for {
		select {
		case e, ok := <-eventChan:
			if !ok {
				return
			}
			wg.Add(1)
			go onEventWorker(e)
		}
	}
}
