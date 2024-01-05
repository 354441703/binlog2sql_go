package conf

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// 定义一个新的类型，实现 flag.Value 接口
type stringSliceFlag []string

func (ssf *stringSliceFlag) Len() int {
	return len(*ssf)
}

func (ssf *stringSliceFlag) String() string {
	return fmt.Sprintf("%v", *ssf)
}

func (ssf *stringSliceFlag) Set(value string) error {
	*ssf = append(*ssf, strings.Split(value, ",")...)
	return nil
}

func (ssf *stringSliceFlag) In(val string) bool {
	for _, l := range *ssf {
		if l == val {
			return true
		}
	}
	return false
}

func (ssf *stringSliceFlag) ToUpper() stringSliceFlag {
	var result stringSliceFlag
	for _, l := range *ssf {
		_ = result.Set(strings.ToUpper(l))
	}
	return result
}

type Config struct {
	Host             string
	User             string
	Password         string
	Port             uint
	StartFile        string
	StopFile         string
	StartPosition    uint
	StopPosition     uint
	Flashback        bool
	NoPk             bool
	startDatetimeStr string
	StartDatetime    time.Time
	stopDatetimeStr  string
	StopDatetime     time.Time
	Databases        stringSliceFlag
	Tables           stringSliceFlag
	Local            bool
	LocalFile        string
	Simple           bool
	StopNever        bool
	SqlType          stringSliceFlag
}

func NewConfig() *Config {
	return &Config{}
}

func usage() {
	fmt.Fprintf(os.Stderr, `
Usage: binlog2sql [[-h] | [-host] HOST] [[-u] | [-user] USER] [[-P] | [-port] PORT] [[-p] | [-password] PASSWORD]
                  [-local --local-files] | [--start-file STARTFILE [--stop-file ENDFILE]]
                  [--start-position STARTPOS] [--stop-position ENDPOS]
                  [--start-datetime STARTTIME] [--stop-datetime STOPTIME]
                  [--stop-never] [--help] [[-d] | [-databases] [DATABASES,[DATABASES ...]]]
                  [[-t] | [-tables] [TABLES,[TABLES ...]]] [-K] [-B] [-sql-type [INSERT,DELETE,UPDATE]]
Options:
`)
	flag.PrintDefaults()
}

func ParseConfig(conf *Config) {
	var help bool
	flag.BoolVar(&help, "help", false, "this help")
	flag.StringVar(&conf.Host, "host", "127.0.0.1", "Host the MySQL database server located")
	flag.StringVar(&conf.Host, "h", "127.0.0.1", "Host the MySQL database server located (short option)")
	flag.StringVar(&conf.User, "user", "root", "MySQL Username to log in as")
	flag.StringVar(&conf.User, "u", "root", "MySQL Username to log in as (short option)")
	flag.StringVar(&conf.Password, "password", "", "MySQL Password to use")
	flag.StringVar(&conf.Password, "p", "", "MySQL Password to use (short option)")
	flag.UintVar(&conf.Port, "port", 3306, "MySQL Port to use")
	flag.UintVar(&conf.Port, "P", 3306, "MySQL Port to use (short option)")
	flag.StringVar(&conf.StartFile, "start-file", "", "Start core file to be parsed")
	flag.StringVar(&conf.StopFile, "stop-file", "", "Stop core file to be parsed. default: '--start-file'")
	flag.UintVar(&conf.StartPosition, "start-position", 4, "Start position of the --start-file")
	flag.UintVar(&conf.StopPosition, "stop-position", 0, "Stop position of --stop-file. default: latest position of '--stop-file'")
	flag.StringVar(&conf.startDatetimeStr, "start-datetime", "", "Start reading the core at first event having a datetime equal or posterior to the argument; the argument must be a date and time in the Local time zone, in any format accepted by the MySQL server for DATETIME and TIMESTAMP types, for example: 2004-12-25 11:25:56 (you should probably use quotes for your shell to set it properly).")
	flag.StringVar(&conf.stopDatetimeStr, "stop-datetime", "", "  Stop reading the core at first event having a datetime equal or posterior to the argument; the argument must be a date and time in the Local time zone, in any format accepted by the MySQL server for DATETIME and TIMESTAMP types, for example: 2004-12-25 11:25:56 (you should probably use quotes for your shell to set it properly).")
	flag.BoolVar(&conf.Flashback, "flashback", false, "Is Flashback data to start_position of start-file (default false)")
	flag.BoolVar(&conf.Flashback, "B", false, "Is Flashback data to start_position of start-file (default false) (short option)")
	flag.BoolVar(&conf.NoPk, "noPK", false, "Generate insert sql without primary key if exists (default false)")
	flag.Var(&conf.SqlType, "sql-type", "Original sql type you want to process, support INSERT, UPDATE, DELETE. (default INSERT,UPDATE,DELETE)")
	flag.Var(&conf.Databases, "databases", "Comma-separated list of dbs you want to process")
	flag.Var(&conf.Databases, "d", "Comma-separated list of dbs you want to process (short option)")
	flag.Var(&conf.Tables, "tables", "Comma-separated list of Tables you want to process")
	flag.Var(&conf.Tables, "t", "Comma-separated list of Tables you want to process (short option)")
	flag.StringVar(&conf.LocalFile, "local-file", "", "The binary logs in Local")
	flag.BoolVar(&conf.Local, "local", false, "Is the binary log exist at Local?")
	flag.BoolVar(&conf.Simple, "simple", false, "Generate update sql in Simple mode, the unchanged column will be excluded ")
	flag.BoolVar(&conf.StopNever, "stop-never", false, "Continuously parse binlog. default: stop at the latest event of '-stop-file'. ")
	flag.Parse()
	flag.Usage = usage
	if help {
		flag.Usage()
	}
	flag.VisitAll(func(f *flag.Flag) {
		if conf.Local && conf.LocalFile == "" || (conf.LocalFile != "" && !conf.Local) {
			fmt.Println("Error: -Local & -Local-files must be used together.")
			flag.Usage()
			os.Exit(1)
		}
		if !conf.Local && conf.StartFile == "" {
			fmt.Println("Error: lack of parameter: -start-file ")
			flag.Usage()
			os.Exit(1)
		}
		if conf.StopFile == "" {
			conf.StopFile = conf.StartFile
		}
		if conf.startDatetimeStr != "" {
			var err error
			if conf.StartDatetime, err = time.Parse("2006-01-02 15:04:05", conf.startDatetimeStr); err != nil {
				fmt.Println("Error: -start-datetime format error")
				flag.Usage()
				os.Exit(1)
			}
		}
		if conf.stopDatetimeStr != "" {
			var err error
			if conf.StopDatetime, err = time.Parse("2006-01-02 15:04:05", conf.stopDatetimeStr); err != nil {
				fmt.Println("Error: -stop-datetime format error")
				flag.Usage()
				os.Exit(1)
			}
		}
		//if conf.StopFile != conf.StartFile && (conf.StartDatetime.IsZero() || conf.StopDatetime.IsZero()) {
		//	fmt.Println("Error: When searching across files, you must specify the -start-datetime and -stop-datetime")
		//	os.Exit(1)
		//}
		if conf.startDatetimeStr != "" && conf.stopDatetimeStr != "" && conf.StopDatetime.Before(conf.StartDatetime) {
			fmt.Println("Error: -stop-datetime Before -start-datetime")
			os.Exit(1)
		}
		if conf.Flashback && conf.NoPk {
			fmt.Println("Error: only one of Flashback or no_pk can be True")
			flag.Usage()
			os.Exit(1)
		}
		if conf.SqlType.Len() == 0 {
			_ = conf.SqlType.Set("INSERT")
			_ = conf.SqlType.Set("DELETE")
			_ = conf.SqlType.Set("UPDATE")
		} else {
			conf.SqlType = conf.SqlType.ToUpper()
		}
	})
}
