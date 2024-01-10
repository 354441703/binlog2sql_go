# binlog2sql_go

golang编写的一个根据binlog生成sql语句的命令行工具

## 特性
- 支持生成 正向/回滚sql
- 在线流式binlog解析
- 离线（-local）binlog解析
- 支持mysql5.5, 5.6, 5.7, 8.0等版本
- 支持多线程、支持按多种条件过滤

## 快速开始

一、 解析在线binlog
```shell
 ./binlog2sql_go -h 127.0.0.1 -u root -P 3306 -p xxx  -start-file mysql-bin.000002
```
二、 解析离线binlog
```shell
./binlog2sql_go -h 127.0.0.1 -u root -P 3306 -p xxx  -local --local-file /usr/local/var/mysql/mysql-bin.000002
```
三、 持续解析在线binlog
```shell
 ./binlog2sql_go -h 127.0.0.1 -u root -P 3306 -p xxx  -start-file mysql-bin.000002 -stop-never
```
四、 仅解析 dml语句中的update语句
```shell
 ./binlog2sql_go -h 127.0.0.1 -u root -P 3306 -p xxx  -start-file mysql-bin.000002 -only-dml -sql-type update
```