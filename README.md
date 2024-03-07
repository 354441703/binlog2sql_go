# binlog2sql_go

一个通过解析MySQL binlog文件（在线、离线），生成原始SQL，回滚SQL语句命令行工具。支持多种特性。是binlog2sql的go版本。


## 特性
- 生成原始SQL/回滚SQL(-flashback/-B)
- 在线流式解析binlog/离线binlog解析(-local -local-file)
- 按多种条件过滤(-start-position,-only-dml,-sql-type...and so on)
- 可以生成不带主键的insert语句(-noPK)
- 生成的update语句可以忽略未变更的列(-simple)
- 在线持续解析(-stop-never)
- 多线程(-threads)

## 用户权限说明
使用的用户需要具有 SELECT,REPLICATION SLAVE,REPLICATION CLIENT权限，授权语句如下：
```mysql
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'your_user'@'%';
```
其中SELECT权限是需要查询MySQL中的元数据信息，
REPLICATION相关的权限是：进行在线解析时通过伪装成MySQL从库拉取binlog而需要。

## 与原版binlog2sql性能对比
| 场景                         | python版binlog2sql | binlog2sql_go |
|----------------------------|-------------------|---------------|
| 在线解析binlog(500M) 全文生成sql文件 | 12m59.034s        | 23.707s       |
| 解析本地binlog(500M) 全文生成sql文件 | 不支持               | 28.634s       |

## 快速开始

一、 解析在线binlog
```shell
 ./binlog2sql_go -h 127.0.0.1 -u root -P 3306 -p xxx  -start-file mysql-bin.000002
```
二、 解析离线binlog，生成回滚sql
```shell
./binlog2sql_go -h 127.0.0.1 -u root -P 3306 -p xxx  -local --local-file /tmp/mysql-bin.000002 -flashback
```
三、 持续解析在线binlog
```shell
 ./binlog2sql_go -h 127.0.0.1 -u root -P 3306 -p xxx  -start-file mysql-bin.000002 -stop-never
```
四、 仅解析 dml语句中的update语句
```shell
 ./binlog2sql_go -h 127.0.0.1 -u root -P 3306 -p xxx  -start-file mysql-bin.000002 -only-dml -sql-type update
```

## 获取方式
### 1、下载二进制版
- [点击下载](https://github.com/354441703/binlog2sql_go/releases)
### 2、编译安装
```shell
git clone https://github.com/354441703/binlog2sql_go.git
cd binlog2sql_go
go build
```

## 限制
 -  本地模式暂时不支持一次解析多个文件