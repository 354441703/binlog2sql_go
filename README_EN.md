# binlog2sql_go

A command-line tool that parses MySQL binlog files (online and offline) and generates raw SQL or rollback SQL. It is a Go version of binlog2sql with various features.

## Features
- Generates raw SQL/rollback SQL (-flashback/-B)
- Supports online streaming binlog parsing/offline binlog parsing (-local -local-file)
- Filters by various conditions (-start-position, -only-dml, -sql-type, etc.)
- Can generate insert statements without primary keys (-noPK)
- Update statements can ignore unchanged columns (-simple)
- Continuous online parsing (-stop-never)
- Multithreading support (-threads)

## User Permission Requirements
The user used must have the SELECT, REPLICATION SLAVE, and REPLICATION CLIENT privileges. The SELECT privilege is needed to query metadata information in MySQL, and the REPLICATION privileges are required for online parsing by pretending to be a MySQL slave to fetch binlogs.

## Performance Comparison with the Original binlog2sql
| Scenario                                             | Python binlog2sql | binlog2sql_go |
|------------------------------------------------------|-------------------|---------------|
| Online parsing binlog (500M) and generating SQL file | 12m59.034s        | 23.707s       |
| Parsing local binlog (500M) and generating SQL file  | Not supported     | 28.634s       |

## Quick Start

1. Parse online binlog
    ```shell
    ./binlog2sql_go -h 127.0.0.1 -u root -P 3306 -p xxx  -start-file mysql-bin.000002
    ```
2. Parse offline binlog and generate rollback SQL
    ```shell
    ./binlog2sql_go -h 127.0.0.1 -u root -P 3306 -p xxx  -local --local-file /tmp/mysql-bin.000002 -flashback
    ```
3. Continuously parse online binlog
    ```shell
   ./binlog2sql_go -h 127.0.0.1 -u root -P 3306 -p xxx  -start-file mysql-bin.000002 -stop-never
   ```
4. Parse only update statements in DML statements
    ```shell
   ./binlog2sql_go -h 127.0.0.1 -u root -P 3306 -p xxx  -start-file mysql-bin.000002 -only-dml -sql-type update
   ```

## How to Get It
### 1、Download the Binary Version
- [Click to Download](https://github.com/354441703/binlog2sql_go/releases)
### 2、Compile and Install
```shell
git clone https://github.com/354441703/binlog2sql_go.git
cd binlog2sql_go
go build
```