# Transfer data from MySQL to ClickHouse and output Parquet

## Start ClickHouse in Docker
```bash
docker run -d -p 8123:8123 -p 9000:9000 \
    -v /Users/hedon/clickhouse/data/:/var/lib/clickhouse/ \
    -v /Users/hedon/clickhouse/logs:/var/log/clickhouse-server/ \
    --name my-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```

## Install ClickHouse client on MaxOS
```bash
brew install clickhouse
clickhouse client
```

## Connect to MySQL in ClickHouse
```sql
CREATE TABLE mysql_table ENGINE = MySQL('host:port', 'database', 'table', 'user', 'password')
```

## Create a local table in ClickHouse
The schema of table is according to the target MySQL table.
```sql
CREATE TABLE clickhouse_table (
    column1 Type1,
    column2 Type2,
    ...
) ENGINE = MergeTree()
ORDER BY column1;
```

## Transfer data from MySQL to ClickHouse
```sql
INSERT INTO clickhouse_table SELECT * FROM mysql_table;
```

## Output data as Parquet
```sql
CREATE TABLE file_parquet
ENGINE = File('Parquet', '/path/to/output/file.parquet')
AS SELECT * FROM clickhouse_table;
```