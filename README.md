Imports:
1. yfinance
2. pandas_market_calendars
3. clickhouse_connect
4. datetime

start clickhouse server: ./clickhouse server
client connect to server in new terminal: ./clickhouse client
Database Schema: 
```
CREATE TABLE Stock_Data (
    timestamp DateTime,
    name String,
    price Float32
)
ENGINE = MergeTree()
PRIMARY KEY (name)
```
run ingester: python3 ingester.py
