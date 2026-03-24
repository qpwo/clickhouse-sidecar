# clickhouse-sidecar

chdb is too weak but full global install is too much!

Start clickhouse proc on the side! Start it when you first connect and stop it when you last disconnect! Never ever leaves orphans behind. Uses random available port. Use clickhouse like its nineteen ninety sqlite.

## python

pip install clickhouse-sidecar clickhouse-connect

```py
import clickhouse_sidecar

def main():
    client = clickhouse_sidecar.get_client("local-db")
    client.command("CREATE TABLE IF NOT EXISTS events (id UInt32) ENGINE = MergeTree ORDER BY id")
    client.insert("events", [[1]], column_names=["id"])
    result = client.query("SELECT * FROM events")
    print(result.result_rows)
    client.close()

if __name__ == "__main__":
    main()
```
