"""a oneline docstring"""
import clickhouse_sidecar

def main():
    print("Acquiring ClickHouse...")
    client = clickhouse_sidecar.get_client("local-db")
    client.command("CREATE TABLE IF NOT EXISTS events (id UInt32, name String) ENGINE = MergeTree ORDER BY id")
    client.insert("events", [[1, 'startup'], [2, 'hello']], column_names=["id", "name"])
    result = client.query("SELECT * FROM events ORDER BY id")
    print(result.result_rows)
    client.close()
    print("Done! Exiting naturally...")

if __name__ == "__main__":
    main()
