"""A non-trivial example of using ClickHouse Sidecar."""
import clickhouse_sidecar
import time

def main():
    print("Acquiring ClickHouse sidecar...")
    client = clickhouse_sidecar.get_client("example-db")

    client.command("CREATE TABLE IF NOT EXISTS sensor_data (ts DateTime, val Float64) ENGINE = MergeTree ORDER BY ts")

    print("Inserting 10,000 rows...")
    rows = [[int(time.time()) - i, float(i) * 1.5] for i in range(10000)]
    client.insert("sensor_data", rows, column_names=["ts", "val"])

    print("Querying analytics...")
    result = client.query("SELECT count(), avg(val), max(val) FROM sensor_data")
    print(f"Results: {result.result_rows}")

    client.close()
    print("Done.")

if __name__ == "__main__":
    main()
