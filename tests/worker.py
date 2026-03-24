"""a oneline docstring"""
import sys
import clickhouse_sidecar

def run(wid):
    print(f"Worker {wid} starting...")
    client = clickhouse_sidecar.get_client("stress-db")
    client.command("CREATE TABLE IF NOT EXISTS workers (id UInt32, val String) ENGINE = MergeTree ORDER BY id")
    client.insert("workers", [[int(wid), f"worker-{wid}"]], column_names=["id", "val"])
    result = client.query("SELECT count() as c FROM workers")
    print(f"Worker {wid} sees {result.result_rows[0][0]} rows.")

if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "0")
