"""A worker process that inserts data and occasionally crashes."""
import sys
import time
import random
import clickhouse_sidecar

def run(wid):
    client = clickhouse_sidecar.get_client("stress-py-db")
    client.command("CREATE TABLE IF NOT EXISTS stress_events (id UInt32, wid UInt32) ENGINE = MergeTree ORDER BY id")

    # 20% chance to crash mid-execution without cleanly closing the client
    if random.random() < 0.2:
        print(f"Worker {wid} crashing intentionally!")
        sys.exit(1)

    for i in range(10):
        client.insert("stress_events", [[int(time.time() * 1000) % 1000000 + i, int(wid)]], column_names=["id", "wid"])
        time.sleep(0.05)

    result = client.query(f"SELECT count() FROM stress_events WHERE wid = {wid}")
    print(f"Worker {wid} sees {result.result_rows[0][0]} of its own rows.")
    client.close()

if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "0")
