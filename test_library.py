import time, threading, urllib.request, urllib.parse, sys
sys.path.append('python-pkg')
import clickhouse_sidecar

client = clickhouse_sidecar.get_client(data_dir='debug-db')
port = urllib.parse.urlparse(client.uri).port

def run_query(query, method="POST"):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/", data=query.encode(), method=method)
    with urllib.request.urlopen(req) as res:
        return res.read()

# Setup
run_query("CREATE TABLE IF NOT EXISTS bench (id UInt32, val String) ENGINE = MergeTree ORDER BY id")
run_query("TRUNCATE TABLE bench")

print("--- Testing transparent proxy ---")
t0 = time.time()
for i in range(1000):
    run_query(f"INSERT INTO bench VALUES ({i}, 'async')")
    if i % 100 == 0:
        res = run_query("SELECT count() FROM bench").decode().strip()
        print(f"Intermediate select at {i}: {res} rows")

t1 = time.time()
count = run_query("SELECT count() FROM bench").decode().strip()
print(f"Time: {t1-t0:.3f}s, Total Rows Selectable: {count}")
assert count == "1000", f"Expected 1000, got {count}"
print("OK. Transparent proxy working perfectly.")
