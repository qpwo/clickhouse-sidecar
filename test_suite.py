import time, urllib.request, urllib.parse, sys
sys.path.append('python-pkg')
import clickhouse_sidecar

client = clickhouse_sidecar.get_client(data_dir='debug-db-tests')
port = urllib.parse.urlparse(client.uri).port

def run_query(query, method="POST"):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/", data=query.encode(), method=method)
    with urllib.request.urlopen(req) as res: return res.read()

run_query("CREATE TABLE IF NOT EXISTS bench_dirty (id UInt32) ENGINE = MergeTree ORDER BY id")
run_query("TRUNCATE TABLE bench_dirty")

print("Inserting 500 rows...")
t0 = time.time()
for i in range(500):
    run_query(f"INSERT INTO bench_dirty VALUES ({i})")
t1 = time.time()
print(f"500 async inserts took {t1-t0:.3f}s")

print("Selecting count...")
t0 = time.time()
count = run_query("SELECT count() FROM bench_dirty").decode().strip()
t1 = time.time()
print(f"Select count after inserts took {t1-t0:.3f}s, returned {count}")
assert count == "500", f"Expected 500, got {count}"

print("Selecting count again (should be fast, no flush)...")
t0 = time.time()
run_query("SELECT count() FROM bench_dirty")
t1 = time.time()
print(f"Second select took {t1-t0:.3f}s")
assert t1-t0 < 0.2, "Second select should not flush!"
print("OK Python Proxy!")
