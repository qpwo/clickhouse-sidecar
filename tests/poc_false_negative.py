"""a oneline docstring"""
import time, os, urllib.request, threading
import clickhouse_sidecar

def run():
    print("\n=== PoC 2: False Negative (Premature Shutdown) ===")
    client = clickhouse_sidecar.get_client("poc-db-2")

    with open("poc-db-2/config.xml") as f:
        config = f.read()
    import re
    http_port = re.search(r"<http_port>(\d+)</http_port>", config).group(1)

    with open("poc-db-2/clickhouse.pid") as f:
        pid = int(f.read().strip())

    print(f"   ClickHouse is running on HTTP port {http_port} (PID: {pid}).")
    client.close()
    print("   Daemon sees 0 active clients and starts its 3s shutdown timer.")
    print("   Instantly firing a direct HTTP query that takes 6 seconds...")

    start = time.time()
    errors = []
    def fetch():
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{http_port}/?query=SELECT+sleepEachRow(0.1)+FROM+system.numbers+LIMIT+60+SETTINGS+max_block_size=10")
            with urllib.request.urlopen(req) as res:
                res.read()
        except Exception as e:
            errors.append(e)

    t = threading.Thread(target=fetch)
    t.start()

    time.sleep(4)
    print("   4 seconds have passed. The daemon's 3s timer has fired.")
    try:
        os.kill(pid, 0)
        print("   ClickHouse PID is still alive.")
    except OSError:
        print("   ClickHouse PID is GONE.")

    print("   Waiting for the query to finish or fail...")
    t.join()

    elapsed = time.time() - start
    if errors:
        print(f"   FAIL: Query threw an error after {elapsed:.2f}s: {errors[0]}")
    else:
        print(f"   SUCCESS! Query finished in {elapsed:.2f}s. ClickHouse survived.")

if __name__ == "__main__":
    run()
