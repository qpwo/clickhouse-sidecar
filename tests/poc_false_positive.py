"""a oneline docstring"""
import time, os, gc, weakref
import clickhouse_sidecar

def run():
    print("=== PoC 1: False Positive (Zombie Daemon) ===")

    refs = {}

    def get_and_drop():
        client = clickhouse_sidecar.get_client("poc-db-1")
        refs['client'] = weakref.ref(client)
        refs['sock'] = weakref.ref(client._lease_sock)

    get_and_drop()
    gc.collect()

    print(f"   [Debug] Client GC'd? {refs['client']() is None}")
    print(f"   [Debug] Socket GC'd? {refs['sock']() is None}")

    with open("poc-db-1/clickhouse.pid") as f:
        pid = int(f.read().strip())

    print(f"   ClickHouse spawned with PID: {pid}")
    print("   Simulating an app doing other things... waiting 7 seconds.")
    time.sleep(7)

    try:
        os.kill(pid, 0)
        print(f"   FAIL: ClickHouse PID {pid} is still running!")
    except OSError:
        print(f"   SUCCESS: ClickHouse PID {pid} stopped.")

if __name__ == "__main__":
    run()
