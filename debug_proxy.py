import sys, urllib.request, urllib.parse, time, threading
sys.path.append('python-pkg')
import clickhouse_sidecar
clickhouse_sidecar.G["data_dir"] = "debug-db-proxy-test"
clickhouse_sidecar._log = lambda m: print(f"LOG: {m}")
try:
    clickhouse_sidecar.run_daemon("debug-db-proxy-test")
except Exception as e:
    print(f"Exception: {e}")
