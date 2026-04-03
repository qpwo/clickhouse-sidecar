import urllib.request, urllib.parse, urllib.error
import http.server
import threading
import time
import sys
import subprocess
import os

sys.path.append('python-pkg')
import clickhouse_sidecar

client = clickhouse_sidecar.get_client(data_dir='debug-db')
import urllib.parse as p
CH_HTTP_PORT = p.urlparse(client.uri).port
PROXY_PORT = 9999

class ProxyHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass # quiet

    def do_GET(self): self.proxy_request("GET")
    def do_POST(self): self.proxy_request("POST")

    def proxy_request(self, method):
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)

        path = self.path
        parsed = urllib.parse.urlparse(path)
        qs = urllib.parse.parse_qs(parsed.query)

        query_param = qs.get('query', [''])[0].strip().upper()
        body_start = body[:200].upper()

        is_select = query_param.startswith('SELECT') or body_start.startswith(b'SELECT')
        is_insert = query_param.startswith('INSERT') or body_start.startswith(b'INSERT')

        if is_insert:
            sep = '&' if parsed.query else '?'
            path += f"{sep}async_insert=1&wait_for_async_insert=0"
        elif is_select:
            try:
                flush_req = urllib.request.Request(
                    f"http://127.0.0.1:{CH_HTTP_PORT}/",
                    data=b"SYSTEM FLUSH ASYNC INSERT QUEUE",
                    method="POST"
                )
                urllib.request.urlopen(flush_req)
            except urllib.error.HTTPError as e:
                print(f"Flush failed: {e.code} - {e.read().decode('utf-8', 'ignore')}")
            except Exception as e:
                print(f"Flush failed: {e}")

        url = f"http://127.0.0.1:{CH_HTTP_PORT}{path}"
        req = urllib.request.Request(url, data=body if body else None, method=method)
        for k, v in self.headers.items():
            if k.lower() not in ['host', 'content-length']:
                req.add_header(k, v)

        try:
            with urllib.request.urlopen(req) as response:
                self.send_response(response.status)
                for k, v in response.headers.items():
                    if k.lower() == 'transfer-encoding' and v.lower() == 'chunked': continue
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(response.read())
        except urllib.error.HTTPError as e:
            self.send_response(e.code)
            for k, v in e.headers.items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(e.read())
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(e).encode())

def start_proxy():
    server = http.server.ThreadingHTTPServer(('127.0.0.1', PROXY_PORT), ProxyHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server

def run_query(port, query, method="POST"):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/", data=query.encode(), method=method)
    with urllib.request.urlopen(req) as res:
        return res.read()

def benchmark():
    # Setup
    run_query(CH_HTTP_PORT, "CREATE TABLE IF NOT EXISTS bench (id UInt32, val String) ENGINE = MergeTree ORDER BY id")
    run_query(CH_HTTP_PORT, "TRUNCATE TABLE bench")

    # 1. Sync Inserts (Direct)
    print("--- 1. Direct Synchronous Inserts (1000 single row inserts) ---")
    t0 = time.time()
    for i in range(1000):
        try:
            run_query(CH_HTTP_PORT, f"INSERT INTO bench VALUES ({i}, 'sync')")
        except Exception as e:
            pass # might fail due to too many parts
    t1 = time.time()
    parts = run_query(CH_HTTP_PORT, "SELECT count() FROM system.parts WHERE table='bench' AND active=1").decode().strip()
    print(f"Time: {t1-t0:.3f}s, Parts created: {parts}")
    run_query(CH_HTTP_PORT, "TRUNCATE TABLE bench")

    # 2. Proxied Async Inserts with Select Flushes
    print("\n--- 2. Proxied Async Inserts + Flush on Select (1000 single row inserts) ---")
    t0 = time.time()
    for i in range(1000):
        run_query(PROXY_PORT, f"INSERT INTO bench VALUES ({i}, 'async')")
        if i % 100 == 0:
            # Simulate a select mixed in
            run_query(PROXY_PORT, "SELECT count() FROM bench")
    t1 = time.time()
    # Final select to ensure all are flushed
    count = run_query(PROXY_PORT, "SELECT count() FROM bench").decode().strip()
    parts = run_query(CH_HTTP_PORT, "SELECT count() FROM system.parts WHERE table='bench' AND active=1").decode().strip()
    print(f"Time: {t1-t0:.3f}s, Parts created: {parts}, Total Rows Selectable: {count}")

if __name__ == '__main__':
    start_proxy()
    time.sleep(0.5)
    benchmark()
