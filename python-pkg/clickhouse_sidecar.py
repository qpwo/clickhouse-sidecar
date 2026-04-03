"""
ClickHouse Sidecar for Python.
Starts a hidden ClickHouse HTTP server behind a single public sidecar port.
The sidecar enables async inserts by default, flushes before reads for read-after-write,
exposes an optional SSE stream for JSONEachRow inserts, and manages lifetime by lease socket.
"""
import atexit
import hashlib
import http.client
import http.server
import json
import os
import queue
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import weakref
from contextlib import suppress

G = {
    "data_dir": "",
    "sock_path": "",
    "public_port": 0,
    "ch_port": 0,
    "active_sockets": [],
    "subscribers": {},
    "subscriber_lock": threading.Lock(),
    "request_count": 0,
    "request_lock": threading.Lock(),
    "insert_serial": 0,
    "flushed_serial": 0,
    "flush_cond": threading.Condition(),
    "flush_in_progress": False,
    "cleaning_up": False,
    "ready": False,
    "srv": None,
    "proxy": None,
    "proxy_thread": None,
    "proc": None
}

def get_client(data_dir=".clickhouse", **client_options):
    """Return a clickhouse_connect client that talks to the sidecar proxy."""
    import clickhouse_connect
    data_dir = os.path.abspath(data_dir)
    os.makedirs(data_dir, exist_ok=True)
    sock = _connect_or_spawn(_get_sock_path(data_dir), data_dir)
    meta = _receive_meta(sock)
    client = clickhouse_connect.get_client(host="127.0.0.1", port=meta["port"], username="default", password="", **client_options)
    _patch_client_for_lease(client, sock, meta["port"])
    return client

def run_daemon(data_dir):
    """Run the detached sidecar daemon."""
    G["data_dir"] = os.path.abspath(data_dir)
    G["sock_path"] = _get_sock_path(G["data_dir"])
    os.makedirs(G["data_dir"], exist_ok=True)
    _log("daemon starting")
    _acquire_lock()
    _setup_signals()
    _maybe_stop_owned_orphan()
    _start_clickhouse(_ensure_clickhouse_binary())
    _wait_for_clickhouse()
    _start_proxy()
    _write_state()
    G["ready"] = True
    _log(f"daemon ready public={G['public_port']} clickhouse={G['ch_port']}")
    threading.Thread(target=_drain_loop, daemon=True).start()
    _accept_loop()

def _get_sock_path(data_dir):
    return f"/tmp/clickhouse-sidecar-{hashlib.sha256(data_dir.encode()).hexdigest()[:16]}.sock"

def _connect_or_spawn(sock_path, data_dir):
    for attempt in range(120):
        sock = _try_connect(sock_path)
        if sock:
            return sock
        if attempt % 10 == 0:
            subprocess.Popen([sys.executable, __file__, "daemon", data_dir], start_new_session=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.2)
    raise RuntimeError("timeout connecting to sidecar daemon")

def _try_connect(sock_path):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    with suppress(Exception):
        if sock.connect_ex(sock_path) == 0:
            return sock
    with suppress(Exception):
        sock.close()
    return None

def _receive_meta(sock):
    buf = b""
    while b"\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError("daemon closed lease socket before sending metadata")
        buf += chunk
    meta = json.loads(buf.decode().strip())
    if meta.get("error"):
        raise RuntimeError(meta["error"])
    return meta

def _patch_client_for_lease(client, sock, port):
    weak_close = weak_close_socket
    weakref.finalize(client, weak_close, sock)
    orig_close = getattr(type(client), "close", None)
    def new_close(self, *args, **kwargs):
        weak_close(sock)
        return orig_close(self, *args, **kwargs)
    client.close = new_close.__get__(client, type(client))
    client._lease_sock = sock
    client.sidecar_url = f"http://127.0.0.1:{port}"
    client.sidecar_port = port
    client.stream_url = lambda table: f"http://127.0.0.1:{port}/_sidecar/events?table={urllib.parse.quote(table)}"

def weak_close_socket(sock):
    with suppress(Exception):
        sock.close()

def _setup_signals():
    signal.signal(signal.SIGINT, _cleanup)
    signal.signal(signal.SIGTERM, _cleanup)
    atexit.register(_cleanup)

def _log(msg):
    if not G["data_dir"]:
        return
    with suppress(Exception):
        with open(os.path.join(G["data_dir"], "daemon.log"), "a") as f:
            f.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {msg}\n")

def _write_state():
    state = {
        "pid": os.getpid(),
        "publicPort": G["public_port"],
        "clickhousePort": G["ch_port"],
        "clickhousePid": G["proc"].pid if G["proc"] else 0,
        "startedAt": time.strftime('%Y-%m-%dT%H:%M:%S')
    }
    with open(os.path.join(G["data_dir"], "sidecar-state.json"), "w") as f:
        f.write(json.dumps(state, indent=2) + "\n")

def _ensure_clickhouse_binary():
    if os.environ.get("CLICKHOUSE_BINARY"):
        return os.environ["CLICKHOUSE_BINARY"]
    bin_dir = os.path.expanduser("~/.shared-clickhouse-bin")
    bin_path = os.path.join(bin_dir, "clickhouse")
    if os.path.exists(bin_path):
        return bin_path
    os.makedirs(bin_dir, exist_ok=True)
    tmp = tempfile.mkdtemp(prefix="download-", dir=bin_dir)
    subprocess.check_call("curl -fsSL https://clickhouse.com/ | sh", shell=True, cwd=tmp)
    os.rename(os.path.join(tmp, "clickhouse"), bin_path)
    os.chmod(bin_path, 0o755)
    return bin_path

def _maybe_stop_owned_orphan():
    pid_path = os.path.join(G["data_dir"], "clickhouse.pid")
    if not os.path.exists(pid_path):
        return
    with open(pid_path) as f:
        text = f.read().strip()
    if not text.isdigit():
        return
    pid = int(text)
    if not _looks_like_owned_clickhouse(pid):
        _log("stale pid file exists but process is not ours")
        return
    _log(f"stopping orphaned clickhouse pid={pid}")
    _stop_pid(pid)

def _looks_like_owned_clickhouse(pid):
    cmd = _get_process_command(pid)
    return "clickhouse" in cmd and os.path.join(G["data_dir"], "config.xml") in cmd

def _get_process_command(pid):
    with suppress(Exception):
        return subprocess.check_output(["ps", "-o", "command=", "-p", str(pid)], text=True).strip()
    return ""

def _stop_pid(pid):
    with suppress(Exception):
        os.kill(pid, signal.SIGTERM)
    for _ in range(30):
        if not _get_process_command(pid):
            return
        time.sleep(0.1)
    with suppress(Exception):
        os.kill(pid, signal.SIGKILL)

def _acquire_lock():
    sock_path = G["sock_path"]
    if os.path.exists(sock_path):
        sock = _try_connect(sock_path)
        if sock:
            sock.close()
            sys.exit(0)
        os.unlink(sock_path)
    G["srv"] = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    G["srv"].bind(sock_path)
    G["srv"].listen(128)

def _start_clickhouse(bin_path):
    G["ch_port"] = _get_free_port()
    for name in ["data", "tmp", "user_files"]:
        os.makedirs(os.path.join(G["data_dir"], name), exist_ok=True)
    with open(os.path.join(G["data_dir"], "config.xml"), "w") as f:
        f.write(_make_config_xml())
    with open(os.path.join(G["data_dir"], "users.xml"), "w") as f:
        f.write(_make_users_xml())
    log_file = open(os.path.join(G["data_dir"], "clickhouse.log"), "ab")
    G["proc"] = subprocess.Popen(
        [bin_path, "server", "--config-file", os.path.join(G["data_dir"], "config.xml")],
        stdout=log_file,
        stderr=log_file,
    )
    with open(os.path.join(G["data_dir"], "clickhouse.pid"), "w") as f:
        f.write(str(G["proc"].pid) + "\n")

def _make_config_xml():
    return "".join([
        "<clickhouse>",
        "<logger><level>warning</level><console>false</console></logger>",
        f"<http_port>{G['ch_port']}</http_port>",
        "<listen_host>127.0.0.1</listen_host>",
        f"<path>{_xml(os.path.join(G['data_dir'], 'data'))}/</path>",
        f"<tmp_path>{_xml(os.path.join(G['data_dir'], 'tmp'))}/</tmp_path>",
        f"<user_files_path>{_xml(os.path.join(G['data_dir'], 'user_files'))}/</user_files_path>",
        f"<users_config>{_xml(os.path.join(G['data_dir'], 'users.xml'))}</users_config>",
        "<mark_cache_size>67108864</mark_cache_size>",
        "<max_server_memory_usage>1073741824</max_server_memory_usage>",
        "</clickhouse>",
    ])

def _make_users_xml():
    return "".join([
        "<clickhouse>",
        "<profiles><default>",
        "<max_threads>1</max_threads>",
        "<max_insert_threads>1</max_insert_threads>",
        "<max_memory_usage>1073741824</max_memory_usage>",
        "<date_time_input_format>best_effort</date_time_input_format>",
        "<async_insert>1</async_insert>",
        "<wait_for_async_insert>0</wait_for_async_insert>",
        "</default></profiles>",
        "<users><default><password></password><networks><ip>127.0.0.1</ip><ip>::1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users>",
        "<quotas><default/></quotas>",
        "</clickhouse>",
    ])

def _wait_for_clickhouse():
    for _ in range(300):
        with suppress(Exception):
            out = _http_request("127.0.0.1", G["ch_port"], "GET", "/ping", b"", {"host": f"127.0.0.1:{G['ch_port']}"}, 1)
            if out["status"] == 200:
                return
        time.sleep(0.2)
    raise RuntimeError("clickhouse failed to become ready")

def _start_proxy():
    G["public_port"] = int(os.environ.get("PORT0") or 0) or _get_free_port()
    class Server(http.server.ThreadingHTTPServer):
        daemon_threads = True
    G["proxy"] = Server(("127.0.0.1", G["public_port"]), SidecarHandler)
    G["proxy_thread"] = threading.Thread(target=G["proxy"].serve_forever, daemon=True)
    G["proxy_thread"].start()

class SidecarHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self._handle()
    def do_POST(self):
        self._handle()
    def do_PUT(self):
        self._handle()
    def do_DELETE(self):
        self._handle()
    def do_HEAD(self):
        self._handle()
    def log_message(self, _fmt, *_args):
        return
    def _handle(self):
        with G["request_lock"]:
            G["request_count"] += 1
        try:
            url = urllib.parse.urlsplit(self.path)
            if url.path == "/ping":
                return self._send(200, b"Ok.\n", {"content-type": "text/plain; charset=utf-8"})
            if url.path == "/_sidecar/health":
                body = json.dumps({
                    "publicPort": G["public_port"],
                    "clickhousePort": G["ch_port"],
                    "dirty": G["flushed_serial"] < G["insert_serial"],
                    "insertSerial": G["insert_serial"],
                    "flushedSerial": G["flushed_serial"],
                    "activeLeases": len(G["active_sockets"]),
                    "subscribers": _count_subscribers(),
                    "requests": G["request_count"],
                    "ready": G["ready"],
                }).encode()
                return self._send(200, body, {"content-type": "application/json; charset=utf-8"})
            if url.path == "/_sidecar/events":
                return self._open_events(url)
            body = self._read_body()
            info = _inspect_request(url, body, self.headers, self.command)
            if info["is_read"]:
                _ensure_flushed()
            out = _http_request("127.0.0.1", G["ch_port"], self.command, self.path, body, _forward_headers(self.headers, body, G["ch_port"]), 300)
            if info["is_insert"] and out["status"] == 200:
                G["insert_serial"] += 1
                _broadcast_json_each_row(info, body)
            self._send(out["status"], out["body"], out["headers"])
        except Exception as e:
            self._send(502, (str(e) + "\n").encode(), {"content-type": "text/plain; charset=utf-8"})
        finally:
            with G["request_lock"]:
                G["request_count"] -= 1
    def _send(self, status, body, headers):
        self.send_response(status)
        for key, value in headers.items():
            if key.lower() in {"connection", "keep-alive", "transfer-encoding", "content-length"}:
                continue
            self.send_header(key, value)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)
    def _open_events(self, url):
        table = (urllib.parse.parse_qs(url.query).get("table") or [""])[0].strip()
        if not table:
            return self._send(400, b"missing table\n", {"content-type": "text/plain; charset=utf-8"})
        q = queue.Queue()
        with G["subscriber_lock"]:
            G["subscribers"].setdefault(table, set()).add(q)
        self.send_response(200)
        self.send_header("content-type", "text/event-stream; charset=utf-8")
        self.send_header("cache-control", "no-cache")
        self.send_header("connection", "keep-alive")
        self.end_headers()
        self.wfile.write(_sse("ready", json.dumps({"table": table})))
        self.wfile.flush()
        try:
            while not G["cleaning_up"]:
                try:
                    item = q.get(timeout=1)
                    self.wfile.write(_sse("row", item))
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")
                self.wfile.flush()
        except Exception:
            pass
        finally:
            with G["subscriber_lock"]:
                if table in G["subscribers"]:
                    G["subscribers"][table].discard(q)
                    if not G["subscribers"][table]:
                        del G["subscribers"][table]
    def _read_body(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            chunks = []
            while True:
                line = self.rfile.readline().strip()
                if not line:
                    continue
                size = int(line.split(b";", 1)[0], 16)
                if size == 0:
                    self.rfile.readline()
                    break
                chunks.append(self.rfile.read(size))
                self.rfile.read(2)
            return b"".join(chunks)
        length = int(self.headers.get("Content-Length", "0") or "0")
        return self.rfile.read(length) if length else b""

def _inspect_request(url, body, headers, method):
    params = urllib.parse.parse_qs(url.query)
    query = (params.get("query") or [""])[0] or _guess_query_from_body(body)
    upper = query.lstrip()[:120].upper()
    content_type = (headers.get("Content-Type") or "").lower()
    is_insert = upper.startswith("INSERT")
    if not is_insert and method == "POST" and "application/octet-stream" in content_type:
        is_insert = True
    return {
        "query": query,
        "is_insert": is_insert,
        "is_read": upper.startswith("SELECT") or upper.startswith("WITH") or upper.startswith("SHOW") or upper.startswith("DESC") or upper.startswith("DESCRIBE") or upper.startswith("EXPLAIN") or upper.startswith("EXISTS"),
        "is_json_each_row": "FORMAT JSONEACHROW" in query.upper(),
        "table": _extract_insert_table(query),
    }

def _guess_query_from_body(body):
    if not body:
        return ""
    prefix = body[:65536].decode(errors="replace")
    if prefix.lstrip().upper().startswith("INSERT"):
        return prefix.splitlines()[0]
    return prefix

def _extract_insert_table(query):
    import re
    m = re.search(r"\bINSERT\s+INTO\s+([`\"]?[A-Za-z_][A-Za-z0-9_$.]*[`\"]?)", query, re.I)
    if not m:
        return ""
    return m.group(1).strip("`\"")

def _broadcast_json_each_row(info, body):
    if not info["table"] or not info["is_json_each_row"]:
        return
    with G["subscriber_lock"]:
        targets = list(G["subscribers"].get(info["table"], set()))
    if not targets:
        return
    text = body.decode() if info["query"] else body.split(b"\n", 1)[1].decode() if b"\n" in body else ""
    for line in text.splitlines():
        if not line.strip():
            continue
        for q in targets:
            with suppress(Exception):
                q.put_nowait(line)

def _ensure_flushed():
    while True:
        with G["flush_cond"]:
            if G["flushed_serial"] >= G["insert_serial"]:
                return
            if G["flush_in_progress"]:
                G["flush_cond"].wait()
                continue
            G["flush_in_progress"] = True
            target = G["insert_serial"]
        err = None
        try:
            out = _http_request(
                "127.0.0.1",
                G["ch_port"],
                "POST",
                "/",
                b"SYSTEM FLUSH ASYNC INSERT QUEUE",
                {
                    "host": f"127.0.0.1:{G['ch_port']}",
                    "content-type": "text/plain; charset=utf-8",
                    "content-length": "31",
                },
                30,
            )
            if out["status"] != 200:
                raise RuntimeError(f"flush failed {out['status']}: {out['body'].decode(errors='replace')}")
        except Exception as e:
            err = e
        with G["flush_cond"]:
            if err is None and G["flushed_serial"] < target:
                G["flushed_serial"] = target
            G["flush_in_progress"] = False
            G["flush_cond"].notify_all()
        if err is not None:
            raise err

def _http_request(host, port, method, path, body, headers, timeout):
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    conn.request(method, path, body=body, headers=headers)
    res = conn.getresponse()
    data = res.read()
    headers_out = {k: v for k, v in res.getheaders()}
    conn.close()
    return {"status": res.status, "headers": headers_out, "body": data}

def _forward_headers(headers, body, port):
    out = {}
    for key, value in headers.items():
        lower = key.lower()
        if lower in {"host", "connection", "keep-alive", "transfer-encoding", "content-length"}:
            continue
        out[key] = value
    out["host"] = f"127.0.0.1:{port}"
    if body:
        out["content-length"] = str(len(body))
    return out

def _accept_loop():
    while not G["cleaning_up"]:
        sock, _ = G["srv"].accept()
        threading.Thread(target=_handle_lease, args=(sock,), daemon=True).start()

def _handle_lease(sock):
    G["active_sockets"].append(sock)
    _log(f"lease connected active={len(G['active_sockets'])}")
    with suppress(Exception):
        sock.sendall((json.dumps({"port": G["public_port"]}) + "\n").encode())
    try:
        while sock.recv(4096):
            pass
    except Exception:
        pass
    with suppress(ValueError):
        G["active_sockets"].remove(sock)
    with suppress(Exception):
        sock.close()
    _log(f"lease disconnected active={len(G['active_sockets'])}")

def _count_subscribers():
    with G["subscriber_lock"]:
        return sum(len(v) for v in G["subscribers"].values())

def _drain_loop():
    quiet_since = 0
    while not G["cleaning_up"]:
        time.sleep(0.5)
        if G["proc"] and G["proc"].poll() is not None:
            _log("clickhouse exited unexpectedly")
            os.kill(os.getpid(), signal.SIGTERM)
            return
        if G["active_sockets"] or _count_subscribers():
            quiet_since = 0
            continue
        with G["request_lock"]:
            if G["request_count"] > 0:
                quiet_since = 0
                continue
        usage = _get_active_usage()
        if usage > 0:
            quiet_since = 0
            continue
        quiet_since = quiet_since or time.time()
        if time.time() - quiet_since >= 3:
            _log("idle shutdown")
            os.kill(os.getpid(), signal.SIGTERM)
            return

def _get_active_usage():
    try:
        sql = "SELECT count() FROM system.processes WHERE is_initial_query=1 AND query NOT LIKE '%system.processes%'"
        out = _http_request("127.0.0.1", G["ch_port"], "GET", "/?query=" + urllib.parse.quote(sql), b"", {"host": f"127.0.0.1:{G['ch_port']}"}, 2)
        if out["status"] != 200:
            return -1
        return int((out["body"].decode().strip() or "0"))
    except Exception:
        return -1

def _cleanup(*_args):
    if G["cleaning_up"]:
        return
    G["cleaning_up"] = True
    _log("cleanup")
    with G["subscriber_lock"]:
        for queues in G["subscribers"].values():
            for q in queues:
                with suppress(Exception):
                    q.put_nowait("")
        G["subscribers"].clear()
    for sock in list(G["active_sockets"]):
        with suppress(Exception):
            sock.close()
    with suppress(Exception):
        if G["proxy"]:
            G["proxy"].shutdown()
            G["proxy"].server_close()
    with suppress(Exception):
        if G["srv"]:
            G["srv"].close()
    with suppress(Exception):
        if G["sock_path"] and os.path.exists(G["sock_path"]):
            os.unlink(G["sock_path"])
    if G["proc"] and G["proc"].poll() is None:
        with suppress(Exception):
            G["proc"].terminate()
        try:
            G["proc"].wait(timeout=1)
        except subprocess.TimeoutExpired:
            with suppress(Exception):
                G["proc"].kill()
            with suppress(Exception):
                G["proc"].wait(timeout=1)
    raise SystemExit(0)

def _get_free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

def _sse(event_name, data):
    return f"event: {event_name}\ndata: {data}\n\n".encode()

def _xml(text):
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "daemon":
        run_daemon(sys.argv[2])
