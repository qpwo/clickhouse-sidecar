"""
ClickHouse Sidecar - An elegant, zero-config embedded ClickHouse manager for Python.
Provides `get_client(data_dir)` which spawns a ClickHouse daemon if needed.
The daemon stays alive as long as there are active client sockets or running queries.
"""

import os, sys, time, socket, subprocess, json, hashlib, urllib.request, urllib.parse, atexit, weakref

# Toplevel mutable globals
DAEMON_SOCK_PATH = ""
CH_HTTP_PORT = 0
ACTIVE_SOCKETS = []
IS_CLEANING_UP = [False]
CH_PROC = [None]
DAEMON_DATA_DIR = ""
DAEMON_SRV = None

def _log(msg):
    if DAEMON_DATA_DIR:
        try:
            with open(os.path.join(DAEMON_DATA_DIR, "daemon.log"), "a") as f:
                f.write(f"{time.time()}: {msg}\n")
        except: pass

def get_client(data_dir=".clickhouse", **client_options):
    """Acquire a ClickHouse client, spawning the daemon if necessary."""
    import clickhouse_connect
    data_dir = os.path.abspath(data_dir)
    os.makedirs(data_dir, exist_ok=True)
    sock_path = _get_sock_path(data_dir)
    sock = _connect_or_spawn(sock_path, data_dir)
    port = _receive_port(sock)
    client = clickhouse_connect.get_client(host="127.0.0.1", port=port, username="default", password="", **client_options)
    _patch_client_for_lease(client, sock)
    return client

def run_daemon(data_dir):
    """Run the sidecar daemon process."""
    global DAEMON_DATA_DIR, DAEMON_SOCK_PATH
    DAEMON_DATA_DIR = data_dir
    _log(f"Starting sidecar for {data_dir}")
    DAEMON_SOCK_PATH = _get_sock_path(data_dir)
    _acquire_lock(DAEMON_SOCK_PATH)
    _setup_atexit()
    bin_path = _ensure_clickhouse_binary()
    _kill_old_pid()
    _start_clickhouse(bin_path)
    _wait_for_clickhouse()
    import threading
    threading.Thread(target=_drain_loop, daemon=True).start()
    _accept_loop()

def _get_sock_path(data_dir):
    """Return the unix socket path for a given data directory."""
    return f"/tmp/ch-emb-{hashlib.sha256(data_dir.encode()).hexdigest()[:16]}.sock"

def _connect_or_spawn(sock_path, data_dir):
    """Connect to daemon socket, spawning daemon if needed."""
    for attempt in range(150):
        if os.path.exists(sock_path):
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            if s.connect_ex(sock_path) == 0:
                return s
            s.close()
        if attempt % 10 == 0:
            subprocess.Popen([sys.executable, __file__, "daemon", data_dir], start_new_session=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.2)
    assert False, "Timeout connecting to ClickHouse daemon socket"

def _receive_port(sock):
    """Read the port from the daemon socket."""
    buf = b""
    while b"\n" not in buf:
        chunk = sock.recv(1024)
        assert chunk, "Daemon closed socket unexpectedly"
        buf += chunk
    data = json.loads(buf.decode().strip())
    assert "error" not in data, f"Daemon error: {data.get('error')}"
    return data["port"]

def _patch_client_for_lease(client, sock):
    """Keep socket alive with client, close socket on client close."""
    weakref.finalize(client, sock.close)
    orig_close = getattr(type(client), "close", None)
    if orig_close:
        def new_close(self, *args, **kwargs):
            try: sock.close()
            except Exception: pass
            return orig_close(self, *args, **kwargs)
        client.close = new_close.__get__(client, type(client))
    client._lease_sock = sock

def _acquire_lock(sock_path):
    """Ensure only one daemon runs per socket path."""
    global DAEMON_SRV
    if os.path.exists(sock_path):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if s.connect_ex(sock_path) == 0:
            s.close()
            sys.exit(0)
        s.close()
        os.remove(sock_path)
    DAEMON_SRV = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    DAEMON_SRV.bind(sock_path)
    DAEMON_SRV.listen(128)

def _setup_atexit():
    """Register cleanup handlers."""
    import signal
    def cleanup(*_):
        if IS_CLEANING_UP[0]: return
        IS_CLEANING_UP[0] = True
        _log("Cleaning up and exiting...")
        if os.path.exists(DAEMON_SOCK_PATH):
            try: os.remove(DAEMON_SOCK_PATH)
            except Exception: pass
        if CH_PROC[0]:
            CH_PROC[0].terminate()
            try:
                CH_PROC[0].wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                CH_PROC[0].kill()
                CH_PROC[0].wait()
        sys.exit(0)
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)
    atexit.register(cleanup)

def _ensure_clickhouse_binary():
    """Download ClickHouse binary if not present."""
    if "CLICKHOUSE_BINARY" in os.environ: return os.environ["CLICKHOUSE_BINARY"]
    bin_path = os.path.expanduser("~/.shared-clickhouse-bin/clickhouse")
    if os.path.exists(bin_path): return bin_path
    os.makedirs(os.path.dirname(bin_path), exist_ok=True)
    import tempfile
    with tempfile.TemporaryDirectory(dir=os.path.dirname(bin_path)) as tmp:
        subprocess.check_call("curl -sL https://clickhouse.com/ | sh", shell=True, cwd=tmp, stdout=subprocess.DEVNULL)
        os.rename(os.path.join(tmp, "clickhouse"), bin_path)
    return bin_path

def _kill_old_pid():
    """Kill old ClickHouse process if pid file exists."""
    pid_file = os.path.join(DAEMON_DATA_DIR, "clickhouse.pid")
    if not os.path.exists(pid_file): return
    try: pid = int(open(pid_file).read().strip())
    except Exception: return
    import signal
    if os.path.exists(f"/proc/{pid}"):
        try: os.kill(pid, signal.SIGTERM)
        except Exception: pass
        time.sleep(0.5)
        if os.path.exists(f"/proc/{pid}"):
            try: os.kill(pid, signal.SIGKILL)
            except Exception: pass

def _get_free_port():
    """Return an available TCP port."""
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port

def _start_clickhouse(bin_path):
    """Write config and spawn ClickHouse."""
    global CH_HTTP_PORT
    CH_HTTP_PORT = _get_free_port()
    tcp_port = _get_free_port()
    for d in ["data", "tmp", "user_files"]: os.makedirs(os.path.join(DAEMON_DATA_DIR, d), exist_ok=True)
    conf = f"<clickhouse><logger><level>none</level><console>false</console></logger><http_port>{CH_HTTP_PORT}</http_port><tcp_port>{tcp_port}</tcp_port><listen_host>127.0.0.1</listen_host><path>{DAEMON_DATA_DIR}/data/</path><tmp_path>{DAEMON_DATA_DIR}/tmp/</tmp_path><user_files_path>{DAEMON_DATA_DIR}/user_files/</user_files_path><users_config>{DAEMON_DATA_DIR}/users.xml</users_config><mark_cache_size>268435456</mark_cache_size></clickhouse>"
    users = "<clickhouse><profiles><default/></profiles><users><default><password></password><networks><ip>127.0.0.1</ip><ip>::1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users><quotas><default/></quotas></clickhouse>"
    open(os.path.join(DAEMON_DATA_DIR, "config.xml"), "w").write(conf)
    open(os.path.join(DAEMON_DATA_DIR, "users.xml"), "w").write(users)
    CH_PROC[0] = subprocess.Popen([bin_path, "server", "--config-file", os.path.join(DAEMON_DATA_DIR, "config.xml")], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    open(os.path.join(DAEMON_DATA_DIR, "clickhouse.pid"), "w").write(str(CH_PROC[0].pid))
    _log(f"Spawned ClickHouse PID {CH_PROC[0].pid} on HTTP {CH_HTTP_PORT}")

def _wait_for_clickhouse():
    """Poll ClickHouse HTTP ping endpoint."""
    for _ in range(300):
        try:
            if urllib.request.urlopen(f"http://127.0.0.1:{CH_HTTP_PORT}/ping", timeout=1).status == 200:
                _log("ClickHouse is ready.")
                return
        except Exception:
            pass
        time.sleep(0.2)
    assert False, "ClickHouse server failed to start within 60 seconds."

def _handle_client_socket(sock):
    """Handle a single client lease socket."""
    ACTIVE_SOCKETS.append(sock)
    _log(f"Client socket connected. Total active: {len(ACTIVE_SOCKETS)}")
    try: sock.sendall((json.dumps({"port": CH_HTTP_PORT}) + "\n").encode())
    except Exception: pass
    while True:
        try:
            d = sock.recv(1024)
            if not d: break
        except Exception as e:
            _log(f"Socket recv error: {e}")
            break
    if sock in ACTIVE_SOCKETS: ACTIVE_SOCKETS.remove(sock)
    _log(f"Client socket disconnected. Total active: {len(ACTIVE_SOCKETS)}")
    try: sock.close()
    except Exception: pass

def _accept_loop():
    """Accept incoming client connections."""
    import threading
    while not IS_CLEANING_UP[0]:
        try:
            sock, _ = DAEMON_SRV.accept()
            threading.Thread(target=_handle_client_socket, args=(sock,), daemon=True).start()
        except Exception:
            pass

def _get_active_usage():
    """Query ClickHouse for active metrics."""
    try:
        q = "SELECT count() FROM system.processes WHERE is_initial_query=1 AND query NOT LIKE '%system.processes%'"
        url = f"http://127.0.0.1:{CH_HTTP_PORT}/?query={urllib.parse.quote(q)}"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=1) as res:
            return int(res.read().decode().strip() or "0")
    except Exception as e:
        _log(f"Error querying active usage: {e}")
        return -1

def _drain_loop():
    """Monitor usage and shutdown if idle."""
    import signal
    quiet_since = None
    while not IS_CLEANING_UP[0]:
        time.sleep(0.5)

        if CH_PROC[0] and CH_PROC[0].poll() is not None:
            _log("ClickHouse process died unexpectedly. Exiting.")
            os.kill(os.getpid(), signal.SIGTERM)
            return

        if ACTIVE_SOCKETS:
            _log(f"Drain loop: ACTIVE_SOCKETS={len(ACTIVE_SOCKETS)}, resetting quiet_since")
            quiet_since = None
            continue

        usage = _get_active_usage()
        _log(f"Drain loop: usage={usage}")
        if usage > 0:
            quiet_since = None
        else:
            quiet_since = quiet_since or time.time()
            _log(f"Drain loop: quiet for {time.time() - quiet_since:.1f}s")
            if time.time() - quiet_since >= 3.0:
                _log("Idle for 3s with 0 clients, shutting down.")
                os.kill(os.getpid(), signal.SIGTERM)
                return

if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "daemon":
        run_daemon(sys.argv[2])
