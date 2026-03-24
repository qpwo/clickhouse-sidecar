"""
ClickHouse Sidecar - Zero-config embedded ClickHouse manager for Python.
Spawns a background ClickHouse daemon and manages its lifecycle via lease sockets.
"""
import os, sys, time, socket, subprocess, json, hashlib, urllib.request, urllib.parse, atexit, weakref, signal, threading
from contextlib import suppress

G = {"sock_path": "", "http_port": 0, "active_sockets": [], "cleaning_up": False, "proc": None, "data_dir": "", "srv": None}

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
    G["data_dir"] = data_dir
    G["sock_path"] = _get_sock_path(data_dir)
    _log(f"Starting sidecar for {data_dir}")
    _acquire_lock()
    _setup_atexit()
    bin_path = _ensure_clickhouse_binary()
    _kill_old_pid()
    _start_clickhouse(bin_path)
    _wait_for_clickhouse()
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
            with suppress(Exception): sock.close()
            return orig_close(self, *args, **kwargs)
        client.close = new_close.__get__(client, type(client))
    client._lease_sock = sock

def _log(msg):
    """Append a log message to the daemon log."""
    if not G["data_dir"]: return
    with open(os.path.join(G["data_dir"], "daemon.log"), "a") as f:
        f.write(f"{time.time()}: {msg}\n")

def _acquire_lock():
    """Ensure only one daemon runs per socket path."""
    sock_path = G["sock_path"]
    if os.path.exists(sock_path):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if s.connect_ex(sock_path) == 0:
            s.close()
            sys.exit(0)
        s.close()
        os.remove(sock_path)
    G["srv"] = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    G["srv"].bind(sock_path)
    G["srv"].listen(128)

def _cleanup(*_):
    """Shutdown ClickHouse and clean up socket."""
    if G["cleaning_up"]: return
    G["cleaning_up"] = True
    _log("Cleaning up and exiting...")
    if os.path.exists(G["sock_path"]):
        os.remove(G["sock_path"])
    if G["proc"]:
        G["proc"].terminate()
        try:
            G["proc"].wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            G["proc"].kill()
            G["proc"].wait()
    sys.exit(0)

def _setup_atexit():
    """Register cleanup handlers."""
    signal.signal(signal.SIGINT, _cleanup)
    signal.signal(signal.SIGTERM, _cleanup)
    atexit.register(_cleanup)

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
    pid_file = os.path.join(G["data_dir"], "clickhouse.pid")
    if not os.path.exists(pid_file): return
    with open(pid_file) as f: pid_str = f.read().strip()
    if not pid_str.isdigit(): return
    pid = int(pid_str)
    if os.path.exists(f"/proc/{pid}"):
        with suppress(Exception): os.kill(pid, signal.SIGTERM)
        time.sleep(0.5)
        if os.path.exists(f"/proc/{pid}"):
            with suppress(Exception): os.kill(pid, signal.SIGKILL)

def _get_free_port():
    """Return an available TCP port."""
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port

def _start_clickhouse(bin_path):
    """Write config and spawn ClickHouse."""
    G["http_port"] = _get_free_port()
    tcp_port = _get_free_port()
    for d in ["data", "tmp", "user_files"]: os.makedirs(os.path.join(G["data_dir"], d), exist_ok=True)
    conf = f"<clickhouse><logger><level>none</level><console>false</console></logger><http_port>{G['http_port']}</http_port><tcp_port>{tcp_port}</tcp_port><listen_host>127.0.0.1</listen_host><path>{G['data_dir']}/data/</path><tmp_path>{G['data_dir']}/tmp/</tmp_path><user_files_path>{G['data_dir']}/user_files/</user_files_path><users_config>{G['data_dir']}/users.xml</users_config><mark_cache_size>268435456</mark_cache_size></clickhouse>"
    users = "<clickhouse><profiles><default/></profiles><users><default><password></password><networks><ip>127.0.0.1</ip><ip>::1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users><quotas><default/></quotas></clickhouse>"
    open(os.path.join(G["data_dir"], "config.xml"), "w").write(conf)
    open(os.path.join(G["data_dir"], "users.xml"), "w").write(users)
    G["proc"] = subprocess.Popen([bin_path, "server", "--config-file", os.path.join(G["data_dir"], "config.xml")], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    open(os.path.join(G["data_dir"], "clickhouse.pid"), "w").write(str(G["proc"].pid))
    _log(f"Spawned ClickHouse PID {G['proc'].pid} on HTTP {G['http_port']}")

def _wait_for_clickhouse():
    """Poll ClickHouse HTTP ping endpoint."""
    for _ in range(300):
        with suppress(Exception):
            if urllib.request.urlopen(f"http://127.0.0.1:{G['http_port']}/ping", timeout=1).status == 200:
                _log("ClickHouse is ready.")
                return
        time.sleep(0.2)
    assert False, "ClickHouse server failed to start within 60 seconds."

def _handle_client_socket(sock):
    """Handle a single client lease socket."""
    G["active_sockets"].append(sock)
    _log(f"Client connected. Active: {len(G['active_sockets'])}")
    with suppress(Exception): sock.sendall((json.dumps({"port": G["http_port"]}) + "\n").encode())
    while True:
        try:
            if not sock.recv(1024): break
        except Exception: break
    if sock in G["active_sockets"]: G["active_sockets"].remove(sock)
    _log(f"Client disconnected. Active: {len(G['active_sockets'])}")
    with suppress(Exception): sock.close()

def _accept_loop():
    """Accept incoming client connections."""
    while not G["cleaning_up"]:
        with suppress(Exception):
            sock, _ = G["srv"].accept()
            threading.Thread(target=_handle_client_socket, args=(sock,), daemon=True).start()

def _get_active_usage():
    """Query ClickHouse for active metrics."""
    try:
        q = urllib.parse.quote("SELECT count() FROM system.processes WHERE is_initial_query=1 AND query NOT LIKE '%system.processes%'")
        req = urllib.request.Request(f"http://127.0.0.1:{G['http_port']}/?query={q}")
        with urllib.request.urlopen(req, timeout=1) as res:
            return int(res.read().decode().strip() or "0")
    except Exception as e:
        _log(f"Error querying usage: {e}")
        return -1

def _drain_loop():
    """Monitor usage and shutdown if idle."""
    quiet_since = None
    while not G["cleaning_up"]:
        time.sleep(0.5)
        if G["proc"] and G["proc"].poll() is not None:
            _log("ClickHouse process died unexpectedly. Exiting.")
            os.kill(os.getpid(), signal.SIGTERM)
            return
        if G["active_sockets"]:
            quiet_since = None
            continue
        usage = _get_active_usage()
        if usage > 0:
            quiet_since = None
        else:
            quiet_since = quiet_since or time.time()
            if time.time() - quiet_since >= 3.0:
                _log("Idle for 3s with 0 clients, shutting down.")
                os.kill(os.getpid(), signal.SIGTERM)
                return

if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "daemon":
        run_daemon(sys.argv[2])
