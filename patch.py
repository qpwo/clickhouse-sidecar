import os

NODE_SRC = """/** ClickHouse Sidecar - Zero-config embedded ClickHouse manager for Node.js. */
import fs from 'node:fs';
import path from 'node:path';
import net from 'node:net';
import { spawn, execSync } from 'node:child_process';
import os from 'node:os';
import crypto from 'node:crypto';
import http from 'node:http';
import { fileURLToPath } from 'node:url';
import { createClient } from '@clickhouse/client';

if (process.platform === 'win32') throw new Error('Linux/macOS only.');

const G = {
    dataDir: '',
    sockPath: '',
    httpPort: 0,
    internalHttpPort: 0,
    tcpPort: 0,
    activeSockets: new Set(),
    pendingSockets: new Set(),
    cleaningUp: false,
    proc: null,
    srv: null,
    chError: null,
    ready: false,
    dirty: false,
    isOwner: false
};

const socketRegistry = new FinalizationRegistry(sock => {
    if (!sock.destroyed) sock.destroy();
});

export async function getClient(options = {}) {
    if (process.env.__SHARED_CLICKHOUSE_DAEMON__ === '1') throw new Error('Cannot call in daemon');
    const dataDir = path.resolve(options.dataDir || path.join(process.cwd(), '.clickhouse'));
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
    const sockPath = getSockPath(dataDir);
    const sock = await connectOrSpawn(sockPath, dataDir);
    const port = await receivePort(sock);
    const client = createClient({ username: 'default', password: '', database: 'default', keep_alive: { enabled: false }, ...options.clientOptions, url: `http://127.0.0.1:${port}` });
    return patchClientForLease(client, sock);
}

export async function runDaemon(dataDir) {
    G.dataDir = dataDir;
    G.sockPath = getSockPath(dataDir);
    log(`Starting sidecar for ${dataDir}`);
    try {
        await acquireLock(G.sockPath);
        attachConnectionListener();
        setupAtexit();
        const binPath = ensureClickhouseBinary();
        await killOldPid();
        await startClickhouse(binPath);
        await waitForClickhouse();
        G.ready = true;
        for (const sock of G.pendingSockets) {
            if (!sock.destroyed) sock.write(JSON.stringify({ port: G.httpPort }) + '\\n');
        }
        G.pendingSockets.clear();
        startDrainLoop();
    } catch (err) {
        G.chError = err.message;
        log(`Daemon startup error: ${err.stack}`);
        for (const sock of G.pendingSockets) {
            if (!sock.destroyed) sock.write(JSON.stringify({ error: G.chError }) + '\\n');
            if (!sock.destroyed) sock.end();
        }
        G.pendingSockets.clear();
        setTimeout(cleanup, 100);
    }
}

function getSockPath(dataDir) {
    const hash = crypto.createHash('sha256').update(dataDir).digest('hex').substring(0, 16);
    return path.join(os.tmpdir(), `ch-emb-${hash}.sock`);
}

async function connectOrSpawn(sockPath, dataDir) {
    const __filename = fileURLToPath(import.meta.url);
    for (let attempt = 0; attempt < 150; attempt++) {
        const sock = await new Promise(resolve => {
            const s = net.createConnection(sockPath);
            s.on('connect', () => resolve(s));
            s.on('error', () => resolve(null));
        });
        if (sock) return sock;
        if (attempt % 10 === 0) {
            const env = { ...process.env, __SHARED_CLICKHOUSE_DAEMON__: '1', CH_DATA_DIR: dataDir };
            spawn(process.execPath, [__filename], { detached: true, stdio: 'ignore', env }).unref();
        }
        await new Promise(r => setTimeout(r, 200));
    }
    throw new Error('Timeout connecting to ClickHouse daemon socket');
}

function receivePort(sock) {
    return new Promise((resolve, reject) => {
        let buf = '';
        const onData = chunk => {
            buf += chunk.toString();
            if (buf.includes('\\n')) {
                cleanupSock();
                try {
                    const data = JSON.parse(buf.trim());
                    if (data.error) reject(new Error(`Daemon error: ${data.error}`));
                    else resolve(data.port);
                } catch (e) { reject(new Error(`Daemon sent invalid JSON: ${buf}`)); }
            }
        };
        const onError = err => { cleanupSock(); reject(err); };
        const onClose = () => { cleanupSock(); if (!buf.includes('\\n')) reject(new Error('Daemon closed socket unexpectedly')); };
        function cleanupSock() {
            sock.removeListener('data', onData);
            sock.removeListener('error', onError);
            sock.removeListener('close', onClose);
        }
        sock.on('data', onData);
        sock.on('error', onError);
        sock.on('close', onClose);
    });
}

function patchClientForLease(client, sock) {
    socketRegistry.register(client, sock);
    const wrapper = new Proxy(client, {
        get(target, prop) {
            if (prop === 'close') {
                return async () => {
                    if (!sock.destroyed) sock.destroy();
                    return target.close();
                };
            }
            if (typeof target[prop] === 'function') return target[prop].bind(target);
            return target[prop];
        }
    });
    socketRegistry.register(wrapper, sock);
    wrapper._leaseSock = sock;
    return wrapper;
}

function log(msg) {
    if (!G.dataDir) return;
    try { fs.appendFileSync(path.join(G.dataDir, 'daemon.log'), `${Date.now()}: ${msg}\\n`); } catch (e) {}
}

function acquireLock(sockPath) {
    return new Promise((resolve, reject) => {
        const tryLock = () => {
            const lockFile = path.join(G.dataDir, 'daemon.lock');
            try {
                const fd = fs.openSync(lockFile, 'wx');
                fs.writeSync(fd, process.pid.toString());
                fs.closeSync(fd);
            } catch (err) {
                if (err.code !== 'EEXIST') return reject(err);
                try {
                    const pid = parseInt(fs.readFileSync(lockFile, 'utf8'), 10);
                    if (pid) process.kill(pid, 0);
                    const test = net.createConnection(sockPath);
                    test.once('connect', () => { test.destroy(); process.exit(0); });
                    test.once('error', () => { test.destroy(); setTimeout(tryLock, 100); });
                    return;
                } catch {
                    try { fs.unlinkSync(lockFile); } catch {}
                    return setTimeout(tryLock, 10);
                }
            }
            try { fs.unlinkSync(sockPath); } catch {}
            G.srv = net.createServer();
            G.srv.once('error', err => { process.exit(0); });
            G.srv.listen(sockPath, () => { G.isOwner = true; resolve(); });
        };
        tryLock();
    });
}

function attachConnectionListener() {
    G.srv.on('connection', sock => {
        if (sock.destroyed) return;
        sock.once('close', () => {
            G.activeSockets.delete(sock);
            G.pendingSockets.delete(sock);
        });
        sock.on('error', () => {});

        G.activeSockets.add(sock);
        if (G.ready) {
            if (!sock.destroyed) sock.write(JSON.stringify({ port: G.httpPort }) + '\\n');
        } else if (G.chError) {
            if (!sock.destroyed) sock.write(JSON.stringify({ error: G.chError }) + '\\n');
            sock.destroy();
        } else {
            G.pendingSockets.add(sock);
        }
    });
}

function cleanup() {
    if (G.cleaningUp) return;
    G.cleaningUp = true;
    if (G.isOwner) {
        try { fs.unlinkSync(G.sockPath); } catch {}
        try { fs.unlinkSync(path.join(G.dataDir, 'daemon.lock')); } catch {}
    }
    if (G.proc) {
        G.proc.kill('SIGTERM');
        setTimeout(() => { try { process.kill(G.proc.pid, 'SIGKILL'); } catch {} }, 3000).unref();
    }
    process.exit(0);
}

function setupAtexit() {
    process.on('SIGINT', cleanup);
    process.on('SIGTERM', cleanup);
    process.on('uncaughtException', err => { log(`Uncaught: ${err}`); cleanup(); });
}

function ensureClickhouseBinary() {
    if (process.env.CLICKHOUSE_BINARY) return process.env.CLICKHOUSE_BINARY;
    const binDir = path.join(os.homedir(), '.shared-clickhouse-bin');
    const binPath = path.join(binDir, 'clickhouse');
    if (fs.existsSync(binPath)) return binPath;
    fs.mkdirSync(binDir, { recursive: true });
    const tmpDir = path.join(binDir, 'tmp-' + crypto.randomBytes(4).toString('hex'));
    fs.mkdirSync(tmpDir, { recursive: true });
    try {
        execSync('curl -sL https://clickhouse.com/ | sh', { cwd: tmpDir, stdio: 'ignore' });
        fs.renameSync(path.join(tmpDir, 'clickhouse'), binPath);
        return binPath;
    } finally {
        fs.rmSync(tmpDir, { recursive: true, force: true });
    }
}

async function killOldPid() {
    const pidFile = path.join(G.dataDir, 'clickhouse.pid');
    if (!fs.existsSync(pidFile)) return;
    const pid = parseInt(fs.readFileSync(pidFile, 'utf8'), 10);
    if (!pid) return;
    try {
        process.kill(pid, 'SIGTERM');
        await new Promise(r => setTimeout(r, 500));
        process.kill(pid, 'SIGKILL');
    } catch {}
}

function getFreePort() {
    return new Promise((resolve, reject) => {
        const srv = net.createServer();
        srv.listen(0, '127.0.0.1', () => {
            const port = srv.address().port;
            srv.close(() => resolve(port));
        });
        srv.on('error', reject);
    });
}

function proxyRequestHandler(req, res) {
    const parsed = new URL(req.url, 'http://127.0.0.1');
    const qStr = (parsed.searchParams.get('query') || '').trim().toUpperCase();
    let isInsert = qStr.startsWith('INSERT');
    let isSelect = qStr.startsWith('SELECT');

    const handleProxy = () => {
        if (!parsed.pathname.startsWith('/ping') && !parsed.pathname.startsWith('/replicas_status')) {
            if (isInsert) G.dirty = true;
            else if (G.dirty && isSelect) {
                G.dirty = false;
                return new Promise(resolve => {
                    const flushReq = http.request(`http://127.0.0.1:${G.internalHttpPort}/`, { method: 'POST', timeout: 5000 }, flushRes => {
                        flushRes.resume();
                        resolve(doForward());
                    });
                    flushReq.on('error', () => { G.dirty = true; resolve(doForward()); });
                    flushReq.on('timeout', () => { G.dirty = true; flushReq.destroy(); resolve(doForward()); });
                    flushReq.write('SYSTEM FLUSH ASYNC INSERT QUEUE');
                    flushReq.end();
                });
            }
        }
        return doForward();
    };

    const doForward = () => {
        const headers = { ...req.headers };
        delete headers['host'];
        delete headers['connection'];
        headers['host'] = `127.0.0.1:${G.internalHttpPort}`;

        const forwardReq = http.request(`http://127.0.0.1:${G.internalHttpPort}${req.url}`, {
            method: req.method,
            headers: headers
        }, forwardRes => {
            delete forwardRes.headers['transfer-encoding'];
            delete forwardRes.headers['connection'];
            try { res.writeHead(forwardRes.statusCode, forwardRes.headers); } catch(e){}
            forwardRes.pipe(res);
        });

        forwardReq.on('error', err => {
            try {
                if (!res.headersSent) res.writeHead(502);
                res.end(err.toString());
            } catch(e){}
        });

        req.on('error', err => forwardReq.destroy(err));
        res.on('error', err => forwardReq.destroy(err));

        if (req.firstChunk) forwardReq.write(req.firstChunk);
        req.pipe(forwardReq);
    };

    if (req.method === 'POST' && !isInsert && !isSelect) {
        let firstChunkCollected = false;
        req.once('data', chunk => {
            firstChunkCollected = true;
            req.firstChunk = chunk;
            const str = chunk.slice(0, 100).toString('utf8').toUpperCase();
            if (str.startsWith('INSERT')) isInsert = true;
            if (str.startsWith('SELECT')) isSelect = true;
            handleProxy();
        });
        req.once('end', () => {
            if (!firstChunkCollected) handleProxy();
        });
    } else {
        handleProxy();
    }
}

async function startClickhouse(binPath) {
    G.internalHttpPort = await getFreePort();
    G.tcpPort = await getFreePort();
    const interserverPort = await getFreePort();
    G.httpPort = process.env.PORT0 ? parseInt(process.env.PORT0, 10) : await getFreePort();
    ['data', 'tmp', 'user_files'].forEach(d => fs.mkdirSync(path.join(G.dataDir, d), { recursive: true }));
    const conf = `<clickhouse><logger><level>none</level><console>false</console></logger><http_port>${G.internalHttpPort}</http_port><tcp_port>${G.tcpPort}</tcp_port><interserver_http_port>${interserverPort}</interserver_http_port><listen_host>127.0.0.1</listen_host><path>${G.dataDir}/data/</path><tmp_path>${G.dataDir}/tmp/</tmp_path><user_files_path>${G.dataDir}/user_files/</user_files_path><users_config>${G.dataDir}/users.xml</users_config><mark_cache_size>67108864</mark_cache_size><max_server_memory_usage>1073741824</max_server_memory_usage></clickhouse>`;
    const users = `<clickhouse><profiles><default><max_threads>1</max_threads><async_insert>1</async_insert><wait_for_async_insert>0</wait_for_async_insert></default></profiles><users><default><password></password><networks><ip>127.0.0.1</ip><ip>::1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users><quotas><default/></quotas></clickhouse>`;
    fs.writeFileSync(path.join(G.dataDir, 'config.xml'), conf);
    fs.writeFileSync(path.join(G.dataDir, 'users.xml'), users);
    G.proc = spawn(binPath, ['server', '--config-file', path.join(G.dataDir, 'config.xml')], { stdio: 'ignore' });
    fs.writeFileSync(path.join(G.dataDir, 'clickhouse.pid'), G.proc.pid.toString());

    const proxySrv = http.createServer(proxyRequestHandler);
    await new Promise(r => proxySrv.listen(G.httpPort, '0.0.0.0', r));
    log(`Spawned ClickHouse PID ${G.proc.pid} on HTTP ${G.internalHttpPort}, Proxy on ${G.httpPort}`);

    G.proc.on('exit', code => {
        if (!G.internalHttpPort || code !== 0) {
            G.chError = `ClickHouse exited prematurely with code ${code}`;
            for (const s of G.pendingSockets) {
                if (!s.destroyed) s.write(JSON.stringify({ error: G.chError }) + '\\n');
            }
            G.pendingSockets.clear();
        }
        cleanup();
    });
}

async function waitForClickhouse() {
    for (let i = 0; i < 300; i++) {
        const ready = await new Promise(resolve => {
            const req = http.get(`http://127.0.0.1:${G.httpPort}/ping`, res => {
                res.resume();
                resolve(res.statusCode === 200);
            });
            req.on('error', () => resolve(false));
            req.setTimeout(1000, () => { req.destroy(); resolve(false); });
        });
        if (ready) return;
        await new Promise(r => setTimeout(r, 200));
    }
    throw new Error("ClickHouse server failed to start within 60 seconds.");
}

function getActiveUsage() {
    return new Promise(resolve => {
        const q = encodeURIComponent("SELECT count() FROM system.processes WHERE is_initial_query=1 AND query NOT LIKE '%system.processes%'");
        const req = http.get(`http://127.0.0.1:${G.httpPort}/?query=${q}`, res => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                if (res.statusCode !== 200) return resolve(-1);
                const num = parseInt(data.trim());
                resolve(isNaN(num) ? -1 : num);
            });
        });
        req.on('error', err => resolve(-1));
        req.setTimeout(1000, () => { req.destroy(); resolve(-1); });
    });
}

function startDrainLoop() {
    let quietSince = null;
    setInterval(async () => {
        if (G.cleaningUp) return;
        if (G.activeSockets.size > 0) { quietSince = null; return; }
        const usage = await getActiveUsage();
        if (usage > 0) { quietSince = null; return; }
        quietSince = quietSince || Date.now();
        if (Date.now() - quietSince >= 3000) {
            cleanup();
        }
    }, 500);
}

if (process.env.__SHARED_CLICKHOUSE_DAEMON__ === '1') {
    runDaemon(process.env.CH_DATA_DIR).catch(err => { log(`Daemon err: ${err.stack}`); process.exit(1); });
}

export * from '@clickhouse/client';
"""

PYTHON_SRC = """\"\"\"
ClickHouse Sidecar - Zero-config embedded ClickHouse manager for Python.
Spawns a background ClickHouse daemon and manages its lifecycle via lease sockets.
\"\"\"
import os, sys, time, socket, subprocess, json, hashlib, urllib.request, urllib.parse, atexit, weakref, signal, threading, http.server, http.client
from contextlib import suppress

G = {"sock_path": "", "http_port": 0, "internal_http_port": 0, "tcp_port": 0, "active_sockets": [], "cleaning_up": False, "proc": None, "data_dir": "", "srv": None, "dirty": False, "is_owner": False}

def get_client(data_dir=".clickhouse", **client_options):
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
    return f"/tmp/ch-emb-{hashlib.sha256(data_dir.encode()).hexdigest()[:16]}.sock"

def _connect_or_spawn(sock_path, data_dir):
    for attempt in range(150):
        if os.path.exists(sock_path):
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            if s.connect_ex(sock_path) == 0: return s
            s.close()
        if attempt % 10 == 0:
            subprocess.Popen([sys.executable, __file__, "daemon", data_dir], start_new_session=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.2)
    assert False, "Timeout connecting to ClickHouse daemon socket"

def _receive_port(sock):
    buf = b""
    while b"\\n" not in buf:
        chunk = sock.recv(1024)
        assert chunk, "Daemon closed socket unexpectedly"
        buf += chunk
    data = json.loads(buf.decode().strip())
    assert "error" not in data, f"Daemon error: {data.get('error')}"
    return data["port"]

def _patch_client_for_lease(client, sock):
    weakref.finalize(client, sock.close)
    orig_close = getattr(type(client), "close", None)
    if orig_close:
        def new_close(self, *args, **kwargs):
            with suppress(Exception): sock.close()
            return orig_close(self, *args, **kwargs)
        client.close = new_close.__get__(client, type(client))
    client._lease_sock = sock

def _log(msg):
    if not G["data_dir"]: return
    with open(os.path.join(G["data_dir"], "daemon.log"), "a") as f:
        f.write(f"{time.time()}: {msg}\\n")

def _acquire_lock():
    lock_file = os.path.join(G["data_dir"], "daemon.lock")
    while True:
        try:
            fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
            break
        except FileExistsError:
            try:
                with open(lock_file) as f: pid = int(f.read().strip())
                os.kill(pid, 0)
                sock_path = G["sock_path"]
                if os.path.exists(sock_path):
                    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    if s.connect_ex(sock_path) == 0:
                        s.close()
                        os._exit(0)
                    s.close()
            except Exception:
                with suppress(OSError): os.remove(lock_file)
            time.sleep(0.1)

    sock_path = G["sock_path"]
    with suppress(OSError): os.remove(sock_path)
    G["srv"] = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        G["srv"].bind(sock_path)
        G["is_owner"] = True
    except OSError:
        os._exit(0)
    G["srv"].listen(128)

def _cleanup(*_):
    if G["cleaning_up"]: return
    G["cleaning_up"] = True
    _log("Cleaning up and exiting...")
    if G.get("is_owner") and os.path.exists(G["sock_path"]):
        with suppress(Exception): os.remove(G["sock_path"])
    if G.get("is_owner"):
        with suppress(Exception): os.remove(os.path.join(G["data_dir"], "daemon.lock"))
    if G["proc"]:
        G["proc"].terminate()
        try: G["proc"].wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            G["proc"].kill()
            G["proc"].wait()
    sys.exit(0)

def _setup_atexit():
    signal.signal(signal.SIGINT, _cleanup)
    signal.signal(signal.SIGTERM, _cleanup)
    atexit.register(_cleanup)

def _ensure_clickhouse_binary():
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
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port

class ProxyHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass
    def do_GET(self): self.proxy_request("GET")
    def do_POST(self): self.proxy_request("POST")
    def proxy_request(self, method):
        path = self.path
        parsed = urllib.parse.urlparse(path)
        qs = urllib.parse.parse_qs(parsed.query)
        q_str = qs.get('query', [''])[0].strip().upper()
        is_insert = q_str.startswith('INSERT')
        is_select = q_str.startswith('SELECT')

        is_chunked = self.headers.get('Transfer-Encoding', '').lower() == 'chunked'
        content_length = int(self.headers.get('Content-Length', 0))

        first_chunk = b""
        if not is_insert and not is_select and method == 'POST':
            if is_chunked:
                line = self.rfile.readline()
                size_str = line.strip().split(b';')[0]
                if size_str:
                    size = int(size_str, 16)
                    if size > 0:
                        first_chunk_data = self.rfile.read(size)
                        crlf = self.rfile.readline()
                        first_chunk = line + first_chunk_data + crlf
                        if first_chunk_data.upper().startswith(b'INSERT'):
                            is_insert = True
                        if first_chunk_data.upper().startswith(b'SELECT'):
                            is_select = True
                    else:
                        first_chunk = line + self.rfile.readline()
            elif content_length > 0:
                peek_size = min(content_length, 8192)
                first_chunk = self.rfile.read(peek_size)
                if first_chunk.upper().startswith(b'INSERT'):
                    is_insert = True
                if first_chunk.upper().startswith(b'SELECT'):
                    is_select = True
                content_length -= len(first_chunk)

        if not path.startswith('/ping') and not path.startswith('/replicas_status'):
            if is_insert:
                G["dirty"] = True
            elif G.get("dirty") and is_select:
                G["dirty"] = False
                try:
                    urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{G['internal_http_port']}/", data=b"SYSTEM FLUSH ASYNC INSERT QUEUE", method="POST"), timeout=5)
                except Exception as e:
                    G["dirty"] = True
                    _log(f"Flush error: {e}")

        try:
            conn = http.client.HTTPConnection("127.0.0.1", G['internal_http_port'], timeout=120)
            headers = {}
            for k, v in self.headers.items():
                if k.lower() not in ('host', 'connection'):
                    headers[k] = v
            conn.putrequest(method, path, skip_host=True, skip_accept_encoding=True)
            conn.putheader("Host", f"127.0.0.1:{G['internal_http_port']}")
            for k, v in headers.items(): conn.putheader(k, v)
            conn.endheaders()

            if first_chunk: conn.send(first_chunk)

            if is_chunked:
                while True:
                    line = self.rfile.readline()
                    conn.send(line)
                    size_str = line.strip().split(b';')[0]
                    if not size_str: break
                    size = int(size_str, 16)
                    if size == 0:
                        conn.send(self.rfile.readline())
                        break
                    chunk = self.rfile.read(size)
                    conn.send(chunk)
                    conn.send(self.rfile.readline())
            elif content_length > 0:
                remaining = content_length
                while remaining > 0:
                    chunk = self.rfile.read(min(remaining, 65536))
                    if not chunk: break
                    conn.send(chunk)
                    remaining -= len(chunk)

            res = conn.getresponse()
            self.send_response(res.status)
            for k, v in res.getheaders():
                if k.lower() not in ('transfer-encoding', 'connection'):
                    self.send_header(k, v)
            self.end_headers()
            while True:
                chunk = res.read(65536)
                if not chunk: break
                self.wfile.write(chunk)
        except Exception as e:
            try:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(str(e).encode())
            except Exception: pass
        finally:
            if 'conn' in locals(): conn.close()

def _start_clickhouse(bin_path):
    G["internal_http_port"] = _get_free_port()
    G["tcp_port"] = _get_free_port()
    interserver_port = _get_free_port()
    G["http_port"] = int(os.environ["PORT0"]) if "PORT0" in os.environ else _get_free_port()

    for d in ["data", "tmp", "user_files"]: os.makedirs(os.path.join(G["data_dir"], d), exist_ok=True)
    conf = f"<clickhouse><logger><level>none</level><console>false</console></logger><http_port>{G['internal_http_port']}</http_port><tcp_port>{G['tcp_port']}</tcp_port><interserver_http_port>{interserver_port}</interserver_http_port><listen_host>127.0.0.1</listen_host><path>{G['data_dir']}/data/</path><tmp_path>{G['data_dir']}/tmp/</tmp_path><user_files_path>{G['data_dir']}/user_files/</user_files_path><users_config>{G['data_dir']}/users.xml</users_config><mark_cache_size>67108864</mark_cache_size><max_server_memory_usage>1073741824</max_server_memory_usage></clickhouse>"
    users = "<clickhouse><profiles><default><max_threads>1</max_threads><async_insert>1</async_insert><wait_for_async_insert>0</wait_for_async_insert></default></profiles><users><default><password></password><networks><ip>127.0.0.1</ip><ip>::1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users><quotas><default/></quotas></clickhouse>"
    open(os.path.join(G["data_dir"], "config.xml"), "w").write(conf)
    open(os.path.join(G["data_dir"], "users.xml"), "w").write(users)
    G["proc"] = subprocess.Popen([bin_path, "server", "--config-file", os.path.join(G["data_dir"], "config.xml")], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    open(os.path.join(G["data_dir"], "clickhouse.pid"), "w").write(str(G["proc"].pid))

    server = http.server.ThreadingHTTPServer(('0.0.0.0', G["http_port"]), ProxyHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    _log(f"Spawned ClickHouse PID {G['proc'].pid} on HTTP {G['internal_http_port']}, Proxy on {G['http_port']}")

def _wait_for_clickhouse():
    for _ in range(300):
        with suppress(Exception):
            if urllib.request.urlopen(f"http://127.0.0.1:{G['http_port']}/ping", timeout=1).status == 200:
                _log("ClickHouse is ready.")
                return
        time.sleep(0.2)
    assert False, "ClickHouse server failed to start within 60 seconds."

def _handle_client_socket(sock):
    with suppress(Exception):
        sock.settimeout(1.0)
        sock.sendall((json.dumps({"port": G["http_port"]}) + "\\n").encode())
        sock.settimeout(None)

    G["active_sockets"].append(sock)
    while True:
        try:
            if not sock.recv(1024): break
        except Exception: break
    if sock in G["active_sockets"]: G["active_sockets"].remove(sock)
    with suppress(Exception): sock.close()

def _accept_loop():
    while not G["cleaning_up"]:
        with suppress(Exception):
            sock, _ = G["srv"].accept()
            threading.Thread(target=_handle_client_socket, args=(sock,), daemon=True).start()

def _get_active_usage():
    try:
        q = urllib.parse.quote("SELECT count() FROM system.processes WHERE is_initial_query=1 AND query NOT LIKE '%system.processes%'")
        req = urllib.request.Request(f"http://127.0.0.1:{G['http_port']}/?query={q}")
        with urllib.request.urlopen(req, timeout=1) as res:
            return int(res.read().decode().strip() or "0")
    except Exception: return -1

def _drain_loop():
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
        if usage > 0: quiet_since = None
        else:
            quiet_since = quiet_since or time.time()
            if time.time() - quiet_since >= 3.0:
                _log("Idle for 3s with 0 clients, shutting down.")
                os.kill(os.getpid(), signal.SIGTERM)
                return

if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "daemon":
        run_daemon(sys.argv[2])
"""

with open("node-pkg/index.mjs", "w") as f:
    f.write(NODE_SRC)

with open("python-pkg/clickhouse_sidecar.py", "w") as f:
    f.write(PYTHON_SRC)
