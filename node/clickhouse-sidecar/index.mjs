/** ClickHouse Sidecar - Zero-config embedded ClickHouse manager for Node.js. */
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
    activeSockets: new Set(),
    pendingSockets: new Set(),
    cleaningUp: false,
    proc: null,
    srv: null,
    chError: null,
    ready: false
};

const socketRegistry = new FinalizationRegistry(sock => {
    if (!sock.destroyed) sock.destroy();
});

/** Acquire a ClickHouse client, spawning the daemon if necessary. */
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

/** Run the sidecar daemon process. */
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
            if (!sock.destroyed) sock.write(JSON.stringify({ port: G.httpPort }) + '\n');
        }
        G.pendingSockets.clear();
        startDrainLoop();
    } catch (err) {
        G.chError = err.message;
        log(`Daemon startup error: ${err.stack}`);
        for (const sock of G.pendingSockets) {
            if (!sock.destroyed) sock.write(JSON.stringify({ error: G.chError }) + '\n');
            sock.destroy();
        }
        G.pendingSockets.clear();
        setTimeout(cleanup, 100);
    }
}

/** Return the unix socket path for a given data directory. */
function getSockPath(dataDir) {
    const hash = crypto.createHash('sha256').update(dataDir).digest('hex').substring(0, 16);
    return path.join(os.tmpdir(), `ch-emb-${hash}.sock`);
}

/** Connect to daemon socket, spawning daemon if needed. */
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

/** Read the port from the daemon socket. */
function receivePort(sock) {
    return new Promise((resolve, reject) => {
        let buf = '';
        const onData = chunk => {
            buf += chunk.toString();
            if (buf.includes('\n')) {
                cleanup();
                try {
                    const data = JSON.parse(buf.trim());
                    if (data.error) reject(new Error(`Daemon error: ${data.error}`));
                    else resolve(data.port);
                } catch (e) {
                    reject(new Error(`Daemon sent invalid JSON: ${buf}`));
                }
            }
        };
        const onError = err => { cleanup(); reject(err); };
        const onClose = () => { cleanup(); if (!buf.includes('\n')) reject(new Error('Daemon closed socket unexpectedly (did it crash?)')); };

        function cleanup() {
            sock.removeListener('data', onData);
            sock.removeListener('error', onError);
            sock.removeListener('close', onClose);
        }

        sock.on('data', onData);
        sock.on('error', onError);
        sock.on('close', onClose);
    });
}

/** Keep socket alive with client, close socket on client close. */
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

/** Append a log message to the daemon log. */
function log(msg) {
    if (!G.dataDir) return;
    try { fs.appendFileSync(path.join(G.dataDir, 'daemon.log'), `${Date.now()}: ${msg}\n`); } catch (e) {}
}

/** Ensure only one daemon runs per socket path. */
function acquireLock(sockPath) {
    return new Promise((resolve, reject) => {
        G.srv = net.createServer();
        G.srv.once('error', err => {
            if (err.code === 'EADDRINUSE') {
                const test = net.createConnection(sockPath);
                test.once('connect', () => { test.destroy(); process.exit(0); });
                test.once('error', () => {
                    try { fs.unlinkSync(sockPath); } catch {}
                    G.srv = net.createServer();
                    G.srv.once('error', reject);
                    G.srv.listen(sockPath, resolve);
                });
            } else reject(err);
        });
        G.srv.listen(sockPath, resolve);
    });
}

/** Accept incoming client connections immediately. */
function attachConnectionListener() {
    G.srv.on('connection', sock => {
        G.activeSockets.add(sock);
        log(`Client connected. Active: ${G.activeSockets.size}`);

        if (G.ready) {
            if (!sock.destroyed) sock.write(JSON.stringify({ port: G.httpPort }) + '\n');
        } else if (G.chError) {
            if (!sock.destroyed) sock.write(JSON.stringify({ error: G.chError }) + '\n');
            sock.destroy();
        } else {
            G.pendingSockets.add(sock);
        }

        sock.on('close', () => {
            G.activeSockets.delete(sock);
            G.pendingSockets.delete(sock);
            log(`Client disconnected. Active: ${G.activeSockets.size}`);
        });
        sock.on('error', () => {});
    });
}

/** Shutdown ClickHouse and clean up socket. */
function cleanup() {
    if (G.cleaningUp) return;
    G.cleaningUp = true;
    log("Cleaning up and exiting...");
    try { fs.unlinkSync(G.sockPath); } catch {}
    if (G.proc) {
        G.proc.kill('SIGTERM');
        setTimeout(() => { try { process.kill(G.proc.pid, 'SIGKILL'); } catch {} }, 3000).unref();
    }
    process.exit(0);
}

/** Register cleanup handlers. */
function setupAtexit() {
    process.on('SIGINT', cleanup);
    process.on('SIGTERM', cleanup);
    process.on('uncaughtException', err => { log(`Uncaught: ${err}`); cleanup(); });
}

/** Download ClickHouse binary if not present. */
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

/** Kill old ClickHouse process if pid file exists. */
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

/** Return an available TCP port. */
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

/** Write config and spawn ClickHouse. */
async function startClickhouse(binPath) {
    G.httpPort = await getFreePort();
    const tcpPort = await getFreePort();
    ['data', 'tmp', 'user_files'].forEach(d => fs.mkdirSync(path.join(G.dataDir, d), { recursive: true }));
    const conf = `<clickhouse><logger><level>none</level><console>false</console></logger><http_port>${G.httpPort}</http_port><tcp_port>${tcpPort}</tcp_port><listen_host>127.0.0.1</listen_host><path>${G.dataDir}/data/</path><tmp_path>${G.dataDir}/tmp/</tmp_path><user_files_path>${G.dataDir}/user_files/</user_files_path><users_config>${G.dataDir}/users.xml</users_config><mark_cache_size>268435456</mark_cache_size></clickhouse>`;
    const users = `<clickhouse><profiles><default/></profiles><users><default><password></password><networks><ip>127.0.0.1</ip><ip>::1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users><quotas><default/></quotas></clickhouse>`;
    fs.writeFileSync(path.join(G.dataDir, 'config.xml'), conf);
    fs.writeFileSync(path.join(G.dataDir, 'users.xml'), users);
    G.proc = spawn(binPath, ['server', '--config-file', path.join(G.dataDir, 'config.xml')], { stdio: 'ignore' });
    fs.writeFileSync(path.join(G.dataDir, 'clickhouse.pid'), G.proc.pid.toString());
    log(`Spawned ClickHouse PID ${G.proc.pid} on HTTP ${G.httpPort}`);

    G.proc.on('exit', code => {
        log(`ClickHouse exited with code ${code}`);
        if (!G.httpPort || code !== 0) {
            G.chError = `ClickHouse exited prematurely with code ${code}`;
            log(G.chError);
            for (const s of G.pendingSockets) {
                if (!s.destroyed) s.write(JSON.stringify({ error: G.chError }) + '\n');
            }
            G.pendingSockets.clear();
        }
        cleanup();
    });
}

/** Poll ClickHouse HTTP ping endpoint. */
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
        if (ready) { log("ClickHouse ready."); return; }
        await new Promise(r => setTimeout(r, 200));
    }
    throw new Error("ClickHouse server failed to start within 60 seconds.");
}

/** Query ClickHouse for active metrics. */
function getActiveUsage() {
    return new Promise(resolve => {
        const q = encodeURIComponent("SELECT count() FROM system.processes WHERE is_initial_query=1 AND query NOT LIKE '%system.processes%'");
        const req = http.get(`http://127.0.0.1:${G.httpPort}/?query=${q}`, res => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                if (res.statusCode !== 200) {
                    log(`Usage check failed with status ${res.statusCode}: ${data}`);
                    return resolve(-1);
                }
                const num = parseInt(data.trim());
                resolve(isNaN(num) ? -1 : num);
            });
        });
        req.on('error', err => { log(`Usage req error: ${err}`); resolve(-1); });
        req.setTimeout(1000, () => { req.destroy(); log("Usage check timeout"); resolve(-1); });
    });
}

/** Monitor usage and shutdown if idle. */
function startDrainLoop() {
    let quietSince = null;
    setInterval(async () => {
        if (G.cleaningUp) return;
        if (G.activeSockets.size > 0) { quietSince = null; return; }
        const usage = await getActiveUsage();
        if (usage > 0) { quietSince = null; return; }
        quietSince = quietSince || Date.now();
        if (Date.now() - quietSince >= 3000) {
            log("Idle for 3s with 0 clients, shutting down.");
            cleanup();
        }
    }, 500);
}

if (process.env.__SHARED_CLICKHOUSE_DAEMON__ === '1') {
    runDaemon(process.env.CH_DATA_DIR).catch(err => { log(`Daemon err: ${err.stack}`); process.exit(1); });
}

export * from '@clickhouse/client';
