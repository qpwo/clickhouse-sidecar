/**
 * ClickHouse Sidecar for Node.js.
 * Starts a hidden ClickHouse HTTP server behind a single public sidecar port.
 * The sidecar enables async inserts by default, flushes before reads for read-after-write,
 * exposes an optional SSE stream for JSONEachRow inserts, and manages lifetime by lease socket.
 */
import fs from 'node:fs';
import path from 'node:path';
import net from 'node:net';
import http from 'node:http';
import os from 'node:os';
import crypto from 'node:crypto';
import { spawn, execSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createClient } from '@clickhouse/client';

if (process.platform === 'win32') throw new Error('linux/mac only');

var G = {
    dataDir: '',
    sockPath: '',
    publicPort: 0,
    chPort: 0,
    activeSockets: new Set(),
    pendingSockets: new Set(),
    subscribers: new Map(),
    requestCount: 0,
    insertSerial: 0,
    flushedSerial: 0,
    flushPromise: null,
    cleaningUp: false,
    ready: false,
    srv: null,
    proxy: null,
    proc: null,
    startupError: '',
    drainTimer: null
};

var socketRegistry = new FinalizationRegistry(closeSocket);

export async function getClient(options = {}) {
    if (process.env.__CLICKHOUSE_SIDECAR_DAEMON__ === '1') throw new Error('cannot getClient inside daemon');
    var dataDir = path.resolve(options.dataDir || path.join(process.cwd(), '.clickhouse'));
    fs.mkdirSync(dataDir, { recursive: true });
    var sockPath = getSockPath(dataDir);
    var sock = await connectOrSpawn(sockPath, dataDir);
    var meta = await receiveMeta(sock);
    var client = createClient({
        username: 'default',
        password: '',
        database: 'default',
        keep_alive: { enabled: false },
        ...options.clientOptions,
        url: `http://127.0.0.1:${meta.port}`
    });
    return patchClientForLease(client, sock, meta.port);
}

export async function runDaemon(dataDir) {
    G.dataDir = path.resolve(dataDir);
    G.sockPath = getSockPath(G.dataDir);
    fs.mkdirSync(G.dataDir, { recursive: true });
    log('daemon starting');
    try {
        await acquireLock(G.sockPath);
        attachLeaseSocketServer();
        setupSignals();
        await maybeStopOwnedOrphan();
        await startClickhouse(ensureClickhouseBinary());
        await waitForClickhouse();
        await startProxy();
        G.ready = true;
        writeState();
        flushPendingMeta();
        startDrainLoop();
        log(`daemon ready public=${G.publicPort} clickhouse=${G.chPort}`);
    } catch (error) {
        G.startupError = String(error?.stack || error);
        log('startup error: ' + G.startupError);
        flushPendingMeta();
        setTimeout(cleanup, 50).unref();
    }
}

if (process.env.__CLICKHOUSE_SIDECAR_DAEMON__ === '1') {
    runDaemon(process.env.CH_DATA_DIR || '').catch(onDaemonCrash);
}

export * from '@clickhouse/client';

function onDaemonCrash(error) {
    G.startupError = String(error?.stack || error);
    log('daemon crash: ' + G.startupError);
    cleanup();
}

function patchClientForLease(client, sock, port) {
    socketRegistry.register(client, sock);
    var wrapper = new Proxy(client, {
        get(target, prop) {
            if (prop === 'close') return closeWrappedClient.bind(null, target, sock);
            if (prop === 'sidecarUrl') return `http://127.0.0.1:${port}`;
            if (prop === 'sidecarPort') return port;
            if (prop === 'streamUrl') return buildStreamUrl.bind(null, port);
            if (prop === '_leaseSock') return sock;
            var value = target[prop];
            if (typeof value === 'function') return value.bind(target);
            return value;
        }
    });
    socketRegistry.register(wrapper, sock);
    return wrapper;
}

async function closeWrappedClient(client, sock) {
    closeSocket(sock);
    return client.close();
}

function buildStreamUrl(port, table) {
    return `http://127.0.0.1:${port}/_sidecar/events?table=${encodeURIComponent(table)}`;
}

function closeSocket(sock) {
    if (!sock || sock.destroyed) return;
    try {
        sock.destroy();
    } catch {}
}

function getSockPath(dataDir) {
    var hash = crypto.createHash('sha256').update(dataDir).digest('hex').slice(0, 16);
    return path.join(os.tmpdir(), `clickhouse-sidecar-${hash}.sock`);
}

async function connectOrSpawn(sockPath, dataDir) {
    var __filename = fileURLToPath(import.meta.url);
    for (var attempt = 0; attempt < 120; attempt++) {
        var sock = await tryConnect(sockPath);
        if (sock) return sock;
        if (attempt % 10 === 0) {
            spawn(process.execPath, [__filename], {
                detached: true,
                stdio: 'ignore',
                env: { ...process.env, __CLICKHOUSE_SIDECAR_DAEMON__: '1', CH_DATA_DIR: dataDir }
            }).unref();
        }
        await sleep(200);
    }
    throw new Error('timeout connecting to sidecar daemon');
}

function tryConnect(sockPath) {
    return new Promise(resolve => {
        var sock = net.createConnection(sockPath);
        sock.once('connect', onConnect);
        sock.once('error', onError);
        function onConnect() {
            sock.removeListener('error', onError);
            resolve(sock);
        }
        function onError() {
            sock.removeListener('connect', onConnect);
            resolve(null);
        }
    });
}

function receiveMeta(sock) {
    return new Promise((resolve, reject) => {
        var buf = '';
        sock.on('data', onData);
        sock.once('error', onError);
        sock.once('close', onClose);
        function onData(chunk) {
            buf += chunk.toString('utf8');
            if (!buf.includes('\n')) return;
            done();
            try {
                var meta = JSON.parse(buf.trim());
                if (meta.error) reject(new Error(meta.error));
                else resolve(meta);
            } catch (error) {
                reject(new Error('invalid daemon metadata: ' + buf));
            }
        }
        function onError(error) {
            done();
            reject(error);
        }
        function onClose() {
            if (buf.includes('\n')) return;
            done();
            reject(new Error('daemon closed lease socket before sending metadata'));
        }
        function done() {
            sock.removeListener('data', onData);
            sock.removeListener('error', onError);
            sock.removeListener('close', onClose);
        }
    });
}

function attachLeaseSocketServer() {
    G.srv.on('connection', onLeaseConnection);
}

function onLeaseConnection(sock) {
    G.activeSockets.add(sock);
    if (G.ready || G.startupError) sendMeta(sock);
    else G.pendingSockets.add(sock);
    log(`lease connected active=${G.activeSockets.size}`);
    sock.on('close', onClose);
    sock.on('error', onSockError);
    function onClose() {
        G.activeSockets.delete(sock);
        G.pendingSockets.delete(sock);
        log(`lease disconnected active=${G.activeSockets.size}`);
    }
    function onSockError() {}
}

function sendMeta(sock) {
    if (sock.destroyed) return;
    var payload = G.startupError ? { error: G.startupError } : { port: G.publicPort };
    sock.write(JSON.stringify(payload) + '\n');
}

function flushPendingMeta() {
    for (var sock of G.pendingSockets) sendMeta(sock);
    G.pendingSockets.clear();
}

function setupSignals() {
    process.on('SIGINT', cleanup);
    process.on('SIGTERM', cleanup);
    process.on('uncaughtException', onUncaughtException);
    process.on('unhandledRejection', onUnhandledRejection);
}

function onUncaughtException(error) {
    log('uncaught exception: ' + String(error?.stack || error));
    cleanup();
}

function onUnhandledRejection(error) {
    log('unhandled rejection: ' + String(error?.stack || error));
    cleanup();
}

function log(msg) {
    if (!G.dataDir) return;
    try {
        fs.appendFileSync(path.join(G.dataDir, 'daemon.log'), `${new Date().toISOString()} ${msg}\n`);
    } catch {}
}

function writeState() {
    var state = {
        pid: process.pid,
        publicPort: G.publicPort,
        clickhousePort: G.chPort,
        clickhousePid: G.proc?.pid ?? 0,
        startedAt: new Date().toISOString()
    };
    try {
        fs.writeFileSync(path.join(G.dataDir, 'sidecar-state.json'), JSON.stringify(state, null, 2) + '\n');
    } catch (error) {
        log('writeState failed: ' + String(error?.stack || error));
    }
}

function ensureClickhouseBinary() {
    if (process.env.CLICKHOUSE_BINARY) return process.env.CLICKHOUSE_BINARY;
    var binDir = path.join(os.homedir(), '.shared-clickhouse-bin');
    var binPath = path.join(binDir, 'clickhouse');
    if (fs.existsSync(binPath)) return binPath;
    fs.mkdirSync(binDir, { recursive: true });
    var tmpDir = fs.mkdtempSync(path.join(binDir, 'download-'));
    execSync('curl -fsSL https://clickhouse.com/ | sh', { cwd: tmpDir, stdio: 'inherit' });
    fs.renameSync(path.join(tmpDir, 'clickhouse'), binPath);
    fs.chmodSync(binPath, 0o755);
    return binPath;
}

async function maybeStopOwnedOrphan() {
    var pidPath = path.join(G.dataDir, 'clickhouse.pid');
    if (!fs.existsSync(pidPath)) return;
    var pid = Number(fs.readFileSync(pidPath, 'utf8').trim());
    if (!pid) return;
    if (!looksLikeOwnedClickhouse(pid)) {
        log('stale pid file exists but process is not ours');
        return;
    }
    log('stopping orphaned clickhouse pid=' + pid);
    await stopPid(pid);
}

function looksLikeOwnedClickhouse(pid) {
    var cmd = getProcessCommand(pid);
    if (!cmd) return false;
    return cmd.includes('clickhouse') && cmd.includes(path.join(G.dataDir, 'config.xml'));
}

function getProcessCommand(pid) {
    try {
        return execSync(`ps -o command= -p ${pid}`, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
    } catch {
        return '';
    }
}

async function stopPid(pid) {
    try {
        process.kill(pid, 'SIGTERM');
    } catch {
        return;
    }
    for (var i = 0; i < 30; i++) {
        if (!getProcessCommand(pid)) return;
        await sleep(100);
    }
    try {
        process.kill(pid, 'SIGKILL');
    } catch {}
}

async function acquireLock(sockPath) {
    G.srv = net.createServer();
    await new Promise((resolve, reject) => {
        G.srv.once('error', onError);
        G.srv.listen(sockPath, onListen);
        function onListen() {
            G.srv.removeListener('error', onError);
            resolve();
        }
        function onError(error) {
            G.srv.removeListener('listening', onListen);
            reject(error);
        }
    }).catch(async error => {
        if (error?.code !== 'EADDRINUSE') throw error;
        var sock = await tryConnect(sockPath);
        if (sock) {
            sock.destroy();
            process.exit(0);
        }
        try {
            fs.unlinkSync(sockPath);
        } catch {}
        G.srv = net.createServer();
        await new Promise((resolve, reject) => {
            G.srv.once('error', reject);
            G.srv.listen(sockPath, resolve);
        });
    });
}

async function startClickhouse(binPath) {
    G.chPort = await getFreePort();
    fs.mkdirSync(path.join(G.dataDir, 'data'), { recursive: true });
    fs.mkdirSync(path.join(G.dataDir, 'tmp'), { recursive: true });
    fs.mkdirSync(path.join(G.dataDir, 'user_files'), { recursive: true });
    fs.writeFileSync(path.join(G.dataDir, 'config.xml'), makeConfigXml());
    fs.writeFileSync(path.join(G.dataDir, 'users.xml'), makeUsersXml());
    var logFile = fs.openSync(path.join(G.dataDir, 'clickhouse.log'), 'a');
    G.proc = spawn(binPath, ['server', '--config-file', path.join(G.dataDir, 'config.xml')], {
        stdio: ['ignore', logFile, logFile]
    });
    fs.writeFileSync(path.join(G.dataDir, 'clickhouse.pid'), String(G.proc.pid) + '\n');
    G.proc.once('exit', onClickhouseExit);
}

function onClickhouseExit(code, signal) {
    log(`clickhouse exit code=${code} signal=${signal}`);
    if (!G.cleaningUp) {
        G.startupError = G.startupError || `clickhouse exited early code=${code} signal=${signal}`;
        flushPendingMeta();
        cleanup();
    }
}

function makeConfigXml() {
    return [
        '<clickhouse>',
        '<logger><level>warning</level><console>false</console></logger>',
        `<http_port>${G.chPort}</http_port>`,
        '<listen_host>127.0.0.1</listen_host>',
        `<path>${xml(path.join(G.dataDir, 'data'))}/</path>`,
        `<tmp_path>${xml(path.join(G.dataDir, 'tmp'))}/</tmp_path>`,
        `<user_files_path>${xml(path.join(G.dataDir, 'user_files'))}/</user_files_path>`,
        `<users_config>${xml(path.join(G.dataDir, 'users.xml'))}</users_config>`,
        '<mark_cache_size>67108864</mark_cache_size>',
        '<max_server_memory_usage>1073741824</max_server_memory_usage>',
        '</clickhouse>'
    ].join('');
}

function makeUsersXml() {
    return [
        '<clickhouse>',
        '<profiles><default>',
        '<max_threads>1</max_threads>',
        '<max_insert_threads>1</max_insert_threads>',
        '<max_memory_usage>1073741824</max_memory_usage>',
        '<date_time_input_format>best_effort</date_time_input_format>',
        '<async_insert>1</async_insert>',
        '<wait_for_async_insert>0</wait_for_async_insert>',
        '</default></profiles>',
        '<users><default><password></password><networks><ip>127.0.0.1</ip><ip>::1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users>',
        '<quotas><default/></quotas>',
        '</clickhouse>'
    ].join('');
}

async function waitForClickhouse() {
    for (var i = 0; i < 300; i++) {
        try {
            var out = await httpRequest({
                host: '127.0.0.1',
                port: G.chPort,
                method: 'GET',
                path: '/ping',
                timeout: 1000,
                headers: { host: `127.0.0.1:${G.chPort}` }
            });
            if (out.statusCode === 200) return;
        } catch {}
        await sleep(200);
    }
    throw new Error('clickhouse failed to become ready');
}

async function startProxy() {
    G.publicPort = Number(process.env.PORT0 || 0) || await getFreePort();
    G.proxy = http.createServer(onProxyRequest);
    await new Promise((resolve, reject) => {
        G.proxy.once('error', reject);
        G.proxy.listen(G.publicPort, '127.0.0.1', resolve);
    });
}

async function onProxyRequest(req, res) {
    G.requestCount++;
    try {
        var url = new URL(req.url, `http://${req.headers.host || `127.0.0.1:${G.publicPort}`}`);
        if (url.pathname === '/ping') return sendPing(res);
        if (url.pathname === '/_sidecar/health') return sendHealth(res);
        if (url.pathname === '/_sidecar/events') return openEventStream(req, res, url);
        var body = await readRequestBody(req);
        var info = inspectRequest(url, body, req.headers, req.method || 'GET');
        if (info.isRead) await ensureFlushed();
        var out = await forwardToClickhouse(req, url, body);
        if (info.isInsert && out.statusCode === 200) {
            G.insertSerial++;
            broadcastJsonEachRow(info, body);
        }
        sendBufferedResponse(res, out);
    } catch (error) {
        sendError(res, 502, String(error?.stack || error));
    } finally {
        G.requestCount--;
    }
}

function sendPing(res) {
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
    res.end('Ok.\n');
}

function sendHealth(res) {
    var payload = {
        publicPort: G.publicPort,
        clickhousePort: G.chPort,
        dirty: G.flushedSerial < G.insertSerial,
        insertSerial: G.insertSerial,
        flushedSerial: G.flushedSerial,
        activeLeases: G.activeSockets.size,
        subscribers: countSubscribers(),
        requests: G.requestCount,
        ready: G.ready
    };
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(payload));
}

function openEventStream(req, res, url) {
    var table = (url.searchParams.get('table') || '').trim();
    if (!table) return sendError(res, 400, 'missing table');
    var set = getSubscriberSet(table);
    set.add(res);
    res.writeHead(200, {
        'content-type': 'text/event-stream; charset=utf-8',
        'cache-control': 'no-cache',
        connection: 'keep-alive'
    });
    res.write(buildSseFrame('ready', JSON.stringify({ table })));
    var timer = setInterval(sendHeartbeat, 15000);
    req.on('close', close);
    req.on('aborted', close);
    res.on('close', close);
    function sendHeartbeat() {
        if (res.destroyed || res.writableEnded) return close();
        res.write(': ping\n\n');
    }
    function close() {
        clearInterval(timer);
        removeSubscriber(table, res);
        try {
            res.end();
        } catch {}
    }
}

function getSubscriberSet(table) {
    if (!G.subscribers.has(table)) G.subscribers.set(table, new Set());
    return G.subscribers.get(table);
}

function removeSubscriber(table, res) {
    var set = G.subscribers.get(table);
    if (!set) return;
    set.delete(res);
    if (set.size === 0) G.subscribers.delete(table);
}

function buildSseFrame(eventName, data) {
    return `event: ${eventName}\ndata: ${data}\n\n`;
}

function broadcastJsonEachRow(info, body) {
    if (!info.table || !info.isJsonEachRow) return;
    var set = G.subscribers.get(info.table);
    if (!set || set.size === 0) return;
    var text = getInsertDataText(info, body);
    if (!text) return;
    for (var line of text.split(/\r?\n/)) {
        if (!line.trim()) continue;
        var frame = buildSseFrame('row', line);
        for (var res of set) {
            if (res.destroyed || res.writableEnded) {
                removeSubscriber(info.table, res);
                continue;
            }
            try {
                res.write(frame);
            } catch {
                removeSubscriber(info.table, res);
            }
        }
    }
}

function inspectRequest(url, body, headers = {}, method = 'GET') {
    var query = url.searchParams.get('query') || guessQueryFromBody(body);
    var trimmed = query.trimStart();
    var upper = trimmed.slice(0, 120).toUpperCase();
    var isInsert = /^INSERT\b/.test(upper);
    if (!isInsert && method === 'POST' && String(headers['content-type'] || headers['Content-Type'] || '').toLowerCase().includes('application/octet-stream')) isInsert = true;
    return {
        query,
        isInsert,
        isRead: /^(SELECT|WITH|SHOW|DESC|DESCRIBE|EXPLAIN|EXISTS)\b/.test(upper),
        isJsonEachRow: /\bFORMAT\s+JSONEachRow\b/i.test(query),
        table: extractInsertTable(query)
    };
}

function guessQueryFromBody(body) {
    if (!body.length) return '';
    var prefix = body.toString('utf8', 0, Math.min(body.length, 65536));
    if (/^\s*INSERT\b/i.test(prefix)) return prefix.split(/\r?\n/, 1)[0];
    return prefix;
}

function extractInsertTable(query) {
    var match = query.match(/\bINSERT\s+INTO\s+([`"]?[A-Za-z_][A-Za-z0-9_$.]*[`"]?)/i);
    if (!match) return '';
    return match[1].replace(/^["`]|["`]$/g, '');
}

function getInsertDataText(info, body) {
    if (!info.isJsonEachRow || !body.length) return '';
    if (info.query) return body.toString('utf8');
    var index = body.indexOf(10);
    if (index < 0) return '';
    return body.subarray(index + 1).toString('utf8');
}

async function ensureFlushed() {
    while (G.flushedSerial < G.insertSerial) await flushRound();
}

function flushRound() {
    if (G.flushPromise) return G.flushPromise;
    var target = G.insertSerial;
    G.flushPromise = postInternalSql('SYSTEM FLUSH ASYNC INSERT QUEUE')
        .then(() => {
            if (G.flushedSerial < target) G.flushedSerial = target;
        })
        .finally(() => {
            G.flushPromise = null;
        });
    return G.flushPromise;
}

async function postInternalSql(sql) {
    var out = await httpRequest({
        host: '127.0.0.1',
        port: G.chPort,
        method: 'POST',
        path: '/',
        timeout: 30000,
        headers: {
            host: `127.0.0.1:${G.chPort}`,
            'content-type': 'text/plain; charset=utf-8',
            'content-length': Buffer.byteLength(sql)
        }
    }, Buffer.from(sql));
    if (out.statusCode !== 200) throw new Error(`flush failed ${out.statusCode}: ${out.body.toString('utf8')}`);
}

async function forwardToClickhouse(req, url, body) {
    return httpRequest({
        host: '127.0.0.1',
        port: G.chPort,
        method: req.method || 'GET',
        path: url.pathname + url.search,
        timeout: 300000,
        headers: buildForwardHeaders(req.headers, body)
    }, body);
}

function buildForwardHeaders(headers, body) {
    var out = {};
    for (var [key, value] of Object.entries(headers || {})) {
        var lower = key.toLowerCase();
        if (lower === 'host') continue;
        if (lower === 'connection') continue;
        if (lower === 'keep-alive') continue;
        if (lower === 'transfer-encoding') continue;
        if (lower === 'content-length') continue;
        if (value != null) out[key] = value;
    }
    out.host = `127.0.0.1:${G.chPort}`;
    if (body.length) out['content-length'] = String(body.length);
    return out;
}

function sendBufferedResponse(res, out) {
    for (var [key, value] of Object.entries(out.headers || {})) {
        var lower = key.toLowerCase();
        if (lower === 'connection') continue;
        if (lower === 'keep-alive') continue;
        if (lower === 'transfer-encoding') continue;
        if (lower === 'content-length') continue;
        if (value != null) res.setHeader(key, value);
    }
    res.statusCode = out.statusCode;
    res.setHeader('content-length', String(out.body.length));
    res.end(out.body);
}

function sendError(res, statusCode, msg) {
    if (res.headersSent) {
        try {
            res.end(msg);
        } catch {}
        return;
    }
    res.writeHead(statusCode, { 'content-type': 'text/plain; charset=utf-8' });
    res.end(msg + '\n');
}

function httpRequest(options, body = Buffer.alloc(0)) {
    return new Promise((resolve, reject) => {
        var req = http.request(options, onResponse);
        req.once('error', reject);
        req.setTimeout(options.timeout || 30000, onTimeout);
        if (body.length) req.write(body);
        req.end();
        function onTimeout() {
            req.destroy(new Error('http timeout'));
        }
        function onResponse(res) {
            var chunks = [];
            res.on('data', onData);
            res.on('end', onEnd);
            res.on('error', reject);
            function onData(chunk) {
                chunks.push(Buffer.from(chunk));
            }
            function onEnd() {
                resolve({
                    statusCode: res.statusCode || 0,
                    headers: res.headers,
                    body: Buffer.concat(chunks)
                });
            }
        }
    });
}

function readRequestBody(req) {
    return new Promise((resolve, reject) => {
        var chunks = [];
        req.on('data', onData);
        req.on('end', onEnd);
        req.on('error', reject);
        function onData(chunk) {
            chunks.push(Buffer.from(chunk));
        }
        function onEnd() {
            resolve(Buffer.concat(chunks));
        }
    });
}

async function getActiveUsage() {
    try {
        var sql = "SELECT count() FROM system.processes WHERE is_initial_query=1 AND query NOT LIKE '%system.processes%'";
        var out = await httpRequest({
            host: '127.0.0.1',
            port: G.chPort,
            method: 'GET',
            path: '/?query=' + encodeURIComponent(sql),
            timeout: 2000,
            headers: { host: `127.0.0.1:${G.chPort}` }
        });
        if (out.statusCode !== 200) return -1;
        return Number(out.body.toString('utf8').trim()) || 0;
    } catch {
        return -1;
    }
}

function countSubscribers() {
    var n = 0;
    for (var set of G.subscribers.values()) n += set.size;
    return n;
}

function startDrainLoop() {
    var quietSince = 0;
    G.drainTimer = setInterval(async () => {
        if (G.cleaningUp) return;
        if (G.activeSockets.size) {
            quietSince = 0;
            return;
        }
        if (G.requestCount > 0 || countSubscribers() > 0) {
            quietSince = 0;
            return;
        }
        var usage = await getActiveUsage();
        if (usage > 0) {
            quietSince = 0;
            return;
        }
        quietSince = quietSince || Date.now();
        if (Date.now() - quietSince >= 3000) {
            log('idle shutdown');
            cleanup();
        }
    }, 500);
    G.drainTimer.unref();
}

function cleanup() {
    if (G.cleaningUp) return;
    G.cleaningUp = true;
    log('cleanup');
    if (G.drainTimer) clearInterval(G.drainTimer);
    for (var set of G.subscribers.values()) {
        for (var res of set) {
            try {
                res.end();
            } catch {}
        }
    }
    G.subscribers.clear();
    for (var sock of G.activeSockets) closeSocket(sock);
    for (var sock of G.pendingSockets) closeSocket(sock);
    try {
        G.proxy?.close();
    } catch {}
    try {
        G.srv?.close();
    } catch {}
    try {
        if (G.sockPath) fs.unlinkSync(G.sockPath);
    } catch {}
    finishCleanup().catch(error => {
        log('finishCleanup failed: ' + String(error?.stack || error));
        process.exit(1);
    });
}

async function finishCleanup() {
    if (G.proc && G.proc.exitCode == null) {
        try {
            G.proc.kill('SIGTERM');
        } catch {}
        for (var i = 0; i < 10; i++) {
            if (G.proc.exitCode != null) {
                process.exit(0);
                return;
            }
            await sleep(100);
        }
        try {
            G.proc.kill('SIGKILL');
        } catch {}
        for (var i = 0; i < 10; i++) {
            if (G.proc.exitCode != null) {
                process.exit(0);
                return;
            }
            await sleep(100);
        }
    }
    process.exit(0);
}

function getFreePort() {
    return new Promise((resolve, reject) => {
        var srv = net.createServer();
        srv.once('error', reject);
        srv.listen(0, '127.0.0.1', onListen);
        function onListen() {
            var port = srv.address().port;
            srv.close(() => resolve(port));
        }
    });
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function xml(s) {
    return String(s).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}
