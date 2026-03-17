import fs from 'node:fs';
import path from 'node:path';
import net from 'node:net';
import { spawn, execSync } from 'node:child_process';
import os from 'node:os';
import crypto from 'node:crypto';
import http from 'node:http';
import { fileURLToPath } from 'node:url';
import { createClient } from '@clickhouse/client';

if (process.platform === 'win32') {
    throw new Error('leased-clickhouse is only supported on Linux and macOS.');
}

const socketRegistry = new FinalizationRegistry((socket) => {
    if (socket && !socket.destroyed) {
        socket.destroy();
    }
});

const __filename = fileURLToPath(import.meta.url);
const IS_DAEMON = process.env.__SHARED_CLICKHOUSE_DAEMON__ === '1';

if (IS_DAEMON) {
    runDaemon().catch(err => {
        const dataDir = process.env.CH_DATA_DIR;
        if (dataDir) {
            try { fs.appendFileSync(path.join(dataDir, 'daemon-error.log'), String(err.stack || err) + '\n'); } catch (e) {}
        }
        process.exit(1);
    });
}

async function acquireLock(sockPath) {
    return new Promise((resolve, reject) => {
        const server = net.createServer();
        server.once('error', (err) => {
            if (err.code === 'EADDRINUSE') {
                const test = net.createConnection(sockPath);
                test.once('connect', () => {
                    test.destroy();
                    reject(new Error('ALREADY_RUNNING'));
                });
                test.once('error', (e) => {
                    if (e.code === 'ECONNREFUSED' || e.code === 'ENOENT') {
                        try { fs.unlinkSync(sockPath); } catch {}
                        const newServer = net.createServer();
                        newServer.once('error', reject);
                        newServer.listen(sockPath, () => resolve(newServer));
                    } else {
                        reject(e);
                    }
                });
            } else {
                reject(err);
            }
        });
        server.listen(sockPath, () => resolve(server));
    });
}

async function getFreePort() {
    return new Promise((resolve, reject) => {
        const srv = net.createServer();
        srv.listen(0, '127.0.0.1', () => {
            const port = srv.address().port;
            srv.close(() => resolve(port));
        });
        srv.on('error', reject);
    });
}

async function runDaemon() {
    const dataDir = process.env.CH_DATA_DIR;
    const sockPath = process.env.CH_SOCK_PATH;
    if (!dataDir || !sockPath) process.exit(1);

    let server;
    try {
        server = await acquireLock(sockPath);
    } catch (e) {
        if (e.message === 'ALREADY_RUNNING') process.exit(0);
        throw e;
    }

    let chHttpPort = null;
    let chError = null;
    const pendingSockets = new Set();
    let activeClients = 0;
    let drainCheckInterval = null;
    let chProc = null;

    let isCleaningUp = false;
    const cleanup = () => {
        if (isCleaningUp) return;
        isCleaningUp = true;
        if (drainCheckInterval) {
            clearInterval(drainCheckInterval);
            drainCheckInterval = null;
        }
        if (chProc) {
            chProc.kill('SIGTERM');
            setTimeout(() => {
                try { process.kill(chProc.pid, 'SIGKILL'); } catch (e) {}
            }, 3000).unref();
        }
        try { fs.unlinkSync(sockPath); } catch (e) {}
        process.exit(0);
    };

    process.on('SIGINT', cleanup);
    process.on('SIGTERM', cleanup);
    process.on('uncaughtException', (err) => {
        try { fs.appendFileSync(path.join(dataDir, 'daemon-error.log'), String(err.stack || err) + '\n'); } catch (e) {}
        cleanup();
    });

    function getActiveUsage() {
        return new Promise((resolve) => {
            const req = http.get(`http://127.0.0.1:${chHttpPort}/?query=SELECT+sum(value)+FROM+system.metrics+WHERE+metric+IN+('HTTPConnection','TCPConnection','Query')`, (res) => {
                let data = '';
                res.on('data', chunk => data += chunk);
                res.on('end', () => resolve(parseInt(data.trim()) || 0));
            });
            req.on('error', () => resolve(999));
            req.setTimeout(1000, () => { req.destroy(); resolve(999); });
        });
    }

    function stopDraining() {
        if (drainCheckInterval) {
            clearInterval(drainCheckInterval);
            drainCheckInterval = null;
        }
    }

    function startDraining() {
        if (drainCheckInterval) return;
        if (!chHttpPort) {
            setTimeout(cleanup, 3000);
            return;
        }
        const drainStart = Date.now();
        let quietSince = null;

        drainCheckInterval = setInterval(async () => {
            if (isCleaningUp) return;

            if (Date.now() - drainStart > 60000) { stopDraining(); cleanup(); return; }

            const usage = await getActiveUsage();
            try { fs.appendFileSync(path.join(process.env.CH_DATA_DIR, 'daemon.log'), `Drain check: usage=${usage}\n`); } catch(e){}

            if (usage > 2) {
                quietSince = null;
            } else if (!quietSince) {
                quietSince = Date.now();
            } else if (Date.now() - quietSince >= 3000) {
                try { fs.appendFileSync(path.join(process.env.CH_DATA_DIR, 'daemon.log'), `Shutting down due to quietSince >= 3000\n`); } catch(e){}
                stopDraining();
                cleanup();
            }
        }, 500);
    }

    server.on('connection', (socket) => {
        activeClients++;
        stopDraining();

        if (chHttpPort) {
            socket.write(JSON.stringify({ port: chHttpPort }) + '\n');
        } else if (chError) {
            socket.write(JSON.stringify({ error: chError }) + '\n');
            socket.destroy();
        } else {
            pendingSockets.add(socket);
        }

        socket.on('close', () => {
            activeClients--;
            pendingSockets.delete(socket);
            if (activeClients <= 0) {
                startDraining();
            }
        });
        socket.on('error', () => {});
    });

    try {
        let binPath = process.env.CLICKHOUSE_BINARY;
        if (!binPath) {
            const binDir = path.join(os.homedir(), '.shared-clickhouse-bin');
            binPath = path.join(binDir, 'clickhouse');
            if (!fs.existsSync(binPath)) {
                fs.mkdirSync(binDir, { recursive: true });
                const tmpDir = path.join(binDir, 'tmp-' + crypto.randomBytes(4).toString('hex'));
                fs.mkdirSync(tmpDir, { recursive: true });
                try {
                    execSync('curl -sL https://clickhouse.com/ | sh', { cwd: tmpDir, stdio: 'ignore' });
                    const downloaded = path.join(tmpDir, 'clickhouse');
                    if (!fs.existsSync(downloaded)) {
                        throw new Error('Curl script executed but binary was not found.');
                    }
                    fs.renameSync(downloaded, binPath);
                } catch (e) {
                    throw new Error('Failed to download official ClickHouse binary.');
                } finally {
                    fs.rmSync(tmpDir, { recursive: true, force: true });
                }
            }
        }

        const pidFile = path.join(dataDir, 'clickhouse.pid');
        if (fs.existsSync(pidFile)) {
            try {
                const oldPid = parseInt(fs.readFileSync(pidFile, 'utf8'), 10);
                if (oldPid) {
                    process.kill(oldPid, 'SIGTERM');
                    await new Promise(r => setTimeout(r, 500));
                    process.kill(oldPid, 'SIGKILL');
                }
            } catch (e) {}
        }

        const httpPort = await getFreePort();
        const tcpPort = await getFreePort();

        fs.mkdirSync(path.join(dataDir, 'data'), { recursive: true });
        fs.mkdirSync(path.join(dataDir, 'tmp'), { recursive: true });
        fs.mkdirSync(path.join(dataDir, 'user_files'), { recursive: true });

        const configXml = `
<clickhouse>
    <logger><level>none</level><console>false</console></logger>
    <http_port>${httpPort}</http_port>
    <tcp_port>${tcpPort}</tcp_port>
    <listen_host>127.0.0.1</listen_host>
    <path>${dataDir}/data/</path>
    <tmp_path>${dataDir}/tmp/</tmp_path>
    <user_files_path>${dataDir}/user_files/</user_files_path>
    <users_config>${dataDir}/users.xml</users_config>
    <mark_cache_size>268435456</mark_cache_size>
</clickhouse>`.trim();

        const usersXml = `
<clickhouse>
    <profiles><default/></profiles>
    <users><default><password></password><networks><ip>127.0.0.1</ip><ip>::1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users>
    <quotas><default/></quotas>
</clickhouse>`.trim();

        fs.writeFileSync(path.join(dataDir, 'config.xml'), configXml);
        fs.writeFileSync(path.join(dataDir, 'users.xml'), usersXml);

        chProc = spawn(binPath, ['server', '--config-file', path.join(dataDir, 'config.xml')], {
            stdio: 'ignore'
        });
        if (chProc.pid) fs.writeFileSync(pidFile, chProc.pid.toString());

        chProc.on('exit', (code) => {
            if (!chHttpPort) {
                chError = `ClickHouse exited prematurely with code ${code}`;
                for (const s of pendingSockets) s.write(JSON.stringify({ error: chError }) + '\n');
                pendingSockets.clear();
            }
            cleanup();
        });

        let ready = false;
        for (let i = 0; i < 300; i++) {
            ready = await new Promise((resolve) => {
                const req = http.get(`http://127.0.0.1:${httpPort}/ping`, (res) => {
                    res.resume();
                    resolve(res.statusCode === 200);
                });
                req.on('error', () => resolve(false));
            });
            if (ready) break;
            await new Promise(r => setTimeout(r, 200));
        }

        if (!ready) throw new Error("ClickHouse server failed to start within 60 seconds.");

        chHttpPort = httpPort;
        for (const s of pendingSockets) s.write(JSON.stringify({ port: chHttpPort }) + '\n');
        pendingSockets.clear();

        if (activeClients <= 0) {
            startDraining();
        }

    } catch (err) {
        chError = err.message;
        for (const s of pendingSockets) {
            s.write(JSON.stringify({ error: chError }) + '\n');
            s.destroy();
        }
        pendingSockets.clear();
        setTimeout(cleanup, 100);
    }
}

export async function getClient(options = {}) {
    if (IS_DAEMON) throw new Error('Cannot be called within daemon context');

    const dataDir = path.resolve(options.dataDir || path.join(process.cwd(), '.clickhouse'));
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });

    const hash = crypto.createHash('sha256').update(dataDir).digest('hex').substring(0, 16);
    const sockPath = path.join(os.tmpdir(), `ch-emb-${hash}.sock`);

    let socket;
    let attempt = 0;

    while (attempt < 150) {
        try {
            socket = await new Promise((resolve, reject) => {
                const s = net.createConnection(sockPath);
                s.on('connect', () => resolve(s));
                s.on('error', reject);
            });
            break;
        } catch (err) {
            if (err.code === 'ENOENT' || err.code === 'ECONNREFUSED') {
                if (attempt === 0 || attempt % 10 === 0) {
                    const child = spawn(process.execPath, [__filename], {
                        detached: true,
                        stdio: 'ignore',
                        env: {
                            ...process.env,
                            __SHARED_CLICKHOUSE_DAEMON__: '1',
                            CH_DATA_DIR: dataDir,
                            CH_SOCK_PATH: sockPath
                        }
                    });
                    child.unref();
                }
            } else {
                throw err;
            }
            await new Promise(r => setTimeout(r, 200));
            attempt++;
        }
    }

    if (!socket) throw new Error('Timeout connecting to ClickHouse daemon socket');

    const { port, error } = await new Promise((resolve, reject) => {
        let buf = '';
        socket.on('data', chunk => {
            buf += chunk.toString();
            if (buf.includes('\n')) {
                try { resolve(JSON.parse(buf.trim())); } catch (e) { reject(e); }
            }
        });
        socket.on('error', reject);
        socket.on('close', () => {
            if (!buf.includes('\n')) reject(new Error('Daemon closed socket unexpectedly (did it crash?)'));
        });
    });

    if (error) {
        socket.destroy();
        throw new Error(`Daemon error: ${error}`);
    }

    socket.unref();

    const client = createClient({
        username: 'default',
        password: '',
        database: 'default',
        ...(options.clientOptions || {}),
        url: `http://127.0.0.1:${port}`
    });

    socketRegistry.register(client, socket);

    const wrapper = new Proxy(client, {
        get(target, prop) {
            if (prop === 'close') {
                return async () => {
                    if (!socket.destroyed) socket.destroy();
                    return target.close();
                };
            }
            if (typeof target[prop] === 'function') {
                return target[prop].bind(target);
            }
            return target[prop];
        }
    });

    socketRegistry.register(wrapper, socket);
    return wrapper;
}

export * from '@clickhouse/client';
