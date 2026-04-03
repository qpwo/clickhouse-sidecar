/**
 * Hard test for the tiny fullstack demo app:
 * app boot, SSE proxy, bulk seed, manual writes, and stats updates.
 */
import http from 'node:http';
import path from 'node:path';
import net from 'node:net';
import { spawn } from 'node:child_process';

await main();

async function main() {
    var port = await getFreePort();
    var proc = spawn(process.execPath, [path.resolve('tiny-fullstack/server.mjs')], {
        env: { ...process.env, APP_PORT: String(port) },
        stdio: ['ignore', 'pipe', 'pipe']
    });
    var logs = '';
    proc.stdout.on('data', chunk => logs += chunk.toString('utf8'));
    proc.stderr.on('data', chunk => logs += chunk.toString('utf8'));
    try {
        await waitForHttp(`http://127.0.0.1:${port}/ping`, 15000);
        var stream = await openStream(`http://127.0.0.1:${port}/api/stream`);
        await waitForEvent(stream, 'ready', 5000);

        var seed = await postJson(`http://127.0.0.1:${port}/api/seed?count=120&batch=40`);
        if (seed.inserted !== 120) throw new Error('bad seed response ' + JSON.stringify(seed));

        var rows = await waitForRows(stream, 120, 10000);
        if (rows.length !== 120) throw new Error('expected 120 rows, got ' + rows.length);

        var manual = await postJson(`http://127.0.0.1:${port}/api/write`, [
            { level: 'manual', source: 'test', msg: 'manual_a' },
            { level: 'manual', source: 'test', msg: 'manual_b' },
            { level: 'manual', source: 'test', msg: 'manual_c' }
        ]);
        if (manual.inserted !== 3) throw new Error('bad write response ' + JSON.stringify(manual));

        var rows2 = await waitForRows(stream, 123, 5000);
        var stats = await getJson(`http://127.0.0.1:${port}/api/stats`);
        if (Number(stats.totals.total) < 123) throw new Error('stats total too small ' + JSON.stringify(stats));
        if (!stats.levels.some(x => x.level === 'manual' && Number(x.n) >= 3)) throw new Error('missing manual level ' + JSON.stringify(stats.levels));

        stream.req.destroy();
        proc.kill('SIGTERM');
        await waitForExit(proc, 10000);
        console.log('fullstack test passed rows=' + rows2.length + ' total=' + stats.totals.total);
    } catch (error) {
        proc.kill('SIGKILL');
        await waitForExit(proc, 3000).catch(() => {});
        throw new Error(String(error?.stack || error) + '\nLOGS:\n' + logs);
    }
}

function getFreePort() {
    return new Promise(resolve => {
        var srv = net.createServer();
        srv.listen(0, '127.0.0.1', () => {
            var port = srv.address().port;
            srv.close(() => resolve(port));
        });
    });
}

function waitForHttp(url, timeoutMs) {
    return retryUntil(timeoutMs, async () => {
        var text = await fetchText(url);
        if (text.trim() !== 'Ok.') throw new Error('bad ping ' + JSON.stringify(text));
    });
}

function waitForExit(proc, timeoutMs) {
    return new Promise((resolve, reject) => {
        var timer = setTimeout(() => reject(new Error('process did not exit')), timeoutMs);
        proc.once('exit', () => {
            clearTimeout(timer);
            resolve();
        });
    });
}

function retryUntil(timeoutMs, fn) {
    return new Promise((resolve, reject) => {
        var started = Date.now();
        run();
        async function run() {
            try {
                await fn();
                resolve();
            } catch (error) {
                if (Date.now() - started >= timeoutMs) reject(error);
                else setTimeout(run, 100);
            }
        }
    });
}

function fetchText(url) {
    return new Promise((resolve, reject) => {
        http.get(url, res => {
            var buf = '';
            res.setEncoding('utf8');
            res.on('data', chunk => buf += chunk);
            res.on('end', () => resolve(buf));
        }).on('error', reject);
    });
}

function getJson(url) {
    return fetchText(url).then(JSON.parse);
}

function postJson(url, body = null) {
    return new Promise((resolve, reject) => {
        var text = body == null ? '' : JSON.stringify(body);
        var req = http.request(url, {
            method: 'POST',
            headers: body == null ? {} : { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text) }
        }, res => {
            var buf = '';
            res.setEncoding('utf8');
            res.on('data', chunk => buf += chunk);
            res.on('end', () => {
                if (res.statusCode !== 200) reject(new Error('http ' + res.statusCode + ' ' + buf));
                else resolve(JSON.parse(buf));
            });
        });
        req.on('error', reject);
        if (text) req.write(text);
        req.end();
    });
}

function openStream(url) {
    return new Promise((resolve, reject) => {
        var req = http.get(url, res => {
            var state = { req, buf: '', events: [], waiters: [] };
            res.setEncoding('utf8');
            res.on('data', chunk => onChunk(state, chunk));
            res.on('error', reject);
            resolve(state);
        });
        req.on('error', reject);
    });
}

function onChunk(state, chunk) {
    state.buf += chunk;
    while (state.buf.includes('\n\n')) {
        var i = state.buf.indexOf('\n\n');
        var block = state.buf.slice(0, i);
        state.buf = state.buf.slice(i + 2);
        if (block.startsWith(':')) continue;
        var eventName = '';
        var data = '';
        for (var line of block.split('\n')) {
            if (line.startsWith('event: ')) eventName = line.slice(7);
            if (line.startsWith('data: ')) data += line.slice(6);
        }
        if (!eventName) continue;
        state.events.push({ event: eventName, data });
        flushWaiters(state);
    }
}

function waitForEvent(state, name, timeoutMs) {
    return new Promise((resolve, reject) => {
        var existing = state.events.find(x => x.event === name);
        if (existing) return resolve(existing);
        var timer = setTimeout(() => reject(new Error('event timeout ' + name)), timeoutMs);
        state.waiters.push({ type: 'event', name, resolve, timer });
    });
}

function waitForRows(state, count, timeoutMs) {
    return new Promise((resolve, reject) => {
        var rows = state.events.filter(x => x.event === 'row').map(x => JSON.parse(x.data));
        if (rows.length >= count) return resolve(rows.slice(0, count));
        var timer = setTimeout(() => reject(new Error('row timeout ' + rows.length + '/' + count)), timeoutMs);
        state.waiters.push({ type: 'rows', count, resolve, timer });
    });
}

function flushWaiters(state) {
    var rows = state.events.filter(x => x.event === 'row').map(x => JSON.parse(x.data));
    state.waiters = state.waiters.filter(waiter => {
        if (waiter.type === 'event') {
            var event = state.events.find(x => x.event === waiter.name);
            if (!event) return true;
            clearTimeout(waiter.timer);
            waiter.resolve(event);
            return false;
        }
        if (rows.length < waiter.count) return true;
        clearTimeout(waiter.timer);
        waiter.resolve(rows.slice(0, waiter.count));
        return false;
    });
}
