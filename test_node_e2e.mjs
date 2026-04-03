/**
 * Hard e2e test for the Node sidecar:
 * read-after-write, hidden clickhouse port, settings, SSE streaming, and clean shutdown.
 */
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { getClient } from './node-pkg/index.mjs';

await main();

async function main() {
    var dataDir = path.resolve('.test-node-e2e-' + Date.now());
    var client = await getClient({ dataDir });
    console.log('sidecar url ' + client.sidecarUrl);
    await assertPing(client.sidecarUrl);
    await client.command({ query: 'DROP TABLE IF EXISTS events' });
    await client.command({ query: 'CREATE TABLE events (id UInt32, level String, ts DateTime64(3)) ENGINE = MergeTree ORDER BY id' });
    await assertSettings(client, dataDir);
    await checkReadAfterWrite(client);
    await checkStreaming(client);
    await checkConcurrentMixedWorkload(client);
    await checkHealth(client.sidecarUrl);
    var state = JSON.parse(fs.readFileSync(path.join(dataDir, 'sidecar-state.json'), 'utf8'));
    await client.close();
    await waitForDead(state.pid, 10000);
    await waitForDead(state.clickhousePid, 10000);
    console.log('node e2e passed');
}

async function assertPing(sidecarUrl) {
    var text = await fetchText(sidecarUrl + '/ping');
    if (text.trim() !== 'Ok.') throw new Error('bad /ping: ' + JSON.stringify(text));
}

async function assertSettings(client, dataDir) {
    var maxThreads = await scalar(client, "SELECT value FROM system.settings WHERE name='max_threads'");
    var waitAsync = await scalar(client, "SELECT value FROM system.settings WHERE name='wait_for_async_insert'");
    var asyncInsert = await scalar(client, "SELECT value FROM system.settings WHERE name='async_insert'");
    if (String(maxThreads) !== '1') throw new Error('max_threads != 1: ' + maxThreads);
    if (String(waitAsync) !== '0') throw new Error('wait_for_async_insert != 0: ' + waitAsync);
    if (String(asyncInsert) !== '1') throw new Error('async_insert != 1: ' + asyncInsert);
    var config = fs.readFileSync(path.join(dataDir, 'config.xml'), 'utf8');
    var users = fs.readFileSync(path.join(dataDir, 'users.xml'), 'utf8');
    if (!config.includes('<max_server_memory_usage>1073741824</max_server_memory_usage>')) throw new Error('missing max_server_memory_usage');
    if (config.includes('<tcp_port>')) throw new Error('tcp_port should not be enabled');
    if (!users.includes('<max_memory_usage>1073741824</max_memory_usage>')) throw new Error('missing max_memory_usage');
}

async function checkReadAfterWrite(client) {
    for (var i = 0; i < 60; i++) {
        await client.insert({
            table: 'events',
            values: [{ id: i, level: i % 2 ? 'warn' : 'info', ts: new Date().toISOString() }],
            format: 'JSONEachRow'
        });
        if ((i + 1) % 10) continue;
        var t1 = Date.now();
        var n1 = Number(await scalar(client, 'SELECT count() FROM events'));
        var dt1 = Date.now() - t1;
        var t2 = Date.now();
        var n2 = Number(await scalar(client, 'SELECT count() FROM events'));
        var dt2 = Date.now() - t2;
        console.log(`after ${i + 1} inserts count=${n1} first_read_ms=${dt1} second_read_ms=${dt2}`);
        if (n1 !== i + 1 || n2 !== i + 1) throw new Error('read-after-write failed at ' + (i + 1));
        if (dt2 > 400) throw new Error('second read should be cheap, got ' + dt2 + 'ms');
    }
}

async function checkStreaming(client) {
    var stream = await openStream(client.streamUrl('events'));
    var start = 1000;
    await client.insert({
        table: 'events',
        values: Array.from({ length: 20 }, (_, i) => ({ id: start + i, level: 'stream', ts: new Date().toISOString() })),
        format: 'JSONEachRow'
    });
    var rows = await waitForRows(stream, 20, 5000);
    var ids = rows.map(row => row.id).sort((a, b) => a - b);
    if (ids[0] !== start || ids[ids.length - 1] !== start + 19) throw new Error('stream ids wrong: ' + JSON.stringify(ids));
    stream.req.destroy();
    console.log('stream rows ok ' + rows.length);
}

async function checkConcurrentMixedWorkload(client) {
    var readers = [];
    var writers = [];
    var lastSeen = 0;
    for (var w = 0; w < 4; w++) writers.push(runWriter(client, w));
    for (var r = 0; r < 4; r++) readers.push(runReader(client, r, updateLastSeen));
    await Promise.all(writers);
    await Promise.all(readers);
    var count = Number(await scalar(client, 'SELECT count() FROM events'));
    console.log('final count ' + count + ' last_seen ' + lastSeen);
    if (count < 60 + 20 + 200) throw new Error('too few rows: ' + count);
    function updateLastSeen(n) {
        if (n < lastSeen) throw new Error('count regressed from ' + lastSeen + ' to ' + n);
        lastSeen = n;
    }
}

async function runWriter(client, wid) {
    for (var i = 0; i < 50; i++) {
        await client.insert({
            table: 'events',
            values: [{ id: 2000 + wid * 100 + i, level: 'w' + wid, ts: new Date().toISOString() }],
            format: 'JSONEachRow'
        });
    }
}

async function runReader(client, rid, onCount) {
    for (var i = 0; i < 20; i++) {
        var n = Number(await scalar(client, 'SELECT count() FROM events'));
        console.log(`reader ${rid} saw ${n}`);
        onCount(n);
    }
}

async function checkHealth(sidecarUrl) {
    var text = await fetchText(sidecarUrl + '/_sidecar/health');
    var json = JSON.parse(text);
    if (json.dirty) throw new Error('sidecar still dirty after final read: ' + text);
    if (!json.ready) throw new Error('sidecar not ready: ' + text);
}

async function scalar(client, query) {
    var res = await client.query({ query, format: 'JSONEachRow' });
    var json = await res.json();
    var rows = Array.isArray(json) ? json : json.data;
    var row = rows[0];
    return row[Object.keys(row)[0]];
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

function openStream(url) {
    return new Promise((resolve, reject) => {
        var req = http.get(url, res => {
            var state = { req, rows: [], waiters: [], buf: '' };
            res.setEncoding('utf8');
            res.on('data', chunk => onData(state, chunk));
            res.on('error', reject);
            resolve(state);
        });
        req.on('error', reject);
    });
}

function onData(state, chunk) {
    state.buf += chunk;
    while (state.buf.includes('\n\n')) {
        var idx = state.buf.indexOf('\n\n');
        var block = state.buf.slice(0, idx);
        state.buf = state.buf.slice(idx + 2);
        if (block.startsWith(':')) continue;
        var eventName = '';
        var data = '';
        for (var line of block.split('\n')) {
            if (line.startsWith('event: ')) eventName = line.slice(7);
            if (line.startsWith('data: ')) data += line.slice(6);
        }
        if (eventName !== 'row' || !data) continue;
        state.rows.push(JSON.parse(data));
        flushWaiters(state);
    }
}

function waitForRows(state, count, timeoutMs) {
    return new Promise((resolve, reject) => {
        if (state.rows.length >= count) return resolve(state.rows.slice(0, count));
        var timer = setTimeout(() => reject(new Error('stream timeout rows=' + state.rows.length)), timeoutMs);
        state.waiters.push({ count, resolve, timer });
    });
}

function flushWaiters(state) {
    state.waiters = state.waiters.filter(waiter => {
        if (state.rows.length < waiter.count) return true;
        clearTimeout(waiter.timer);
        waiter.resolve(state.rows.slice(0, waiter.count));
        return false;
    });
}

async function waitForDead(pid, timeoutMs) {
    var started = Date.now();
    while (Date.now() - started < timeoutMs) {
        try {
            process.kill(pid, 0);
        } catch (error) {
            if (error.code === 'ESRCH') return;
            throw error;
        }
        await sleep(100);
    }
    throw new Error('pid still alive: ' + pid);
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
