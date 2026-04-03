/**
 * Benchmarks the Node sidecar for bulk ingest, flush-read latency, query burst, and SSE delivery.
 */
import http from 'node:http';
import path from 'node:path';
import { getClient } from './node-pkg/index.mjs';

await main();

async function main() {
    var client = await getClient({ dataDir: path.resolve('.bench-sidecar-' + Date.now()) });
    try {
        await client.command({ query: 'DROP TABLE IF EXISTS events' });
        await client.command({
            query: 'CREATE TABLE events (id UInt64, level String, ts DateTime64(3), msg String) ENGINE = MergeTree ORDER BY id'
        });

        var bulk = await benchBulkInsert(client);
        var flush = await benchFlushRead(client);
        var queries = await benchQueryBurst(client);
        var stream = await benchStream(client);

        console.log(JSON.stringify({ bulk, flush, queries, stream }, null, 2));
    } finally {
        await client.close();
    }
}

async function benchBulkInsert(client) {
    var total = 50000;
    var batch = 2000;
    var started = Date.now();
    for (var i = 0; i < total; i += batch) {
        await client.insert({
            table: 'events',
            values: Array.from({ length: batch }, (_, j) => ({
                id: i + j,
                level: j % 17 === 0 ? 'error' : j % 7 === 0 ? 'warn' : 'info',
                ts: new Date(Date.now() + i + j).toISOString(),
                msg: 'bulk_' + (i + j)
            })),
            format: 'JSONEachRow'
        });
    }
    var elapsedMs = Date.now() - started;
    return { total, batch, elapsedMs, rowsPerSec: Math.round(total * 1000 / Math.max(elapsedMs, 1)) };
}

async function benchFlushRead(client) {
    await client.insert({
        table: 'events',
        values: [{ id: 999999001, level: 'flush', ts: new Date().toISOString(), msg: 'flush_1' }],
        format: 'JSONEachRow'
    });
    var t1 = Date.now();
    await scalar(client, 'SELECT count() FROM events');
    var dirtyReadMs = Date.now() - t1;

    var t2 = Date.now();
    await scalar(client, 'SELECT count() FROM events');
    var cleanReadMs = Date.now() - t2;

    return { dirtyReadMs, cleanReadMs };
}

async function benchQueryBurst(client) {
    var total = 300;
    var concurrency = 30;
    var started = Date.now();
    var active = 0;
    var done = 0;
    await new Promise((resolve, reject) => {
        spawn();
        function spawn() {
            while (active < concurrency && done + active < total) {
                active++;
                scalar(client, "SELECT countIf(level='error') FROM events")
                    .then(onDone, reject);
            }
            if (done >= total && active === 0) resolve();
        }
        function onDone() {
            active--;
            done++;
            spawn();
        }
    });
    var elapsedMs = Date.now() - started;
    return { total, concurrency, elapsedMs, qps: Math.round(total * 1000 / Math.max(elapsedMs, 1)) };
}

async function benchStream(client) {
    var stream = await openStream(client.streamUrl('events'));
    await waitForEvent(stream, 'ready', 5000);

    var total = 1000;
    var started = Date.now();
    await client.insert({
        table: 'events',
        values: Array.from({ length: total }, (_, i) => ({
            id: 199999000 + i,
            level: 'stream',
            ts: new Date(Date.now() + i).toISOString(),
            msg: 'stream_' + i
        })),
        format: 'JSONEachRow'
    });
    await waitForRowCount(stream, total, 10000);
    stream.req.destroy();
    var elapsedMs = Date.now() - started;
    return { total, elapsedMs, rowsPerSec: Math.round(total * 1000 / Math.max(elapsedMs, 1)) };
}

async function scalar(client, query) {
    var res = await client.query({ query, format: 'JSONEachRow' });
    var json = await res.json();
    var rows = Array.isArray(json) ? json : json.data;
    var row = rows[0];
    return row[Object.keys(row)[0]];
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

function waitForRowCount(state, count, timeoutMs) {
    return new Promise((resolve, reject) => {
        var rows = state.events.filter(x => x.event === 'row');
        if (rows.length >= count) return resolve(rows.length);
        var timer = setTimeout(() => reject(new Error('row timeout ' + rows.length + '/' + count)), timeoutMs);
        state.waiters.push({ type: 'rows', count, resolve, timer });
    });
}

function flushWaiters(state) {
    var rowCount = state.events.filter(x => x.event === 'row').length;
    state.waiters = state.waiters.filter(waiter => {
        if (waiter.type === 'event') {
            var event = state.events.find(x => x.event === waiter.name);
            if (!event) return true;
            clearTimeout(waiter.timer);
            waiter.resolve(event);
            return false;
        }
        if (rowCount < waiter.count) return true;
        clearTimeout(waiter.timer);
        waiter.resolve(rowCount);
        return false;
    });
}
