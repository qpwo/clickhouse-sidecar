/**
 * Tiny fullstack demo for clickhouse-sidecar.
 * Serves a small web UI, bulk inserts synthetic events, proxies the sidecar SSE stream,
 * exposes stats, and accepts ad hoc writes over HTTP.
 */
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { fileURLToPath } from 'node:url';
import { getClient } from '../node-pkg/index.mjs';

var __dirname = path.dirname(fileURLToPath(import.meta.url));
var indexHtml = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
var client = null;
var server = null;
var nextId = Date.now() * 1000;

await main();

async function main() {
    client = await getClient({ dataDir: path.join(__dirname, 'app-db') });
    await ensureSchema();
    var port = Number(process.env.APP_PORT || '7070');
    server = http.createServer(onRequest);
    setupSignals();
    await new Promise(resolve => server.listen(port, '127.0.0.1', resolve));
    console.log(JSON.stringify({ appUrl: `http://127.0.0.1:${port}`, sidecarUrl: client.sidecarUrl }));
}

function setupSignals() {
    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
}

async function shutdown() {
    try {
        server?.close();
    } catch {}
    try {
        await client?.close();
    } catch (error) {
        console.error(error);
    }
    process.exit(0);
}

async function onRequest(req, res) {
    try {
        var url = new URL(req.url, 'http://127.0.0.1');
        if (req.method === 'GET' && url.pathname === '/ping') return sendText(res, 200, 'Ok.\n');
        if (req.method === 'GET' && url.pathname === '/_app/health') return sendJson(res, 200, {
            ok: true,
            sidecarUrl: client.sidecarUrl,
            sidecarPort: client.sidecarPort
        });
        if (req.method === 'GET' && url.pathname === '/api/stats') return sendJson(res, 200, await getStats());
        if (req.method === 'POST' && url.pathname === '/api/seed') return sendJson(res, 200, await seedEndpoint(url));
        if (req.method === 'POST' && url.pathname === '/api/write') return sendJson(res, 200, await writeEndpoint(req));
        if (req.method === 'GET' && url.pathname === '/api/stream') return proxyStream(req, res);
        if (req.method === 'GET' && url.pathname === '/') return sendHtml(res, indexHtml);
        sendText(res, 404, 'not found\n');
    } catch (error) {
        console.error(error);
        sendText(res, 500, String(error?.stack || error) + '\n');
    }
}

async function ensureSchema() {
    await client.command({
        query: [
            'CREATE TABLE IF NOT EXISTS events (',
            'id UInt64,',
            'level LowCardinality(String),',
            'source LowCardinality(String),',
            'ts DateTime64(3),',
            'msg String',
            ') ENGINE = MergeTree ORDER BY (ts, id)'
        ].join(' ')
    });
}

async function getStats() {
    var totals = await queryRows(
        "SELECT count() AS total, max(id) AS max_id, min(ts) AS min_ts, max(ts) AS max_ts, countIf(level='error') AS error_count, countIf(level='warn') AS warn_count FROM events"
    );
    var levels = await queryRows('SELECT level, count() AS n FROM events GROUP BY level ORDER BY n DESC, level ASC LIMIT 8');
    var sources = await queryRows('SELECT source, count() AS n FROM events GROUP BY source ORDER BY n DESC, source ASC LIMIT 8');
    return { totals: totals[0], levels, sources };
}

async function seedEndpoint(url) {
    var count = Number(url.searchParams.get('count') || '1000');
    var batch = Number(url.searchParams.get('batch') || '1000');
    var rows = makeRows(count, 'seed', 'seed');
    var started = Date.now();
    for (var i = 0; i < rows.length; i += batch) {
        await client.insert({
            table: 'events',
            values: rows.slice(i, i + batch),
            format: 'JSONEachRow'
        });
    }
    var elapsedMs = Date.now() - started;
    return { inserted: rows.length, batch, elapsedMs, rowsPerSec: Math.round(rows.length * 1000 / Math.max(elapsedMs, 1)) };
}

async function writeEndpoint(req) {
    var payload = JSON.parse(await readText(req) || '[]');
    var rows = Array.isArray(payload) ? payload : [payload];
    rows = rows.map(normalizeRow);
    await client.insert({ table: 'events', values: rows, format: 'JSONEachRow' });
    return { inserted: rows.length, firstId: rows[0]?.id ?? null, lastId: rows[rows.length - 1]?.id ?? null };
}

function normalizeRow(row) {
    return {
        id: Number(row.id ?? nextId++),
        level: String(row.level ?? 'info'),
        source: String(row.source ?? 'api'),
        ts: String(row.ts ?? new Date().toISOString()),
        msg: String(row.msg ?? `event_${nextId}`)
    };
}

function makeRows(count, levelPrefix, source) {
    var rows = [];
    for (var i = 0; i < count; i++) {
        rows.push({
            id: nextId++,
            level: pickLevel(i, levelPrefix),
            source,
            ts: new Date(Date.now() + i).toISOString(),
            msg: `event_${nextId}`
        });
    }
    return rows;
}

function pickLevel(i, fallback) {
    if (i % 17 === 0) return 'error';
    if (i % 7 === 0) return 'warn';
    if (i % 5 === 0) return 'debug';
    return fallback || 'info';
}

function proxyStream(req, res) {
    var upstream = http.get(client.streamUrl('events'), streamRes => {
        res.writeHead(streamRes.statusCode || 200, scrubHeaders(streamRes.headers));
        streamRes.pipe(res);
        req.on('close', closeBoth);
        res.on('close', closeBoth);
        function closeBoth() {
            streamRes.destroy();
            res.end();
        }
    });
    upstream.on('error', error => sendText(res, 502, String(error?.stack || error) + '\n'));
}

function scrubHeaders(headers) {
    var out = {};
    for (var [key, value] of Object.entries(headers || {})) {
        if (key.toLowerCase() === 'connection') continue;
        out[key] = value;
    }
    return out;
}

async function queryRows(query) {
    var res = await client.query({ query, format: 'JSONEachRow' });
    return await res.json();
}

function readText(req) {
    return new Promise((resolve, reject) => {
        var chunks = [];
        req.on('data', chunk => chunks.push(Buffer.from(chunk)));
        req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
        req.on('error', reject);
    });
}

function sendHtml(res, text) {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    res.end(text);
}

function sendJson(res, status, value) {
    var body = JSON.stringify(value);
    res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'content-length': Buffer.byteLength(body) });
    res.end(body);
}

function sendText(res, status, text) {
    res.writeHead(status, { 'content-type': 'text/plain; charset=utf-8', 'content-length': Buffer.byteLength(text) });
    res.end(text);
}
