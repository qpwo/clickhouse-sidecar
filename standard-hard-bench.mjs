/**
 * standard-hard-bench.mjs
 * heavily benchmarks the sidecar implementations using concurrent inserts and reads.
 * usage: node standard-hard-bench.mjs ./path/to/node-pkg/index.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import net from 'node:net';

const pkgPath = path.resolve(process.argv[2] || thrw('missing pkg path'));
const pkg = await import(pkgPath);
const getClient = pkg.getClient || thrw('no getClient exported');
const createLiveTable = pkg.createLiveTable;

function thrw(msg) {
    throw new Error(msg);
}

function getFreePort() {
    return new Promise(resolve => {
        const s = net.createServer();
        s.listen(0, '127.0.0.1', onListen);
        function onListen() {
            const port = s.address().port;
            s.close(() => resolve(port));
        }
    });
}

await main();

async function main() {
    if (!process.env.PORT0) process.env.PORT0 = String(await getFreePort());
    const dataDir = path.resolve('.bench-db-hard');
    fs.rmSync(dataDir, { recursive: true, force: true });

    console.log('benchmarking ' + pkgPath + ' on port ' + process.env.PORT0);
    const c = await getClient({ dataDir });

    if (typeof createLiveTable === 'function') {
        await createLiveTable(c, 'events', 'ts UInt64, msg String', { orderBy: 'ts', liveRows: 100000 });
    } else {
        await c.command({ query: 'CREATE TABLE events (ts UInt64, msg String) ENGINE = MergeTree ORDER BY ts' });
    }

    await runInsertBench(c);
    await runReadBench(c);

    await c.close();
    console.log('benchmark complete');
}

async function runInsertBench(c) {
    const totalRows = 500000;
    const batchSize = 5000;
    const concurrency = 20;
    console.log('inserting ' + totalRows + ' rows with concurrency ' + concurrency + ' and batch size ' + batchSize + '...');

    let active = 0;
    let inserted = 0;
    let pidx = 0;
    const start = Date.now();

    await new Promise(resolve => {
        function spawn() {
            if (inserted >= totalRows) {
                if (active === 0) resolve();
                return;
            }
            while (active < concurrency && inserted < totalRows) {
                active++;
                inserted += batchSize;
                const chunk = Array.from({ length: batchSize }, (_, i) => ({ ts: pidx++, msg: 'bench_msg_' + Date.now() }));
                c.insert({ table: 'events', values: chunk, format: 'JSONEachRow' }).then(onDone).catch(onError);
            }
        }
        function onDone() {
            active--;
            spawn();
        }
        function onError(err) {
            thrw(err.message);
        }
        spawn();
    });

    const elapsed = (Date.now() - start) / 1000;
    console.log('inserted ' + totalRows + ' rows in ' + elapsed.toFixed(2) + 's (' + (totalRows / elapsed).toFixed(0) + ' rows/sec)');
}

async function runReadBench(c) {
    console.log('benchmarking live table reads (1000 queries, concurrency 50)...');
    const readStart = Date.now();
    let readActive = 0;
    let readDone = 0;
    const readTotal = 1000;

    await new Promise(resolve => {
        function spawnRead() {
            if (readDone >= readTotal) {
                if (readActive === 0) resolve();
                return;
            }
            while (readActive < 50 && readDone + readActive < readTotal) {
                readActive++;
                c.query({ query: 'SELECT max(ts) AS m FROM events_live FORMAT JSONEachRow' }).then(onQuery).catch(onError);
            }
        }
        function onQuery(res) {
            res.json().then(onDone).catch(onError);
        }
        function onDone() {
            readActive--;
            readDone++;
            spawnRead();
        }
        function onError(err) {
            thrw(err.message);
        }
        spawnRead();
    });

    const readElapsed = (Date.now() - readStart) / 1000;
    console.log('completed ' + readTotal + ' live queries in ' + readElapsed.toFixed(2) + 's (' + (readTotal / readElapsed).toFixed(0) + ' qps)');
}
