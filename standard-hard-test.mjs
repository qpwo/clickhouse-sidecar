/**
 * standard-hard-test.mjs
 * tests e2e compliance of clickhouse live-table sidecar implementations.
 * usage: node standard-hard-test.mjs ./path/to/node-pkg/index.mjs
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

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

await main();

async function main() {
    if (!process.env.PORT0) process.env.PORT0 = String(await getFreePort());
    const dataDir = path.resolve('.test-db-hard');
    fs.rmSync(dataDir, { recursive: true, force: true });

    console.log('testing ' + pkgPath + ' on port ' + process.env.PORT0);
    const c = await getClient({ dataDir });

    if (typeof createLiveTable === 'function') {
        console.log('using explicit createLiveTable function');
        await createLiveTable(c, 'events', 'ts UInt64, msg String', { orderBy: 'ts', liveRows: 100000 });
    } else {
        console.log('relying on proxy auto-magic for live table');
        await c.command({ query: 'CREATE TABLE events (ts UInt64, msg String) ENGINE = MergeTree ORDER BY ts' });
    }

    console.log('inserting 120k rows...');
    const rows = Array.from({ length: 120000 }, (_, i) => ({ ts: i, msg: 'msg_' + i }));
    for (let i = 0; i < rows.length; i += 10000) {
        await c.insert({ table: 'events', values: rows.slice(i, i + 10000), format: 'JSONEachRow' });
    }

    const mainCountObj = await (await c.query({ query: 'SELECT count() AS n FROM events FORMAT JSONEachRow' })).json();
    const mainCount = Number(mainCountObj.data[0].n);
    console.log('main count: ' + mainCount);
    if (mainCount !== 120000) thrw('expected 120000, got ' + mainCount);

    const liveCountObj = await (await c.query({ query: 'SELECT count() AS n FROM events_live FORMAT JSONEachRow' })).json();
    const liveCount = Number(liveCountObj.data[0].n);
    console.log('live count: ' + liveCount);
    if (liveCount === 0) thrw('live table is empty. read-after-write failed.');
    if (liveCount > 110000 && typeof createLiveTable !== 'function') {
        console.log('warning: proxy did not aggressively truncate yet, or it is > 100k');
    }

    console.log('closing client and waiting 4s for daemon to drain...');
    await c.close();
    await sleep(4000);

    console.log('reconnecting to test daemon persistence...');
    const c2 = await getClient({ dataDir });

    console.log('inserting 10 new rows...');
    const newRows = Array.from({ length: 10 }, (_, i) => ({ ts: 999990 + i, msg: 'new_' + i }));
    await c2.insert({ table: 'events', values: newRows, format: 'JSONEachRow' });

    const liveCountObj2 = await (await c2.query({ query: 'SELECT count() AS n FROM events_live FORMAT JSONEachRow' })).json();
    console.log('live count after restart and insert: ' + liveCountObj2.data[0].n);
    if (Number(liveCountObj2.data[0].n) === 0) thrw('live table died or failed to reconstruct after restart');

    await c2.close();
    console.log('standard hard test passed flawlessly');
}
