import { getClient } from '../js-lib/index.mjs';
import fs from 'node:fs';
import path from 'node:path';

async function run() {
    console.log("\n=== PoC 2: False Negative (Premature Shutdown) ===");

    console.log("1. Acquiring client to spawn daemon...");
    const db = await getClient({ dataDir: './poc-db-2' });

    const configPath = path.resolve('./poc-db-2/config.xml');
    const configXml = fs.readFileSync(configPath, 'utf8');
    const httpPort = configXml.match(/<http_port>(\d+)<\/http_port>/)[1];

    const pidFile = path.resolve('./poc-db-2/clickhouse.pid');
    const pid = parseInt(fs.readFileSync(pidFile, 'utf8'), 10);
    console.log(`   ClickHouse is running on HTTP port ${httpPort} (PID: ${pid}).`);

    console.log("2. Calling db.close() to release the socket lease.");
    await db.close();
    console.log("   Daemon sees 0 active clients and starts its 3s shutdown timer.");

    console.log("3. Instantly firing a direct HTTP query that takes 6 seconds...");
    const start = Date.now();
    let fetchError = null;

    // Fire the fetch asynchronously
    // Using max_block_size=10 so sleepEachRow(0.1) only asks for 1s per block, bypassing the 3s safeguard
    const queryPromise = fetch(`http://127.0.0.1:${httpPort}/?query=SELECT+sleepEachRow(0.1)+FROM+system.numbers+LIMIT+60+SETTINGS+max_block_size=10`)
        .then(async r => {
            const text = await r.text();
            if (!r.ok) throw new Error(`HTTP ${r.status}: ${text}`);
            return text;
        })
        .catch(e => { fetchError = e; });

    // Check at 4 seconds (after daemon's 3s timer has fired and sent SIGTERM)
    await new Promise(r => setTimeout(r, 4000));
    console.log("4. 4 seconds have passed. The daemon's 3s timer has fired.");
    try {
        process.kill(pid, 0);
        console.log("   ClickHouse PID is still alive (it intercepts SIGTERM and waits).");
    } catch(e) {
        console.log("   ClickHouse PID is GONE.");
    }

    console.log("5. Waiting for the query to finish or fail...");
    await queryPromise;

    const elapsed = Date.now() - start;
    if (fetchError) {
        console.log(`   FAIL: Query threw an error after ${elapsed}ms:\n   ${fetchError.message}`);
        console.log("   -> The daemon prematurely killed ClickHouse, breaking live HTTP clients!");
        process.exit(1);
    } else {
        console.log(`   SUCCESS: Query finished in ${elapsed}ms. (ClickHouse survived SIGTERM)`);
    }
}

run().catch(err => { console.error(err); process.exit(1); });
