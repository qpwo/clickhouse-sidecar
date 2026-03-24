import { getClient } from 'clickhouse-sidecar';
import fs from 'node:fs';
import path from 'node:path';

async function run() {
    console.log("=== PoC 1: False Positive (Zombie Daemon) ===");

    let pid;
    console.log("1. Acquiring client via getClient()...");
    await (async () => {
        const db = await getClient({ dataDir: './poc-db-1' });
        const pidFile = path.resolve('./poc-db-1/clickhouse.pid');
        pid = parseInt(fs.readFileSync(pidFile, 'utf8'), 10);
        console.log(`   ClickHouse spawned with PID: ${pid}`);
        console.log("2. Dropping JS reference to the client without calling close()...");
    })();

    // Explicitly force GC outside of the function scope
    if (global.gc) {
        global.gc();
        await new Promise(r => setTimeout(r, 100));
        global.gc(); // twice for safety
    }

    console.log("3. Simulating an app that stays alive doing other things... waiting 5 seconds.");
    // The daemon would normally shut down after 3 seconds if 0 clients.
    // We wait 5 seconds to prove the daemon still thinks there's an active client.
    await new Promise(r => setTimeout(r, 5000));

    try {
        process.kill(pid, 0);
        console.log(`   FAIL: ClickHouse PID ${pid} is still running!`);
        console.log(`   -> The daemon's socket lease is held open purely by the Node process OS-level fd,`);
        console.log(`   -> even though the application has logically "forgotten" about ClickHouse.`);
    } catch (e) {
        console.log(`   SUCCESS: ClickHouse PID ${pid} stopped.`);
    }
}

run();
