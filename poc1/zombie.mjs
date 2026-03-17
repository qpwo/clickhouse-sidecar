import { getClient } from 'leased-clickhouse';
import fs from 'node:fs';

async function createZombie() {
    const db = await getClient({ dataDir: './poc-db' });
    console.log("2. Connected to ClickHouse. Socket is open.");
    // we return without closing db
}

async function main() {
    console.log("--- Zombie Lease POC ---");
    console.log("1. Simulating a request handler that forgets to call db.close()");

    await createZombie();

    // Keep the process alive like a web server would
    const timer = setInterval(() => {}, 1000);

    console.log("3. Dropped reference to DB client. Waiting 6 seconds...");
    console.log("   (If it worked correctly, DB would shut down after 3s of inactivity)");

    // Force GC to simulate memory pressure causing V8 to clean up
    if (global.gc) {
        console.log("   [Forcing Garbage Collection for POC]");
        global.gc();
    }

    setTimeout(() => {
        const pidFile = './poc-db/clickhouse.pid';
        if (fs.existsSync(pidFile)) {
            const pid = fs.readFileSync(pidFile, 'utf8');
            try {
                process.kill(parseInt(pid), 0);
                console.log(` Daemon is still running (PID ${pid}) due to the zombie lease!`);
            } catch (e) {
                console.log(` Daemon is NOT running.`);
                console.log(` SUCCESS: ClickHouse PID ${pid} stopped.`);
            }
        } else {
            console.log(` PID file not found.`);
        }
        clearInterval(timer);
        process.exit(0);
    }, 6000);
}
main();
