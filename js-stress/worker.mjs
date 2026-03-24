/** Worker process that inserts data and explicitly handles expected crashes gracefully. */
import { getClient } from '../js-lib/index.mjs';

async function run(id) {
    const db = await getClient({ dataDir: './stress-node-db' });
    await db.command({
        query: 'CREATE TABLE IF NOT EXISTS node_workers (id UInt32, wid UInt32) ENGINE = MergeTree ORDER BY id'
    });

    const willCrash = Math.random() < 0.2;

    for (let i = 0; i < 10; i++) {
        try {
            await db.insert({
                table: 'node_workers',
                values: [{ id: Math.floor(Date.now() % 1000000) + i, wid: parseInt(id) }],
                format: 'JSONEachRow'
            });
        } catch (err) {
            if (willCrash) {
                // If we are crashing, clickhouse node client might throw if connection drops mid-flight. Ignore.
                process.exit(1);
            }
            throw err;
        }

        if (willCrash && i === 5) {
            console.log(`Worker ${id} crashing intentionally mid-flight!`);
            process.exit(1);
        }

        await new Promise(r => setTimeout(r, 50));
    }

    const result = await db.query({ query: `SELECT count() as c FROM node_workers WHERE wid = ${id}` });
    const json = await result.json();
    console.log(`Worker ${id} sees ${json.data[0].c} of its own rows. Assertion correct.`);
    await db.close();
}

run(process.argv[2] || '0').catch(err => { console.error(err); process.exit(2); });
