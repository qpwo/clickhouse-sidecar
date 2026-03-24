/** Nontrivial example of using clickhouse-sidecar with TypeScript */
import { getClient } from 'clickhouse-sidecar';

async function main() {
    console.log("Acquiring ClickHouse sidecar...");
    const db = await getClient({ dataDir: './example-db' });

    try {
        await db.command({
            query: 'CREATE TABLE IF NOT EXISTS user_activity (id String, action String, timestamp DateTime) ENGINE = MergeTree ORDER BY timestamp'
        });

        console.log("Inserting activity records...");
        const records = Array.from({ length: 5000 }).map((_, i) => ({
            id: `usr_${i % 100}`,
            action: i % 2 === 0 ? 'login' : 'purchase',
            timestamp: Math.floor(Date.now() / 1000) - i
        }));

        await db.insert({ table: 'user_activity', values: records, format: 'JSONEachRow' });

        console.log("Querying aggregates...");
        const result = await db.query({
            query: 'SELECT action, count() as cnt FROM user_activity GROUP BY action ORDER BY cnt DESC'
        });
        console.log(await result.json());

    } finally {
        await db.close();
    }
    console.log("Done.");
}

main().catch(err => { console.error(err); process.exit(1); });
