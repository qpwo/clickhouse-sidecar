import { getClient } from 'leased-clickhouse';

async function main() {
    console.log("--- Premature Shutdown POC ---");
    const db = await getClient({ dataDir: './poc-db' });
    console.log("1. Connected to ClickHouse.");

    console.log("2. Starting massive GROUP BY query (should take >5 seconds)...");

    const queryPromise = db.query({
        query: 'SELECT count() FROM numbers(500000000) GROUP BY number % 10000000'
    }).then(r => r.json());

    // Prevent unhandled rejection crash in Node
    queryPromise.catch(() => {});

    // Give the query 500ms to reach the server and begin executing
    await new Promise(r => setTimeout(r, 500));

    console.log("3. Calling db.close() while query is running in background...");
    const closePromise = db.close().catch(e => console.log("close() error:", e.message));

    try {
        await queryPromise;
        console.log(" Query succeeded! (This means it didn't fail as expected)");
    } catch (e) {
        console.log(" Query failed as expected! The daemon killed ClickHouse from underneath the query.");
        console.log(" Error:", e.message.substring(0, 150) + '...');
    }
}
main();
