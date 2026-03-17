import { getClient } from 'leased-clickhouse';

async function run(id) {
    console.log(`Worker ${id} starting...`);
    const db = await getClient({ dataDir: './stress-db' });
    await db.exec({
        query: 'CREATE TABLE IF NOT EXISTS workers (id UInt32, val String) ENGINE = MergeTree ORDER BY id'
    });
    await db.insert({
        table: 'workers',
        values: [{ id: parseInt(id), val: `worker-${id}` }],
        format: 'JSONEachRow'
    });
    const result = await db.query({ query: 'SELECT count() as c FROM workers' });
    const json = await result.json();
    console.log(`Worker ${id} sees ${json.data[0].c} rows.`);
}

run(process.argv[2] || '0').catch(console.error);
