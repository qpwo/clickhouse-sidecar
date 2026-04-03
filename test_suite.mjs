import { getClient } from './node-pkg/index.mjs';
const client = await getClient({ dataDir: 'debug-db-node-tests' });
await client.exec({ query: 'CREATE TABLE IF NOT EXISTS bench_dirty (id UInt32) ENGINE = Memory' });
await client.exec({ query: 'TRUNCATE TABLE bench_dirty' });

console.log("Inserting 500 rows...");
const t0 = Date.now();
for (let i = 0; i < 500; i++) {
    await client.insert({ table: 'bench_dirty', values: [{id: i}], format: 'JSONEachRow' });
}
console.log(`500 async inserts took ${Date.now() - t0}ms`);

console.log("Selecting count...");
let t1 = Date.now();
let res = await client.query({ query: 'SELECT count() as c FROM bench_dirty', format: 'JSONEachRow' });
let json = await res.json();
console.log(`Select count took ${Date.now() - t1}ms, returned ${json[0].c}`);
if (json[0].c != 500) throw new Error("Expected 500");

console.log("Selecting count again (should be fast, no flush)...");
t1 = Date.now();
await client.query({ query: 'SELECT count() as c FROM bench_dirty', format: 'JSONEachRow' });
let dt = Date.now() - t1;
console.log(`Second select took ${dt}ms`);
if (dt > 200) throw new Error("Second select should not flush! Took " + dt + "ms");
console.log("OK Node Proxy!");
process.exit(0);
