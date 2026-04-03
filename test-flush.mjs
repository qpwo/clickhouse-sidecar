import { getClient } from './node-pkg/index.mjs';
const client = await getClient({ dataDir: 'debug-db-node' });
await client.exec({ query: 'CREATE TABLE IF NOT EXISTS test (a Int32) ENGINE = Memory' });
await client.exec({ query: 'TRUNCATE TABLE test' });
for (let i=0; i<10; i++) {
    await client.insert({ table: 'test', values: [{a: i}], format: 'JSONEachRow' });
}
const res = await client.query({ query: 'SELECT count() as c FROM test', format: 'JSONEachRow' });
const json = await res.json();
console.log("Count:", json[0].c);
if (json[0].c != 10) throw new Error("Expected 10");
console.log("OK node proxy.");
process.exit(0);
