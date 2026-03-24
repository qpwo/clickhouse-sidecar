# clickhouse-sidecar

chdb is too weak but full global install is too much!

Start clickhouse proc on the side! Start it when you first connect and stop it when you last disconnect! Never ever leaves orphans behind. Uses random available port. Use clickhouse like its nineteen ninety sqlite.

## node.js

npm install clickhouse-sidecar @clickhouse/client

```ts
import { getClient } from 'clickhouse-sidecar';

async function main() {
    const db = await getClient({ dataDir: './local-db' });
    await db.command({ query: 'CREATE TABLE IF NOT EXISTS events (id UInt32) ENGINE = MergeTree ORDER BY id' });
    await db.insert({ table: 'events', values: [{ id: 1 }], format: 'JSONEachRow' });
    const result = await db.query({ query: 'SELECT * FROM events' });
    console.log(await result.json());
    await db.close();
}

main().catch(console.error);
```
