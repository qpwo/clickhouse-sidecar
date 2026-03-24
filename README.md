# ClickHouse Sidecar

A zero-config embedded ClickHouse manager for Node.js and Python.

## Guarantees & Assertions
- **Zero Config**: Downloads the binary automatically on first run and binds to available ports.
- **Concurrency Safe**: Safe to be called by multiple concurrent processes or threads. Only one daemon will spawn.
- **Graceful Lifecycle**: Shuts down automatically exactly 3 seconds after the last client disconnects or the last query finishes.
- **Crash Resilient**: Automatically recovers and cleans up if a client application crashes abruptly (zombie lease prevention) or if the database itself is `kill -9`'d.

## Node.js

Requires Node.js >= 18.0.0.

```bash
npm install clickhouse-sidecar @clickhouse/client
```

```typescript
import { getClient } from 'clickhouse-sidecar';

const db = await getClient({ dataDir: './local-db' });
await db.command({ query: 'CREATE TABLE IF NOT EXISTS events (id UInt32) ENGINE = MergeTree ORDER BY id' });
await db.insert({ table: 'events', values: [{ id: 1 }], format: 'JSONEachRow' });
const result = await db.query({ query: 'SELECT * FROM events' });
console.log(await result.json());
await db.close();
```

## Python

Requires Python >= 3.8.

```bash
pip install clickhouse-sidecar clickhouse-connect
```

```python
import clickhouse_sidecar

client = clickhouse_sidecar.get_client("local-db")
client.command("CREATE TABLE IF NOT EXISTS events (id UInt32) ENGINE = MergeTree ORDER BY id")
client.insert("events", [[1]], column_names=["id"])
result = client.query("SELECT * FROM events")
print(result.result_rows)
client.close()
```
