# clickhouse-sidecar

embedded clickhouse manager for node.js and python.

## guarantees
- zero config: downloads the binary automatically and binds to available ports.
- concurrency safe: multiple processes and threads can call it safely. only one daemon will spawn.
- graceful lifecycle: shuts down automatically 3 seconds after the last client disconnects or the last query finishes.
- crash resilient: recovers and cleans up if a client crashes (zombie lease prevention) or if the database process is killed.

## non-guarantees
- not for production clusters.
- no security. binds to localhost without auth.
- no persistence guarantees beyond the local data directory.
- not a service manager. does not start on boot.

## node.js

requires node.js >= 18.0.0.

npm install clickhouse-sidecar @clickhouse/client

```typescript
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

## python

requires python >= 3.8.

pip install clickhouse-sidecar clickhouse-connect

```python
import clickhouse_sidecar

def main():
    client = clickhouse_sidecar.get_client("local-db")
    client.command("CREATE TABLE IF NOT EXISTS events (id UInt32) ENGINE = MergeTree ORDER BY id")
    client.insert("events", [[1]], column_names=["id"])
    result = client.query("SELECT * FROM events")
    print(result.result_rows)
    client.close()

if __name__ == "__main__":
    main()
```
