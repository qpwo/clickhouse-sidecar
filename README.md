# clickhouse-sidecar

embedded clickhouse manager for node.js and python.

the whole point is that it starts with the first proc using it and dies with the last proc using it.
it downloads the binary automatically, binds to available ports, and safely shares one daemon across multiple processes and threads.
it shuts down automatically a few seconds after the last client disconnects or the last query finishes.
it recovers and cleans up if a client crashes to prevent zombie leases.
no security. binds to localhost without auth.

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

## python

pip install clickhouse-sidecar clickhouse-connect

```py
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
