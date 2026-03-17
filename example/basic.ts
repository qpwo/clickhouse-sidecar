import { getClient } from 'leased-clickhouse'

async function main() {
    console.log("Acquiring ClickHouse...")
    const db = await getClient({ dataDir: './local-db' })

    try {
        await db.command({
            query: 'CREATE TABLE IF NOT EXISTS events (id UInt32, name String) ENGINE = MergeTree ORDER BY id'
        })

        await db.insert({
            table: 'events',
            values: [{ id: 1, name: 'startup' }, { id: 2, name: 'hello' }],
            format: 'JSONEachRow'
        })

        const result = await db.query({ query: 'SELECT * FROM events ORDER BY id' })
        console.log(await result.json())

    } finally {
        await db.close()
    }

    console.log("Done! Exiting naturally...")
}

main()
