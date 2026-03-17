import { createClient } from '@clickhouse/client';

const fr = new FinalizationRegistry(val => console.log('GC RUN FOR:', val));

async function create() {
    const client = createClient({
        username: 'default',
        url: 'http://127.0.0.1:8123'
    });

    // We register the client itself
    fr.register(client, 'clickhouse-client');
}

await create();
global.gc();
setTimeout(() => console.log('done'), 100);
