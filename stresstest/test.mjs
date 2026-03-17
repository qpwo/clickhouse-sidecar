import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

async function main() {
    const procs = [];
    console.log("Spawning workers...");
    for (let i = 0; i < 5; i++) {
        const p = spawn('node', [path.join(__dirname, 'worker.mjs'), i.toString()], { stdio: 'inherit' });
        procs.push(new Promise((resolve) => p.on('exit', resolve)));
    }
    await Promise.all(procs);
    console.log("All workers finished. Daemon should shut down shortly after.");
}

main();
