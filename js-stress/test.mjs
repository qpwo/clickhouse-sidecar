/** Rigorous stress test spawning 20 Node workers, some of which will crash. */
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

async function main() {
    const procs = [];
    console.log("Spawning 20 Node workers. Some will crash intentionally.");
    for (let i = 0; i < 20; i++) {
        const p = spawn('node', [path.join(__dirname, 'worker.mjs'), i.toString()], { stdio: 'inherit' });
        procs.push(new Promise((resolve) => p.on('exit', resolve)));
    }
    const codes = await Promise.all(procs);
    if (codes.some(c => c !== 0 && c !== 1)) {
        console.error("Some workers failed unexpectedly!");
        process.exit(1);
    }
    console.log("All Node workers finished. Daemon should shut down shortly after.");
}

main().catch(err => { console.error(err); process.exit(1); });
