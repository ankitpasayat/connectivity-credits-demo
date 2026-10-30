import { Runtime } from './runtime.js';
import { readConfig } from './config.js';
import { DomainError, sleep } from './common.js';

const runtime = await Runtime.create(readConfig()), once = process.argv.includes('--once');
let stopping = false;
process.on('SIGINT', () => { stopping = true; }); process.on('SIGTERM', () => { stopping = true; });
try {
  while (!stopping) {
    try {
      const result = await runtime.runWorker(50);
      if (once || Object.values(result).some(Boolean)) console.log(JSON.stringify({ transport: runtime.config.transport, ...result }));
    } catch (error) {
      console.error(JSON.stringify({ error: error instanceof Error ? error.message : String(error), status_code: error instanceof DomainError ? error.statusCode : 500 }));
      if (once) process.exitCode = 1;
    }
    if (once) break;
    await sleep(1000);
  }
} finally { await runtime.close(); }
