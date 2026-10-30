import { AsyncLocalStorage } from 'node:async_hooks';
import { DomainError, sleep } from './common.js';
import type { Store } from './database.js';

const held = new AsyncLocalStorage<Set<Store>>();
/** Dedicated-session advisory locks release on process/connection death.
 * A session is never returned to the pool with its lock still held. */
export async function withWorkerLock<T>(store: Store, _timeoutMs: number, work: () => Promise<T> | T): Promise<T> {
  if (held.getStore()?.has(store)) return work();
  const client = await store.pool.connect(), key = store.config.databaseSchema + ':worker';
  let acquired = false, releaseError: Error | undefined;
  try {
    const deadline = Date.now() + 2000;
    do {
      acquired = (await client.query('SELECT pg_try_advisory_lock(hashtextextended($1,0)) AS acquired', [key])).rows[0].acquired;
      if (acquired) break;
      if (Date.now() >= deadline) throw new DomainError(409, 'Another worker action is in progress. Please retry shortly.');
      await sleep(50);
    } while (true);
    return await held.run(new Set([...(held.getStore() ?? []), store]), work);
  } finally {
    if (acquired) {
      try { await client.query('SELECT pg_advisory_unlock(hashtextextended($1,0))', [key]); }
      catch (error) { releaseError = error instanceof Error ? error : new Error(String(error)); }
    }
    client.release(releaseError);
  }
}
