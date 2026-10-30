import { DomainError } from './common.js';
import type { Store } from './database.js';

// One process serves the shared public prototype. No per-visitor maps or queued
// requests: an active action owns the slot until its promise settles, even if
// the browser disconnects. Existing PostgreSQL worker locks remain in place.
export const PUBLIC_DEMO_LIMITS = {
  mutationBurst: 8, mutationsPerMinute: 20,
  resetBurst: 2, resetsPerMinute: 2,
  rows: 5_000, actionRowReserve: 64, workerBatch: 50,
} as const;

export class DemoRequestError extends DomainError {
  constructor(status: number, message: string, public readonly retryAfterSeconds: number) { super(status, message); }
}

class Budget {
  private tokens: number;
  private updatedAt: number;
  constructor(private readonly capacity: number, private readonly perMinute: number, private readonly clock: () => number) {
    this.tokens = capacity;
    this.updatedAt = clock();
  }
  take() {
    const timestamp = this.clock();
    this.tokens = Math.min(this.capacity, this.tokens + Math.max(0, timestamp - this.updatedAt) * this.perMinute / 60_000);
    this.updatedAt = timestamp;
    if (this.tokens < 1) {
      const seconds = Math.max(1, Math.ceil((1 - this.tokens) * 60 / this.perMinute));
      throw new DemoRequestError(429, `The shared demo is receiving too many actions. Please retry in ${seconds} seconds.`, seconds);
    }
    this.tokens--;
  }
}

export class PublicDemoGuard {
  private active = false;
  private readonly mutations: Budget;
  private readonly resets: Budget;
  constructor(private readonly enabled: boolean, private readonly store: Pick<Store, 'get'>, clock: () => number = Date.now) {
    this.mutations = new Budget(PUBLIC_DEMO_LIMITS.mutationBurst, PUBLIC_DEMO_LIMITS.mutationsPerMinute, clock);
    this.resets = new Budget(PUBLIC_DEMO_LIMITS.resetBurst, PUBLIC_DEMO_LIMITS.resetsPerMinute, clock);
  }
  async run<T>(work: () => Promise<T> | T, reset = false): Promise<T> {
    if (!this.enabled) return work();
    if (this.active) throw new DemoRequestError(409, 'Another shared demo action is in progress. Please retry shortly.', 1);
    (reset ? this.resets : this.mutations).take();
    this.active = true;
    try {
      if (!reset) {
        // Count every growing business/audit/transport table, reserving future
        // inbox rows and three completion rows for each unfinished event.
        // A public worker handles at most 50 events; 64 spare rows also cover
        // a three-reading scenario or failed-worker bookkeeping. Processing
        // already reserved work cannot silently grow past the row budget.
        const row = await this.store.get(`SELECT
          (SELECT COUNT(*) FROM measurements) + (SELECT COUNT(*) FROM events) +
          (SELECT COUNT(*) FROM credits) + (SELECT COUNT(*) FROM decisions) +
          (SELECT COUNT(*) FROM activity) + (SELECT COUNT(*) FROM rules) +
          (SELECT COUNT(*) FROM broker_outbox) + (SELECT COUNT(*) FROM broker_inbox) +
          (SELECT COUNT(*) FROM event_transport_state) +
          (SELECT COUNT(*) FROM broker_outbox WHERE consumed_at IS NULL) +
          3 * (SELECT COUNT(*) FROM events WHERE status!='processed') AS reserved_rows`);
        if (!row || Number(row.reserved_rows) + PUBLIC_DEMO_LIMITS.actionRowReserve > PUBLIC_DEMO_LIMITS.rows)
          throw new DomainError(409, 'The shared demo has reached its data limit. Use Reset demo data to restore the synthetic seed.');
      }
      return await work();
    } finally { this.active = false; }
  }
}

/** Short-lived health result, with one shared in-flight probe on cache misses. */
export function cacheProbe<T>(probe: () => Promise<T>, ttlMs: number, clock: () => number = Date.now) {
  let cached: { value: T; until: number } | undefined, pending: Promise<T> | undefined;
  return (): Promise<T> => {
    if (cached && clock() < cached.until) return Promise.resolve(cached.value);
    if (pending) return pending;
    pending = Promise.resolve().then(probe).then(value => {
      cached = { value, until: clock() + ttlMs };
      return value;
    }).finally(() => { pending = undefined; });
    return pending;
  };
}
