import test from 'node:test';
import assert from 'node:assert/strict';
import { readConfig } from '../dist/config.js';
import { DomainError } from '../dist/common.js';
import { PublicDemoGuard, PUBLIC_DEMO_LIMITS, cacheProbe } from '../dist/public-demo.js';
import { createApp } from '../dist/app.js';
import { Runtime } from '../dist/runtime.js';

const deferred = () => {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};
const emptyStore = { get: async () => ({ reserved_rows: 0 }) };

test('public demo is explicitly opt-in and validates configuration', () => {
  assert.equal(readConfig({}).publicDemo, false);
  assert.equal(readConfig({ CONNECTIVITY_PUBLIC_DEMO: 'false' }).publicDemo, false);
  assert.equal(readConfig({ CONNECTIVITY_PUBLIC_DEMO: 'true' }).publicDemo, true);
  assert.throws(() => readConfig({ CONNECTIVITY_PUBLIC_DEMO: 'yes' }), /must be true or false/);
});

test('public mutations have one slot and release it after work or preflight fails', async () => {
  const guard = new PublicDemoGuard(true, emptyStore), first = deferred();
  const action = guard.run(() => first.promise);
  await assert.rejects(guard.run(() => assert.fail('must not start competing action')), error => error.statusCode === 409);
  first.reject(new Error('worker failed'));
  await assert.rejects(action, /worker failed/);
  assert.equal(await guard.run(() => 'recovered'), 'recovered');
  let fail = true;
  const preflight = new PublicDemoGuard(true, { get: async () => { if (fail) throw new Error('database failed'); return { reserved_rows: 0 }; } });
  await assert.rejects(preflight.run(() => assert.fail()), /database failed/);
  fail = false;
  assert.equal(await preflight.run(() => 'recovered'), 'recovered');
});

test('global mutation budget refills without IP storage; reset has an independent budget', async () => {
  let clock = 0;
  const guard = new PublicDemoGuard(true, emptyStore, () => clock);
  for (let i = 0; i < PUBLIC_DEMO_LIMITS.mutationBurst; i++) await guard.run(() => {});
  await assert.rejects(guard.run(() => assert.fail()), error => error.statusCode === 429 && error.retryAfterSeconds === 3);
  await guard.run(() => {}, true);
  await guard.run(() => {}, true);
  await assert.rejects(guard.run(() => assert.fail(), true), error => error.statusCode === 429 && error.retryAfterSeconds === 30);
  clock += 3_000;
  assert.equal(await guard.run(() => 'refilled'), 'refilled');
  await assert.rejects(guard.run(() => assert.fail()), error => error.statusCode === 429);
  clock += 60_000;
  for (let i = 0; i < PUBLIC_DEMO_LIMITS.mutationBurst; i++) await guard.run(() => {});
  await assert.rejects(guard.run(() => assert.fail()), error => error.statusCode === 429);
});

test('row-budget exhaustion blocks growth but reset bypasses capacity checks', async () => {
  let rows = PUBLIC_DEMO_LIMITS.rows - PUBLIC_DEMO_LIMITS.actionRowReserve;
  const guard = new PublicDemoGuard(true, { get: async () => ({ reserved_rows: rows }) });
  await guard.run(() => { rows++; });
  await assert.rejects(guard.run(() => assert.fail()), error => error.statusCode === 409 && /Reset demo data/.test(error.message));
  await guard.run(() => { rows = 1_000; }, true);
  assert.equal(await guard.run(() => 'available'), 'available');
});

test('local mode keeps concurrent mutations and skips hosted row/rate limits', async () => {
  const guard = new PublicDemoGuard(false, { get: async () => assert.fail('local mode must not count rows') });
  const hold = deferred();
  const results = Array.from({ length: 20 }, () => guard.run(() => hold.promise));
  hold.resolve('local');
  assert.deepEqual(await Promise.all(results), Array(20).fill('local'));
});

test('health probes coalesce, expire, and recover after a rejected probe', async () => {
  let clock = 0, calls = 0, pending = deferred();
  const probe = cacheProbe(() => { calls++; return pending.promise; }, 5_000, () => clock);
  const first = probe(), same = probe();
  pending.resolve({ broker: 'ok' });
  assert.deepEqual(await first, await same);
  await probe();
  assert.equal(calls, 1);
  clock = 5_000;
  pending = deferred();
  const failed = probe();
  pending.reject(new Error('probe failed'));
  await assert.rejects(failed, /probe failed/);
  pending = deferred();
  const recovered = probe();
  pending.resolve({ broker: 'unavailable' });
  assert.deepEqual(await recovered, { broker: 'unavailable' });
  assert.equal(calls, 3);
});

async function httpFixture(t, publicDemo = true) {
  const state = { rows: 1_000, workerLimit: null, resets: 0 };
  const runtime = {
    store: { get: async () => ({ reserved_rows: state.rows }) },
    engine: { ingest: async () => ({ duplicate: false }), updateRule: async () => ({ version: 2 }) },
    runWorker: async limit => { state.workerLimit = limit; return { processed: 0 }; },
    retry: async () => ({ status: 'processed' }),
    scenario: async () => ({ processed: 1 }),
    reset: async () => { state.resets++; state.rows = 1_000; return { status: 'reset' }; },
  };
  t.mock.method(Runtime, 'create', async () => runtime);
  const { app } = await createApp({ ...readConfig({}), publicDemo });
  const server = await new Promise(resolve => { const value = app.listen(0, '127.0.0.1', () => resolve(value)); });
  t.after(() => new Promise(resolve => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}`;
  async function request(path, body = {}, method = 'POST') {
    const response = await fetch(base + path, { method, headers: { 'content-type': 'application/json' },
      body: method === 'GET' ? undefined : JSON.stringify(body) });
    return { status: response.status, headers: response.headers, body: await response.json() };
  }
  return { base, runtime, state, request };
}

test('HTTP health exposes public mode and worker batches are bounded only in public mode', async t => {
  for (const publicDemo of [true, false]) {
    await t.test(String(publicDemo), async t => {
      const { request, state } = await httpFixture(t, publicDemo);
      const health = await request('/api/health', undefined, 'GET');
      assert.equal(health.body.public_demo, publicDemo);
      assert.equal(health.headers.get('cache-control'), 'no-store');
      assert.equal((await request('/api/worker/run', { limit: 500 })).status, 200);
      assert.equal(state.workerLimit, publicDemo ? 50 : 500);
      assert.equal((await request('/api/worker/run', { limit: 0 })).status, 422);
      assert.equal((await request('/api/worker/run')).status, 200);
    });
  }
});

test('every HTTP mutation respects capacity; reads and reset remain available', async t => {
  const { request, state } = await httpFixture(t);
  state.rows = PUBLIC_DEMO_LIMITS.rows;
  for (const [path, method] of [
    ['/api/measurements', 'POST'], ['/api/rules', 'PUT'], ['/api/worker/run', 'POST'],
    ['/api/events/test/retry', 'POST'], ['/api/demo/scenarios/clean_batch', 'POST'],
  ]) {
    const result = await request(path, {}, method);
    assert.equal(result.status, 409);
    assert.match(result.body.detail, /Reset demo data/);
  }
  assert.equal((await request('/api/health', undefined, 'GET')).status, 200);
  assert.equal((await request('/api/demo/reset')).status, 200);
  assert.equal((await request('/api/worker/run')).status, 200);
  assert.equal(state.resets, 1);
});

test('HTTP rate responses include retry hints and cannot consume the reset allowance', async t => {
  const { request } = await httpFixture(t);
  for (let i = 0; i < PUBLIC_DEMO_LIMITS.mutationBurst; i++)
    assert.equal((await request('/api/worker/run')).status, 200);
  const busy = await request('/api/worker/run');
  assert.equal(busy.status, 429);
  assert.match(busy.headers.get('retry-after'), /^[1-3]$/);
  assert.equal((await request('/api/demo/reset')).status, 200);
});

test('disconnecting a browser keeps its mutation slot until work settles; errors free it', async t => {
  const { base, request, runtime } = await httpFixture(t), started = deferred(), work = deferred(), settled = deferred();
  runtime.scenario = async () => {
    started.resolve();
    try { return await work.promise; } finally { settled.resolve(); }
  };
  const abort = new AbortController();
  const abandoned = fetch(base + '/api/demo/scenarios/clean_batch', { method: 'POST', signal: abort.signal }).catch(error => error);
  await started.promise;
  abort.abort();
  assert.equal((await request('/api/demo/reset')).status, 409);
  work.reject(new DomainError(503, 'broker unavailable'));
  await settled.promise;
  await abandoned;
  assert.equal((await request('/api/worker/run')).status, 200);
});
