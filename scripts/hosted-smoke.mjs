// Run inside a fresh, disposable API container, never against someone else's data.
import assert from 'node:assert/strict';

async function request(path, method = 'GET') {
  const response = await fetch(`http://127.0.0.1:3000/api${path}`, {
    method,
    signal: AbortSignal.timeout(45000),
    ...(method === 'POST' ? { headers: { 'Content-Type': 'application/json' }, body: '{}' } : {}),
  });
  const data = await response.json();
  assert.equal(response.ok, true, `${method} ${path}: ${response.status} ${JSON.stringify(data)}`);
  return data;
}

const health = await request('/health');
assert.equal(health.status, 'ok');
assert.equal(health.database_engine, 'postgresql');
assert.equal(health.broker, 'ok');
const clean = await request('/demo/scenarios/clean_batch', 'POST');
assert.equal(clean.credits_created, 3);
const duplicate = await request('/demo/scenarios/duplicate_delivery', 'POST');
assert.equal(duplicate.credits_created, 1);
assert.equal(duplicate.duplicates_prevented, 2);
const failure = await request('/demo/scenarios/worker_failure', 'POST');
assert.equal(failure.credits_created, 0);
const retried = await request(`/events/${failure.event_ids[0]}/retry`, 'POST');
assert.equal(retried.status, 'processed');
await request('/demo/reset', 'POST');
console.log('Hosted stack passed: PostgreSQL, real Kafka, clean batch, duplicate protection, rollback/retry and reset.');
