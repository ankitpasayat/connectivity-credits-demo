import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { createApp } from '../dist/app.js';
import { readConfig } from '../dist/config.js';

export async function fixture(test, overrides = {}) {
  const folder = mkdtempSync(join(tmpdir(), 'connectivity-node-test-'));
  const config = { ...readConfig(), transport: 'local', databaseSchema: 'node_test_' + randomUUID().replaceAll('-', ''),
    topic: 'connectivity-node-test-' + randomUUID().replaceAll('-', ''), timeoutMs: 10_000, ...overrides };
  const { app, runtime } = await createApp(config);
  const server = await new Promise(resolve => { const value = app.listen(0, '127.0.0.1', () => resolve(value)); });
  const base = 'http://127.0.0.1:' + server.address().port;
  test.after(async () => {
    await new Promise(resolve => server.close(resolve));
    try { await runtime.store.exec(`DROP SCHEMA "${config.databaseSchema}" CASCADE`); }
    finally { await runtime.close(); rmSync(folder, { recursive: true, force: true }); }
  });
  async function request(path, body, method = body === undefined ? 'GET' : 'POST') {
    const response = await fetch(base + path, { method, headers: { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body) });
    return { status: response.status, body: await response.json(), response };
  }
  return { runtime, config, base, request, folder };
}
export function payload(external_id = 'first', overrides = {}) {
  return { source: 'node-tests', external_id, school_id: 'school-ke-01', provider_id: 'provider-horizon',
    period: '2020-01-01', download_mbps: 42, upload_mbps: 12, latency_ms: 25, uptime_pct: 99.5,
    verification_status: 'verified', ...overrides };
}
export const businessTables = ['measurements', 'credits', 'decisions', 'events', 'rules', 'broker_outbox', 'broker_inbox', 'event_transport_state'];
export const snapshot = async store => Object.fromEntries(await Promise.all(businessTables.map(async table => [table, await store.all(`SELECT * FROM ${table} ORDER BY 1`)])));
