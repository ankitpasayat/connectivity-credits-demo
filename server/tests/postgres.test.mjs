import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { join } from 'node:path';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { fixture, payload } from './helpers.mjs';
import { SCHEMA } from '../dist/schema.js';
import { Runtime } from '../dist/runtime.js';
import { Store } from '../dist/database.js';
import { importSqlite, postgresSnapshot, readSqliteSnapshot, fingerprint, IMPORT_TABLES } from '../dist/import-sqlite.js';

test('real PostgreSQL handles overlapping duplicate/conflicting ingestion and unique concurrent policy versions', async t => {
  const { runtime, request } = await fixture(t);
  assert.match((await runtime.store.get('SELECT version() AS version')).version, /PostgreSQL/);
  assert.equal((await request('/api/health')).body.database_engine, 'postgresql');
  const outcomes = await Promise.all(Array.from({ length: 20 }, (_, index) => request('/api/measurements', payload('concurrent-conflict', { download_mbps: index % 2 ? 45 : 42 }))));
  assert.equal(outcomes.filter(response => response.status === 201).length, 1);
  assert.equal(outcomes.filter(response => response.status === 200).length, 9);
  assert.equal(outcomes.filter(response => response.status === 409).length, 10);
  assert.equal((await runtime.store.get("SELECT COUNT(*) AS n FROM measurements WHERE source='node-tests' AND external_id='concurrent-conflict'")).n, 1);
  const { version, updated_at, ...rule } = (await request('/api/rules')).body;
  const policies = await Promise.all(Array.from({ length: 12 }, (_, index) => request('/api/rules', { ...rule, credits_per_period: 10 + index }, 'PUT')));
  assert(policies.every(response => response.status === 200));
  assert.deepEqual(policies.map(response => response.body.version).sort((a,b) => a-b), Array.from({ length: 12 }, (_, index) => index + 2));
  assert.equal((await request('/api/rules')).body.version, 13);
});

test('explicit read-only SQLite import preserves all records exactly and refuses overwrite atomically', async t => {
  const { runtime, config, folder } = await fixture(t);
  const created = (await runtime.engine.ingest(payload('pre-migration-python-json'))).measurement;
  await runtime.process([(await runtime.store.get('SELECT id FROM events WHERE measurement_id=?', created.id)).id]);
  const snapshot = await postgresSnapshot(runtime.store), path = join(folder, 'source.sqlite3');
  const sqlite = new DatabaseSync(path); sqlite.exec(SCHEMA);
  for (const table of IMPORT_TABLES) for (const row of snapshot[table]) {
    const columns = Object.keys(row);
    sqlite.prepare(`INSERT INTO "${table}"(${columns.map(column => '"'+column+'"').join(',')}) VALUES(${columns.map(() => '?').join(',')})`).run(...columns.map(column => row[column]));
  }
  const pythonPayload = JSON.stringify(payload('pre-migration-python-json')).replace('"download_mbps":42', '"download_mbps":42.0').replaceAll(':', ': ').replaceAll(',', ', ');
  sqlite.prepare('UPDATE measurements SET payload_json=? WHERE id=?').run(pythonPayload, created.id); sqlite.close();
  const sourceHash = createHash('sha256').update(readFileSync(path)).digest('hex');
  const target = { ...config, databaseSchema: config.databaseSchema + '_import' };
  const imported = await importSqlite(target, path);
  assert.equal(imported.status, 'imported'); assert.equal(imported.source_sha256, imported.postgres_sha256);
  assert.equal(imported.counts.measurements, snapshot.measurements.length);
  assert.equal(createHash('sha256').update(readFileSync(path)).digest('hex'), sourceHash);
  const migrated = await Runtime.create(target);
  t.after(async () => { await migrated.store.exec(`DROP SCHEMA "${target.databaseSchema}" CASCADE`); await migrated.close(); });
  assert.equal(fingerprint(await postgresSnapshot(migrated.store)), fingerprint(await readSqliteSnapshot(path)));
  const replay = await migrated.engine.ingest(payload('pre-migration-python-json'));
  assert.equal(replay.duplicate, true); assert.equal(replay.measurement.id, created.id);
  assert.equal((await importSqlite(target, path, true)).status, 'verified');
  assert.equal((await importSqlite(target, path)).status, 'already_imported');
  await migrated.engine.ingest(payload('new-postgres-work', { period: '2020-03-03' }));
  const before = fingerprint(await postgresSnapshot(migrated.store));
  await assert.rejects(importSqlite(target, path), /refusing to overwrite/);
  assert.equal(fingerprint(await postgresSnapshot(migrated.store)), before);
});

test('failed PostgreSQL import rolls back all rows and does not mutate source', async t => {
  const { runtime, config, folder } = await fixture(t);
  const snapshot = await postgresSnapshot(runtime.store), path = join(folder, 'invalid-source.sqlite3');
  const sqlite = new DatabaseSync(path); sqlite.exec(SCHEMA); sqlite.exec('PRAGMA foreign_keys=OFF');
  // Deliberately omit providers while retaining schools: PostgreSQL must reject
  // the FK violation and roll back metadata and every earlier inserted row.
  for (const table of IMPORT_TABLES) if (table !== 'providers') for (const row of snapshot[table]) {
    const columns = Object.keys(row);
    sqlite.prepare(`INSERT INTO "${table}"(${columns.map(column => '"'+column+'"').join(',')}) VALUES(${columns.map(() => '?').join(',')})`).run(...columns.map(column => row[column]));
  }
  sqlite.close();
  const target = { ...config, databaseSchema: config.databaseSchema + '_rollback' };
  await assert.rejects(importSqlite(target, path), /foreign key/);
  const store = new Store(target);
  try { assert(Object.values(await postgresSnapshot(store)).every(rows => rows.length === 0)); }
  finally { await store.exec(`DROP SCHEMA "${target.databaseSchema}" CASCADE`); await store.close(); }
});
