import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { join } from 'node:path';
import { fixture, payload, snapshot } from './helpers.mjs';
import { Runtime } from '../dist/runtime.js';
import { SCHOOLS, PROVIDERS } from '../dist/fixtures.js';
import { V2_SCHOOLS, V2_PROVIDERS } from '../dist/neutral-fixtures-v2.js';
import { SCHEMA } from '../dist/schema.js';
import { importSqlite, IMPORT_TABLES, postgresSnapshot, fingerprint } from '../dist/import-sqlite.js';

async function setFormerCountries(store) {
  await store.tx(async () => {
    for (const [id, , country] of V2_PROVIDERS) await store.run('UPDATE providers SET country=? WHERE id=?', country, id);
    for (const [id, , country] of V2_SCHOOLS) await store.run('UPDATE schools SET country=? WHERE id=?', country, id);
    await store.run("DELETE FROM metadata WHERE key='country_labels_version'");
  });
}

test('country-only startup migration preserves custom work and is repeatably idempotent', async t => {
  const { runtime, config, request } = await fixture(t);
  const issued = (await runtime.engine.ingest(payload('before-country-restore-issued'))).measurement;
  await runtime.process([(await runtime.store.get('SELECT id FROM events WHERE measurement_id=?', issued.id)).id]);
  await runtime.engine.ingest(payload('before-country-restore-pending', { period: '2020-03-03' }));
  await runtime.engine.updateRule({ min_download_mbps: 35, min_upload_mbps: 8, max_latency_ms: 80, min_uptime_pct: 99, credits_per_period: 27 });
  await setFormerCountries(runtime.store);
  const business = await snapshot(runtime.store);
  const activity = await runtime.store.all('SELECT * FROM activity ORDER BY id');
  const metadata = await runtime.store.all('SELECT * FROM metadata ORDER BY key');
  const formerSchools = await runtime.store.all('SELECT * FROM schools ORDER BY id');
  const formerProviders = await runtime.store.all('SELECT * FROM providers ORDER BY id');
  const overviewBefore = (await request('/api/overview')).body;
  const migrated = await Runtime.create(config);
  try {
    assert.deepEqual(await snapshot(migrated.store), business);
    assert.deepEqual(await migrated.store.all('SELECT * FROM activity ORDER BY id'), activity);
    assert.deepEqual(await migrated.store.all("SELECT * FROM metadata WHERE key!='country_labels_version' ORDER BY key"), metadata);
    assert.equal((await migrated.store.get("SELECT value FROM metadata WHERE key='country_labels_version'")).value, '1');
    assert.equal((await migrated.store.get("SELECT value FROM metadata WHERE key='synthetic_labels_version'")).value, '2');
    assert.deepEqual(await migrated.store.all('SELECT * FROM schools ORDER BY id'), formerSchools.map(row => ({ ...row, country: SCHOOLS.find(school => school[0] === row.id)[2] })));
    assert.deepEqual(await migrated.store.all('SELECT * FROM providers ORDER BY id'), formerProviders.map(row => ({ ...row, country: PROVIDERS.find(provider => provider[0] === row.id)[2] })));
    const overviewAfter = (await request('/api/overview')).body;
    assert.deepEqual(overviewAfter.summary, overviewBefore.summary);
    assert.deepEqual(overviewAfter.trends, overviewBefore.trends);
    const restored = { 'Demo country A': 'Kenya', 'Demo country B': 'Rwanda', 'Demo country C': 'Nepal' };
    for (const former of overviewBefore.country_breakdown) {
      assert.deepEqual(overviewAfter.country_breakdown.find(row => row.country === restored[former.country]), { ...former, country: restored[former.country] });
    }
    assert(!(await migrated.store.all("SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='schools'")).some(row => ['latitude', 'longitude'].includes(row.column_name)));
    const beforeRestart = fingerprint(await postgresSnapshot(migrated.store));
    const restarted = await Runtime.create(config);
    try { assert.equal(fingerprint(await postgresSnapshot(restarted.store)), beforeRestart); }
    finally { await restarted.close(); }
  } finally { await migrated.close(); }
});

test('country migration leaves custom labels, names and unknown reference IDs untouched', async t => {
  const { runtime, config } = await fixture(t);
  await setFormerCountries(runtime.store);
  await runtime.store.tx(async () => {
    await runtime.store.run("UPDATE schools SET country='Custom school country',name='Custom school' WHERE id='school-ke-01'");
    await runtime.store.run("UPDATE providers SET country='Custom provider country',name='Custom provider' WHERE id='provider-horizon'");
    await runtime.store.run("INSERT INTO providers(id,name,country) VALUES('user-provider','User provider','Demo country A')");
    await runtime.store.run("INSERT INTO schools(id,name,country,region,provider_id) VALUES('user-school','User school','Demo country A','User region','user-provider')");
  });
  const migrated = await Runtime.create(config);
  try {
    assert.equal((await migrated.store.get("SELECT country FROM schools WHERE id='school-ke-01'")).country, 'Custom school country');
    assert.equal((await migrated.store.get("SELECT name FROM schools WHERE id='school-ke-01'")).name, 'Custom school');
    assert.equal((await migrated.store.get("SELECT country FROM providers WHERE id='provider-horizon'")).country, 'Custom provider country');
    assert.equal((await migrated.store.get("SELECT name FROM providers WHERE id='provider-horizon'")).name, 'Custom provider');
    assert.equal((await migrated.store.get("SELECT country FROM schools WHERE id='user-school'")).country, 'Demo country A');
    assert.equal((await migrated.store.get("SELECT country FROM providers WHERE id='user-provider'")).country, 'Demo country A');
    assert.equal((await migrated.store.get("SELECT country FROM schools WHERE id='school-rw-01'")).country, 'Rwanda');
  } finally { await migrated.close(); }
});

test('identical good and bad readings produce identical eligibility in Kenya, Rwanda and Nepal', async t => {
  const { runtime, request } = await fixture(t);
  for (const eligible of [true, false]) {
    const measurements = [], eventIds = [];
    for (const [index, school] of [SCHOOLS[0], SCHOOLS[4], SCHOOLS[8]].entries()) {
      const measurement = (await runtime.engine.ingest(payload(`country-independent-${eligible}-${index}`, {
        school_id: school[0], provider_id: school[4], period: eligible ? '2020-04-01' : '2020-04-02', download_mbps: eligible ? 42 : 3,
      }))).measurement;
      measurements.push(measurement);
      eventIds.push((await runtime.store.get('SELECT id FROM events WHERE measurement_id=?', measurement.id)).id);
    }
    assert.equal((await runtime.process(eventIds)).issued, eligible ? 3 : 0);
    const decisions = await Promise.all(measurements.map(async row => (await request('/api/measurements/' + row.id)).body));
    assert.deepEqual(new Set(decisions.map(row => row.country)), new Set(['Kenya', 'Rwanda', 'Nepal']));
    for (const row of decisions) {
      assert.deepEqual(row.checks, decisions[0].checks);
      assert.equal(row.status, eligible ? 'eligible' : 'ineligible');
      assert.equal(row.credit_issued, eligible);
      assert.equal(row.rule_version, 1);
    }
  }
});

test('neutral SQLite import remains exact before the independent startup-country migration', async t => {
  const { runtime, config, folder } = await fixture(t);
  await setFormerCountries(runtime.store);
  const source = await postgresSnapshot(runtime.store), path = join(folder, 'former-countries.sqlite3');
  const sqlite = new DatabaseSync(path); sqlite.exec(SCHEMA);
  for (const table of IMPORT_TABLES) for (const row of source[table]) {
    const columns = Object.keys(row);
    sqlite.prepare(`INSERT INTO "${table}"(${columns.map(column => '"' + column + '"').join(',')}) VALUES(${columns.map(() => '?').join(',')})`).run(...columns.map(column => row[column]));
  }
  sqlite.close();
  const target = { ...config, databaseSchema: config.databaseSchema + '_country' };
  const imported = await importSqlite(target, path);
  assert.equal(imported.source_sha256, imported.postgres_sha256);
  assert.equal((await importSqlite(target, path, true)).status, 'verified');
  const migrated = await Runtime.create(target);
  try {
    const actual = await postgresSnapshot(migrated.store);
    for (const table of IMPORT_TABLES.filter(table => !['schools', 'providers', 'metadata'].includes(table))) assert.deepEqual(actual[table].map(row => JSON.stringify(row)).sort(), source[table].map(row => JSON.stringify(row)).sort());
    assert.deepEqual(new Set(actual.schools.map(row => row.country)), new Set(['Kenya', 'Rwanda', 'Nepal']));
    // The verifier still detects any source/target difference. Country migration
    // is an explicit later step, never a reason to weaken exact import checks.
    await assert.rejects(importSqlite(target, path, true), /differs from the SQLite snapshot/);
  } finally { await migrated.store.exec(`DROP SCHEMA "${target.databaseSchema}" CASCADE`); await migrated.close(); }
});
