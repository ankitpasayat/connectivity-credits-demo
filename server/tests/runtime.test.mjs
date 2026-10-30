import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { fixture, payload, snapshot } from './helpers.mjs';
import { Runtime } from '../dist/runtime.js';
import { evaluate } from '../dist/engine.js';
import { SCHOOLS } from '../dist/fixtures.js';
import { LEGACY_PROVIDERS, LEGACY_SCHOOLS } from '../dist/legacy-labels.js';
test('Express APIs validate, deduplicate concurrently and retain country-blind checks', async (t) => {
    const { request, base, runtime } = await fixture(t);
    const health = await request('/api/health');
    assert.equal(health.body.runtime, 'node');
    assert.equal(health.body.framework, 'express');
    assert.equal(health.body.transport, 'local');
    assert.deepEqual(new Set((await request('/api/schools')).body.map(row => row.country)), new Set(['Kenya', 'Rwanda', 'Nepal']));
    const responses = await Promise.all(Array.from({ length: 12 }, () => request('/api/measurements', payload())));
    assert.equal(responses.filter(row => row.status === 201).length, 1);
    assert.equal(responses.filter(row => row.status === 200 && row.body.duplicate).length, 11);
    const mid = responses[0].body.measurement.id;
    const eid = (await runtime.store.get('SELECT id FROM events WHERE measurement_id=?', mid)).id;
    assert.equal((await request('/api/measurements', payload('first', { download_mbps: 43 }))).status, 409);
    assert.equal((await runtime.process([eid])).issued, 1);
    const decision = (await request('/api/measurements/' + mid)).body;
    for (const country of ['Kenya', 'Rwanda', 'Nepal', 'Any demo label']) {
        assert.deepEqual(evaluate({ ...decision, country }, await runtime.store.get('SELECT * FROM rules WHERE version=1')), {
            eligible: true, checks: decision.checks,
        });
    }
    for (const changes of [{ download_mbps: -1 }, { uptime_pct: 101 }, { verification_status: 'assumed' },
        { school_id: 'unknown' }, { provider_id: 'provider-imizi' }, { period: '2026-02-30' },
        { period: '2999-01-01' }, { download_mbps: '42' }, { download_mbps: true }, { extra: 1 }]) {
        assert.equal((await request('/api/measurements', payload('invalid-' + JSON.stringify(changes), changes))).status, 422);
    }
    for (const value of ['NaN', 'Infinity', '1e400']) {
        const response = await fetch(base + '/api/measurements', { method: 'POST', headers: { 'content-type': 'application/json' },
            body: JSON.stringify(payload('invalid-number')).replace('"download_mbps":42', '"download_mbps":' + value) });
        assert.equal(response.status, 422);
        assert.match(response.headers.get('content-type'), /application\/json/);
    }
    for (const method of ['GET', 'POST', 'PUT', 'DELETE']) {
        assert.equal((await request('/api/not-a-route', undefined, method)).status, 404);
    }
    assert.equal((await request('/api/measurements/missing')).status, 404);
});
test('credit insertion failure rolls back decision, credit and audit; retry issues once', async (t) => {
    const { request, runtime } = await fixture(t);
    const before = await snapshot(runtime.store);
    const scenario = (await request('/api/demo/scenarios/worker_failure', {})).body;
    const mid = scenario.measurement_ids[0], eid = scenario.event_ids[0];
    assert.equal(scenario.credits_created, 0);
    assert.equal(scenario.processed, 0);
    assert.deepEqual((await snapshot(runtime.store)).credits, before.credits);
    assert.equal((await runtime.store.get('SELECT COUNT(*) AS n FROM decisions WHERE measurement_id=?', mid)).n, 0);
    assert.equal((await runtime.store.get('SELECT COUNT(*) AS n FROM activity WHERE id=?', 'activity_completed_' + mid)).n, 0);
    const failed = (await request('/api/events')).body.find(event => event.id === eid);
    assert.equal(failed.status, 'failed');
    assert.match(failed.last_error, /after credit INSERT/);
    assert.equal(failed.attempts, 1);
    for (let i = 0; i < 3; i++) {
        const retry = await request('/api/events/' + eid + '/retry', {});
        assert.equal(retry.body.status, 'processed');
        assert.equal(retry.body.attempts, 2);
    }
    assert.equal((await runtime.store.get('SELECT COUNT(*) AS n FROM credits WHERE measurement_id=?', mid)).n, 1);
    assert.equal((await runtime.store.get('SELECT COUNT(*) AS n FROM decisions WHERE measurement_id=?', mid)).n, 1);
});
test('policy history and daily uniqueness survive a new version and repeated scenarios', async (t) => {
    const { request, runtime } = await fixture(t);
    const first = (await request('/api/measurements', payload())).body.measurement;
    const firstEvent = (await runtime.store.get('SELECT id FROM events WHERE measurement_id=?', first.id)).id;
    await runtime.process([firstEvent]);
    const old = { ...await runtime.store.get('SELECT * FROM decisions WHERE measurement_id=?', first.id) };
    const rule = (await request('/api/rules')).body;
    const { version, updated_at, ...newRule } = rule;
    newRule.min_download_mbps = 40;
    newRule.credits_per_period = 37;
    assert.equal((await request('/api/rules', newRule, 'PUT')).body.version, 2);
    const second = (await request('/api/measurements', payload('same-day-second'))).body.measurement;
    const result = await runtime.process([(await runtime.store.get('SELECT id FROM events WHERE measurement_id=?', second.id)).id]);
    assert.equal(result.issued, 0);
    assert.deepEqual({ ...await runtime.store.get('SELECT * FROM decisions WHERE measurement_id=?', first.id) }, old);
    const credit = await runtime.store.get("SELECT * FROM credits WHERE school_id='school-ke-01' AND period='2020-01-01'");
    assert.equal(credit.rule_version, 1);
    assert.equal(credit.amount, 10);
    for (let i = 0; i < 2; i++) {
        assert.equal((await request('/api/demo/scenarios/clean_batch', {})).body.credits_created, 3);
        assert.equal((await request('/api/demo/scenarios/below_threshold', {})).body.credits_created, 0);
        const replay = (await request('/api/demo/scenarios/duplicate_delivery', {})).body;
        assert.equal(replay.duplicates_prevented, 2);
        assert.equal(replay.credits_created, 1);
    }
});
test('Node restart preserves historical rows and replays Python-formatted numeric JSON semantically', async (t) => {
    const { runtime, config, request } = await fixture(t);
    const created = (await request('/api/measurements', payload('python-formatted'))).body.measurement;
    const eid = (await runtime.store.get('SELECT id FROM events WHERE measurement_id=?', created.id)).id;
    await runtime.process([eid]);
    // Actual Python JSON formatting includes spaces, sorted keys and .0 numbers.
    const originalPayload = Object.fromEntries(Object.entries(payload('python-formatted')).sort(([a], [b]) => a.localeCompare(b)));
    const pythonJson = JSON.stringify(originalPayload).replaceAll('42,', '42.0,').replaceAll('12,', '12.0,').replaceAll(':', ': ').replaceAll(',', ', ');
    await runtime.store.run('UPDATE measurements SET payload_json=? WHERE id=?', pythonJson, created.id);
    const before = await snapshot(runtime.store);
    const secondRuntime = await Runtime.create(config);
    try {
        assert.deepEqual(await snapshot(secondRuntime.store), before);
        const duplicate = await secondRuntime.engine.ingest(payload('python-formatted'));
        assert.equal(duplicate.duplicate, true);
        assert.equal(duplicate.measurement.id, created.id);
        assert.deepEqual(await snapshot(secondRuntime.store), before);
    }
    finally {
        await secondRuntime.close();
    }
});
test('neutral startup migration preserves custom work; resetting deterministically seeds only the demo DB', async (t) => {
    const { runtime, config, request } = await fixture(t);
    const seeded = (await request('/api/measurements')).body;
    const custom = (await request('/api/measurements', payload('custom-pending'))).body.measurement;
    await runtime.store.tx(async () => {
        await runtime.store.exec('ALTER TABLE schools ADD COLUMN latitude DOUBLE PRECISION NOT NULL DEFAULT 0; ALTER TABLE schools ADD COLUMN longitude DOUBLE PRECISION NOT NULL DEFAULT 0');
        for (const [pid, [name, country]] of Object.entries(LEGACY_PROVIDERS))
            await runtime.store.run('UPDATE providers SET name=?,country=? WHERE id=?', name, country, pid);
        for (const [sid, [name, country, region]] of Object.entries(LEGACY_SCHOOLS)) {
            const neutral = SCHOOLS.find(row => row[0] === sid)[1];
            await runtime.store.run('UPDATE activity SET detail=REPLACE(detail,?,?)', neutral, name);
            await runtime.store.run('UPDATE schools SET name=?,country=?,region=? WHERE id=?', name, country, region, sid);
        }
        await runtime.store.run("DELETE FROM metadata WHERE key='synthetic_labels_version'");
        await runtime.store.run("DELETE FROM metadata WHERE key='country_labels_version'");
    });
    const before = await snapshot(runtime.store), migrated = await Runtime.create(config);
    try {
        assert.deepEqual(await snapshot(migrated.store), before);
        assert.equal((await migrated.store.get('SELECT status FROM measurements WHERE id=?', custom.id)).status, 'pending');
        assert(!(await migrated.store.all("SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='schools'")).some(row => ['latitude', 'longitude'].includes(row.column_name)));
    }
    finally {
        await migrated.close();
    }
    assert.equal((await request('/api/demo/reset', {})).body.status, 'reset');
    assert.deepEqual((await request('/api/measurements')).body, seeded);
});
test('PostgreSQL advisory worker lock recovers automatically when owning process dies', async (t) => {
    const { runtime } = await fixture(t);
    const child = spawn(process.execPath, ['--input-type=module', '-e', `import pg from 'pg';const client=new pg.Client({connectionString:process.env.DATABASE_URL});await client.connect();await client.query('SELECT pg_advisory_lock(hashtextextended($1,0))',[process.env.LOCK_KEY]);console.log('locked');setInterval(()=>{},1000);`], {
      cwd: new URL('..', import.meta.url), env: { ...process.env, DATABASE_URL: runtime.config.databaseUrl, LOCK_KEY: runtime.config.databaseSchema + ':worker' }, stdio: ['ignore','pipe','pipe'],
    });
    t.after(() => child.kill('SIGKILL'));
    await once(child.stdout, 'data');
    await assert.rejects(runtime.runWorker(), error => error.statusCode === 409);
    child.kill('SIGKILL'); await once(child, 'exit');
    assert.equal((await runtime.runWorker()).processed, 3);
});
test('oversized and malformed bodies produce useful JSON errors', async (t) => {
    const { base } = await fixture(t);
    for (const [body, status] of [['{', 422], [JSON.stringify({ huge: 'x'.repeat(300000) }), 413]]) {
        const response = await fetch(base + '/api/measurements', { method: 'POST', headers: { 'content-type': 'application/json' }, body });
        assert.equal(response.status, status);
        assert.equal(typeof (await response.json()).detail, 'string');
    }
});
