import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import kafka from '@confluentinc/kafka-javascript';
import { fixture, payload } from './helpers.mjs';
import { connectClient, disconnectClient } from '../dist/kafka.js';
const realBroker = process.env.RUN_KAFKA_TESTS === '1' || Boolean(process.env.CONNECTIVITY_KAFKA_TEST_BOOTSTRAP_SERVERS);
const bootstrap = process.env.CONNECTIVITY_KAFKA_TEST_BOOTSTRAP_SERVERS ?? process.env.CONNECTIVITY_KAFKA_BOOTSTRAP_SERVERS ?? '127.0.0.1:19092';
const consumerConfig = (config, group) => ({ 'metadata.broker.list': config.bootstrap, 'group.id': group,
    'enable.auto.commit': false, 'enable.auto.offset.store': false, log_level: 0 });
async function committed(consumer, topic) {
    return new Promise((resolve, reject) => consumer.committed([{ topic, partition: 0 }], 5000, (error, data) => error ? reject(error) : resolve(data[0].offset)));
}
async function inspectRecord(config, offset) {
    const inspector = new kafka.KafkaConsumer(consumerConfig(config, 'independent-node-inspector-' + randomUUID()), {});
    try {
        await connectClient(inspector, 5000);
        inspector.assign([{ topic: config.topic, partition: 0, offset }]);
        inspector.setDefaultConsumeTimeout(10000);
        const data = await new Promise((resolve, reject) => inspector.consume(1, (error, messages) => error ? reject(error) : resolve(messages)));
        assert.equal(data.length, 1);
        return data[0];
    }
    finally {
        await disconnectClient(inspector);
    }
}
async function rawReplay(config, value) {
    const producer = new kafka.Producer({ 'metadata.broker.list': config.bootstrap, acks: -1, dr_cb: true, log_level: 0 });
    try {
        await connectClient(producer, 5000);
        producer.setPollInterval(20);
        const delivered = new Promise((resolve, reject) => producer.once('delivery-report', (error, report) => error ? reject(error) : resolve(report)));
        producer.produce(config.topic, 0, Buffer.from(value), 'independent-raw-duplicate');
        await new Promise((resolve, reject) => producer.flush(5000, error => error ? reject(error) : resolve()));
        return await delivered;
    }
    finally {
        producer.setPollInterval(0);
        await disconnectClient(producer);
    }
}
test('real Node Kafka: actual wire record and committed offset, rollback, retries, replay and reset', { skip: !realBroker }, async (t) => {
    const { runtime, request, config } = await fixture(t, { transport: 'kafka', bootstrap, timeoutMs: 15000 });
    assert.equal((await request('/api/health')).body.broker, 'ok');
    const beforeCredits = (await request('/api/credits')).body;
    const baselineEvents = (await request('/api/events')).body;
    const failure = await request('/api/demo/scenarios/worker_failure', {});
    assert.equal(failure.status, 200, JSON.stringify(failure.body));
    assert.equal(failure.body.credits_created, 0);
    const mid = failure.body.measurement_ids[0], eid = failure.body.event_ids[0];
    assert.deepEqual((await request('/api/credits')).body, beforeCredits);
    const failed = (await request('/api/events')).body.find(event => event.id === eid);
    assert.equal(failed.status, 'failed');
    assert.match(failed.last_error, /after credit INSERT/);
    assert.equal((await runtime.store.get('SELECT COUNT(*) AS n FROM decisions WHERE measurement_id=?', mid)).n, 0);
    assert.equal((await runtime.store.get('SELECT COUNT(*) AS n FROM activity WHERE id=?', 'activity_completed_' + mid)).n, 0);
    const outbox = await runtime.store.get('SELECT * FROM broker_outbox WHERE event_id=?', eid);
    assert(outbox.published_at && outbox.consumed_at);
    const actual = await inspectRecord(config, outbox.broker_offset);
    assert.equal(actual.offset, outbox.broker_offset);
    assert.deepEqual(JSON.parse(actual.value.toString()), JSON.parse(outbox.payload_json));
    assert.equal(JSON.parse(actual.value.toString()).measurement_id, mid);
    const instance = (await runtime.store.get("SELECT value FROM metadata WHERE key='broker_namespace'")).value;
    const group = new kafka.KafkaConsumer(consumerConfig(config, `${config.group}-${instance}`), {});
    try {
        await connectClient(group, 5000);
        assert(await committed(group, config.topic) > outbox.broker_offset);
    }
    finally {
        await disconnectClient(group);
    }
    // Failed retries and already-processed retries each traverse a fresh broker record.
    for (let i = 0; i < 3; i++) {
        const retry = await request('/api/events/' + eid + '/retry', {});
        assert.equal(retry.status, 200, JSON.stringify(retry.body));
        assert.equal(retry.body.status, 'processed');
        assert.equal(retry.body.attempts, 2);
    }
    assert.equal((await runtime.store.get('SELECT COUNT(*) AS n FROM broker_inbox WHERE event_id=?', eid)).n, 4);
    assert.equal((await runtime.store.get('SELECT COUNT(*) AS n FROM credits WHERE measurement_id=?', mid)).n, 1);
    const replay = await request('/api/demo/scenarios/duplicate_delivery', {});
    assert.equal(replay.status, 200, JSON.stringify(replay.body));
    assert.equal(replay.body.duplicates_prevented, 2);
    assert.equal(replay.body.credits_created, 1);
    assert.equal((await runtime.store.get('SELECT COUNT(*) AS n FROM broker_inbox WHERE event_id=?', replay.body.event_ids[0])).n, 3);
    assert.equal((await request('/api/demo/scenarios/clean_batch', {})).body.credits_created, 3);
    assert.equal((await request('/api/demo/scenarios/below_threshold', {})).body.credits_created, 0);
    const currentEvents = (await request('/api/events')).body;
    for (const event of baselineEvents)
        assert.deepEqual(currentEvents.find(value => value.id === event.id), event);
    // A direct second copy of the SAME wire envelope is consumed without new effects.
    const duplicate = await rawReplay(config, outbox.payload_json);
    assert(duplicate.offset > outbox.broker_offset);
    const newReading = (await request('/api/measurements', payload('after-wire-duplicate', { period: '2020-02-02' }))).body.measurement;
    const newEvent = (await runtime.store.get('SELECT id FROM events WHERE measurement_id=?', newReading.id)).id;
    assert.equal((await runtime.process([newEvent])).issued, 1);
    assert.equal((await runtime.store.get('SELECT COUNT(*) AS n FROM credits WHERE measurement_id=?', mid)).n, 1);
    assert.equal((await runtime.store.get('SELECT COUNT(*) AS n FROM broker_inbox WHERE delivery_id=?', outbox.id)).n, 1);
    // Reset consumes/skips old namespace records without resurrecting old work.
    assert.equal((await request('/api/demo/reset', {})).status, 200);
    assert.notEqual((await runtime.store.get("SELECT value FROM metadata WHERE key='broker_namespace'")).value, instance);
    assert.deepEqual((await request('/api/credits')).body, beforeCredits);
    assert.equal((await request('/api/worker/run', {})).body.processed, 3);
    assert.equal((await runtime.store.get('SELECT COUNT(*) AS n FROM credits WHERE measurement_id=?', mid)).n, 0);
});
test('broker outage returns503 without local effects and real recovery processes the same queued work', { skip: !realBroker }, async (t) => {
    const { runtime, request } = await fixture(t, { transport: 'kafka', bootstrap, timeoutMs: 10000 });
    await runtime.runWorker();
    const created = (await request('/api/measurements', payload('outage-recovery'))).body.measurement;
    const before = (await request('/api/credits')).body;
    runtime.config.bootstrap = '127.0.0.1:1';
    runtime.config.timeoutMs = 1000;
    const outage = await request('/api/worker/run', {});
    assert.equal(outage.status, 503, JSON.stringify(outage.body));
    assert.match(outage.body.detail, /no local fallback/);
    assert.deepEqual((await request('/api/credits')).body, before);
    assert.equal((await request('/api/measurements/' + created.id)).body.status, 'pending');
    assert.equal((await request('/api/health')).body.broker, 'unavailable');
    assert.equal((await runtime.store.get('SELECT published_at FROM broker_outbox o JOIN events e ON e.id=o.event_id WHERE e.measurement_id=?', created.id)).published_at, null);
    runtime.config.bootstrap = bootstrap;
    runtime.config.timeoutMs = 10000;
    assert.equal((await request('/api/worker/run', {})).body.issued, 1);
    assert.equal((await request('/api/worker/run', {})).body.issued, 0);
});
