/** Real native librdkafka transport, not KafkaJS emulation or a local fallback.
 * Explicit one-partition assignment avoids recurring group rebalances. The
 * durable inbox preserves out-of-scope scenario work before offsets advance.
 * SQL commit (or durable failed state) ALWAYS precedes broker offset commit. */
import kafka from '@confluentinc/kafka-javascript';
import type { KafkaConsumer, Producer, Metadata, Message, TopicPartitionOffsetAndMetadata } from '@confluentinc/kafka-javascript';
import type { Config } from './config.js';
import { DomainError, now, sameJson, sleep, type Row } from './common.js';
import type { Engine } from './engine.js';
import { evaluate } from './engine.js';
import { namespace, preparePending } from './outbox.js';
import { currentRule, getEvent, getMeasurement } from './read-models.js';
import { totals } from './models.js';
import { withWorkerLock } from './locking.js';
const clientConfig = (config: Config) => ({ 'metadata.broker.list': config.bootstrap, 'client.id': 'connectivity-credits-node',
    'socket.timeout.ms': config.timeoutMs, 'socket.connection.setup.timeout.ms': Math.max(1000, config.timeoutMs), 'log_level': 0 });
export async function connectClient(client: Producer | KafkaConsumer, timeoutMs: number): Promise<Metadata> {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('Kafka connection timed out')), timeoutMs + 250);
        client.connect({ timeout: timeoutMs, allTopics: true }, (error, metadata) => {
            clearTimeout(timer);
            if (error)
                reject(error);
            else
                resolve(metadata);
        });
    });
}
export async function disconnectClient(client: Producer | KafkaConsumer) {
    await new Promise<void>(resolve => {
        const timer = setTimeout(resolve, 2000);
        timer.unref();
        try {
            client.disconnect(() => { clearTimeout(timer); resolve(); });
        }
        catch {
            clearTimeout(timer);
            resolve();
        }
    });
}
function metadata(client: Producer, topic: string, timeoutMs: number): Promise<Metadata> {
    return new Promise((resolve, reject) => client.getMetadata({ topic, timeout: timeoutMs }, (error, data) => error ? reject(error) : resolve(data)));
}
export async function brokerHealth(config: Config) {
    if (config.transport === 'local')
        return { transport: 'local', broker: 'not_used', broker_detail: null };
    const client = new kafka.Producer(clientConfig({ ...config, timeoutMs: Math.min(2000, config.timeoutMs) }));
    try {
        const result = await connectClient(client, Math.min(2000, config.timeoutMs));
        if (!result.brokers.length)
            throw new Error('No Kafka brokers returned metadata');
        return { transport: 'kafka', broker: 'ok', broker_detail: null };
    }
    catch (error) {
        return { transport: 'kafka', broker: 'unavailable', broker_detail: `Kafka is unavailable: ${String(error).slice(0, 250)}` };
    }
    finally {
        await disconnectClient(client);
    }
}
async function ensureTopic(producer: Producer, config: Config, initial: Metadata) {
    let topic = initial.topics.find(value => value.name === config.topic);
    if (!topic) {
        const admin = kafka.AdminClient.create(clientConfig(config));
        try {
            await new Promise<void>((resolve, reject) => admin.createTopic({ topic: config.topic, num_partitions: 1, replication_factor: 1 }, config.timeoutMs, error => error && error.code !== 36 ? reject(error) : resolve()));
        }
        finally {
            admin.disconnect();
        }
        const deadline = Date.now() + config.timeoutMs;
        do {
            topic = (await metadata(producer, config.topic, config.timeoutMs)).topics.find(value => value.name === config.topic);
            if (topic?.partitions.length)
                break;
            await sleep(100);
        } while (Date.now() < deadline);
    }
    if (!topic || topic.partitions.length !== 1)
        throw new Error('This demo requires a Kafka topic with exactly one partition');
}
async function retryCoordinator<T>(operation: () => Promise<T> | T, timeoutMs: number): Promise<T> {
    const deadline = Date.now() + timeoutMs;
    while (true) {
        try {
            return await operation();
        }
        catch (error) {
            const value = error as {
                code?: number;
                isRetriable?: boolean;
            };
            if ((!value.isRetriable && ![14, 15, 16].includes(value.code ?? 0)) || Date.now() >= deadline)
                throw error;
            await sleep(100);
        }
    }
}
async function publishPending(engine: Engine, producer: Producer) {
    const store = engine.store;
    const rows = await store.all(`SELECT o.* FROM broker_outbox o JOIN events e ON e.id=o.event_id
    WHERE o.published_at IS NULL AND (e.status='queued' OR o.kind IN ('retry','replay')) ORDER BY o.created_at,o.id`);
    if (!rows.length)
        return;
    const delivered = new Map<string, {
        error: unknown;
        partition: number;
        offset: number;
    }>();
    producer.on('delivery-report', (error, report) => delivered.set(String(report.opaque), { error, partition: report.partition, offset: report.offset }));
    producer.setPollInterval(20);
    for (const row of rows)
        producer.produce(engine.config.topic, 0, Buffer.from(row.payload_json), row.event_id, Date.now(), row.id);
    let flushError: unknown;
    try {
        await new Promise<void>((resolve, reject) => producer.flush(engine.config.timeoutMs, error => error ? reject(error) : resolve()));
    }
    catch (error) {
        flushError = error;
    }
    const deadline = Date.now() + 500;
    while (delivered.size < rows.length && Date.now() < deadline) {
        producer.poll();
        await sleep(10);
    }
    const errors: string[] = [];
    await store.tx(async () => {
        for (const row of rows) {
            const report = delivered.get(row.id);
            if (!report || report.error) {
                const message = String(report?.error ?? flushError ?? 'Kafka delivery timed out');
                errors.push(message);
                await store.run('UPDATE broker_outbox SET last_error=? WHERE id=?', message.slice(0, 1000), row.id);
            }
            else
                await store.run('UPDATE broker_outbox SET published_at=?,broker_partition=?,broker_offset=?,last_error=NULL WHERE id=?', now(), report.partition, report.offset, row.id);
        }
    });
    if (errors.length || flushError)
        throw new Error(errors[0] ?? String(flushError));
}
async function receive(engine: Engine, record: Message, instance: string) {
    let envelope: Row;
    try {
        envelope = JSON.parse(record.value?.toString() ?? 'null');
    }
    catch {
        return;
    }
    if (!envelope || typeof envelope !== 'object' || envelope.database_instance !== instance || typeof envelope.delivery_id !== 'string')
        return;
    const store = engine.store;
    await store.tx(async () => {
        const outbox = await store.get('SELECT * FROM broker_outbox WHERE id=? AND namespace=?', envelope.delivery_id, instance);
        if (!outbox || !sameJson(outbox.payload_json, envelope))
            return;
        await store.run(`INSERT INTO broker_inbox(delivery_id,event_id,topic,broker_partition,broker_offset,received_at)
      VALUES(?,?,?,?,?,?) ON CONFLICT(delivery_id) DO NOTHING`, outbox.id, outbox.event_id, record.topic, record.partition, record.offset, now());
        await store.run('UPDATE broker_outbox SET consumed_at=COALESCE(consumed_at,?) WHERE id=?', now(), outbox.id);
    });
}
export async function processKafka(engine: Engine, eventIds: string[], simulateFailure = false) {
    return withWorkerLock(engine.store, engine.config.timeoutMs, async () => {
        const config = engine.config, store = engine.store, result = totals();
        const producer = new kafka.Producer({ ...clientConfig(config), 'enable.idempotence': true, 'acks': -1, 'dr_cb': true,
            'message.timeout.ms': config.timeoutMs });
        let consumer: KafkaConsumer | undefined;
        try {
            const initial = await connectClient(producer, config.timeoutMs);
            await ensureTopic(producer, config, initial); // Never fall back when the broker is unavailable.
            const { instance, deliveries, failureId } = await store.tx(async () => {
                await preparePending(store);
                for (const eventId of eventIds)
                    await getEvent(store, eventId);
                const rule = await currentRule(store);
                let failureId: string | undefined;
                if (simulateFailure) {
                    for (const eid of eventIds) {
                        if (evaluate(await getMeasurement(store, (await getEvent(store, eid)).measurement_id), rule).eligible) { failureId = eid; break; }
                    }
                    failureId ??= eventIds[0];
                }
                const deliveries = eventIds.length ? (await store.all(`SELECT o.id FROM broker_outbox o LEFT JOIN event_transport_state t ON t.event_id=o.event_id
          WHERE o.event_id IN (${eventIds.map(() => '?').join(',')}) AND o.consumed_at IS NULL
          AND (o.id=t.required_delivery_id OR o.kind IN ('retry','replay'))`, ...eventIds)).map(row => row.id)
                    : (await store.all("SELECT id FROM broker_outbox WHERE consumed_at IS NULL AND kind IN ('retry','replay')")).map(row => row.id);
                return { instance: await namespace(store), deliveries, failureId };
            });
            await publishPending(engine, producer);
            const dispatchReady = async () => {
                for (const eventId of eventIds) {
                    const ready = await store.get(`SELECT 1 FROM events e JOIN event_transport_state t ON t.event_id=e.id
            JOIN broker_inbox i ON i.delivery_id=t.required_delivery_id WHERE e.id=? AND e.status='queued'`, eventId);
                    if (!ready)
                        continue;
                    const processed = await engine.processLocal([eventId], eventId === failureId);
                    for (const key of ['processed', 'issued', 'rejected', 'failed'] as const)
                        result[key] += processed[key];
                }
            };
            const complete = async () => {
                for (const deliveryId of deliveries) if (!await store.get('SELECT 1 FROM broker_inbox WHERE delivery_id=?', deliveryId)) return false;
                return true;
            };
            await dispatchReady();
            if (await complete())
                return result;
            consumer = new kafka.KafkaConsumer({ ...clientConfig(config), 'group.id': `${config.group}-${instance}`,
                'enable.auto.commit': false, 'enable.auto.offset.store': false, 'auto.offset.reset': 'earliest' }, {});
            await connectClient(consumer, config.timeoutMs);
            const activeConsumer = consumer;
            const partitions = await retryCoordinator(() => new Promise<TopicPartitionOffsetAndMetadata[]>((resolve, reject) => activeConsumer.committed([{ topic: config.topic, partition: 0 }], config.timeoutMs, (error, data) => error ? reject(error) : data.some(p => p.error) ? reject(data.find(p => p.error)!.error) : resolve(data))), config.timeoutMs);
            activeConsumer.assign([{ topic: config.topic, partition: 0, offset: partitions[0]?.offset >= 0 ? partitions[0].offset : -2 }]);
            activeConsumer.setDefaultConsumeTimeout(500);
            const deadline = Date.now() + config.timeoutMs;
            while (Date.now() < deadline) {
                const messages = await new Promise<Message[]>((resolve, reject) => activeConsumer.consume(1, (error, data) => error ? reject(error) : resolve(data)));
                for (const message of messages) {
                    await receive(engine, message, instance);
                    await dispatchReady();
                    // All business changes or failed-event bookkeeping committed above.
                    await retryCoordinator(() => activeConsumer.commitMessageSync(message), config.timeoutMs);
                    if (await complete())
                        return result;
                }
            }
            throw new Error('Timed out waiting for Kafka delivery. Queued work remains durable; retry the worker action');
        }
        catch (error) {
            if (error instanceof DomainError)
                throw error;
            throw new DomainError(503, `Kafka transport unavailable: ${String(error).slice(0, 350)}. Queued work is preserved; no local fallback was used.`);
        }
        finally {
            if (consumer)
                await disconnectClient(consumer);
            producer.setPollInterval(0);
            await disconnectClient(producer);
        }
    });
}
