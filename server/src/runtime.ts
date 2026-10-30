import { type Config, readConfig } from './config.js';
import { day, DomainError, id, shiftDay } from './common.js';
import { Store } from './database.js';
import { Engine } from './engine.js';
import { processKafka } from './kafka.js';
import { withWorkerLock } from './locking.js';
import { migrateLabels, restoreCountryLabels } from './migrations.js';
import { enqueueDelivery, preparePending } from './outbox.js';
import { currentRule, getEvent, getMeasurement } from './read-models.js';
import { seed } from './seed.js';
import type { MeasurementInput } from './models.js';
const scenarios: Record<string, string> = { clean_batch: 'A clean daily batch', below_threshold: 'A reading that misses the policy',
    duplicate_delivery: 'The same delivery, three times', worker_failure: 'A worker interruption, safely rolled back' };
export class Runtime {
    readonly store: Store;
    readonly engine: Engine;
    constructor(public readonly config: Config = readConfig()) {
        this.store = new Store(config);
        this.engine = new Engine(this.store, config);
    }
    static async create(config: Config = readConfig()) {
        const runtime = new Runtime(config);
        try {
            await runtime.store.initialize();
            await runtime.store.tx(async () => {
                await runtime.store.run("SELECT pg_advisory_xact_lock(hashtextextended(?,0))", config.databaseSchema + ":startup");
                await migrateLabels(runtime.store);
                if (!await runtime.store.get("SELECT value FROM metadata WHERE key='seed_version'")) await seed(runtime.engine);
                await restoreCountryLabels(runtime.store);
                await preparePending(runtime.store);
            });
            return runtime;
        } catch (error) { await runtime.close(); throw error; }
    }
    process(eventIds: string[], simulateFailure = false) {
        return this.config.transport === 'kafka' ? processKafka(this.engine, eventIds, simulateFailure)
            : withWorkerLock(this.store, this.config.timeoutMs, async () => await this.engine.processLocal(eventIds, simulateFailure));
    }
    async runWorker(limit = 50, simulateFailure = false) {
        const ids = (await this.store.all("SELECT id FROM events WHERE status='queued' ORDER BY created_at,id LIMIT ?", limit)).map(row => row.id as string);
        return this.process(ids, simulateFailure);
    }
    retry(eventId: string) {
        return withWorkerLock(this.store, this.config.timeoutMs, async () => {
            const event = await this.store.tx(async () => {
                const event = await getEvent(this.store, eventId);
                if (event.status === 'failed')
                    await this.store.run("UPDATE events SET status='queued',last_error=NULL,processed_at=NULL WHERE id=?", eventId);
                if (this.config.transport === 'kafka')
                    await enqueueDelivery(this.store, eventId, 'retry');
                return event;
            });
            if (event.status !== 'processed' || this.config.transport === 'kafka')
                await this.process([eventId]);
            return await getEvent(this.store, eventId);
        });
    }
    scenario(scenario: string) {
        if (!(scenario in scenarios))
            throw new DomainError(404, 'Unknown demo scenario');
        return withWorkerLock(this.store, this.config.timeoutMs, async () => {
            const store = this.store, mids: string[] = [], eids: string[] = [];
            let replays = 0;
            const rule = await store.tx(async () => {
                const rule = await currentRule(store), schools = await store.all('SELECT id,provider_id FROM schools ORDER BY id');
                for (const school of schools.slice(0, scenario === 'clean_batch' ? 3 : 1)) {
                    let period = day(-1);
                    while (await store.get('SELECT 1 FROM measurements WHERE school_id=? AND period=?', school.id, period))
                        period = shiftDay(period, -1);
                    const values: MeasurementInput = { source: `demo.${scenario}`, external_id: id('delivery'), school_id: school.id, provider_id: school.provider_id, period,
                        download_mbps: Math.max(42, rule.min_download_mbps + 10), upload_mbps: Math.max(12, rule.min_upload_mbps + 5),
                        latency_ms: Math.min(32, rule.max_latency_ms), uptime_pct: Math.max(99.6, rule.min_uptime_pct), verification_status: 'verified' };
                    if (scenario === 'below_threshold')
                        Object.assign(values, { download_mbps: Math.max(0, rule.min_download_mbps - 7),
                            uptime_pct: Math.max(0, rule.min_uptime_pct - 8), verification_status: 'unverified' });
                    const inserted = await this.engine.insert(values);
                    mids.push(inserted.measurementId);
                    eids.push(inserted.eventId!);
                    if (scenario === 'duplicate_delivery')
                        for (let i = 0; i < 2; i++)
                            replays += Number((await this.engine.insert(values)).duplicate);
                }
                return rule;
            });
            const result = await this.process(eids, scenario === 'worker_failure');
            const failed = scenario === 'below_threshold' ? (await getMeasurement(store, mids[0])).checks.filter((check: {
                passed: boolean;
            }) => !check.passed).map((check: {
                label: string;
            }) => check.label).join(', ') : '';
            const message = scenario === 'worker_failure'
                ? 'Interrupted after credit INSERT. Credit, decision and completion audit were rolled back. Retry the failed event to issue once.'
                : scenario === 'duplicate_delivery' ? 'Three identical deliveries produced one measurement, one event and one daily credit. Two replays were deduplicated.'
                    : scenario === 'below_threshold' ? `No demo credits issued. Failed checks: ${failed}.`
                        : `Three verified daily readings passed policy v${rule.version}. ${result.issued} daily credit issuances created.`;
            return { title: scenarios[scenario], message, measurement_ids: mids, event_ids: eids,
                processed: result.processed, credits_created: result.issued, duplicates_prevented: replays };
        });
    }
    reset() {
        return withWorkerLock(this.store, this.config.timeoutMs, async () => await this.store.tx(async () => {
            for (const table of ['broker_inbox', 'event_transport_state', 'broker_outbox', 'decisions', 'credits', 'activity', 'events', 'measurements', 'rules', 'schools', 'providers', 'metadata']) {
                await this.store.exec(`DELETE FROM ${table}`);
            }
            await seed(this.engine);
            await preparePending(this.store);
            return { status: 'reset' };
        }));
    }
    async close() { await this.store.close(); }
}
