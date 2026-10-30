import type { Config } from './config.js';
import type { Store } from './database.js';
import { canonical, DomainError, id, now, sameJson, type Row } from './common.js';
import { enqueueDelivery } from './outbox.js';
import { currentRule, getEvent, getMeasurement } from './read-models.js';
import { totals, type Check, type MeasurementInput, type RuleInput, type Totals } from './models.js';
export function evaluate(measurement: Row, rule: Row): {
    eligible: boolean;
    checks: Check[];
} {
    const definitions = [
        ['download_mbps', 'Download speed (Mbps)', 'min_download_mbps', true],
        ['upload_mbps', 'Upload speed (Mbps)', 'min_upload_mbps', true],
        ['latency_ms', 'Latency (ms)', 'max_latency_ms', false],
        ['uptime_pct', 'Uptime (%)', 'min_uptime_pct', true],
    ] as const;
    const checks: Check[] = definitions.map(([key, label, requiredKey, minimum]) => ({ key, label,
        actual: measurement[key], required: rule[requiredKey], passed: minimum ? measurement[key] >= rule[requiredKey] : measurement[key] <= rule[requiredKey] }));
    checks.push({ key: 'verification_status', label: 'Verifier status', actual: measurement.verification_status,
        required: 'verified', passed: measurement.verification_status === 'verified' });
    return { eligible: checks.every(check => check.passed), checks };
}
export class Engine {
    constructor(public readonly store: Store, public readonly config: Config) { }
    async activity(type: string, title: string, detail: string, timestamp = now(), activityId = id('activity')) {
        await this.store.run('INSERT INTO activity(id,type,title,detail,created_at) VALUES(?,?,?,?,?)', activityId, type, title, detail, timestamp);
    }
    async insert(payload: MeasurementInput, options: {
        timestamp?: string;
        measurementId?: string;
        eventId?: string;
        delivery?: boolean;
    } = {}): Promise<{ measurementId: string; eventId: string | null; duplicate: boolean }> {
        const store = this.store, delivery = options.delivery ?? true;
        const existing = await store.get('SELECT id,payload_json FROM measurements WHERE source=? AND external_id=?', payload.source, payload.external_id);
        if (existing) {
            if (!sameJson(existing.payload_json, payload))
                throw new DomainError(409, 'This source/external_id already exists with a different payload');
            if (this.config.transport === 'kafka' && delivery) {
                await enqueueDelivery(store, (await store.get('SELECT id FROM events WHERE measurement_id=?', existing.id))!.id, 'replay', false);
            }
            return { measurementId: existing.id as string, eventId: null, duplicate: true };
        }
        const school = await store.get('SELECT name,provider_id FROM schools WHERE id=?', payload.school_id);
        if (!school)
            throw new DomainError(422, 'Unknown school_id');
        if (school.provider_id !== payload.provider_id)
            throw new DomainError(422, "provider_id does not match the school's assigned provider");
        const mid = options.measurementId ?? id('measurement'), eid = options.eventId ?? id('event'), timestamp = options.timestamp ?? now();
        const inserted = await store.run(`INSERT INTO measurements(id,source,external_id,payload_json,school_id,provider_id,period,
      download_mbps,upload_mbps,latency_ms,uptime_pct,verification_status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(source,external_id) DO NOTHING RETURNING id`, mid, payload.source, payload.external_id, canonical(payload), payload.school_id, payload.provider_id, payload.period, payload.download_mbps, payload.upload_mbps, payload.latency_ms, payload.uptime_pct, payload.verification_status, timestamp);
        if (!inserted.changes) {
            // A concurrent ingestion won the SQL identity constraint. Under
            // READ COMMITTED its committed measurement/event is now visible.
            return this.insert(payload, options);
        }
        await store.run('INSERT INTO events(id,measurement_id,created_at) VALUES(?,?,?)', eid, mid, timestamp);
        if (delivery)
            await enqueueDelivery(store, eid);
        await this.activity('measurement.received', 'Measurement queued', `${school.name} · ${payload.period} · awaiting policy evaluation`, timestamp, `activity_received_${mid}`);
        return { measurementId: mid, eventId: eid, duplicate: false };
    }
    async ingest(payload: MeasurementInput) {
        return await this.store.tx(async () => {
            const result = await this.insert(payload);
            return { measurement: await getMeasurement(this.store, result.measurementId), duplicate: result.duplicate };
        });
    }
    async processEvent(eventId: string, forceFailure = false, timestamp = now()): Promise<Totals | null> {
        const store = this.store;
        await store.get('SELECT id FROM events WHERE id=? FOR UPDATE', eventId);
        const event = await getEvent(store, eventId);
        if (event.status !== 'queued')
            return null;
        const measurement = await getMeasurement(store, event.measurement_id), rule = await currentRule(store);
        const { eligible, checks } = evaluate(measurement, rule);
        let issued = false;
        if (eligible) {
            const result = await store.run(`INSERT INTO credits(id,school_id,provider_id,period,amount,measurement_id,rule_version,created_at)
        VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(school_id,period) DO NOTHING`, `credit_${measurement.id}`, measurement.school_id, measurement.provider_id, measurement.period, rule.credits_per_period, measurement.id, rule.version, timestamp);
            issued = Number(result.changes) === 1;
            // The injected exception escapes the enclosing transaction AFTER INSERT.
            if (forceFailure)
                throw new Error('Simulated worker interruption after credit INSERT; transaction rolled back. Retry is safe.');
        }
        else if (forceFailure)
            throw new Error('Simulated worker interruption before decision commit; transaction rolled back. Retry is safe.');
        const status = eligible ? 'eligible' : 'ineligible', encoded = JSON.stringify(checks);
        await store.run(`INSERT INTO decisions(id,measurement_id,event_id,status,rule_version,rule_snapshot_json,checks_json,credit_issued,created_at)
      VALUES(?,?,?,?,?,?,?,?,?)`, `decision_${measurement.id}`, measurement.id, eventId, status, rule.version, canonical(rule), encoded, Number(issued), timestamp);
        await store.run('UPDATE measurements SET status=?,checks_json=?,rule_version=?,credit_issued=?,evaluated_at=? WHERE id=?', status, encoded, rule.version, Number(issued), timestamp, measurement.id);
        await store.run("UPDATE events SET status='processed',attempts=attempts+1,last_error=NULL,processed_at=? WHERE id=?", timestamp, eventId);
        const title = issued ? `${rule.credits_per_period} demo credits issued` : eligible ? 'Duplicate daily issuance prevented' : 'Measurement did not qualify';
        const detail = issued ? `${measurement.school_name} · ${measurement.period} · policy v${rule.version} · all checks passed` : eligible
            ? `${measurement.school_name} · ${measurement.period} already has a credit; eligible reading retained`
            : `${measurement.school_name} · ${measurement.period} · failed: ${checks.filter(c => !c.passed).map(c => c.label).join(', ')}`;
        await this.activity(issued ? 'credit.issued' : eligible ? 'credit.prevented' : 'measurement.rejected', title, detail, timestamp, `activity_completed_${measurement.id}`);
        return { processed: 1, issued: Number(issued), rejected: Number(!eligible), failed: 0 };
    }
    async processLocal(eventIds: string[], simulateFailure = false): Promise<Totals> {
        const result = totals(), store = this.store;
        let failureId: string | undefined;
        if (simulateFailure && eventIds.length) {
            const rule = await currentRule(store);
            for (const eid of eventIds) {
                const event = await getEvent(store, eid);
                if (event.status === 'queued' && evaluate(await getMeasurement(store, event.measurement_id), rule).eligible) { failureId = eid; break; }
            }
            failureId ??= eventIds[0];
        }
        for (const eventId of eventIds) {
            try {
                const processed = await store.tx(async () => await this.processEvent(eventId, eventId === failureId));
                if (processed)
                    for (const key of ['processed', 'issued', 'rejected', 'failed'] as const)
                        result[key] += processed[key];
            }
            catch (error) {
                await store.tx(async () => {
                    const event = await getEvent(store, eventId);
                    if (event.status === 'processed')
                        return;
                    const message = error instanceof Error ? error.message : String(error);
                    await store.run("UPDATE events SET status='failed',attempts=attempts+1,last_error=?,processed_at=NULL WHERE id=?", message.slice(0, 1000), eventId);
                    await this.activity('worker.failed', 'Worker transaction rolled back', `${event.school_name} · ${message.slice(0, 500)}`);
                });
                result.failed++;
            }
        }
        return result;
    }
    async updateRule(payload: RuleInput) {
        return await this.store.tx(async () => {
            await this.store.run('SELECT pg_advisory_xact_lock(hashtextextended(?,0))', this.config.databaseSchema + ':policy-version');
            const version = (await currentRule(this.store)).version + 1;
            await this.store.run(`INSERT INTO rules(version,min_download_mbps,min_upload_mbps,max_latency_ms,min_uptime_pct,credits_per_period,updated_at)
        VALUES(?,?,?,?,?,?,?)`, version, payload.min_download_mbps, payload.min_upload_mbps, payload.max_latency_ms, payload.min_uptime_pct, payload.credits_per_period, now());
            await this.activity('policy.updated', `Policy v${version} activated`, 'New decisions use this version. Existing decisions and issued demo credits retain their original policy.');
            return await currentRule(this.store);
        });
    }
}
