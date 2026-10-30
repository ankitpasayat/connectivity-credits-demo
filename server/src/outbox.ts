import type { Store } from './database.js';
import { canonical, id, now } from './common.js';
export async function namespace(store: Store): Promise<string> {
    const existing = await store.get("SELECT value FROM metadata WHERE key='broker_namespace'");
    if (existing)
        return existing.value;
    const value = id('namespace');
    await store.run("INSERT INTO metadata(key,value) VALUES('broker_namespace',?)", value);
    return value;
}
export async function enqueueDelivery(store: Store, eventId: string, kind = 'measurement', required = true): Promise<string> {
    const event = (await store.get('SELECT measurement_id FROM events WHERE id=?', eventId))!;
    const deliveryId = id('delivery'), instance = await namespace(store);
    const envelope = { schema_version: 1, database_instance: instance, delivery_id: deliveryId,
        event_id: eventId, measurement_id: event.measurement_id, type: 'measurement.received', kind };
    await store.run('INSERT INTO broker_outbox(id,event_id,kind,namespace,payload_json,created_at) VALUES(?,?,?,?,?,?)', deliveryId, eventId, kind, instance, canonical(envelope), now());
    if (required)
        await store.run(`INSERT INTO event_transport_state(event_id,required_delivery_id) VALUES(?,?)
    ON CONFLICT(event_id) DO UPDATE SET required_delivery_id=excluded.required_delivery_id`, eventId, deliveryId);
    return deliveryId;
}
export async function preparePending(store: Store) {
    await namespace(store);
    for (const event of await store.all(`SELECT e.id FROM events e LEFT JOIN event_transport_state t ON t.event_id=e.id
    WHERE e.status='queued' AND t.event_id IS NULL`))
        await enqueueDelivery(store, event.id);
}
