import type { Store } from './database.js';
import { DomainError, type Row } from './common.js';
const measurementSelect = `SELECT m.*,s.name AS school_name,p.name AS provider_name,s.country
 FROM measurements m JOIN schools s ON s.id=m.school_id JOIN providers p ON p.id=m.provider_id`;
const eventSelect = `SELECT e.*,s.name AS school_name FROM events e
 JOIN measurements m ON m.id=e.measurement_id JOIN schools s ON s.id=m.school_id`;
function measurement(row: Row) {
    const { payload_json: _payload, checks_json, ...result } = row;
    return { ...result, checks: JSON.parse(checks_json), credit_issued: Boolean(result.credit_issued) } as Row;
}
export async function getMeasurement(store: Store, value: string): Promise<Row> {
    const row = await store.get(measurementSelect + ' WHERE m.id=?', value);
    if (!row)
        throw new DomainError(404, 'Measurement not found');
    return measurement(row);
}
export async function listMeasurements(store: Store) { return (await store.all(measurementSelect + ' ORDER BY m.created_at DESC,m.id DESC')).map(measurement); }
export async function getEvent(store: Store, value: string): Promise<Row> {
    const event = await store.get(eventSelect + ' WHERE e.id=?', value);
    if (!event)
        throw new DomainError(404, 'Event not found');
    return event;
}
export const listEvents = async (store: Store) => await store.all(eventSelect + ' ORDER BY e.created_at DESC,e.id DESC');
export const currentRule = async (store: Store): Promise<Row> => (await store.get('SELECT * FROM rules ORDER BY version DESC LIMIT 1'))!;
export const listCredits = async (store: Store) => await store.all(`SELECT c.*,s.name AS school_name,p.name AS provider_name,s.country
 FROM credits c JOIN schools s ON s.id=c.school_id JOIN providers p ON p.id=c.provider_id ORDER BY c.created_at DESC,c.id DESC`);
export const listSchools = async (store: Store) => await store.all(`
 SELECT s.*,p.name AS provider_name,
 CASE WHEN m.id IS NULL OR m.status='pending' THEN 'awaiting_data'
 WHEN m.status='eligible' THEN 'connected' ELSE 'needs_attention' END AS status,
 COALESCE((SELECT SUM(c.amount) FROM credits c WHERE c.school_id=s.id),0) AS credits_earned,
 m.download_mbps AS latest_download_mbps,m.uptime_pct AS latest_uptime_pct
 FROM schools s JOIN providers p ON p.id=s.provider_id LEFT JOIN measurements m ON m.id=(
 SELECT lm.id FROM measurements lm WHERE lm.school_id=s.id ORDER BY lm.period DESC,lm.created_at DESC,lm.id DESC LIMIT 1)
 ORDER BY s.country,s.name`);
export async function overview(store: Store) {
    const schools = await listSchools(store);
    const counts = (await store.get(`SELECT COUNT(*) AS total_measurements,COUNT(*) FILTER (WHERE status='eligible') AS eligible_measurements,
    COUNT(*) FILTER (WHERE status!='pending') AS evaluated FROM measurements`))!;
    const queue = (await store.get(`SELECT COUNT(*) FILTER (WHERE status='queued') AS queued_events,COUNT(*) FILTER (WHERE status='failed') AS failed_events FROM events`))!;
    const summary = { schools_connected: schools.filter(s => s.status === 'connected').length, schools_total: schools.length,
        credits_issued: (await store.get('SELECT COALESCE(SUM(amount),0) AS total FROM credits'))!.total,
        eligible_measurements: counts.eligible_measurements, total_measurements: counts.total_measurements,
        pass_rate: counts.evaluated ? Math.round(1000 * counts.eligible_measurements / counts.evaluated) / 10 : 0, ...queue };
    const trends = await store.all(`SELECT m.period AS date,COUNT(*) AS measurements,
    COALESCE((SELECT SUM(c.amount) FROM credits c WHERE c.period=m.period),0) AS credits
    FROM measurements m GROUP BY m.period ORDER BY m.period`);
    const country_breakdown = await store.all(`SELECT s.country,COUNT(DISTINCT s.id) AS schools,
    (SELECT COALESCE(SUM(c.amount),0) FROM credits c JOIN schools cs ON cs.id=c.school_id WHERE cs.country=s.country) AS credits,
    (CASE WHEN COUNT(m.id) FILTER (WHERE m.status!='pending')>0 THEN ROUND(100.0*(COUNT(m.id) FILTER (WHERE m.status='eligible'))/(COUNT(m.id) FILTER (WHERE m.status!='pending')),1) ELSE 0 END)::double precision AS pass_rate
    FROM schools s LEFT JOIN measurements m ON m.school_id=s.id GROUP BY s.country ORDER BY s.country`);
    return { summary, trends, country_breakdown,
        recent_activity: await store.all('SELECT * FROM activity ORDER BY created_at DESC,id DESC LIMIT 12') };
}
