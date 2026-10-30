import { day, shiftDay } from './common.js';
import type { Engine } from './engine.js';
import { PROVIDERS, SCHOOLS } from './fixtures.js';
export async function seed(engine: Engine) {
    const store = engine.store, end = day(-1), start = shiftDay(end, -13);
    for (const row of PROVIDERS)
        await store.run('INSERT INTO providers(id,name,country) VALUES(?,?,?)', ...row);
    for (const row of SCHOOLS)
        await store.run('INSERT INTO schools(id,name,country,region,provider_id) VALUES(?,?,?,?,?)', ...row);
    await store.run(`INSERT INTO rules(version,min_download_mbps,min_upload_mbps,max_latency_ms,min_uptime_pct,credits_per_period,updated_at)
    VALUES(1,20,5,100,98,10,?)`, `${start}T00:00:00Z`);
    for (let d = 0; d < 14; d++) {
        const period = shiftDay(start, d);
        for (let i = 0; i < SCHOOLS.length; i++) {
            const school = SCHOOLS[i], key = (d + i) % 9, mid = `seed-${school[0]}-${period}`, eid = `event-${mid}`;
            const minute = String(i).padStart(2, '0');
            await engine.insert({ source: 'synthetic.seed', external_id: `${school[0]}:${period}`, school_id: school[0], provider_id: school[4], period,
                download_mbps: key === 0 ? 11.5 : 30 + ((d * 7 + i * 3) % 31), upload_mbps: key === 1 ? 3.2 : 7 + ((d + i) % 8),
                latency_ms: key === 2 ? 143 : 25 + ((d * 3 + i * 4) % 48), uptime_pct: key === 3 ? 94.8 : Math.round((98.5 + ((d + i) % 15) / 10) * 10) / 10,
                verification_status: key === 4 ? 'unverified' : 'verified',
            }, { measurementId: mid, eventId: eid, timestamp: `${period}T11:${minute}:00Z`, delivery: false });
            if (d === 13 && i >= 8) {
                if (i === 11) {
                    await store.run("UPDATE events SET status='failed',attempts=1,last_error=? WHERE id=?", 'Synthetic interrupted worker: no decision or credit committed. Retry is safe.', eid);
                    await engine.activity('worker.failed', 'A synthetic event needs retry', `${school[1]} · no business state committed`, `${period}T11:31:00Z`, 'activity_seed_failure');
                }
            }
            else
                await engine.processEvent(eid, false, `${period}T11:${minute}:30Z`);
        }
    }
    await store.run("INSERT INTO metadata(key,value) VALUES('seed_version','1')");
    await store.run("INSERT INTO metadata(key,value) VALUES('synthetic_labels_version','2') ON CONFLICT(key) DO UPDATE SET value=excluded.value");
    await store.run("INSERT INTO metadata(key,value) VALUES('country_labels_version','1') ON CONFLICT(key) DO UPDATE SET value=excluded.value");
    await store.run("INSERT INTO metadata(key,value) VALUES('seed_end_date',?)", end);
}
