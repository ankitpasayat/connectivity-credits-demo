import type { Store } from './database.js';
import { PROVIDERS, SCHOOLS } from './fixtures.js';
import { V2_PROVIDERS, V2_SCHOOLS } from './neutral-fixtures-v2.js';
import { LEGACY_PROVIDERS, LEGACY_SCHOOLS } from './legacy-labels.js';
export async function migrateLabels(store: Store) {
    if ((await store.get("SELECT value FROM metadata WHERE key='synthetic_labels_version'"))?.value === '2')
        return;
    const columns = new Set((await store.all("SELECT column_name AS name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='schools'")).map(row => row.name));
    for (const column of ['latitude', 'longitude'])
        if (columns.has(column))
            await store.exec(`ALTER TABLE schools DROP COLUMN ${column}`);
    for (const [providerId, name, country] of V2_PROVIDERS) {
        const [oldName, oldCountry] = LEGACY_PROVIDERS[providerId];
        await store.run('UPDATE providers SET name=? WHERE id=? AND name=?', name, providerId, oldName);
        await store.run('UPDATE providers SET country=? WHERE id=? AND country=?', country, providerId, oldCountry);
    }
    for (const [schoolId, name, country, region] of V2_SCHOOLS) {
        const [oldName, oldCountry, oldRegion] = LEGACY_SCHOOLS[schoolId];
        await store.run('UPDATE schools SET name=? WHERE id=? AND name=?', name, schoolId, oldName);
        await store.run('UPDATE schools SET country=? WHERE id=? AND country=?', country, schoolId, oldCountry);
        await store.run('UPDATE schools SET region=? WHERE id=? AND region=?', region, schoolId, oldRegion);
        await store.run('UPDATE activity SET detail=REPLACE(detail,?,?) WHERE POSITION(? IN detail)>0', oldName, name, oldName);
        await store.run('UPDATE activity SET title=REPLACE(title,?,?) WHERE POSITION(? IN title)>0', oldName, name, oldName);
    }
    await store.run("INSERT INTO metadata(key,value) VALUES('synthetic_labels_version','2') ON CONFLICT(key) DO UPDATE SET value=excluded.value");
}

/** User-selected display-country restoration. Caller holds the startup
 * transaction. Only exact former neutral labels on known fixture IDs change;
 * names, regions and all business/transport records remain untouched. */
export async function restoreCountryLabels(store: Store) {
    if ((await store.get("SELECT value FROM metadata WHERE key='country_labels_version'"))?.value === '1') return;
    for (const [providerId, , country] of PROVIDERS) {
        const formerCountry = V2_PROVIDERS.find(row => row[0] === providerId)![2];
        await store.run('UPDATE providers SET country=? WHERE id=? AND country=?', country, providerId, formerCountry);
    }
    for (const [schoolId, , country] of SCHOOLS) {
        const formerCountry = V2_SCHOOLS.find(row => row[0] === schoolId)![2];
        await store.run('UPDATE schools SET country=? WHERE id=? AND country=?', country, schoolId, formerCountry);
    }
    await store.run("INSERT INTO metadata(key,value) VALUES('country_labels_version','1') ON CONFLICT(key) DO UPDATE SET value=excluded.value");
}
