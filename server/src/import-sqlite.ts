/** One-off, explicit migration only. The running API never imports node:sqlite. */
import { DatabaseSync, backup } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { Store } from './database.js';
import { canonical, type Row } from './common.js';
import type { Config } from './config.js';
import { withWorkerLock } from './locking.js';

export const IMPORT_TABLES = ['metadata', 'providers', 'schools', 'rules', 'measurements', 'events', 'credits', 'decisions',
  'activity', 'broker_outbox', 'event_transport_state', 'broker_inbox', 'runtime_locks'] as const;
export type Snapshot = Record<string, Row[]>;
const sorted = (rows: Row[]) => rows.map(canonical).sort();
export const fingerprint = (snapshot: Snapshot) => createHash('sha256').update(canonical(Object.fromEntries(
  Object.entries(snapshot).map(([table, rows]) => [table, sorted(rows)])))).digest('hex');

export async function readSqliteSnapshot(sourcePath: string): Promise<Snapshot> {
  const folder = mkdtempSync(join(tmpdir(), 'connectivity-sqlite-import-'));
  let source: DatabaseSync | undefined, copy: DatabaseSync | undefined;
  try {
    // SQLite backup API captures a consistent WAL-aware snapshot without writes
    // to the source. The immutable temporary copy is the sole import input.
    source = new DatabaseSync(resolve(sourcePath), { readOnly: true });
    const copyPath = join(folder, 'snapshot.sqlite3');
    await backup(source, copyPath); source.close(); source = undefined;
    copy = new DatabaseSync(copyPath, { readOnly: true });
    const tables = copy.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all().map(row => String(row.name));
    if (!tables.includes('metadata') || !tables.includes('measurements')) throw new Error('Source is not a Connectivity Credits database');
    if (tables.some(table => !IMPORT_TABLES.includes(table as typeof IMPORT_TABLES[number]))) throw new Error('Source has unsupported tables; refusing an incomplete import');
    const snapshot: Snapshot = {};
    for (const table of IMPORT_TABLES) snapshot[table] = tables.includes(table)
      ? copy.prepare(`SELECT * FROM "${table}"`).all().map(row => ({ ...row })) : [];
    return snapshot;
  } finally { source?.close(); copy?.close(); rmSync(folder, { recursive: true, force: true }); }
}
export async function postgresSnapshot(store: Store): Promise<Snapshot> {
  const result: Snapshot = {};
  for (const table of IMPORT_TABLES) result[table] = await store.all(`SELECT * FROM "${table}"`);
  return result;
}
export async function importSqlite(config: Config, sourcePath: string, verifyOnly = false) {
  const source = await readSqliteSnapshot(sourcePath), store = new Store(config);
  try {
    if (!verifyOnly) await store.initialize();
    return await withWorkerLock(store, config.timeoutMs, () => store.tx(async () => {
      await store.run('SELECT pg_advisory_xact_lock(hashtextextended(?,0))', config.databaseSchema + ':startup');
      await store.exec('LOCK TABLE ' + IMPORT_TABLES.map(table => `"${table}"`).join(',') + ' IN ACCESS EXCLUSIVE MODE');
      const existing = await postgresSnapshot(store);
      const identical = fingerprint(existing) === fingerprint(source);
      if (verifyOnly) {
        if (!identical) throw new Error('PostgreSQL differs from the SQLite snapshot; no data was changed');
      } else if (Object.values(existing).some(rows => rows.length)) {
        if (!identical) throw new Error('Target PostgreSQL schema is not empty and differs from the source; refusing to overwrite data');
      } else {
        for (const table of IMPORT_TABLES) {
          const allowed = new Set((await store.all('SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=?', table)).map(row => row.column_name));
          for (const row of source[table]) {
            const columns = Object.keys(row);
            if (columns.some(column => !allowed.has(column) || !/^[a-z_][a-z0-9_]*$/.test(column))) throw new Error(`Unsupported source columns in ${table}`);
            await store.run(`INSERT INTO "${table}"(${columns.map(column => `"${column}"`).join(',')}) VALUES(${columns.map(() => '?').join(',')})`, ...columns.map(column => row[column]));
          }
        }
      }
      const actual = await postgresSnapshot(store);
      if (fingerprint(actual) !== fingerprint(source)) throw new Error('Exact PostgreSQL import verification failed; transaction rolled back');
      return { status: verifyOnly ? 'verified' : identical ? 'already_imported' : 'imported', database_engine: 'postgresql',
        schema: config.databaseSchema, source_read_only: true,
        counts: Object.fromEntries(IMPORT_TABLES.map(table => [table, source[table].length])),
        source_sha256: fingerprint(source), postgres_sha256: fingerprint(actual) };
    }));
  } finally { await store.close(); }
}
