import { AsyncLocalStorage } from 'node:async_hooks';
import pg, { type PoolClient } from 'pg';
import type { Config } from './config.js';
import type { Row } from './common.js';
import { SCHEMA } from './schema.js';

pg.types.setTypeParser(20, value => {
  const number = Number(value);
  if (!Number.isSafeInteger(number)) throw new Error('Database integer exceeds safe JSON integer range');
  return number;
});
const context = new AsyncLocalStorage<{ store: Store; client: PoolClient }>();
export class Store {
  readonly pool: pg.Pool;
  constructor(public readonly config: Config) {
    if (!/^[a-z][a-z0-9_]{0,62}$/.test(config.databaseSchema)) throw new Error('Invalid PostgreSQL schema identifier');
    this.pool = new pg.Pool({ connectionString: config.databaseUrl, max: 10,
      connectionTimeoutMillis: 5000, idleTimeoutMillis: 30_000,
      options: `-c search_path=${config.databaseSchema} -c statement_timeout=30000` });
    this.pool.on('error', error => console.error('PostgreSQL pool connection failed:', error.message));
  }
  async initialize() {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [this.config.databaseSchema + ':schema-init']);
      await client.query(`CREATE SCHEMA IF NOT EXISTS "${this.config.databaseSchema}"`);
      await client.query(SCHEMA);
      await client.query('COMMIT');
    } catch (error) { await client.query('ROLLBACK'); throw error; }
    finally { client.release(); }
  }
  private async query(sql: string, params: unknown[]) {
    // Values remain separate bind parameters; identifiers are internal constants.
    let index = 0;
    const statement = sql.replace(/\?/g, () => '$' + ++index);
    const active = context.getStore();
    return active?.store === this ? active.client.query(statement, params) : this.pool.query(statement, params);
  }
  async get(sql: string, ...params: unknown[]): Promise<Row | undefined> { return (await this.query(sql, params)).rows[0]; }
  async all(sql: string, ...params: unknown[]): Promise<Row[]> { return (await this.query(sql, params)).rows; }
  async run(sql: string, ...params: unknown[]) {
    const result = await this.query(sql, params);
    return { changes: result.rowCount ?? 0, rows: result.rows };
  }
  async exec(sql: string) { await this.query(sql, []); }
  async tx<T>(work: () => Promise<T> | T): Promise<T> {
    if (context.getStore()?.store === this) return work();
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await context.run({ store: this, client }, work);
      await client.query('COMMIT');
      return result;
    } catch (error) { await client.query('ROLLBACK'); throw error; }
    finally { client.release(); }
  }
  async close() { await this.pool.end(); }
}
