import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
export interface Config {
  databaseUrl: string;
  databaseSchema: string;
  transport: 'local' | 'kafka';
  bootstrap: string;
  topic: string;
  group: string;
  timeoutMs: number;
  port: number;
  publicDemo: boolean;
}
export function readConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const publicDemo = env.CONNECTIVITY_PUBLIC_DEMO ?? 'false';
  if (publicDemo !== 'true' && publicDemo !== 'false') throw new Error('CONNECTIVITY_PUBLIC_DEMO must be true or false');
  const transport = env.CONNECTIVITY_TRANSPORT ?? 'local';
  if (transport !== 'local' && transport !== 'kafka') throw new Error('CONNECTIVITY_TRANSPORT must be local or kafka');
  const timeout = Number(env.CONNECTIVITY_KAFKA_TIMEOUT_SECONDS ?? 12);
  const port = Number(env.PORT ?? 3000);
  if (!Number.isFinite(timeout) || timeout < 1 || timeout > 120) throw new Error('Kafka timeout must be 1–120 seconds');
  if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error('PORT must be a valid port');
  const databaseSchema = env.CONNECTIVITY_DB_SCHEMA ?? 'public';
  if (!/^[a-z][a-z0-9_]{0,62}$/.test(databaseSchema)) throw new Error('CONNECTIVITY_DB_SCHEMA must be a simple lowercase SQL identifier');
  return { databaseUrl: env.DATABASE_URL ?? env.CONNECTIVITY_DATABASE_URL ?? 'postgresql://127.0.0.1:5433/connectivity_credits', databaseSchema,
    transport, bootstrap: env.CONNECTIVITY_KAFKA_BOOTSTRAP_SERVERS ?? '127.0.0.1:19092',
    topic: env.CONNECTIVITY_KAFKA_TOPIC ?? 'connectivity-measurements-v1',
    group: env.CONNECTIVITY_KAFKA_GROUP_ID ?? 'connectivity-credits-demo', timeoutMs: timeout * 1000, port, publicDemo: publicDemo === 'true' };
}
