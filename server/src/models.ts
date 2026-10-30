import { z } from 'zod';
import { day } from './common.js';

const label = (max: number) => z.string().min(1).max(max).refine(value => value.trim().length > 0, 'must not be blank');
const number = (max: number) => z.number().finite().min(0).max(max);
const period = z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'period must be YYYY-MM-DD')
  .refine(value => value.slice(0, 4) !== '0000' && Number.isFinite(Date.parse(`${value}T00:00:00Z`)) &&
    new Date(`${value}T00:00:00Z`).toISOString().slice(0, 10) === value, 'period must be a valid calendar date')
  .refine(value => value <= day(), 'future measurement periods are not supported in this demo');
export const measurementInput = z.object({
  source: label(100), external_id: label(160), school_id: label(100), provider_id: label(100), period,
  download_mbps: number(1_000_000), upload_mbps: number(1_000_000), latency_ms: number(1_000_000),
  uptime_pct: number(100), verification_status: z.enum(['verified', 'unverified']),
}).strict();
export const ruleInput = z.object({ min_download_mbps: number(100_000), min_upload_mbps: number(100_000),
  max_latency_ms: number(100_000), min_uptime_pct: number(100), credits_per_period: z.number().int().min(1).max(1_000_000),
}).strict();
export const workerInput = z.object({ limit: z.number().int().min(1).max(500).default(50),
  simulate_failure: z.boolean().default(false) }).strict();
export type MeasurementInput = z.infer<typeof measurementInput>;
export type RuleInput = z.infer<typeof ruleInput>;
export interface Check { key: string; label: string; actual: number | string; required: number | string; passed: boolean; }
export interface Totals { processed: number; issued: number; rejected: number; failed: number; }
export const totals = (): Totals => ({ processed: 0, issued: 0, rejected: 0, failed: 0 });
