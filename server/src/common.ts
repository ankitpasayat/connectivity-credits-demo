import { randomUUID } from 'node:crypto';

export class DomainError extends Error {
  constructor(public statusCode: number, message: string) { super(message); }
}
export type Row = Record<string, any>;
export const now = () => new Date().toISOString();
export const id = (prefix: string) => `${prefix}_${randomUUID().replaceAll('-', '')}`;
export const day = (offset = 0) => new Date(Date.now() + offset * 86_400_000).toISOString().slice(0, 10);
export function shiftDay(value: string, offset: number) {
  return new Date(Date.parse(value + 'T00:00:00Z') + offset * 86_400_000).toISOString().slice(0, 10);
}
// Semantic JSON equality is essential for pre-existing Python JSON (1.0 == 1).
export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record).sort().map(key => `${JSON.stringify(key)}:${canonical(record[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}
export function sameJson(stored: string, value: unknown) { return canonical(JSON.parse(stored)) === canonical(value); }
export const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
