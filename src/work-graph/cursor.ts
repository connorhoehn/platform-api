import { createHmac, timingSafeEqual } from 'crypto';
import type { WorkGraphPolicyScope } from './policy';

export interface WorkGraphCursorClaims extends WorkGraphPolicyScope {
  schemaVersion: 1;
  watermark: number;
  subscriptionGeneration: string;
  issuedAt: string;
  expiresAt: string;
}

export interface WorkGraphCursorCodec {
  issue(claims: WorkGraphCursorClaims): string;
  verify(cursor: string, scope: WorkGraphPolicyScope, now: string): WorkGraphCursorClaims | null;
}

function encode(value: string): string {
  return Buffer.from(value, 'utf8').toString('base64url');
}

function decode(value: string): string {
  return Buffer.from(value, 'base64url').toString('utf8');
}

function sameScope(claims: WorkGraphCursorClaims, scope: WorkGraphPolicyScope): boolean {
  return claims.organizationId === scope.organizationId
    && claims.viewerId === scope.viewerId
    && claims.personId === scope.personId
    && claims.day === scope.day
    && claims.timezone === scope.timezone
    && claims.policyRevision === scope.policyRevision;
}

function isClaims(value: unknown): value is WorkGraphCursorClaims {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const row = value as Record<string, unknown>;
  const keys = Object.keys(row);
  const expected = [
    'schemaVersion', 'organizationId', 'viewerId', 'personId', 'day', 'timezone',
    'policyRevision', 'watermark', 'subscriptionGeneration', 'issuedAt', 'expiresAt',
  ];
  return keys.length === expected.length
    && keys.every((key) => expected.includes(key))
    && row.schemaVersion === 1
    && expected.slice(1, 7).every((key) => typeof row[key] === 'string' && (row[key] as string).length > 0)
    && Number.isSafeInteger(row.watermark)
    && (row.watermark as number) >= 0
    && typeof row.subscriptionGeneration === 'string'
    && typeof row.issuedAt === 'string'
    && typeof row.expiresAt === 'string'
    && Number.isFinite(Date.parse(row.issuedAt))
    && Number.isFinite(Date.parse(row.expiresAt));
}

/** HMAC-signed, scope-bound cursor codec for snapshot-to-stream handoff. */
export function createWorkGraphCursorCodec(secret: string): WorkGraphCursorCodec {
  if (Buffer.byteLength(secret, 'utf8') < 32) {
    throw new RangeError('work graph cursor secret must contain at least 32 bytes');
  }
  const sign = (body: string): string => createHmac('sha256', secret).update(body).digest('base64url');
  return {
    issue(claims) {
      const body = encode(JSON.stringify(claims));
      return `wgc1.${body}.${sign(body)}`;
    },
    verify(cursor, scope, now) {
      const parts = cursor.split('.');
      if (parts.length !== 3 || parts[0] !== 'wgc1' || parts[1].length > 4_096 || parts[2].length > 128) return null;
      const expected = Buffer.from(sign(parts[1]), 'utf8');
      const received = Buffer.from(parts[2], 'utf8');
      if (expected.length !== received.length || !timingSafeEqual(expected, received)) return null;
      let value: unknown;
      try { value = JSON.parse(decode(parts[1])); } catch { return null; }
      if (!isClaims(value)
        || !sameScope(value, scope)
        || Date.parse(value.expiresAt) <= Date.parse(now)
        || Date.parse(value.issuedAt) > Date.parse(now)) return null;
      return value;
    },
  };
}
