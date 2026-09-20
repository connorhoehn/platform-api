import {
  WorkGraphCursorCodec as SharedWorkGraphCursorCodec,
  WorkGraphCursorError,
} from '@connorhoehn/realtime-modules/work-graph/server';
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

/**
 * Compatibility facade over the one shared cursor implementation consumed by
 * the gateway. The explicit timestamps keep platform route tests
 * deterministic without creating a second wire format.
 */
export function createWorkGraphCursorCodec(secret: string): WorkGraphCursorCodec {
  if (Buffer.byteLength(secret, 'utf8') < 32) {
    throw new RangeError('work graph cursor secret must contain at least 32 bytes');
  }
  return {
    issue(claims) {
      const codec = new SharedWorkGraphCursorCodec({
        secret,
        now: () => Date.parse(claims.issuedAt),
      });
      return codec.issue({
        scope: {
          organizationId: claims.organizationId,
          viewerId: claims.viewerId,
          personId: claims.personId,
          day: claims.day,
          timezone: claims.timezone,
          policyRevision: claims.policyRevision,
        },
        watermark: claims.watermark,
        subscriptionGeneration: claims.subscriptionGeneration,
        expiresAt: claims.expiresAt,
      });
    },
    verify(cursor, scope, now) {
      try {
        const codec = new SharedWorkGraphCursorCodec({
          secret,
          now: () => Date.parse(now),
        });
        return codec.verify(cursor, scope);
      } catch (error) {
        if (error instanceof WorkGraphCursorError) return null;
        throw error;
      }
    },
  };
}
