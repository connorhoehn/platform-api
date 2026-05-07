// social-api/src/ingest/audit/append.ts
//
// Single-call helper for appending a row to the ingest audit log. Used by
// hub#2215's call-site instrumentation pass; the repository itself lives
// at ingest/repositories/IngestAuditRepository.ts.
//
// Errors are caught and logged so a stuck audit table can never block a
// legitimate operator action — audit is best-effort, not blocking.

import { randomUUID } from 'crypto';
import { ingestAuditRepo } from '../repositories';
import { withContext } from '../../lib/logger';
import type {
  IngestAuditEntry,
  IngestAuditEventType,
  IngestAuditTargetType,
} from '../types';

const log = withContext({ component: 'ingest.audit.append' });

export interface AppendAuditArgs {
  actor: string;
  eventType: IngestAuditEventType;
  targetType: IngestAuditTargetType;
  targetId: string;
  summary: string;
  detail?: Record<string, unknown>;
}

/**
 * Append a row to the ingest audit log. Errors are logged but never
 * rethrown — instrumentation must not break the call sites it instruments.
 * Returns the inserted entry (for tests + read-after-write).
 */
export async function appendAudit(args: AppendAuditArgs): Promise<IngestAuditEntry | null> {
  const entry: IngestAuditEntry = {
    auditId: randomUUID(),
    createdAt: new Date().toISOString(),
    actor: args.actor,
    eventType: args.eventType,
    targetType: args.targetType,
    targetId: args.targetId,
    summary: args.summary,
    detail: args.detail ?? {},
  };
  try {
    await ingestAuditRepo.put(entry);
    return entry;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    log.error({ err: msg, eventType: args.eventType, targetId: args.targetId }, 'audit append failed');
    return null;
  }
}
