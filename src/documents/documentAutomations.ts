// Document automations — a "doc bot": an instruction a person writes once and
// a cadence, run against ONE document on a schedule by the same read → plan →
// review → approve → apply chain a /doc command uses, in suggest mode. What
// the bot proposes lands as tracked changes the person reviews in the
// document's Suggestions panel; nothing is applied without them.
//
// An automation IS a pipeline definition — the user's own, with a schedule
// trigger — so the schedule evaluator, the runs list, retry and the run page
// all work on it with nothing new. What this module adds is the shape: a
// definition built from the document pipeline, its trigger payload pinned to
// the document and instruction, and an `automation` block the routes read
// back rather than parsing the DAG.

import { randomUUID } from 'crypto';
import { buildDocumentAgentEditDefinition } from './documentAgentPipeline';
import { parseCronExpression, cronMatches } from '../services/scheduleEvaluator';

export const DOCUMENT_AUTOMATION_PREFIX = 'doc-auto:';
export const MAX_INSTRUCTION_LENGTH = 500;

export interface DocumentAutomation {
  id: string;
  documentId: string;
  instruction: string;
  /** 5-field cron, evaluated in the server's local time. */
  cron: string;
  createdAt: string;
  createdBy: string;
  createdByName: string;
  /** When it next fires, or null when the cron never matches in the next year. */
  nextRunAt: string | null;
}

/** The record stored on the definition; `nextRunAt` is derived on read. */
export type StoredAutomation = Omit<DocumentAutomation, 'nextRunAt'>;

export interface NewAutomationInput {
  documentId: string;
  instruction: string;
  cron: string;
  userId: string;
  userName: string;
}

export function automationId(documentId: string): string {
  return `${DOCUMENT_AUTOMATION_PREFIX}${documentId.slice(0, 8)}:${randomUUID().slice(0, 8)}`;
}

/** The pipeline definition an automation is. Status published, so the evaluator fires it. */
export function buildAutomationDefinition(input: NewAutomationInput, id = automationId(input.documentId)): Record<string, unknown> {
  const base = buildDocumentAgentEditDefinition(undefined, { approverUserId: input.userId });
  const nodes = (base.nodes as Array<Record<string, unknown>>).map((n) =>
    n.id === 'trigger'
      ? { ...n, data: { type: 'trigger', triggerType: 'schedule', schedule: input.cron, label: 'On a schedule' } }
      : n,
  );
  const stored: StoredAutomation = {
    id,
    documentId: input.documentId,
    instruction: input.instruction,
    cron: input.cron,
    createdAt: new Date().toISOString(),
    createdBy: input.userId,
    createdByName: input.userName,
  };
  return {
    ...base,
    id,
    name: `Doc bot · ${input.instruction.length > 48 ? `${input.instruction.slice(0, 47)}…` : input.instruction}`,
    description: `Runs "${input.instruction}" against one document on the schedule ${input.cron}; the edit lands as suggestions to review.`,
    nodes,
    triggerBinding: {
      event: 'schedule',
      schedule: input.cron,
      // What the run starts from — the same fields a /doc command posts.
      payload: {
        documentId: input.documentId,
        instruction: input.instruction,
        requestedBy: input.userId,
        requestedByName: input.userName,
        automationId: id,
      },
    },
    automation: stored,
  };
}

/** The automation a definition carries, or null for any other pipeline. */
export function automationOf(def: Record<string, unknown> | null | undefined): StoredAutomation | null {
  const a = def?.automation as Partial<StoredAutomation> | undefined;
  if (!a || typeof a.id !== 'string' || typeof a.documentId !== 'string' || typeof a.instruction !== 'string' || typeof a.cron !== 'string') return null;
  return {
    id: a.id,
    documentId: a.documentId,
    instruction: a.instruction,
    cron: a.cron,
    createdAt: typeof a.createdAt === 'string' ? a.createdAt : '',
    createdBy: typeof a.createdBy === 'string' ? a.createdBy : '',
    createdByName: typeof a.createdByName === 'string' ? a.createdByName : '',
  };
}

/** The next minute the cron matches after `from`, within a year; null when there is none. */
export function nextRunAt(cron: string, from: Date = new Date()): string | null {
  const parsed = parseCronExpression(cron);
  if (!parsed) return null;
  const t = new Date(from);
  t.setSeconds(0, 0);
  t.setMinutes(t.getMinutes() + 1);
  const limit = 366 * 24 * 60;
  for (let i = 0; i < limit; i++) {
    if (cronMatches(parsed, t)) return t.toISOString();
    t.setMinutes(t.getMinutes() + 1);
  }
  return null;
}

export function withNextRun(a: StoredAutomation, from?: Date): DocumentAutomation {
  return { ...a, nextRunAt: nextRunAt(a.cron, from) };
}

/** Validate what a route was sent. Returns the error, or null. */
export function validateAutomationInput(body: unknown): { instruction: string; cron: string } | { error: string } {
  const b = (body ?? {}) as Record<string, unknown>;
  const instruction = typeof b.instruction === 'string' ? b.instruction.trim() : '';
  const cron = typeof b.cron === 'string' ? b.cron.trim() : '';
  if (!instruction) return { error: 'instruction is required' };
  if (instruction.length > MAX_INSTRUCTION_LENGTH) return { error: `instruction must be at most ${MAX_INSTRUCTION_LENGTH} characters` };
  if (!cron) return { error: 'cron is required' };
  if (!parseCronExpression(cron)) return { error: 'cron must be a 5-field expression: minute hour day-of-month month day-of-week' };
  return { instruction, cron };
}
