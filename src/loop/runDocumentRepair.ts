// Repair an /agent run whose document never reached its conversation.
//
// Runs from before the agent-document change (348f95d) left two kinds of
// damage, and a Done card with nothing to open is what both look like:
//
//   produced   the run made a page (`document:create:<step>` output) but the
//              row has no `channel` / the other members as editors, and the
//              card never turned into the document card;
//   legacy     the plan had a `kind: 'document'` step, but it ran as an
//              ordinary devbox agent — which cannot create pages, listed an
//              empty workspace and said DONE — so there is no page at all
//              (alice ↔ grace, 2026-09-28 21:50, "Blank page title it pizza
//              oven"). The page the step promised is created now, as
//              `document:create` would have: the step's title
//              (`documentStepTitle`), no body — the draft never happened — and
//              the card's run line says it was made on repair.
//
// Both then go through `putRunDocumentInConversation` — the function create
// time uses — so the repaired page is shared and announced exactly as a new
// one is. Idempotent end to end: a legacy page's id is derived from the run and
// step (a second pass finds it), the row step skips an attached row, the card
// step skips a card that already is this page's, and a repair never posts a
// card it cannot find.
//
// Callers: the run-status read (routes/index.ts, `scheduleRunDocumentRepair`,
// once per run per process) and the backfill (`repairAllRunDocuments`, behind
// POST /api/admin/agent-documents/repair).

import { createHash } from 'node:crypto';
import { GetCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { DOCUMENTS_TABLE } from '../deck/presentationDocuments';
import { docClient } from '../lib/aws-clients';
import log from '../lib/logger';
import type { PipelineRunItem } from '../repositories/PipelineRunRepository';
import { documentStepTitle, documentLoopNodeId, type LoopStep } from '../chat/agentLoopPlanner';
import { putRunDocumentInConversation, type ConversationDocumentDeps, type ConversationDocumentOutcome } from './conversationDocument';
import { CREATED_DOCUMENT_ICON, pageOps, putConversationPageRow, writeThroughGateway } from './documentCreateStepHandler';
import { conversationMembers } from '../chat/conversationMembers';

export const AGENT_LOOP_PIPELINE_ID = 'chat-agent-loop';
export const REPAIR_NOTE = 'Created when the run was repaired';

type Send = (command: unknown) => Promise<unknown>;
type Ctx = Record<string, unknown>;

const str = (v: unknown, limit = 512): string | undefined =>
  typeof v === 'string' && v.trim() ? v.trim().slice(0, limit) : undefined;

/** What the repair needs to know about one run, read off its stored rows. */
export interface RunDocumentRepairPlan {
  loopId: string;
  executorRunId: string;
  channel: string;
  requester: { userId: string; displayName?: string };
  instruction?: string;
  /** Pages the run made. */
  produced: Array<{ stepId: string; documentId: string; title: string }>;
  /** Document steps that never made their page (the devbox-agent era). */
  missing: Array<{ stepId: string; documentId: string; title: string }>;
}

/**
 * A legacy page's id, derived from the run and the step, as a UUID — so a
 * second repair of the same run finds the page the first one made instead of
 * making another.
 */
export function repairDocumentId(executorRunId: string, stepId: string): string {
  const h = createHash('sha1').update(`agent-document-repair:${executorRunId}:${stepId}`).digest('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-5${h.slice(13, 16)}-${((parseInt(h[16], 16) & 0x3) | 0x8).toString(16)}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

function contextOf(item: PipelineRunItem): Ctx {
  return ((item.checkpoint as { context?: Ctx } | undefined)?.context ?? {}) as Ctx;
}

function stepsOf(ctx: Ctx): LoopStep[] {
  const plan = (ctx.plan ?? (ctx.trigger as Ctx | undefined)?.plan) as { steps?: unknown } | undefined;
  return Array.isArray(plan?.steps) ? plan!.steps.filter((s): s is LoopStep => !!s && typeof s === 'object' && typeof (s as LoopStep).id === 'string') : [];
}

/**
 * The repair plan for a finished loop, or null when there is nothing a repair
 * may touch: not completed, no conversation, no requester, or no document step.
 */
export function planRunDocumentRepair(loopId: string, executor: PipelineRunItem): RunDocumentRepairPlan | null {
  if (executor.status !== 'completed') return null;
  const ctx = contextOf(executor);
  const trigger = (ctx.trigger ?? {}) as Ctx;
  const channel = str(ctx.chatChannel) ?? str(trigger.chatChannel);
  const userId = str(ctx.requestedBy) ?? str(trigger.requestedBy) ?? str(executor.triggeredBy);
  if (!channel || !userId || userId.startsWith('agent:')) return null;
  const displayName = str(ctx.requestedByName, 120) ?? str(trigger.requestedByName, 120);
  const summary = (executor.stepsSummary ?? {}) as Record<string, { status?: string; output?: Ctx }>;
  const produced: RunDocumentRepairPlan['produced'] = [];
  const missing: RunDocumentRepairPlan['missing'] = [];
  for (const step of stepsOf(ctx)) {
    if (step.kind !== 'document') continue;
    const node = summary[documentLoopNodeId(step.id)];
    const documentId = str(node?.output?.documentId);
    if (node?.status === 'completed' && documentId) {
      produced.push({ stepId: step.id, documentId, title: str(node.output?.title, 120) ?? documentStepTitle(step) });
    } else if (!node) {
      // The step never had a document:create node: the devbox agent ran it.
      missing.push({ stepId: step.id, documentId: repairDocumentId(executor.runId, step.id), title: documentStepTitle(step) });
    }
  }
  if (!produced.length && !missing.length) return null;
  return {
    loopId,
    executorRunId: executor.runId,
    channel,
    requester: { userId, ...(displayName ? { displayName } : {}) },
    instruction: str(ctx.instruction, 160) ?? str(trigger.instruction, 160),
    produced,
    missing,
  };
}

export interface RunDocumentRepairDeps extends ConversationDocumentDeps {
  getRun: (pipelineId: string, runId: string) => Promise<PipelineRunItem | null>;
  /** Write a legacy page's body (its title) through the collaboration channel. */
  writeBody?: typeof writeThroughGateway;
  now?: () => Date;
}

export interface RunDocumentRepairResult {
  loopId: string;
  executorRunId?: string;
  channel?: string;
  /** Nothing to repair, and why. */
  skipped?: 'no-run' | 'not-a-loop' | 'nothing-to-repair';
  documents: Array<{ documentId: string; title: string; created: boolean } & Pick<ConversationDocumentOutcome, 'document'> & { card: string }>;
}

function isConditionalFailure(err: unknown): boolean {
  return !!err && typeof err === 'object' && (err as { name?: string }).name === 'ConditionalCheckFailedException';
}

/**
 * Repair one run, asked about by either id: the loop id the card carries (its
 * row points at the executor through `agentLoopRunId`) or the executor's own.
 */
export async function repairRunDocuments(
  pipelineId: string,
  runId: string,
  deps: RunDocumentRepairDeps,
  opts: { apply?: boolean } = {},
): Promise<RunDocumentRepairResult> {
  const apply = opts.apply !== false;
  const row = await deps.getRun(pipelineId, runId);
  if (!row) return { loopId: runId, skipped: 'no-run', documents: [] };
  const pointer = typeof row.agentLoopRunId === 'string' && row.agentLoopRunId && row.agentLoopRunId !== row.runId ? row.agentLoopRunId : undefined;
  const executor = pointer ? await deps.getRun(pipelineId, pointer) : row;
  if (!executor) return { loopId: runId, skipped: 'no-run', documents: [] };
  const loopId = pointer ? runId : (str(contextOf(executor).loopId) ?? runId);
  const plan = planRunDocumentRepair(loopId, executor);
  if (!plan) return { loopId, executorRunId: executor.runId, skipped: 'nothing-to-repair', documents: [] };

  const documents: RunDocumentRepairResult['documents'] = [];
  const base = {
    channel: plan.channel,
    requester: plan.requester,
    runIds: [plan.loopId, plan.executorRunId],
    pipelineId,
    instruction: plan.instruction,
    icon: CREATED_DOCUMENT_ICON,
    postIfNoCard: false,
  };
  for (const doc of plan.produced) {
    if (!apply) { documents.push({ ...doc, created: false, document: 'already', card: 'dry-run' }); continue; }
    const out = await putRunDocumentInConversation({ ...base, documentId: doc.documentId, title: doc.title }, deps);
    documents.push({ documentId: doc.documentId, title: doc.title, created: false, document: out.document, card: out.card.outcome });
  }
  for (const doc of plan.missing) {
    if (!apply) {
      // A dry run still says whether an earlier repair already made the page.
      const found = await (deps.documents ?? (docClient as unknown as { send: Send })).send(new GetCommand({ TableName: DOCUMENTS_TABLE, Key: { documentId: doc.documentId } })) as { Item?: unknown };
      documents.push({ documentId: doc.documentId, title: doc.title, created: !found?.Item, document: found?.Item ? 'already' : 'missing', card: 'dry-run' });
      continue;
    }
    let created = false;
    const members = deps.members ?? ((channel: string, requester: string) => conversationMembers(channel, requester));
    const editors = await members(plan.channel, plan.requester.userId);
    try {
      await putConversationPageRow(deps.documents ?? (docClient as unknown as { send: Send }), {
        documentId: doc.documentId,
        title: doc.title,
        requestedBy: plan.requester.userId,
        requestedByName: plan.requester.displayName,
        channel: plan.channel,
        editors,
        at: (deps.now?.() ?? new Date()).toISOString(),
        runId: plan.executorRunId,
      });
      created = true;
    } catch (err) {
      if (!isConditionalFailure(err)) throw err; // an earlier repair made it
    }
    if (created) {
      await (deps.writeBody ?? writeThroughGateway)(doc.documentId, doc.title, pageOps({ title: doc.title, blocks: [] }), plan.requester.userId)
        .catch((err) => log.warn({ documentId: doc.documentId, err: String(err) }, 'repair: the page title could not be written through the collaboration channel'));
    }
    const out = await putRunDocumentInConversation({ ...base, documentId: doc.documentId, title: doc.title, note: REPAIR_NOTE }, { ...deps, members: async () => editors });
    documents.push({ documentId: doc.documentId, title: doc.title, created, document: out.document, card: out.card.outcome });
  }
  log.info({ loopId: plan.loopId, executorRunId: plan.executorRunId, channel: plan.channel, documents }, 'agent run documents repaired');
  return { loopId: plan.loopId, executorRunId: plan.executorRunId, channel: plan.channel, documents };
}

// ─── Read time ────────────────────────────────────────────────────────────

const attempted = new Map<string, number>();
const RETRY_AFTER_MS = 60_000;

/**
 * A Done agent-loop card was read: repair its run in the background, once per
 * run per process (a failure is retried a minute later). Never throws, never
 * delays the read.
 */
export function scheduleRunDocumentRepair(
  pipelineId: string,
  runId: string,
  status: unknown,
  deps: RunDocumentRepairDeps,
  now: () => number = Date.now,
): Promise<RunDocumentRepairResult | null> {
  if (pipelineId !== AGENT_LOOP_PIPELINE_ID || status !== 'completed' || !runId) return Promise.resolve(null);
  const last = attempted.get(runId);
  if (last !== undefined && (last === Infinity || now() - last < RETRY_AFTER_MS)) return Promise.resolve(null);
  attempted.set(runId, now());
  return repairRunDocuments(pipelineId, runId, deps)
    .then((result) => { attempted.set(runId, Infinity); return result; })
    .catch((err) => {
      log.warn({ runId, err: err instanceof Error ? err.message : String(err) }, 'agent run document repair failed; will retry on a later read');
      return null;
    });
}

/** Test seam: forget which runs were attempted. */
export function resetRunDocumentRepairMemo(): void {
  attempted.clear();
}

// ─── Backfill ─────────────────────────────────────────────────────────────

export interface RunDocumentBackfillResult {
  scanned: number;
  repaired: RunDocumentRepairResult[];
  failed: Array<{ loopId: string; error: string }>;
}

/**
 * Every /agent loop row (the accept-time rows that point at an executor),
 * repaired. `apply: false` reports what would be done.
 */
export async function repairAllRunDocuments(
  deps: RunDocumentRepairDeps & { query?: Send },
  opts: { apply?: boolean; limit?: number } = {},
): Promise<RunDocumentBackfillResult> {
  const query = deps.query ?? ((command: unknown) => docClient.send(command as never) as Promise<unknown>);
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { tableName } = require('../lib/ddb-table-name') as typeof import('../lib/ddb-table-name');
  const table = tableName('pipeline-runs');
  const loops: PipelineRunItem[] = [];
  let key: Record<string, unknown> | undefined;
  do {
    const page = await query(new QueryCommand({
      TableName: table,
      KeyConditionExpression: 'pipelineId = :p',
      ExpressionAttributeValues: { ':p': AGENT_LOOP_PIPELINE_ID },
      ...(key ? { ExclusiveStartKey: key } : {}),
    })) as { Items?: PipelineRunItem[]; LastEvaluatedKey?: Record<string, unknown> };
    for (const item of page.Items ?? []) {
      if (typeof item.agentLoopRunId === 'string' && item.agentLoopRunId && item.agentLoopRunId !== item.runId) loops.push(item);
    }
    key = page.LastEvaluatedKey;
  } while (key && (!opts.limit || loops.length < opts.limit));

  const result: RunDocumentBackfillResult = { scanned: loops.length, repaired: [], failed: [] };
  for (const loop of opts.limit ? loops.slice(0, opts.limit) : loops) {
    try {
      const out = await repairRunDocuments(AGENT_LOOP_PIPELINE_ID, loop.runId, deps, { apply: opts.apply });
      if (!out.skipped) result.repaired.push(out);
    } catch (err) {
      result.failed.push({ loopId: loop.runId, error: err instanceof Error ? err.message : String(err) });
    }
  }
  return result;
}

