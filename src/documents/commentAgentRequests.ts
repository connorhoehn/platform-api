// platform-api/src/documents/commentAgentRequests.ts
//
// WS-J lane B: a comment handed to the document agent ("Ask agent" on a margin
// card, `/agent …` in its reply box). The agent-edit route links the run to the
// comment; when the run settles, the comment gets ONE reply saying what
// happened and — when the asker left "Resolve when applied" on and the run
// landed an edit — is resolved.
//
// Where the state lives:
//   - the comment row's `agentRequest` (document-comments): who asked, which
//     run, `pending` → addressed | refused | failed. The settle is a
//     compare-and-set on (runId, pending) — the exactly-once point.
//   - `idempotency-claims` row `comment-agent:<runId>` → {documentId,
//     commentId}, 7-day TTL: how a replica that never saw the run start (the
//     run resumed there after its owner died) finds the comment from a
//     terminal event that names only the run.
//
// Nothing here runs in a browser tab: a person who asks and closes the tab
// still gets the reply, and with two replicas whichever observes the terminal
// event first writes it; the other's CAS fails and it writes nothing.

import { ulid } from 'ulid';
import { documentCommentRepo } from '../repositories';
import type {
  CommentAgentRequestState,
  DocumentComment,
  DocumentCommentRepository,
} from '../repositories/DocumentCommentRepository';
import { publishSignal } from '../lib/event-catalog';
import { DynamoClaimStore, type ClaimStore } from '../deck/deckReviseRequests';
import { IDEMPOTENCY_CLAIMS_TABLE } from '../middleware/idempotency';
import log from '../lib/logger';

export const COMMENT_AGENT_LINK_TTL_SECONDS = 7 * 24 * 60 * 60;
export const COMMENT_AGENT = { userId: 'agent:haiku', displayName: 'Haiku', color: '#7c3aed' } as const;

export interface CommentRunLink { documentId: string; commentId: string; pipelineId: string }

let linkStore: ClaimStore | null = null;
function links(): ClaimStore {
  linkStore ??= new DynamoClaimStore(undefined, undefined, IDEMPOTENCY_CLAIMS_TABLE);
  return linkStore;
}
/** Test seam. */
export function setCommentLinkStore(next: ClaimStore | null): void { linkStore = next; }

const linkKey = (runId: string) => `comment-agent:${runId}`;

export async function linkRunToComment(runId: string, link: CommentRunLink): Promise<void> {
  await links().create(linkKey(runId), JSON.stringify(link), COMMENT_AGENT_LINK_TTL_SECONDS);
}

export async function commentForRun(runId: string): Promise<CommentRunLink | null> {
  try {
    const raw = await links().get(linkKey(runId));
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Partial<CommentRunLink>;
    if (typeof parsed.documentId !== 'string' || typeof parsed.commentId !== 'string') return null;
    return { documentId: parsed.documentId, commentId: parsed.commentId, pipelineId: String(parsed.pipelineId ?? '') };
  } catch (err) {
    log.warn({ runId, err: String(err) }, 'comment-agent link lookup failed');
    return null;
  }
}

const oneLine = (text: string, max: number) => {
  const flat = text.replace(/\s+/g, ' ').trim();
  return flat.length > max ? `${flat.slice(0, max - 1).trimEnd()}…` : flat;
};

/**
 * The instruction the pipeline sees: what the person typed, then the comment
 * it answers and — when the comment is anchored — the span to keep the edit to.
 * The model reads a numbered outline, so the scope is stated as the text to
 * find, not as offsets it cannot see.
 */
export function commentScopedInstruction(instruction: string, comment: Pick<DocumentComment, 'displayName' | 'text' | 'anchor'>): string {
  const lines = [instruction.trim(), '', `This addresses a comment by ${comment.displayName}: “${oneLine(comment.text, 600)}”`];
  const quote = comment.anchor?.quote?.trim();
  if (quote) {
    lines.push(`The comment is about this text: “${oneLine(quote, 500)}”. Change only the block(s) containing it unless the comment asks for more.`);
  }
  return lines.join('\n');
}

interface ApplyOp { op?: string; index?: number; text?: string; kind?: string }
export interface ApplyOutcome { applied?: number; skipped?: number; reason?: string; mode?: string; ops?: ApplyOp[] }

function describeOp(op: ApplyOp): string {
  const text = typeof op.text === 'string' && op.text.trim() ? ` “${oneLine(op.text, 60)}”` : '';
  switch (op.op) {
    case 'replaceBlock': return `rewrote block ${typeof op.index === 'number' ? op.index : '?'}${text ? ` to${text}` : ''}`;
    case 'insertBlock': return `inserted a ${op.kind ?? 'block'}${text}`;
    case 'appendBlock': return `added a ${op.kind ?? 'block'}${text}`;
    case 'deleteBlock': return `removed block ${typeof op.index === 'number' ? op.index : '?'}`;
    case 'setTitle': return `renamed the page${text ? ` to${text}` : ''}`;
    default: return op.op ?? 'changed the page';
  }
}

/** One line: what the run did, for the reply. Pure. */
export function summarizeApply(outcome: ApplyOutcome | null | undefined): string {
  const ops = Array.isArray(outcome?.ops) ? outcome!.ops!.filter((o) => o && o.op !== 'skip') : [];
  const count = typeof outcome?.applied === 'number' ? outcome.applied : ops.length;
  const verb = outcome?.mode === 'suggest' ? 'Suggested' : 'Made';
  if (ops.length === 0) return `${verb} ${count} change${count === 1 ? '' : 's'} on the page`;
  const first = describeOp(ops[0]);
  const more = ops.length > 1 ? ` (+${ops.length - 1} more)` : '';
  return `${verb} ${count} change${count === 1 ? '' : 's'}: ${first}${more}${outcome?.mode === 'suggest' ? ' — review it on the page' : ''}`;
}

export type RunSettlement =
  | { kind: 'completed'; outcome: ApplyOutcome | null }
  | { kind: 'rejected'; reason?: string }
  | { kind: 'failed'; reason: string }
  | { kind: 'cancelled' };

export function runPath(pipelineId: string, runId: string): string {
  return `/pipelines/${encodeURIComponent(pipelineId)}/runs/${encodeURIComponent(runId)}`;
}

/** The reply's words and the request's final state. Pure. */
export function settlementReply(settlement: RunSettlement, pipelineId: string, runId: string): { state: Exclude<CommentAgentRequestState, 'pending'>; text: string } {
  const link = `[run ${runId.slice(0, 8)}](${runPath(pipelineId, runId)})`;
  switch (settlement.kind) {
    case 'completed': {
      const applied = settlement.outcome?.applied;
      if (settlement.outcome && typeof applied === 'number' && applied === 0) {
        const why = settlement.outcome.reason ? oneLine(settlement.outcome.reason, 200) : 'the agent proposed no change';
        return { state: 'refused', text: `Not changed by ${link} — ${why}. Left open.` };
      }
      return { state: 'addressed', text: `Addressed by ${link} — ${summarizeApply(settlement.outcome)}.` };
    }
    case 'rejected':
      return { state: 'refused', text: `Not changed by ${link} — the edit was declined at approval${settlement.reason ? `: ${oneLine(settlement.reason, 160)}` : ''}. Left open.` };
    case 'failed':
      return { state: 'failed', text: `${link} failed — ${oneLine(settlement.reason || 'no reason given', 200)}. Left open.` };
    case 'cancelled':
      return { state: 'failed', text: `${link} was cancelled before it changed the page. Left open.` };
  }
}

type SettleRepo = Pick<DocumentCommentRepository, 'getComment' | 'settleAgentRequest' | 'createComment' | 'resolveThread'>;

/**
 * Writes the settle reply (and the resolve) for one run, at most once across
 * replicas. Returns what it did, for tests and logs.
 */
export async function settleCommentAgentRun(
  link: CommentRunLink,
  runId: string,
  settlement: RunSettlement,
  deps: { repo?: SettleRepo; publish?: typeof publishSignal; now?: () => string } = {},
): Promise<'settled' | 'resolved' | 'skipped'> {
  const repo = deps.repo ?? documentCommentRepo;
  const publish = deps.publish ?? publishSignal;
  const now = (deps.now ?? (() => new Date().toISOString()))();
  const root = await repo.getComment(link.documentId, link.commentId);
  const request = root?.agentRequest;
  if (!root || !request || request.runId !== runId || request.state !== 'pending') return 'skipped';

  const { state, text } = settlementReply(settlement, request.pipelineId || link.pipelineId, runId);
  const replyId = ulid();
  // The exactly-once point: only the caller that moves (runId, pending) writes.
  const won = await repo.settleAgentRequest(link.documentId, link.commentId, runId, state, replyId, now);
  if (!won) return 'skipped';

  const reply = await repo.createComment({
    commentId: replyId,
    documentId: link.documentId,
    parentCommentId: link.commentId,
    text,
    userId: COMMENT_AGENT.userId,
    displayName: COMMENT_AGENT.displayName,
    color: COMMENT_AGENT.color,
    timestamp: now,
  });
  publish(`doc-comments:${link.documentId}`, 'doc:comment_added', { documentId: link.documentId, comment: reply })
    .catch((err: unknown) => log.error({ event: 'doc:comment_added', err: String(err) }, 'signal publish failed'));

  const resolve = state === 'addressed' && request.resolveOnApply && !root.resolved;
  if (resolve) {
    // `resolvedBy` is what the card prints ("Resolved by …"); the agent has no
    // user record to look a name up from, so it is stored as its name.
    await repo.resolveThread(link.documentId, link.commentId, COMMENT_AGENT.displayName, now);
    publish(`doc-comments:${link.documentId}`, 'doc:comment_resolved', {
      documentId: link.documentId, commentId: link.commentId, resolved: true, resolvedBy: COMMENT_AGENT.displayName, resolvedAt: now,
    }).catch((err: unknown) => log.error({ event: 'doc:comment_resolved', err: String(err) }, 'signal publish failed'));
  }
  log.info({ runId, documentId: link.documentId, commentId: link.commentId, state, resolved: resolve }, 'comment agent request settled');
  return resolve ? 'resolved' : 'settled';
}
