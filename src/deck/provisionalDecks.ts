// Provisional decks (realtime-examples Loop 38 ppt, NFR #233).
//
// The All documents composer's "/workflow presentation-deck" creates the
// presentation BEFORE the run: the run draft, its pane and Dispatch live on a
// document. A draft that is never dispatched (or whose run never writes) left
// an empty presentation behind for good (live 2026-09-24, audit-l3).
//
// So the target is marked provisional when its draft is first saved — only
// when the client says the document was created for this draft
// (`provisionalTarget: true`) AND the row agrees: a presentation, created by
// the same person in the last few minutes, with no revision yet. An existing
// deck is never marked. The first revision written into it removes the mark
// (presentationDocuments.ts `appendRevision`), and it is an ordinary deck.
//
// The sweep leader (ClusterSweeper, one replica under `platform-sweep-leader`)
// deletes a provisional deck that still has no revision once its draft has
// been idle past the TTL. The delete is conditional on exactly that (the mark
// for that draft, and no revision), so a deck that got its first revision a
// moment ago, or a second sweeper racing a lapsed lease, deletes nothing.

import { DeleteCommand, ScanCommand, UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { docClient as defaultDocClient } from '../lib/aws-clients';
import log from '../lib/logger';
import { DOCUMENTS_TABLE, PRESENTATION_DOCUMENT_TYPE } from './presentationDocuments';

/** A mark only lands on a document created this recently (the composer creates it, then saves the draft). */
export const PROVISIONAL_MARK_WINDOW_MS = 10 * 60 * 1000;
/** Idle time after which an empty provisional deck is reaped. */
export const DEFAULT_PROVISIONAL_DECK_TTL_MS = 60 * 60 * 1000;

export interface ProvisionalMark { draftId: string; by: string; since: string }

const NO_REVISION = 'attribute_not_exists(headRevision) AND attribute_not_exists(revisionCount) AND (attribute_not_exists(revisions) OR size(revisions) = :zero)';

const isConditionFailure = (err: unknown): boolean => (err as { name?: string })?.name === 'ConditionalCheckFailedException';

/**
 * Marks `documentId` provisional for `draftId`. False when the row is not an
 * empty presentation this person just created (nothing is marked then).
 */
export async function markProvisionalDeck(input: {
  documentId: string; draftId: string; userId: string; now: string;
  docClient?: DynamoDBDocumentClient;
}): Promise<boolean> {
  const client = input.docClient ?? defaultDocClient;
  const recent = new Date(Date.parse(input.now) - PROVISIONAL_MARK_WINDOW_MS).toISOString();
  try {
    await client.send(new UpdateCommand({
      TableName: DOCUMENTS_TABLE,
      Key: { documentId: input.documentId },
      UpdateExpression: 'SET provisional = :mark',
      ConditionExpression: `attribute_exists(documentId) AND #type = :type AND createdBy = :by AND createdAt >= :recent AND attribute_not_exists(provisional) AND ${NO_REVISION}`,
      ExpressionAttributeNames: { '#type': 'type' },
      ExpressionAttributeValues: {
        ':mark': { draftId: input.draftId, by: input.userId, since: input.now } satisfies ProvisionalMark,
        ':type': PRESENTATION_DOCUMENT_TYPE, ':by': input.userId, ':recent': recent, ':zero': 0,
      },
    }));
    return true;
  } catch (err) {
    if (isConditionFailure(err)) return false;
    throw err;
  }
}

export interface ProvisionalDraftLookup {
  get(documentId: string, draftId: string): Promise<{ status: string; updatedAt?: string; dispatchedAt?: string; dispatchStartedAt?: string } | null>;
  /** Every draft on the document, so a reaped deck leaves none behind. */
  listForDocument(documentId: string): Promise<Array<{ draftId: string }>>;
  delete(documentId: string, draftId: string): Promise<void>;
}

export interface ProvisionalSweepResult {
  scanned: number;
  reaped: string[];
  kept: number;
  /** Run drafts deleted along with their reaped deck. */
  draftsRemoved: number;
  /** Draft deletes that failed (logged; the deck still counts as reaped). */
  draftsFailed: number;
}

/**
 * After a reaped deck's row is gone, delete its run drafts: the marked one and
 * any other listed for the document. Without this the draft stayed in
 * `document-run-drafts` with no document to hang off (live 2026-09-25). Never
 * throws; a failure is logged and counted, and the next sweep will not see the
 * document again, so the draft is left for an operator.
 */
async function removeDraftsOf(drafts: ProvisionalDraftLookup, documentId: string, markedDraftId: string, result: ProvisionalSweepResult): Promise<void> {
  const ids = new Set<string>([markedDraftId]);
  try {
    for (const d of await drafts.listForDocument(documentId)) if (d.draftId) ids.add(d.draftId);
  } catch (err) {
    log.warn({ documentId, draftId: markedDraftId, err: err instanceof Error ? err.message : String(err) }, 'provisional deck sweep: listing the reaped deck\'s drafts failed; deleting the marked one only');
  }
  for (const draftId of ids) {
    try {
      await drafts.delete(documentId, draftId);
      result.draftsRemoved += 1;
    } catch (err) {
      result.draftsFailed += 1;
      log.warn({ documentId, draftId, err: err instanceof Error ? err.message : String(err) }, 'provisional deck sweep: run draft of a reaped deck not deleted');
    }
  }
}

/**
 * One pass: every provisional row with no revision whose draft has been idle
 * past `ttlMs` is deleted. A draft mid-dispatch is never reaped.
 */
export async function sweepProvisionalDecks(opts: {
  drafts: ProvisionalDraftLookup;
  ttlMs?: number;
  now?: () => number;
  docClient?: DynamoDBDocumentClient;
}): Promise<ProvisionalSweepResult> {
  const client = opts.docClient ?? defaultDocClient;
  const ttlMs = opts.ttlMs ?? DEFAULT_PROVISIONAL_DECK_TTL_MS;
  const nowMs = (opts.now ?? Date.now)();
  const result: ProvisionalSweepResult = { scanned: 0, reaped: [], kept: 0, draftsRemoved: 0, draftsFailed: 0 };
  let startKey: Record<string, unknown> | undefined;
  do {
    const page = await client.send(new ScanCommand({
      TableName: DOCUMENTS_TABLE,
      FilterExpression: `attribute_exists(provisional) AND ${NO_REVISION}`,
      ProjectionExpression: 'documentId, provisional',
      ExpressionAttributeValues: { ':zero': 0 },
      ...(startKey ? { ExclusiveStartKey: startKey } : {}),
    }));
    for (const item of (page.Items ?? []) as Array<{ documentId: string; provisional?: Partial<ProvisionalMark> }>) {
      result.scanned += 1;
      const mark = item.provisional;
      if (!mark?.draftId || !mark.since) { result.kept += 1; continue; }
      const draft = await opts.drafts.get(item.documentId, mark.draftId);
      if (draft?.status === 'dispatching') { result.kept += 1; continue; }
      const lastTouched = Math.max(...[mark.since, draft?.updatedAt, draft?.dispatchedAt, draft?.dispatchStartedAt]
        .map((at) => (at ? Date.parse(at) : 0)).filter((ms) => Number.isFinite(ms)));
      if (nowMs - lastTouched < ttlMs) { result.kept += 1; continue; }
      try {
        await client.send(new DeleteCommand({
          TableName: DOCUMENTS_TABLE,
          Key: { documentId: item.documentId },
          ConditionExpression: `provisional.draftId = :draftId AND ${NO_REVISION}`,
          ExpressionAttributeValues: { ':draftId': mark.draftId, ':zero': 0 },
        }));
      } catch (err) {
        if (!isConditionFailure(err)) throw err;
        result.kept += 1; // it got a revision (or a new mark) since the scan read it
        continue; // and its drafts stay: only a deleted deck's drafts go
      }
      result.reaped.push(item.documentId);
      await removeDraftsOf(opts.drafts, item.documentId, mark.draftId, result);
    }
    startKey = page.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (startKey);
  if (result.reaped.length) log.info({ reaped: result.reaped, kept: result.kept, draftsRemoved: result.draftsRemoved, draftsFailed: result.draftsFailed, ttlMs }, 'provisional deck sweep: empty abandoned decks deleted');
  return result;
}

/** ClusterSweeper task: at most one pass per `everyMs` (default 10 min). */
export function createProvisionalDeckSweepTask(opts: {
  drafts: ProvisionalDraftLookup; ttlMs?: number; everyMs?: number; now?: () => number; docClient?: DynamoDBDocumentClient;
}): { name: string; run: () => Promise<void> } {
  const everyMs = opts.everyMs ?? 10 * 60 * 1000;
  const now = opts.now ?? Date.now;
  let lastRunMs = -Infinity;
  return {
    name: 'provisional-deck-sweep',
    run: async () => {
      if (now() - lastRunMs < everyMs) return;
      lastRunMs = now();
      await sweepProvisionalDecks({ ...opts, now });
    },
  };
}
