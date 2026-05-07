// social-api/src/ingest/routes/ingestJiraWebhook.ts
//
// Public webhook endpoint for inbound Jira issue + comment updates.
// Mounted at /api/ingest/webhook/jira/:sourceId in app.ts BEFORE the auth
// middleware (Atlassian's webhook callers don't carry our JWT).
//
// Translates Jira's webhook payload into our normalized `IngestEvent`
// stream. The two webhook event families we care about:
//
//   jira:issue_created / jira:issue_updated   → 'sync.file.fetched'
//   comment_created / comment_updated         → 'sync.file.fetched'
//
// Each payload triggers a sync run via the engine handle (so the cursor
// advances and the canonical doc materializes), and the receiver emits
// per-file 'sync.file.fetched' IngestEvents on the bus so the operator UI
// reflects the inbound activity in real time.
//
// Companion to JiraSourceAdapter in ../adapters/jira.ts. Path mapping:
//
//   issue:    `${projectKey}/${issueKey}`
//   comment:  `${projectKey}/${issueKey}/comment/${commentId}`

import { Router, type Request, type Response } from 'express';
import { withContext } from '../../lib/logger';
import type { IngestEvent, IngestSource } from '../types';
import { issuePath, commentPath } from '../adapters/jira';

const log = withContext({ route: 'ingestJiraWebhook' });

// ─── Dependency interfaces ────────────────────────────────────────────────

export interface JiraWebhookSourceRepository {
  get(sourceId: string): Promise<IngestSource | null>;
}

export interface JiraWebhookSyncProducer {
  send(msg: { sourceId: string }): Promise<{ deduped: boolean }>;
}

export interface JiraWebhookEventBus {
  emit(event: IngestEvent): void;
}

export interface JiraWebhookDeps {
  sourceRepository: JiraWebhookSourceRepository;
  syncProducer: JiraWebhookSyncProducer;
  eventBus?: JiraWebhookEventBus;
}

// ─── Mapped change shape (also used by tests) ─────────────────────────────

export interface JiraChange {
  /** "issue" or "comment" — picked from the webhook event name. */
  kind: 'issue' | 'comment';
  /** Path inside our ingest namespace (issuePath / commentPath). */
  path: string;
  /** Jira issue key (e.g. "ENG-123"). */
  issueKey: string;
  /** Comment id when kind === 'comment'. */
  commentId?: string;
  /** Atlassian event name verbatim (e.g. "jira:issue_updated"). */
  webhookEvent: string;
}

// ─── Payload parsing ──────────────────────────────────────────────────────

interface JiraIssuePayload {
  webhookEvent?: string;
  issue_event_type_name?: string;
  issue?: { id?: string; key?: string; fields?: { project?: { key?: string } } };
  comment?: { id?: string; body?: string; created?: string; updated?: string };
}

/**
 * Translate a Jira webhook payload + the source's projectKey config into a
 * list of normalized JiraChange records. Atlassian fires one webhook per
 * event, so the typical result is 1 item; we still return an array so the
 * caller can handle batched payloads or future fan-out events uniformly.
 */
export function mapJiraPayload(
  body: unknown,
  projectKey: string,
): JiraChange[] {
  if (!body || typeof body !== 'object') return [];
  const p = body as JiraIssuePayload;
  const event = p.webhookEvent ?? '';

  const issueKey = p.issue?.key;
  if (!issueKey) return [];

  // Defensive: don't ingest cross-project events leaking from a shared webhook.
  const eventProjectKey = p.issue?.fields?.project?.key;
  if (eventProjectKey && eventProjectKey !== projectKey) return [];

  // Comment events carry both an issue and a comment payload. Atlassian's
  // event names: "comment_created", "comment_updated", "comment_deleted",
  // and the legacy "jira:issue_commented" variant.
  const isCommentEvent =
    event.startsWith('comment_') ||
    event === 'jira:issue_commented' ||
    p.issue_event_type_name === 'issue_commented';

  if (isCommentEvent && p.comment?.id) {
    return [
      {
        kind: 'comment',
        path: commentPath(projectKey, issueKey, p.comment.id),
        issueKey,
        commentId: p.comment.id,
        webhookEvent: event,
      },
    ];
  }

  // Issue created/updated/deleted/* — emit one issue change.
  if (event.startsWith('jira:issue_') || event === 'jira:issue' || event === '') {
    return [
      {
        kind: 'issue',
        path: issuePath(projectKey, issueKey),
        issueKey,
        webhookEvent: event,
      },
    ];
  }

  return [];
}

/** Lift one JiraChange into an `item.status` IngestEvent (best-fit). */
export function changeToIngestEvent(
  sourceId: string,
  change: JiraChange,
): IngestEvent {
  return {
    type: 'item.status',
    ingestId: `jira:${sourceId}:${change.path}`,
    status: 'queued',
    detail: `${change.kind} ${change.webhookEvent || 'change'} from Jira webhook`,
    timestamp: new Date().toISOString(),
  };
}

// ─── Factory ──────────────────────────────────────────────────────────────

/**
 * Mount Jira webhook handler at `/jira/:sourceId`. The factory takes
 * injected deps so it's unit-testable without booting the engine.
 */
export function createJiraWebhookRoutes(deps: JiraWebhookDeps): Router {
  const router = Router();

  router.post('/jira/:sourceId', async (req: Request, res: Response) => {
    const sourceId = req.params.sourceId;

    // 1. Look up source.
    const source = await deps.sourceRepository.get(sourceId);
    if (!source) {
      return res.status(404).json({ error: `source ${sourceId} not found` });
    }
    if (source.type !== 'jira') {
      return res.status(400).json({ error: `source ${sourceId} is not a jira source` });
    }

    const projectKey = String((source.config as Record<string, unknown>).projectKey ?? '');
    if (!projectKey) {
      return res.status(400).json({ error: 'source missing projectKey config' });
    }

    // 2. Map the payload into normalized JiraChange records.
    const changes = mapJiraPayload(req.body, projectKey);
    if (changes.length === 0) {
      return res.status(200).json({ status: 'no_changes' });
    }

    // 3. Emit one IngestEvent per change so the operator UI reflects the
    //    inbound activity in real time. Failures are logged but do not
    //    fail the request — webhook delivery semantics matter more than
    //    UI fan-out.
    if (deps.eventBus) {
      for (const change of changes) {
        try {
          deps.eventBus.emit(changeToIngestEvent(sourceId, change));
        } catch (err) {
          log.error(
            { err: err instanceof Error ? err.message : String(err), sourceId },
            'jira webhook event-bus emit failed',
          );
        }
      }
    }

    // 4. Trigger an actual sync. The cursor + adapter will fetch the
    //    issue/comment bodies; this keeps the webhook handler small.
    const { deduped } = await deps.syncProducer.send({ sourceId });

    log.info(
      { sourceId, changeCount: changes.length, deduped, projectKey },
      'jira webhook accepted',
    );

    return res.status(200).json({
      status: deduped ? 'sync_in_progress' : 'sync_triggered',
      changes,
    });
  });

  return router;
}
