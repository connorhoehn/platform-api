// platform-api/src/services/summary-projector.ts
//
// Wave A (2026-08-25), Flow 1 "retro built from the call": when a
// meeting-summary is ready, project its `## Action Items` table onto the
// document the call was bound to, as real section items — the same rows
// the doc-editor's TaskItem UI and GET /api/items/mine serve.
//
// Single durable consumer of `transcription:summary.ready` (the MQ
// transport is one-queue-per-event with competing consumers, so the
// pipeline dispatch for this event ALSO lives here — after projection,
// `dispatchPlatformEvent` fires any summary-bound pipelines).
//
// Document binding: doc-editor calls use the documentId as the lobbyName
// (videoSessions accepts `documentId` as the lobby alias), so a
// recording's `lobbyName` IS the candidate documentId. We only project
// when that document actually has sections (plain hangout lobbies don't).
//
// Owner resolution: summary Action-Items owners are speaker labels
// (display names). We resolve them against the recording's participant
// roster (displayName → userId, case-insensitive) so `assignee` is a
// real userId whenever possible and items show up in /api/items/mine;
// unresolvable owners keep the raw label.
//
// Idempotency: redelivery re-parses the same summary — before inserting
// we list the target section's existing items and skip texts that are
// already present.

import type { EventEnvelope, Subscription } from '@connorhoehn/event-catalog';
import { catalog } from '../lib/event-catalog';
import {
  callRecordingRepo,
  documentSectionRepo,
  sectionItemRepo,
} from '../repositories';
import { dispatchPlatformEvent } from './pipeline-event-dispatcher';
import log from '../lib/logger';

export const ACTION_ITEMS_SECTION_ID = 'action-items';

interface SummaryReadyPayload {
  recordingId: string;
  callId: string;
  channelArn?: string;
  lobbyName?: string;
  summaryChars?: number;
}

export interface ParsedActionItem {
  owner: string;
  action: string;
  deadline?: string;
}

/**
 * Parse the `## Action Items` markdown table the summary prompt mandates:
 *
 *   | Owner | Action | Deadline |
 *   |-------|--------|----------|
 *   | Name  | What they committed to do | When (or "TBD") |
 *
 * Returns [] when the section is absent (the prompt omits empty sections).
 * Exposed for tests.
 */
export function parseActionItems(summaryMarkdown: string): ParsedActionItem[] {
  const lines = summaryMarkdown.split('\n');
  const headerIdx = lines.findIndex((l) => /^##\s+Action Items\s*$/i.test(l.trim()));
  if (headerIdx === -1) return [];

  const items: ParsedActionItem[] = [];
  for (let i = headerIdx + 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (/^##\s/.test(line)) break; // next section
    if (!line.startsWith('|')) {
      if (line === '' || items.length === 0) continue;
      break;
    }
    // Split a `| a | b | c |` row; drop the empty edge cells.
    const cells = line.split('|').map((c) => c.trim());
    if (cells.length < 4) continue;
    const [owner, action, deadline] = [cells[1], cells[2], cells[3] ?? ''];
    // Skip the header row and the |---|---| separator.
    if (/^-+$/.test(owner.replace(/\s/g, '')) || /^owner$/i.test(owner)) continue;
    if (!action) continue;
    const item: ParsedActionItem = { owner, action };
    if (deadline && !/^tbd$/i.test(deadline)) item.deadline = deadline;
    items.push(item);
  }
  return items;
}

interface ProjectorDeps {
  recordings: Pick<typeof callRecordingRepo, 'getRecording'>;
  sections: Pick<typeof documentSectionRepo, 'getSectionsForDocument'>;
  items: Pick<typeof sectionItemRepo, 'createItem' | 'getItemsForSection'>;
  dispatch: typeof dispatchPlatformEvent;
}

const defaultDeps: ProjectorDeps = {
  recordings: callRecordingRepo,
  sections: documentSectionRepo,
  items: sectionItemRepo,
  dispatch: dispatchPlatformEvent,
};

/**
 * Project one summary onto its call-bound document. Returns what happened
 * (for logs/tests). Exposed so tests can drive it without a subscription.
 */
export async function projectSummary(
  payload: SummaryReadyPayload,
  deps: ProjectorDeps = defaultDeps,
): Promise<{ projected: number; skipped: number; documentId?: string; reason?: string }> {
  const { recordingId, callId } = payload;
  const detail = await deps.recordings.getRecording(callId, recordingId);
  if (!detail?.summaryMarkdown) {
    return { projected: 0, skipped: 0, reason: 'recording or summary missing' };
  }

  const documentId = payload.lobbyName ?? detail.lobbyName;
  if (!documentId) {
    return { projected: 0, skipped: 0, reason: 'no lobbyName on recording' };
  }

  // Only doc-bound calls project — a plain hangout lobby has no sections.
  const sections = await deps.sections.getSectionsForDocument(documentId);
  if (sections.length === 0) {
    return { projected: 0, skipped: 0, documentId, reason: 'lobby is not a document' };
  }

  const actionItems = parseActionItems(detail.summaryMarkdown);
  if (actionItems.length === 0) {
    return { projected: 0, skipped: 0, documentId, reason: 'no action items in summary' };
  }

  // Owner label → userId via the participant roster.
  const byDisplayName = new Map<string, string>();
  for (const p of detail.participants ?? []) {
    if (p.displayName && p.userId) {
      byDisplayName.set(p.displayName.trim().toLowerCase(), p.userId);
    }
  }

  // Prefer a real section literally titled "Action Items" when the
  // document has one; otherwise use the well-known synthetic section id.
  const actionSection = sections.find(
    (s) => /action items/i.test((s as { title?: string }).title ?? ''),
  );
  const sectionId = actionSection
    ? (actionSection as { sectionId: string }).sectionId
    : ACTION_ITEMS_SECTION_ID;

  const existing = await deps.items.getItemsForSection(documentId, sectionId);
  const existingTexts = new Set(existing.map((i) => i.text.trim().toLowerCase()));

  let projected = 0;
  let skipped = 0;
  for (const item of actionItems) {
    if (existingTexts.has(item.action.trim().toLowerCase())) {
      skipped++;
      continue;
    }
    // Resolved → a real userId; unresolved → the raw speaker label. `assignee`
    // keeps whichever it was (unchanged), and `assigneeId` is written ONLY when
    // we actually resolved an identity — it is the key GET /api/items/mine is
    // now keyed on, and a speaker label in it would be a lie the index cannot
    // detect.
    const resolvedId = byDisplayName.get(item.owner.trim().toLowerCase());
    const assignee = resolvedId ?? item.owner;
    await deps.items.createItem({
      documentId,
      sectionId,
      text: item.action,
      assignee,
      ...(resolvedId ? { assigneeId: resolvedId } : {}),
      category: 'action-item',
      ...(item.deadline ? { dueDate: item.deadline } : {}),
    });
    projected++;
  }
  return { projected, skipped, documentId };
}

export function startSummaryProjector(
  opts: {
    _catalog?: { subscribe: typeof catalog.subscribe };
    _deps?: ProjectorDeps;
  } = {},
): { stop: () => Promise<void> } {
  const cat = opts._catalog ?? catalog;
  const deps = opts._deps ?? defaultDeps;

  const subscription: Subscription = cat.subscribe<SummaryReadyPayload>(
    'transcription:summary.ready',
    async (envelope: EventEnvelope<SummaryReadyPayload>) => {
      const payload = envelope.payload;
      try {
        const outcome = await projectSummary(payload, deps);
        log.info(
          { recordingId: payload.recordingId, ...outcome },
          '[summary-projector] projection done',
        );
      } catch (err) {
        // Projection is retryable (DDB blips) — rethrow so the durable
        // transport redelivers; the existing-text dedupe makes the retry
        // safe. But dispatch below must NOT run twice on such retries…
        log.error(
          {
            err: err instanceof Error ? err.message : String(err),
            recordingId: payload.recordingId,
          },
          '[summary-projector] projection failed; will redeliver',
        );
        throw err;
      }
      // …so pipelines fire only after a successful projection pass.
      // dispatchPlatformEvent never throws.
      await deps.dispatch('transcription:summary.ready', { ...payload });
    },
  );

  return {
    async stop() {
      try {
        await subscription.unsubscribe();
      } catch (err) {
        log.warn(
          { err: err instanceof Error ? err.message : String(err) },
          '[summary-projector] unsubscribe failed',
        );
      }
    },
  };
}
