// platform-api/src/services/pipeline-event-dispatcher.ts
//
// Wave A (2026-08-25): the missing connective tissue between platform
// events and pipeline runs. Pipelines have carried
// `triggerBinding: { event: 'document.finalize', ... }` in their
// definitions (the ingest templates seed them) since Phase 54, but
// nothing ever matched those bindings against live events — the only
// working triggers were manual, webhook, and (as of today) schedule.
//
// This module is deliberately NOT a queue subscriber for events that
// already have a durable consumer. The MQ transport is one-queue-per-
// event with competing consumers, so a second `catalog.subscribe()` to
// e.g. `activity.recorded` would STEAL messages from the activity
// consumer. Instead, the existing consumers call
// `dispatchPlatformEvent()` in-process after their own handling
// succeeds:
//
//   activity-consumer          → dispatch(payload.eventType, detail)
//                                 ('doc.finalize' → 'document.finalize')
//   transcription-summary-*    → dispatch('transcription:transcript.ready')
//   summary-projector          → dispatch('transcription:summary.ready')
//
// Matching: definition.status === 'published' AND
// triggerBinding.event ∈ aliases(eventName). When the binding also pins
// `documentTypeId`, the event payload must carry the same one — an
// event with no documentTypeId does NOT fire a type-pinned binding
// (conservative; mirrors IngestEventTrigger).
//
// Dispatch failures are per-pipeline: one bad definition can't stop
// sibling pipelines from firing, and errors never propagate to the
// calling consumer (a dispatch failure must not poison e.g. the
// activity drain into DDB or trigger MQ redelivery → duplicate runs).

import { pipelineDefinitionsCache } from '../pipeline/definitions-cache';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { recordPipelineTrigger } from '../observability/metrics';
import log from '../lib/logger';

/**
 * Activity-feed eventType → triggerBinding.event aliases. The frontend
 * publishes `doc.finalize` through the gateway ActivityService; the
 * pipeline templates bind `document.finalize`. Both spellings match.
 */
const ACTIVITY_ALIAS: Record<string, string> = {
  'doc.finalize': 'document.finalize',
  'doc.unlock': 'document.unlock',
  'doc.comment': 'document.comment',
  'doc.submit': 'document.submit',
};

/** All binding-event spellings that should fire for a live event name. */
export function bindingEventsFor(eventName: string): string[] {
  const alias = ACTIVITY_ALIAS[eventName];
  return alias ? [eventName, alias] : [eventName];
}

interface DispatchablePipelineShape {
  id?: unknown;
  status?: unknown;
  triggerBinding?: { event?: unknown; documentTypeId?: unknown };
}

export interface DispatchDeps {
  listDefinitions: () => readonly unknown[];
  getBridge: typeof getPipelineBridge;
}

const defaultDeps: DispatchDeps = {
  listDefinitions: () => pipelineDefinitionsCache.all(),
  getBridge: getPipelineBridge,
};

export interface DispatchResult {
  fired: Array<{ pipelineId: string; runId: string }>;
  failed: Array<{ pipelineId: string; error: string }>;
}

/**
 * Fire every published pipeline whose triggerBinding matches `eventName`.
 * Never throws; per-pipeline errors are logged and reported in the result.
 */
export async function dispatchPlatformEvent(
  eventName: string,
  payload: Record<string, unknown>,
  deps: DispatchDeps = defaultDeps,
): Promise<DispatchResult> {
  const result: DispatchResult = { fired: [], failed: [] };
  const wanted = new Set(bindingEventsFor(eventName));

  let definitions: readonly unknown[];
  try {
    definitions = deps.listDefinitions();
  } catch (err: unknown) {
    log.error(
      { eventName, errMessage: err instanceof Error ? err.message : String(err) },
      'pipeline-event-dispatcher: definitions listing failed',
    );
    return result;
  }

  const matches: Array<{ pipelineId: string; definition: unknown }> = [];
  for (const raw of definitions) {
    const d = raw as DispatchablePipelineShape;
    if (typeof d?.id !== 'string' || d.status !== 'published') continue;
    const tb = d.triggerBinding;
    if (!tb || typeof tb.event !== 'string' || !wanted.has(tb.event)) continue;
    if (typeof tb.documentTypeId === 'string' && tb.documentTypeId.length > 0) {
      if (payload.documentTypeId !== tb.documentTypeId) continue;
    }
    matches.push({ pipelineId: d.id, definition: raw });
  }
  if (matches.length === 0) return result;

  const bridge = deps.getBridge();
  if (!bridge?.trigger) {
    log.warn(
      { eventName, matchCount: matches.length },
      'pipeline-event-dispatcher: matching pipelines but bridge unavailable',
    );
    for (const m of matches) {
      result.failed.push({ pipelineId: m.pipelineId, error: 'pipeline bridge unavailable' });
    }
    return result;
  }

  for (const m of matches) {
    try {
      const out = await bridge.trigger({
        pipelineId: m.pipelineId,
        definition: m.definition,
        triggerPayload: { triggerType: eventName, event: eventName, ...payload },
        // Deliberately a synthetic actor (same convention as webhook:*
        // and schedule:*): event-driven runs must be distinguishable
        // from user-driven ones in the audit trail.
        triggeredBy: { userId: `event:${eventName}` },
      });
      recordPipelineTrigger();
      result.fired.push({ pipelineId: m.pipelineId, runId: out.runId });
      log.info(
        { eventName, pipelineId: m.pipelineId, runId: out.runId },
        'pipeline-event-dispatcher: fired',
      );
    } catch (err: unknown) {
      const errMessage = err instanceof Error ? err.message : String(err);
      result.failed.push({ pipelineId: m.pipelineId, error: errMessage });
      log.error(
        { eventName, pipelineId: m.pipelineId, errMessage },
        'pipeline-event-dispatcher: trigger failed',
      );
    }
  }
  return result;
}
