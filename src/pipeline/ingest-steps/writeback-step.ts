// social-api/src/pipeline/ingest-steps/writeback-step.ts
//
// `ingest:writeback` step type. Given (documentId, change), enqueues a
// `WriteBackChange` on the writeback dispatcher. The dispatcher (Agent F)
// owns delivery — drivers may run out-of-process per the vision doc — so
// this step's job is to *enqueue* and surface the ack on the run context.
//
// The step does NOT block on the writeback completing against the source
// system. The pipeline's downstream nodes can include their own polling /
// approval logic if synchronous semantics are required.

import type {
  IWriteBackDispatcher,
  IngestWritebackConfig,
  StepContext,
  StepHandler,
  StepOutcome,
} from './types';

export function createWritebackStep(deps: {
  writebackDispatcher: IWriteBackDispatcher;
}): StepHandler<IngestWritebackConfig> {
  return async function ingestWritebackStep(
    config: IngestWritebackConfig,
    ctx: StepContext,
  ): Promise<StepOutcome> {
    const documentId = config.documentId
      ?? readStringFromContextOrTrigger(ctx, 'documentId');
    if (!documentId) {
      return {
        status: 'failed',
        error: 'ingest:writeback: documentId is required (none in config, context, or triggerPayload)',
        sourceHandle: 'error',
      };
    }

    const sourceRef = config.sourceRef
      ?? readStringFromContextOrTrigger(ctx, 'sourceRef')
      // Fall back to documentId — drivers that store the source-side id on
      // the document can re-derive it. Empty string is rejected by the
      // dispatcher contract; null/undefined here means "driver figures it out".
      ?? documentId;

    const change = config.change ?? ctx.context['change'];
    if (change === undefined || change === null) {
      return {
        status: 'failed',
        error: 'ingest:writeback: change payload is required (none in config or context.change)',
        sourceHandle: 'error',
      };
    }

    try {
      const ack = await deps.writebackDispatcher.enqueue({
        documentId,
        sourceRef,
        baseVersion: config.baseVersion,
        change,
        timestamp: new Date().toISOString(),
      });
      return {
        status: 'completed',
        output: {
          documentId,
          sourceRef,
          ackId: ack.ackId,
          enqueuedAt: ack.enqueuedAt,
        },
        sourceHandle: 'out',
      };
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      return {
        status: 'failed',
        error: `ingest:writeback: ${msg}`,
        sourceHandle: 'error',
      };
    }
  };
}

function readStringFromContextOrTrigger(ctx: StepContext, key: string): string | undefined {
  const fromContext = ctx.context[key];
  if (typeof fromContext === 'string') return fromContext;
  const fromTrigger = ctx.triggerPayload[key];
  if (typeof fromTrigger === 'string') return fromTrigger;
  // Walk per-step outputs as a last resort — `documentId` is the canonical
  // example: the prior `ingest:materialize` step writes it under its own
  // step output, which the executor mirrors into `context.steps[<nodeId>]`.
  const steps = ctx.context['steps'] as Record<string, unknown> | undefined;
  if (steps) {
    for (const out of Object.values(steps)) {
      const v = (out as Record<string, unknown> | null)?.[key];
      if (typeof v === 'string') return v;
    }
  }
  return undefined;
}
