// platform-api/src/pipeline/ingest-steps/fetch-step.ts
//
// `ingest:fetch` step type. Given (sourceId, path), calls the source
// adapter's `fetch()` and surfaces the resulting `FileDescriptor` +
// `FileContent` into the run context.
//
// Falls back to `triggerPayload.sourceId` and `triggerPayload.event.filePath`
// when the step config omits them — useful for ingest-event-triggered runs
// where the originating event already carries those fields.

import type {
  ISourceFetcher,
  IngestFetchConfig,
  StepContext,
  StepHandler,
  StepOutcome,
} from './types';

export function createFetchStep(deps: { sourceFetcher: ISourceFetcher }): StepHandler<IngestFetchConfig> {
  return async function ingestFetchStep(
    config: IngestFetchConfig,
    ctx: StepContext,
  ): Promise<StepOutcome> {
    const sourceId = config.sourceId ?? readSourceIdFromTrigger(ctx);
    const path = config.path ?? readPathFromTrigger(ctx);

    if (!sourceId) {
      return {
        status: 'failed',
        error: 'ingest:fetch: sourceId is required (none in config or triggerPayload)',
        sourceHandle: 'error',
      };
    }
    if (!path) {
      return {
        status: 'failed',
        error: 'ingest:fetch: path is required (none in config or triggerPayload.event.filePath)',
        sourceHandle: 'error',
      };
    }

    try {
      const { file, content } = await deps.sourceFetcher.fetch(sourceId, path);
      return {
        status: 'completed',
        output: {
          sourceId,
          file,
          content,
        },
        sourceHandle: 'out',
      };
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      return {
        status: 'failed',
        error: `ingest:fetch: ${msg}`,
        sourceHandle: 'error',
      };
    }
  };
}

function readSourceIdFromTrigger(ctx: StepContext): string | undefined {
  const sid = ctx.triggerPayload['sourceId'];
  return typeof sid === 'string' ? sid : undefined;
}

function readPathFromTrigger(ctx: StepContext): string | undefined {
  // Path may live on `triggerPayload.path` (manual) or on the originating
  // ingest event under `event.filePath`. Prefer the explicit field.
  const direct = ctx.triggerPayload['path'];
  if (typeof direct === 'string') return direct;
  const event = ctx.triggerPayload['event'] as { filePath?: unknown } | undefined;
  if (event && typeof event.filePath === 'string') return event.filePath;
  return undefined;
}
