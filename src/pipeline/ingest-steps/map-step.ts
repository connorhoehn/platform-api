// social-api/src/pipeline/ingest-steps/map-step.ts
//
// `ingest:map` step type. Calls the mapping engine on a previously-fetched
// file descriptor (from a prior `ingest:fetch` step in the same run) and
// surfaces the resulting `MappingDecision`.
//
// The step looks for `file` and (optionally) `content` on the run context,
// either from the immediately-prior fetch step's output or from the
// originating trigger payload. The mapping engine's real implementation
// is owned by Agent E (Mapping engine v2); this step is portable across
// whatever it ends up calling.

import type {
  FileContent,
  FileDescriptor,
} from '../../ingest/types';
import type {
  IMappingEngine,
  IngestMapConfig,
  StepContext,
  StepHandler,
  StepOutcome,
} from './types';

export function createMapStep(deps: { mappingEngine: IMappingEngine }): StepHandler<IngestMapConfig> {
  return async function ingestMapStep(
    config: IngestMapConfig,
    ctx: StepContext,
  ): Promise<StepOutcome> {
    const sourceId = config.sourceId ?? readStringFromTrigger(ctx, 'sourceId');
    if (!sourceId) {
      return {
        status: 'failed',
        error: 'ingest:map: sourceId is required (none in config or triggerPayload)',
        sourceHandle: 'error',
      };
    }

    const file = readFileDescriptor(ctx);
    if (!file) {
      return {
        status: 'failed',
        error: 'ingest:map: no file descriptor on context (run an ingest:fetch step first)',
        sourceHandle: 'error',
      };
    }

    const content = readFileContent(ctx);

    try {
      const decision = await deps.mappingEngine.map({ sourceId, file, content });
      return {
        status: 'completed',
        output: {
          mapping: decision,
          mappedTypeId: decision.targetTypeId,
          mappingMethod: decision.method,
          mappingConfidence: decision.confidence,
        },
        sourceHandle: 'out',
      };
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      return {
        status: 'failed',
        error: `ingest:map: ${msg}`,
        sourceHandle: 'error',
      };
    }
  };
}

function readStringFromTrigger(ctx: StepContext, key: string): string | undefined {
  const v = ctx.triggerPayload[key];
  return typeof v === 'string' ? v : undefined;
}

function readFileDescriptor(ctx: StepContext): FileDescriptor | undefined {
  // Walk the per-step output map for the most recent `file` entry. The
  // executor mirrors prior step outputs into `context.steps[<nodeId>]`;
  // we don't enforce an order here — first match wins.
  const direct = ctx.context['file'];
  if (isFileDescriptor(direct)) return direct;
  const steps = ctx.context['steps'] as Record<string, unknown> | undefined;
  if (steps) {
    for (const out of Object.values(steps)) {
      const f = (out as Record<string, unknown> | null)?.['file'];
      if (isFileDescriptor(f)) return f;
    }
  }
  return undefined;
}

function readFileContent(ctx: StepContext): FileContent | undefined {
  const direct = ctx.context['content'];
  if (isFileContent(direct)) return direct;
  const steps = ctx.context['steps'] as Record<string, unknown> | undefined;
  if (steps) {
    for (const out of Object.values(steps)) {
      const c = (out as Record<string, unknown> | null)?.['content'];
      if (isFileContent(c)) return c;
    }
  }
  return undefined;
}

function isFileDescriptor(v: unknown): v is FileDescriptor {
  if (!v || typeof v !== 'object') return false;
  const o = v as Record<string, unknown>;
  return typeof o['path'] === 'string' && typeof o['hash'] === 'string';
}

function isFileContent(v: unknown): v is FileContent {
  if (!v || typeof v !== 'object') return false;
  const o = v as Record<string, unknown>;
  return Buffer.isBuffer(o['data']);
}
