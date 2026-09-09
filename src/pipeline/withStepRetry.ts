// Retry for custom step kinds.
//
// The executor honours `retryPolicy` on its built-in `llm` and `transform`
// nodes and dispatches every custom kind exactly once. A step that waits on
// the outside world — LVS composing a file, a speech engine, a gateway —
// needs the same policy, so the platform's custom handlers are wrapped here:
// the node's `data.retryPolicy` is read, attempts are announced on the bus
// the way the executor announces its own, and the last failure is the
// outcome. Belongs in distributed-core eventually (the executor should treat
// every kind alike); until that lands, this is the one place it lives.
import type { StepHandler, StepOutcome, PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';

export interface RetryPolicyLike {
  maxAttempts: number;
  backoffMs: number;
  backoffMultiplier: number;
  retryUnknown?: boolean;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export function readRetryPolicy(node: PipelineNode): RetryPolicyLike | null {
  const rp = (node.data as { retryPolicy?: unknown }).retryPolicy as Partial<RetryPolicyLike> | undefined;
  if (!rp || typeof rp.maxAttempts !== 'number' || rp.maxAttempts <= 1) return null;
  return { maxAttempts: rp.maxAttempts, backoffMs: rp.backoffMs ?? 1000, backoffMultiplier: rp.backoffMultiplier ?? 2, retryUnknown: rp.retryUnknown };
}

export function withStepRetry(handler: StepHandler, opts: { sleep?: (ms: number) => Promise<unknown> } = {}): StepHandler {
  const wait = opts.sleep ?? sleep;
  return {
    kind: handler.kind,
    async execute(node: PipelineNode, context: Record<string, unknown>, helpers: StepHandlerHelpers): Promise<StepOutcome> {
      const policy = readRetryPolicy(node);
      if (!policy) return handler.execute(node, context, helpers);
      const emit = helpers.emit as unknown as (event: string, payload: Record<string, unknown>) => Promise<void>;
      let last: StepOutcome = { status: 'failed', error: 'no attempts' };
      for (let attempt = 1; attempt <= policy.maxAttempts; attempt++) {
        void emit('pipeline:step:attempt:started', { runId: helpers.runId, stepId: node.id, attemptNumber: attempt, maxAttempts: policy.maxAttempts, at: new Date().toISOString() }).catch(() => undefined);
        try {
          last = await handler.execute(node, context, helpers);
        } catch (err) {
          last = { status: 'failed', error: err instanceof Error ? err.message : String(err) };
        }
        if (last.status !== 'failed') return last;
        const willRetry = attempt < policy.maxAttempts && policy.retryUnknown !== false;
        void emit('pipeline:step:attempt:failed', { runId: helpers.runId, stepId: node.id, attemptNumber: attempt, maxAttempts: policy.maxAttempts, error: last.error ?? 'failed', willRetry, at: new Date().toISOString() }).catch(() => undefined);
        if (!willRetry) break;
        await wait(policy.backoffMs * Math.pow(policy.backoffMultiplier, attempt - 1));
      }
      return last;
    },
  };
}
