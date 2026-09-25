import { describe, it, expect, jest } from '@jest/globals';
import { createJudgeStepHandler } from '../JudgeStepHandler';
import type { JudgeClientCallResult } from '../JudgeClient';
import type {
  JudgmentRepositoryLike,
  JudgeBudgetGuardLike,
} from '../JudgeStepHandler';
import type { LlmJudgeStepConfig, JudgmentResult } from '../types';
import type { PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';

// ── Test fixtures ───────────────────────────────────────────────────────

function buildHelpers(overrides: Partial<StepHandlerHelpers> = {}): {
  helpers: StepHandlerHelpers;
  emitted: Array<{ event: string; payload: unknown }>;
} {
  const emitted: Array<{ event: string; payload: unknown }> = [];
  const helpers: StepHandlerHelpers = {
    emit: async (event, payload) => {
      emitted.push({ event: event as string, payload });
    },
    runId: 'run-1',
    stepId: 'judge-1',
    ownerNodeId: 'node-1',
    abortSignal: new AbortController().signal,
    sleep: async () => undefined,
    isCancelled: () => false,
    ...overrides,
  };
  return { helpers, emitted };
}

function buildNode(config: Partial<LlmJudgeStepConfig> = {}): PipelineNode {
  const data: LlmJudgeStepConfig = {
    type: 'llm:judge',
    mode: 'absolute',
    rubric: {
      id: 'r-1',
      version: 1,
      overall: 'weighted-mean',
      passThreshold: 0.5,
      criteria: [
        { id: 'a', prompt: 'Is it correct?', scale: '1-5', weight: 1 },
        { id: 'b', prompt: 'Is it concise?', scale: '1-5', weight: 1 },
      ],
    },
    input: { candidate: '$.steps.upstream.output' },
    ...config,
  };
  return {
    id: 'n-judge',
    type: 'llm:judge',
    position: { x: 0, y: 0 },
    data,
  } as unknown as PipelineNode;
}

function makeJudgeClient(callResult: JudgeClientCallResult) {
  return {
    judge: jest.fn(async () => callResult),
  } as unknown as import('../JudgeClient').JudgeClient;
}

// ── Tests ───────────────────────────────────────────────────────────────

describe('JudgeStepHandler — happy path', () => {
  it('persists, emits scored, returns completed with JudgmentResult as output', async () => {
    const callResult: JudgeClientCallResult = {
      output: {
        criteria: [
          { criterionId: 'a', score: 4, rationale: 'mostly correct' },
          { criterionId: 'b', score: 5, rationale: 'tight' },
        ],
        confidence: 0.9,
      },
      usage: {
        input_tokens: 100,
        output_tokens: 50,
        cache_read_input_tokens: 800,
      },
      rawText: 'tool output',
      latencyMs: 1234,
      cacheHit: true,
    };
    const persistJudgment = jest.fn(async () => undefined);
    const repo: JudgmentRepositoryLike = { persistJudgment };
    const handler = createJudgeStepHandler({
      judgeClient: makeJudgeClient(callResult),
      repository: repo,
      pipelineIdLookup: () => 'p-1',
    });
    const { helpers, emitted } = buildHelpers();
    const node = buildNode();
    const context = { steps: { upstream: { output: 'candidate text' } } };

    const outcome = await handler.execute(node, context, helpers);

    expect(outcome.status).toBe('completed');
    expect(outcome.sourceHandle).toBe('out');
    const result = outcome.output as JudgmentResult;
    expect(result.criteria).toHaveLength(2);
    expect(result.criteria[0].score).toBe(0.75); // 4 on 1-5 → 0.75
    expect(result.criteria[1].score).toBe(1);    // 5 on 1-5 → 1
    expect(result.overallScore).toBeCloseTo(0.875, 4);
    expect(result.overallPassed).toBe(true);
    expect(result.cacheHit).toBe(true);
    expect(persistJudgment).toHaveBeenCalledTimes(1);

    const started = emitted.find((e) => e.event === 'pipeline:judge:started');
    const scored = emitted.find((e) => e.event === 'pipeline:judge:scored');
    expect(started).toBeDefined();
    expect(scored).toBeDefined();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const scoredPayload = scored!.payload as any;
    expect(scoredPayload.result.judgmentId).toBe(result.judgmentId);
    expect(scoredPayload.costUsd).toBeGreaterThan(0);
  });
});

describe('JudgeStepHandler — failure paths', () => {
  it('does not throw on JudgeClient malformed output; emits failed and returns status:failed', async () => {
    const callResult: JudgeClientCallResult = {
      output: null,
      usage: { input_tokens: 100, output_tokens: 0 },
      rawText: 'free-form text',
      latencyMs: 500,
      cacheHit: false,
      failureReason: 'malformed',
    };
    const handler = createJudgeStepHandler({
      judgeClient: makeJudgeClient(callResult),
      pipelineIdLookup: () => 'p-1',
    });
    const { helpers, emitted } = buildHelpers();
    const node = buildNode();
    const context = { steps: { upstream: { output: 'candidate text' } } };

    const outcome = await handler.execute(node, context, helpers);
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/malformed/);
    const failedEvent = emitted.find((e) => e.event === 'pipeline:judge:failed');
    expect(failedEvent).toBeDefined();
    // Usage propagates even on parse failure — required by audit rule #1
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect((failedEvent!.payload as any).tokensIn).toBe(100);
  });

  it('returns failed when input jsonpath cannot be resolved', async () => {
    const handler = createJudgeStepHandler({
      judgeClient: makeJudgeClient({
        output: { criteria: [] },
        usage: { input_tokens: 0, output_tokens: 0 },
        rawText: '',
        latencyMs: 0,
        cacheHit: false,
      }),
      pipelineIdLookup: () => 'p-1',
    });
    const { helpers, emitted } = buildHelpers();
    const node = buildNode({ input: { candidate: '$.nonexistent.path' } });

    const outcome = await handler.execute(node, {}, helpers);
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/input resolution/);
    expect(emitted.find((e) => e.event === 'pipeline:judge:failed')).toBeDefined();
  });
});

describe('JudgeStepHandler — budget guard', () => {
  it('skips (not fails) when budget exhausted, emits budget-exceeded', async () => {
    const budgetGuard: JudgeBudgetGuardLike = {
      isExhausted: jest.fn(async () => true) as JudgeBudgetGuardLike['isExhausted'],
      record: jest.fn(async () => undefined) as JudgeBudgetGuardLike['record'],
    };
    const handler = createJudgeStepHandler({
      judgeClient: makeJudgeClient({
        output: { criteria: [] },
        usage: { input_tokens: 0, output_tokens: 0 },
        rawText: '',
        latencyMs: 0,
        cacheHit: false,
      }),
      budgetGuard,
      pipelineIdLookup: () => 'p-1',
    });
    const { helpers, emitted } = buildHelpers();
    const outcome = await handler.execute(
      buildNode(),
      { steps: { upstream: { output: 'x' } } },
      helpers,
    );
    expect(outcome.status).toBe('completed');
    expect(outcome.output).toMatchObject({ skipped: 'budget-exhausted' });
    expect(emitted.find((e) => e.event === 'pipeline:judge:budget-exceeded')).toBeDefined();
    expect(budgetGuard.record).not.toHaveBeenCalled();
  });

  it('records cost on successful call', async () => {
    const budgetGuard: JudgeBudgetGuardLike = {
      isExhausted: jest.fn(async () => false) as JudgeBudgetGuardLike['isExhausted'],
      record: jest.fn(async () => undefined) as JudgeBudgetGuardLike['record'],
    };
    const handler = createJudgeStepHandler({
      judgeClient: makeJudgeClient({
        output: { criteria: [{ criterionId: 'a', score: 4, rationale: '' }, { criterionId: 'b', score: 4, rationale: '' }] },
        usage: { input_tokens: 1_000_000, output_tokens: 0 },
        rawText: '',
        latencyMs: 0,
        cacheHit: false,
      }),
      budgetGuard,
      pipelineIdLookup: () => 'p-1',
    });
    const { helpers } = buildHelpers();
    await handler.execute(buildNode(), { steps: { upstream: { output: 'x' } } }, helpers);
    expect(budgetGuard.record).toHaveBeenCalledTimes(1);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const recordedCost = (budgetGuard.record as any).mock.calls[0][1];
    expect(recordedCost).toBeCloseTo(3, 4); // 1M input × $3/Mtok for sonnet
  });
});

describe('JudgeStepHandler — registry validation interaction', () => {
  it('kind matches the dispatch key', () => {
    const handler = createJudgeStepHandler({
      judgeClient: makeJudgeClient({
        output: { criteria: [] },
        usage: { input_tokens: 0, output_tokens: 0 },
        rawText: '',
        latencyMs: 0,
        cacheHit: false,
      }),
      pipelineIdLookup: () => 'p-1',
    });
    expect(handler.kind).toBe('llm:judge');
  });
});
