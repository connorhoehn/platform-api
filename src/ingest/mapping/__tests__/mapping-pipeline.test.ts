// social-api/src/ingest/mapping/__tests__/mapping-pipeline.test.ts
//
// Tests for runMappingPipeline — the v2 mapping engine. Covers:
//   - Rule fast path returns 'accepted' without invoking the LLM.
//   - LLM-match above threshold returns 'accepted'.
//   - LLM-match below threshold queues for review and writes audit row.
//   - Generative tier kicks in once cluster size is reached.
//   - LLM transport failure routes to needs-review with reason.
//   - Per-source confidence threshold + model overrides are honored.
//   - Audit row is persisted in every branch with method/latency/model.

import { FixtureLLMClient } from 'distributed-core';
import {
  runMappingPipeline,
  DEFAULT_CONFIDENCE_THRESHOLD,
  DEFAULT_MAPPING_MODEL,
  DEFAULT_PROPOSAL_CLUSTER_SIZE,
  type MappingPipelineDeps,
  type MappingPipelineInput,
  type MappingDecisionRepository,
  type MappingReviewQueueRepository,
  type DocTypeProposalRepository,
} from '../mapping-pipeline';
import type {
  IngestSource,
  FileDescriptor,
  MappingRule,
  MappingDecision,
  ReviewQueueEntry,
  DocTypeProposalEntry,
} from '../../types';

// ─── In-memory repository fakes ──────────────────────────────────────────

function makeDecisionRepo(): MappingDecisionRepository & {
  store: MappingDecision[];
} {
  const store: MappingDecision[] = [];
  return {
    store,
    put: jest.fn(async (d: MappingDecision) => { store.push(d); }),
    listByIngestId: jest.fn(async (id: string) =>
      store.filter((d) => d.ingestId === id),
    ),
  };
}

function makeReviewRepo(): MappingReviewQueueRepository & {
  store: ReviewQueueEntry[];
} {
  const store: ReviewQueueEntry[] = [];
  return {
    store,
    put: jest.fn(async (e: ReviewQueueEntry) => {
      const i = store.findIndex((s) => s.ingestId === e.ingestId);
      if (i >= 0) store[i] = e; else store.push(e);
    }),
    listPendingBySource: jest.fn(async (sid: string) =>
      store.filter((e) => e.sourceId === sid && e.status === 'pending'),
    ),
  };
}

function makeProposalRepo(): DocTypeProposalRepository & {
  store: DocTypeProposalEntry[];
} {
  const store: DocTypeProposalEntry[] = [];
  return {
    store,
    put: jest.fn(async (e: DocTypeProposalEntry) => { store.push(e); }),
    listPendingBySignature: jest.fn(async (sid: string, sig: string) =>
      store.filter((e) => e.sourceId === sid && e.signature === sig && e.status === 'pending'),
    ),
  };
}

// ─── Builders ──────────────────────────────────────────────────────────────

function makeSource(overrides: Partial<IngestSource> = {}): IngestSource {
  return {
    sourceId: 'src-test',
    name: 'Test Source',
    type: 'local',
    config: {},
    syncSchedule: null,
    mappingRules: [],
    defaultPipelineId: null,
    status: 'active',
    lastSyncAt: null,
    lastSyncStats: null,
    createdAt: '2026-01-01T00:00:00Z',
    updatedAt: '2026-01-01T00:00:00Z',
    ...overrides,
  };
}

function makeFile(overrides: Partial<FileDescriptor> = {}): FileDescriptor {
  return {
    path: '/docs/report.txt',
    name: 'report.txt',
    hash: 'h-1',
    size: 100,
    mimeType: 'text/plain',
    lastModified: '2026-01-01T00:00:00Z',
    ...overrides,
  };
}

function makeDeps(
  llm: FixtureLLMClient,
  overrides: Partial<MappingPipelineDeps> = {},
): MappingPipelineDeps & {
  decisionRepo: ReturnType<typeof makeDecisionRepo>;
  reviewQueueRepo: ReturnType<typeof makeReviewRepo>;
  proposalRepo: ReturnType<typeof makeProposalRepo>;
} {
  const decisionRepo = makeDecisionRepo();
  const reviewQueueRepo = makeReviewRepo();
  const proposalRepo = makeProposalRepo();
  let counter = 1_700_000_000_000;
  return {
    llmClient: llm,
    loadExistingTypes: jest.fn(async () => [
      {
        typeId: 'meeting-notes',
        name: 'Meeting Notes',
        description: 'Notes from a meeting',
        fields: [{ name: 'title', sectionType: 'text' }],
      },
    ]),
    countSimilarLowConfidence: jest.fn(async () => 0),
    decisionRepo,
    reviewQueueRepo,
    proposalRepo,
    now: () => counter++,
    ...overrides,
  };
}

function makeInput(overrides: Partial<MappingPipelineInput> = {}): MappingPipelineInput {
  return {
    ingestId: 'item-1',
    source: makeSource(),
    file: makeFile(),
    contentSample: 'sample content',
    ...overrides,
  };
}

// ─── Tier 1: Rule fast path ─────────────────────────────────────────────────

describe('runMappingPipeline — rule fast path', () => {
  test('matched rule produces accepted outcome and skips the LLM', async () => {
    const rule: MappingRule = {
      ruleId: 'r1',
      priority: 1,
      condition: { field: 'mimeType', op: 'eq', value: 'text/plain' },
      targetTypeId: 'plain-doc',
      fieldMapping: { name: 'title' },
    };
    const llm = new FixtureLLMClient([]);   // never called
    const deps = makeDeps(llm);
    const input = makeInput({ source: makeSource({ mappingRules: [rule] }) });

    const outcome = await runMappingPipeline(input, deps);
    expect(outcome.kind).toBe('accepted');
    if (outcome.kind !== 'accepted') return;
    expect(outcome.result.method).toBe('rule');
    expect(outcome.result.targetTypeId).toBe('plain-doc');
    expect(outcome.result.confidence).toBe(1.0);
    expect(deps.loadExistingTypes).not.toHaveBeenCalled();
  });

  test('rule path persists a MappingDecision with method=rule', async () => {
    const rule: MappingRule = {
      ruleId: 'r1',
      priority: 1,
      condition: { field: 'extension', op: 'eq', value: 'txt' },
      targetTypeId: 'plain-doc',
      fieldMapping: {},
    };
    const llm = new FixtureLLMClient([]);
    const deps = makeDeps(llm);
    await runMappingPipeline(
      makeInput({ source: makeSource({ mappingRules: [rule] }) }),
      deps,
    );
    expect(deps.decisionRepo.store).toHaveLength(1);
    const d = deps.decisionRepo.store[0];
    expect(d.method).toBe('rule');
    expect(d.routedTo).toBe('accepted');
    expect(d.ruleId).toBe('r1');
    expect(d.model).toBeNull();
  });
});

// ─── Tier 2: LLM match above threshold ──────────────────────────────────────

describe('runMappingPipeline — LLM fallback above threshold', () => {
  test('high-confidence LLM match is accepted', async () => {
    const llm = new FixtureLLMClient([
      JSON.stringify({
        typeId: 'meeting-notes',
        confidence: 0.95,
        fieldMapping: { title: 'name' },
      }),
    ]);
    const deps = makeDeps(llm);
    const outcome = await runMappingPipeline(makeInput(), deps);
    expect(outcome.kind).toBe('accepted');
    if (outcome.kind !== 'accepted') return;
    expect(outcome.result.method).toBe('llm-match');
    expect(outcome.result.targetTypeId).toBe('meeting-notes');
    expect(outcome.result.confidence).toBe(0.95);
    expect(deps.reviewQueueRepo.store).toHaveLength(0);
  });

  test('LLM accept persists decision with method=llm-match and model set', async () => {
    const llm = new FixtureLLMClient([
      JSON.stringify({ typeId: 'meeting-notes', confidence: 0.9, fieldMapping: {} }),
    ]);
    const deps = makeDeps(llm);
    await runMappingPipeline(makeInput(), deps);
    expect(deps.decisionRepo.store).toHaveLength(1);
    const d = deps.decisionRepo.store[0];
    expect(d.method).toBe('llm-match');
    expect(d.routedTo).toBe('accepted');
    expect(d.model).toBe(DEFAULT_MAPPING_MODEL);
    expect(d.latencyMs).toBeGreaterThanOrEqual(0);
  });
});

// ─── Tier 2: LLM match below threshold → review queue ──────────────────────

describe('runMappingPipeline — LLM fallback below threshold', () => {
  test('low-confidence LLM match queues for review', async () => {
    const llm = new FixtureLLMClient([
      JSON.stringify({
        typeId: 'meeting-notes',
        confidence: 0.5,
        fieldMapping: { title: 'name' },
      }),
    ]);
    const deps = makeDeps(llm);
    const outcome = await runMappingPipeline(makeInput(), deps);
    expect(outcome.kind).toBe('needs-review');
    if (outcome.kind !== 'needs-review') return;
    expect(outcome.suggested?.confidence).toBe(0.5);
    expect(deps.reviewQueueRepo.store).toHaveLength(1);
    const entry = deps.reviewQueueRepo.store[0];
    expect(entry.ingestId).toBe('item-1');
    expect(entry.status).toBe('pending');
    expect(entry.threshold).toBe(DEFAULT_CONFIDENCE_THRESHOLD);
    expect(entry.suggestedTypeId).toBe('meeting-notes');
  });

  test('null LLM match (no parseable response) queues for review with reason', async () => {
    const llm = new FixtureLLMClient(['not valid json at all']);
    const deps = makeDeps(llm);
    const outcome = await runMappingPipeline(makeInput(), deps);
    expect(outcome.kind).toBe('needs-review');
    if (outcome.kind !== 'needs-review') return;
    expect(outcome.suggested).toBeNull();
    expect(outcome.decision.confidence).toBe(0);
    expect(outcome.decision.reason).toContain('no LLM match');
  });

  test('audit row has routedTo=needs-review and reason describes threshold', async () => {
    const llm = new FixtureLLMClient([
      JSON.stringify({ typeId: 'meeting-notes', confidence: 0.4, fieldMapping: {} }),
    ]);
    const deps = makeDeps(llm);
    await runMappingPipeline(makeInput(), deps);
    expect(deps.decisionRepo.store).toHaveLength(1);
    const d = deps.decisionRepo.store[0];
    expect(d.routedTo).toBe('needs-review');
    expect(d.reason).toContain('below threshold');
    expect(d.reason).toContain('0.70');
  });
});

// ─── Per-source threshold & model overrides ────────────────────────────────

describe('runMappingPipeline — per-source config', () => {
  test('per-source confidenceThreshold lets sub-default scores accept', async () => {
    const llm = new FixtureLLMClient([
      JSON.stringify({ typeId: 'meeting-notes', confidence: 0.5, fieldMapping: {} }),
    ]);
    const deps = makeDeps(llm);
    const source = makeSource({
      config: { 'mapping.confidenceThreshold': 0.4 },
    });
    const outcome = await runMappingPipeline(makeInput({ source }), deps);
    expect(outcome.kind).toBe('accepted');
  });

  test('per-source llmModel is honoured in audit row', async () => {
    const llm = new FixtureLLMClient([
      JSON.stringify({ typeId: 'meeting-notes', confidence: 0.95, fieldMapping: {} }),
    ]);
    const deps = makeDeps(llm);
    const source = makeSource({
      config: { 'mapping.llmModel': 'claude-sonnet-4-5' },
    });
    await runMappingPipeline(makeInput({ source }), deps);
    expect(deps.decisionRepo.store[0].model).toBe('claude-sonnet-4-5');
  });

  test('per-source proposalClusterSize triggers generative tier earlier', async () => {
    // Cluster size = 1 — first low-confidence item escalates immediately.
    const matchResp = JSON.stringify({
      typeId: 'meeting-notes', confidence: 0.2, fieldMapping: {},
    });
    const proposalResp = JSON.stringify({
      name: 'CustomDoc',
      description: 'auto',
      fields: [{ name: 'body', sectionType: 'text', required: true }],
      reasoning: 'looked novel',
    });
    const llm = new FixtureLLMClient([matchResp, proposalResp]);
    const deps = makeDeps(llm);
    const source = makeSource({
      config: { 'mapping.proposalClusterSize': 1 },
    });
    const outcome = await runMappingPipeline(makeInput({ source }), deps);
    expect(outcome.kind).toBe('doctype-proposal');
  });
});

// ─── Tier 3: Generative tier (cluster size reached) ────────────────────────

describe('runMappingPipeline — DocType proposal tier', () => {
  test('generative tier kicks in when N similar low-conf items already queued', async () => {
    const matchResp = JSON.stringify({
      typeId: 'meeting-notes', confidence: 0.3, fieldMapping: {},
    });
    const proposalResp = JSON.stringify({
      name: 'NoveltyDoc',
      description: 'auto-discovered',
      fields: [
        { name: 'title', sectionType: 'text', required: true },
        { name: 'body', sectionType: 'text', required: false },
      ],
      reasoning: 'similar files repeatedly fall below threshold',
    });
    const llm = new FixtureLLMClient([matchResp, proposalResp]);
    const deps = makeDeps(llm, {
      // Pretend N-1 similar items already accumulated.
      countSimilarLowConfidence: jest.fn(async () => DEFAULT_PROPOSAL_CLUSTER_SIZE - 1),
    });

    const outcome = await runMappingPipeline(makeInput(), deps);
    expect(outcome.kind).toBe('doctype-proposal');
    if (outcome.kind !== 'doctype-proposal') return;
    expect(outcome.proposal.proposedName).toBe('NoveltyDoc');
    expect(outcome.proposal.proposedFields).toHaveLength(2);
    expect(outcome.proposal.status).toBe('pending');
    expect(outcome.proposal.sampleIngestIds).toEqual(['item-1']);
    expect(deps.proposalRepo.store).toHaveLength(1);
    expect(deps.decisionRepo.store[0].method).toBe('llm-generate');
    expect(deps.decisionRepo.store[0].routedTo).toBe('doctype-proposal');
  });

  test('generative tier failure falls back to a "failed" outcome with reason', async () => {
    const matchResp = JSON.stringify({
      typeId: 'meeting-notes', confidence: 0.3, fieldMapping: {},
    });
    // Invalid proposal JSON → generateTypeProposal throws.
    const proposalResp = 'not valid';
    const llm = new FixtureLLMClient([matchResp, proposalResp]);
    const deps = makeDeps(llm, {
      countSimilarLowConfidence: jest.fn(async () => DEFAULT_PROPOSAL_CLUSTER_SIZE - 1),
    });
    const outcome = await runMappingPipeline(makeInput(), deps);
    expect(outcome.kind).toBe('failed');
    if (outcome.kind !== 'failed') return;
    expect(outcome.reason).toContain('failed to parse');
    expect(deps.decisionRepo.store[0].routedTo).toBe('failed');
  });
});

// ─── Audit-row persistence (cross-cutting) ─────────────────────────────────

describe('runMappingPipeline — audit invariants', () => {
  test('every outcome writes exactly one decision row', async () => {
    // 1 — rule path
    const ruleDeps = makeDeps(new FixtureLLMClient([]));
    await runMappingPipeline(
      makeInput({ source: makeSource({
        mappingRules: [{
          ruleId: 'r1', priority: 1, targetTypeId: 't1', fieldMapping: {},
          condition: { field: 'extension', op: 'eq', value: 'txt' },
        }],
      })}),
      ruleDeps,
    );
    expect(ruleDeps.decisionRepo.store).toHaveLength(1);

    // 2 — LLM accept path
    const acceptDeps = makeDeps(new FixtureLLMClient([
      JSON.stringify({ typeId: 'meeting-notes', confidence: 0.99, fieldMapping: {} }),
    ]));
    await runMappingPipeline(makeInput(), acceptDeps);
    expect(acceptDeps.decisionRepo.store).toHaveLength(1);

    // 3 — needs-review path
    const reviewDeps = makeDeps(new FixtureLLMClient([
      JSON.stringify({ typeId: 'meeting-notes', confidence: 0.5, fieldMapping: {} }),
    ]));
    await runMappingPipeline(makeInput(), reviewDeps);
    expect(reviewDeps.decisionRepo.store).toHaveLength(1);
  });

  test('decision row carries latencyMs ≥ 0 and a stable createdAt', async () => {
    const llm = new FixtureLLMClient([
      JSON.stringify({ typeId: 'meeting-notes', confidence: 0.95, fieldMapping: {} }),
    ]);
    const deps = makeDeps(llm);
    await runMappingPipeline(makeInput(), deps);
    const d = deps.decisionRepo.store[0];
    expect(d.latencyMs).toBeGreaterThanOrEqual(0);
    expect(new Date(d.createdAt).getTime()).not.toBeNaN();
    expect(d.decisionId.startsWith('md-')).toBe(true);
  });
});
