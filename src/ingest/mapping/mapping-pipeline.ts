// social-api/src/ingest/mapping/mapping-pipeline.ts
//
// Mapping engine v2 — chained fast-path + LLM fallback + needs-review queue
// + DocType-proposal queue. Wraps the existing rule-matcher (rule-matcher.ts)
// and type-generator (type-generator.ts) so the engine can call one function
// to resolve a file to a target document type.
//
// Decision flow:
//
//   1. Rule fast-path                        → accept immediately.
//   2. LLM match (Haiku-class)              → if confidence ≥ threshold,
//                                              accept; otherwise route to
//                                              needs-review queue and remember
//                                              for clustering.
//   3. Generative tier (when N similar      → propose new DocType. Recorded
//      low-confidence items accumulate)       only; never auto-promoted.
//
// Every attempt produces a MappingDecision row regardless of outcome — that
// is the audit trail (method, confidence, latencyMs, model, routedTo).
//
// Design rules:
// - The pipeline is the single entry point (`runMappingPipeline`); it returns
//   either an "accepted" result (caller materializes), a "needs-review"
//   placeholder (caller stops at mapping stage), or a "failed" sentinel
//   (caller marks the item failed). All three persist a MappingDecision.
// - Repositories are injected so the pipeline is unit-testable without DDB.
// - LLM client is injected so tests can swap in FixtureLLMClient.
// - No singletons. The bootstrap layer wires everything; the engine receives
//   the configured pipeline via `IngestEngineDeps`.

import { randomUUID } from 'crypto';
import type { LLMClient } from '../../pipeline/LLMClient';
import type {
  FileDescriptor,
  MappingResult,
  MappingRule,
  IngestSource,
  MappingDecision,
  ReviewQueueEntry,
  DocTypeProposalEntry,
} from '../types';
import { matchByRules } from './rule-matcher';
import {
  suggestTypeMatch,
  generateTypeProposal,
  type ExistingTypeDescriptor,
} from './type-generator';

// ---------------------------------------------------------------------------
// Configuration defaults
// ---------------------------------------------------------------------------

/**
 * Default model for both LLM tiers. Haiku-class — cheap and fast; the v2
 * mapping engine is on the hot ingest path so cost matters.
 *
 * Operators override per-source via `config['mapping.llmModel']`.
 */
export const DEFAULT_MAPPING_MODEL = 'claude-haiku-4-5';

/** Default confidence threshold below which results queue for review. */
export const DEFAULT_CONFIDENCE_THRESHOLD = 0.7;

/** Default cluster size before the generative tier proposes a new DocType. */
export const DEFAULT_PROPOSAL_CLUSTER_SIZE = 3;

// ---------------------------------------------------------------------------
// Repository contracts
// ---------------------------------------------------------------------------

export interface MappingDecisionRepository {
  put(decision: MappingDecision): Promise<void>;
  listByIngestId(ingestId: string): Promise<MappingDecision[]>;
}

export interface MappingReviewQueueRepository {
  put(entry: ReviewQueueEntry): Promise<void>;
  listPendingBySource(sourceId: string): Promise<ReviewQueueEntry[]>;
}

export interface DocTypeProposalRepository {
  put(entry: DocTypeProposalEntry): Promise<void>;
  /** Used to count similar low-confidence items already proposed. */
  listPendingBySignature(sourceId: string, signature: string): Promise<DocTypeProposalEntry[]>;
}

// ---------------------------------------------------------------------------
// Pipeline I/O
// ---------------------------------------------------------------------------

export interface MappingPipelineDeps {
  llmClient: LLMClient;
  /** Loader for the catalog of known DocTypes — passed to the LLM-match tier. */
  loadExistingTypes: () => Promise<ExistingTypeDescriptor[]>;
  /**
   * Loader for already-seen low-confidence items keyed by signature, so the
   * pipeline can decide when to escalate to the generative tier. Returns the
   * count of similar items in the needs-review queue.
   */
  countSimilarLowConfidence: (sourceId: string, signature: string) => Promise<number>;
  decisionRepo: MappingDecisionRepository;
  reviewQueueRepo: MappingReviewQueueRepository;
  proposalRepo: DocTypeProposalRepository;
  /** Optional clock injection (defaults to Date.now) for deterministic tests. */
  now?: () => number;
}

export interface MappingPipelineInput {
  ingestId: string;
  source: IngestSource;
  file: FileDescriptor;
  /**
   * Optional file content (utf-8 sample) for the LLM tiers. The engine fetches
   * this lazily — if absent, only the file metadata is sent to the LLM.
   */
  contentSample?: string;
}

export type MappingPipelineOutcome =
  | {
      kind: 'accepted';
      result: MappingResult;
      decision: MappingDecision;
    }
  | {
      kind: 'needs-review';
      decision: MappingDecision;
      reviewEntry: ReviewQueueEntry;
      /** Suggestion the operator will see in the review UI. */
      suggested: MappingResult | null;
    }
  | {
      kind: 'doctype-proposal';
      decision: MappingDecision;
      proposal: DocTypeProposalEntry;
    }
  | {
      kind: 'failed';
      decision: MappingDecision;
      reason: string;
    };

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function resolveConfig(source: IngestSource): {
  threshold: number;
  model: string;
  proposalClusterSize: number;
} {
  const cfg = source.config as Record<string, unknown>;
  const threshold = typeof cfg['mapping.confidenceThreshold'] === 'number'
    ? (cfg['mapping.confidenceThreshold'] as number)
    : DEFAULT_CONFIDENCE_THRESHOLD;
  const model = typeof cfg['mapping.llmModel'] === 'string'
    ? (cfg['mapping.llmModel'] as string)
    : DEFAULT_MAPPING_MODEL;
  const proposalClusterSize = typeof cfg['mapping.proposalClusterSize'] === 'number'
    ? (cfg['mapping.proposalClusterSize'] as number)
    : DEFAULT_PROPOSAL_CLUSTER_SIZE;
  return { threshold, model, proposalClusterSize };
}

/**
 * Cluster signature for grouping similar low-confidence items into a
 * DocType proposal. mimeType + extension is good enough for v1; can grow
 * into content-embedding similarity later.
 */
function clusterSignature(file: FileDescriptor): string {
  const dot = file.name.lastIndexOf('.');
  const ext = dot >= 0 ? file.name.slice(dot + 1).toLowerCase() : '';
  return `${file.mimeType}|${ext}`;
}

function buildDecision(
  partial: Omit<MappingDecision, 'decisionId' | 'createdAt'>,
  now: () => number,
): MappingDecision {
  return {
    ...partial,
    decisionId: `md-${now()}-${randomUUID().slice(0, 8)}`,
    createdAt: new Date(now()).toISOString(),
  };
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Run the chained mapping pipeline for a single ingest item. Returns an
 * outcome the engine uses to decide whether to materialize, defer to review,
 * or fail. Persists a MappingDecision row in every branch.
 */
export async function runMappingPipeline(
  input: MappingPipelineInput,
  deps: MappingPipelineDeps,
): Promise<MappingPipelineOutcome> {
  const { ingestId, source, file, contentSample } = input;
  const now = deps.now ?? Date.now;
  const { threshold, model, proposalClusterSize } = resolveConfig(source);

  // ---- Tier 1: Rule fast path ------------------------------------------
  const ruleStart = now();
  const ruleResult = matchByRules(file, source.mappingRules ?? []);
  const ruleLatencyMs = now() - ruleStart;

  if (ruleResult) {
    const decision = buildDecision(
      {
        ingestId,
        sourceId: source.sourceId,
        method: 'rule',
        targetTypeId: ruleResult.targetTypeId,
        confidence: ruleResult.confidence,
        latencyMs: ruleLatencyMs,
        model: null,
        ruleId: ruleResult.ruleId ?? null,
        routedTo: 'accepted',
        fieldMapping: ruleResult.fieldMapping,
        reason: null,
      },
      now,
    );
    await deps.decisionRepo.put(decision);
    return { kind: 'accepted', result: ruleResult, decision };
  }

  // ---- Tier 2: LLM match -----------------------------------------------
  const existingTypes = await deps.loadExistingTypes();
  const llmStart = now();
  const sample = contentSample ?? `(no content sample — metadata only: ${file.name}, ${file.mimeType})`;

  let llmResult: MappingResult | null = null;
  try {
    llmResult = await suggestTypeMatch(
      sample,
      file.name,
      file.mimeType,
      existingTypes,
      deps.llmClient,
      model,
    );
  } catch {
    // Treat LLM transport / parse errors as "no match" — the audit row will
    // still be written below with confidence=0.
    llmResult = null;
  }
  const llmLatencyMs = now() - llmStart;

  // 2a. LLM-match accepted (confidence ≥ threshold)
  if (llmResult && llmResult.confidence >= threshold) {
    const decision = buildDecision(
      {
        ingestId,
        sourceId: source.sourceId,
        method: 'llm-match',
        targetTypeId: llmResult.targetTypeId,
        confidence: llmResult.confidence,
        latencyMs: llmLatencyMs,
        model,
        ruleId: null,
        routedTo: 'accepted',
        fieldMapping: llmResult.fieldMapping,
        reason: null,
      },
      now,
    );
    await deps.decisionRepo.put(decision);
    return { kind: 'accepted', result: llmResult, decision };
  }

  // 2b. LLM-match below threshold OR null — needs review
  // Before queuing, check whether we've accumulated enough similar items to
  // escalate to the generative tier instead.
  const signature = clusterSignature(file);
  const existingClusterCount = await deps.countSimilarLowConfidence(
    source.sourceId,
    signature,
  );

  if (existingClusterCount + 1 >= proposalClusterSize) {
    // ---- Tier 3: Generative tier — propose new DocType ----------------
    const genStart = now();
    try {
      const proposal = await generateTypeProposal(
        sample,
        file.name,
        file.mimeType,
        deps.llmClient,
        model,
      );
      const genLatencyMs = now() - genStart;

      const proposalEntry: DocTypeProposalEntry = {
        proposalId: `dtp-${now()}-${randomUUID().slice(0, 8)}`,
        sourceId: source.sourceId,
        signature,
        sampleIngestIds: [ingestId],
        proposedName: proposal.name,
        proposedDescription: proposal.description,
        proposedFields: proposal.fields,
        reasoning: proposal.reasoning,
        status: 'pending',
        createdAt: new Date(now()).toISOString(),
        reviewedAt: null,
        reviewedBy: null,
      };

      const decision = buildDecision(
        {
          ingestId,
          sourceId: source.sourceId,
          method: 'llm-generate',
          targetTypeId: null,
          confidence: llmResult?.confidence ?? 0,
          latencyMs: llmLatencyMs + genLatencyMs,
          model,
          ruleId: null,
          routedTo: 'doctype-proposal',
          fieldMapping: {},
          reason: proposal.reasoning,
        },
        now,
      );

      await deps.proposalRepo.put(proposalEntry);
      await deps.decisionRepo.put(decision);
      return { kind: 'doctype-proposal', decision, proposal: proposalEntry };
    } catch (err) {
      // Generative tier failed — fall through to needs-review with the
      // failure reason captured.
      const reason = err instanceof Error ? err.message : String(err);
      const decision = buildDecision(
        {
          ingestId,
          sourceId: source.sourceId,
          method: 'llm-generate',
          targetTypeId: null,
          confidence: llmResult?.confidence ?? 0,
          latencyMs: llmLatencyMs + (now() - genStart),
          model,
          ruleId: null,
          routedTo: 'failed',
          fieldMapping: {},
          reason,
        },
        now,
      );
      await deps.decisionRepo.put(decision);
      return { kind: 'failed', decision, reason };
    }
  }

  // 2c. Default: needs-review queue
  const decision = buildDecision(
    {
      ingestId,
      sourceId: source.sourceId,
      method: 'llm-match',
      targetTypeId: llmResult?.targetTypeId ?? null,
      confidence: llmResult?.confidence ?? 0,
      latencyMs: llmLatencyMs,
      model,
      ruleId: null,
      routedTo: 'needs-review',
      fieldMapping: llmResult?.fieldMapping ?? {},
      reason: llmResult ? `confidence ${llmResult.confidence.toFixed(2)} below threshold ${threshold.toFixed(2)}` : 'no LLM match',
    },
    now,
  );

  const reviewEntry: ReviewQueueEntry = {
    ingestId,
    sourceId: source.sourceId,
    decisionId: decision.decisionId,
    suggestedTypeId: llmResult?.targetTypeId ?? null,
    suggestedFieldMapping: llmResult?.fieldMapping ?? {},
    confidence: llmResult?.confidence ?? 0,
    threshold,
    reason: decision.reason,
    status: 'pending',
    createdAt: decision.createdAt,
    reviewedAt: null,
    reviewedBy: null,
  };

  await deps.reviewQueueRepo.put(reviewEntry);
  await deps.decisionRepo.put(decision);

  return {
    kind: 'needs-review',
    decision,
    reviewEntry,
    suggested: llmResult,
  };
}
