// Pipeline HTTP contracts.
//
// IMPORTANT — module-shaped artifact:
//   This file is the public request/response shape for every pipeline HTTP
//   route. It is the design seam for the eventual relocation of the
//   PipelineModule into distributed-core. The frontend imports its types
//   from here (or a sibling copy with the same shape) so a backend change
//   that breaks the wire contract becomes a frontend compile error.
//
//   Constraints:
//     - Zero framework imports (no Express, no platform-api middleware, no
//       distributed-core internals). Plain TypeScript interfaces only.
//     - Zero runtime side effects.
//     - When PipelineModule moves into distributed-core, this file moves
//       with it via `git mv`. Anything that depends on it via type-only
//       imports continues to compile across the relocation.
//
// Refs:
//   - .planning/HANDOFF-distributed-core-platform-api-absorption.md
//     §"What 'module-shaped' actually means"

// Type-only, zero runtime effect, and from the shared contract package rather
// than a framework — so it holds the constraints above. On relocation into
// distributed-core this becomes an import of DC's own `RunStatus`, which
// `PipelineRunStatus` already mirrors one-for-one.
import type { PipelineRunStatus } from '@connorhoehn/pipeline-runtime';

// ---------------------------------------------------------------------------
// Auth context — host-supplied per request.
// ---------------------------------------------------------------------------

/**
 * Identity attached to every request handled by the pipeline router. The
 * host (platform-api today, distributed-core later) populates this from its
 * own auth middleware and injects a getter into the router factory; the
 * route handlers themselves never speak to Cognito / IAM / etc.
 *
 * `userId` is a stable opaque identifier — no assumption about format.
 * `scopes` is an optional capability list; routes that need fine-grained
 * authorization should check membership rather than guess from `userId`.
 */
export interface AuthContext {
  userId: string;
  scopes?: readonly string[];
}

// ---------------------------------------------------------------------------
// Trigger — POST /api/pipelines/:pipelineId/runs
// ---------------------------------------------------------------------------

/**
 * Inline pipeline definition shipped on a trigger request. Optional: when
 * absent, the server resolves the definition from its persistent store.
 *
 * Carrying the full definition is keyed by `id`; everything else is
 * adapter-/version-specific and stays opaque from the contracts layer.
 */
export interface InlinePipelineDefinition {
  id: string;
  [key: string]: unknown;
}

export interface PipelineTriggerRequest {
  /** Optional inline definition. When absent, server reads from store. */
  definition?: InlinePipelineDefinition;
  /** Free-form key/value passed to the trigger node's evaluation context. */
  triggerPayload?: Record<string, unknown>;
  /** Node IDs at which the run should pause for step-by-step debugging. */
  breakpoints?: readonly string[];
}

export interface PipelineTriggerResponse {
  runId: string;
  pipelineId: string;
  triggeredBy: {
    userId: string;
    triggerType: string;
  };
  /** ISO 8601 timestamp the server stamped the trigger acknowledgement. */
  at: string;
}

// ---------------------------------------------------------------------------
// Approvals — POST /api/pipelines/:runId/approvals
// ---------------------------------------------------------------------------

export interface PipelineApprovalResolveRequest {
  stepId: string;
  decision: 'approve' | 'reject';
  /** Optional free-form note recorded alongside the decision. */
  comment?: string;
}

/**
 * 204 No Content when this replica's executor held the gate and resolved it.
 *
 * 202 Accepted when the gate is held by another replica (NFR #115): the
 * decision was recorded on the pipeline's rollup row and forwarded to the
 * run's owner over the cluster pubsub; the run proceeds within the pubsub's
 * delivery time. A dead owner's decision is applied when the run is resumed.
 * Retrying with the same `Idempotency-Key` reuses the same `decisionId`.
 *
 * Never a 204 for a run this replica does not hold: 404 `run-not-found`,
 * 409 `run-not-awaiting-approval` / `gate-not-pending`, or 503
 * `approval-forwarding-unavailable` (the decision reached neither the row
 * nor the wire — retry with the same key).
 */
export type PipelineApprovalResolveResponse = void | PipelineApprovalForwardedResponse;

export interface PipelineApprovalForwardedResponse {
  decisionId: string;
  runId: string;
  stepId: string;
  decision: 'approve' | 'reject';
  pipelineId: string;
  /** The replica that owns the run, per its run row; null when the row has none. */
  ownerNodeId: string | null;
  forwarded: true;
}

// ---------------------------------------------------------------------------
// Listings — GET /api/pipelines/:pipelineId/runs
// ---------------------------------------------------------------------------

export interface PipelineRunsListQuery {
  /** Page size, clamped server-side to [1, 100]. */
  limit?: number;
  /** Opaque cursor returned by a prior page. */
  cursor?: string;
}

export interface PipelineRunSummary {
  runId: string;
  pipelineId: string;
  /**
   * Canonical run status. `interrupted` is a platform-api-only lifecycle state
   * (orphan scan) and is not part of the shared `PipelineRunStatus` union, so
   * it is spelled out alongside it.
   */
  status: PipelineRunStatus | 'interrupted';
  startedAt: string;
  completedAt?: string;
  triggeredBy: {
    userId?: string;
    triggerType: string;
    payload?: Record<string, unknown>;
  };
  triggerType?: string;
  /** Per-step state map. Keyed by node ID; shape evolves with the framework. */
  steps?: Record<string, unknown>;
  /** Structured terminal-error envelope. Absent on non-failed runs. */
  error?: {
    nodeId: string;
    message: string;
    errorCategory?: string;
    retryability?: string;
    [key: string]: unknown;
  };
}

export interface PipelineRunsListResponse {
  runs: readonly PipelineRunSummary[];
  /** Present when more pages exist; pass to the next call as `cursor`. */
  nextCursor?: string;
}

// ---------------------------------------------------------------------------
// Run detail — GET /api/pipelines/:pipelineId/runs/:runId
// ---------------------------------------------------------------------------

/**
 * Full run snapshot. Same shape as `PipelineRunSummary` today; kept as a
 * distinct alias because the detail endpoint may grow fields that aren't
 * meaningful in list responses (full step input/output, attempt history,
 * audit trail) without churning the list shape.
 */
export type PipelineRunDetailResponse = PipelineRunSummary;

// ---------------------------------------------------------------------------
// Pending approvals — GET /api/pipelines/approvals
// ---------------------------------------------------------------------------

export interface PipelinePendingApprover {
  type: 'role' | 'user';
  value: string;
}

export interface PipelinePendingApproval {
  runId: string;
  pipelineId: string;
  stepId: string;
  approvers: readonly PipelinePendingApprover[];
  /** Display message authored on the approval node. */
  message?: string;
  /** ISO 8601 of the first request. */
  requestedAt: string;
  /** Number of approvals required to advance the step. Default 1. */
  requiredCount?: number;
  /** Approvals already recorded against the step. */
  recordedCount?: number;
  /** Human-readable pipeline name (from the run's definition snapshot). */
  pipelineName?: string;
  /** Label of the approval node — so the queue isn't a wall of uuids. */
  stepName?: string;
  /** Who triggered the run awaiting this decision. */
  requestedBy?: string;
  /** How the run was triggered (`manual`, `schedule`, `webhook`, …). */
  triggerType?: string;
  /**
   * Output of the steps feeding directly into this approval node — what the
   * approver is actually being asked to judge.
   */
  upstream?: readonly {
    stepId: string;
    name: string;
    type: string;
    status?: string;
    output?: unknown;
  }[];
}

export interface PipelinePendingApprovalsResponse {
  approvals: readonly PipelinePendingApproval[];
}

// ---------------------------------------------------------------------------
// Cancel — POST /api/pipelines/:runId/cancel
// ---------------------------------------------------------------------------

export interface PipelineCancelRequest {
  /** Optional reason recorded in the audit log. */
  reason?: string;
}

/** 204 No Content on success. */
export type PipelineCancelResponse = void;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/** Structured error body returned on bridge / framework failures. */
export interface PipelineErrorBody {
  error: string;
  /** Logical action attempted ("trigger", "cancel", "resolveApproval", ...). */
  action: string;
}
