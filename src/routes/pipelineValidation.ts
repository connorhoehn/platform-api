// social-api/src/routes/pipelineValidation.ts
//
// POST /api/pipelines/validate
//
// Backend-side pipeline validator. Used primarily by the MCP server's
// `pipeline_validate` tool so MCP consumers (Claude Desktop / Code) get
// validation results without having to duplicate the rule logic in their
// host environment.
//
// Scope: STRUCTURAL rules only for v1 — the bare minimum to decide whether
// a pipeline is publishable / runnable:
//   - NO_TRIGGER         (error)  Pipeline has zero trigger nodes.
//   - MULTIPLE_TRIGGERS  (error)  Pipeline has more than one trigger node.
//   - CYCLE_DETECTED     (error)  The directed graph contains a cycle.
//
// Deliberately deferred (NOT implemented here):
//   - Field-level lints: LLM_NO_MAX_TOKENS, UNGUARDED_APPROVAL_TIMEOUT,
//     LARGE_FORK, DEEP_CHAIN, DUPLICATE_NODE_NAME,
//     LOW_TEMPERATURE_NON_DETERMINISTIC_PROMPT, NO_PUBLISHED_VERSION_BUT_TRIGGER_SCHEDULED.
//   - Per-node config checks: MISSING_CONFIG (LLM provider/model/prompts,
//     Transform expression, Fork branchCount, Join mode/n, Approval approvers).
//   - Edge handle compatibility: INVALID_HANDLE.
//   - JOIN_INSUFFICIENT_INPUTS, ORPHAN_NODE, DEAD_END,
//     UNUSED_FORK_BRANCH, UNUSED_CONDITION_BRANCH, NO_ERROR_HANDLER.
// Those live in `frontend/src/components/pipelines/validation/validatePipeline.ts`.
// Phase 4 will either:
//   (a) extract a shared `pipeline-validator` module both sides import, or
//   (b) port the remaining rules here verbatim and document the duplication.
// Issue codes match the frontend exactly so MCP consumers get identical
// shapes regardless of which side runs the rules.
//
// IMPORTANT: this file is intentionally a near-duplicate of the frontend
// validator's structural-rules subset. Cross-tree imports between
// `frontend/` and `social-api/` get messy (different tsconfigs, build
// targets, dependency graphs), so the two stay separate by design.

import { Router, type Request, type Response } from 'express';
import { asyncHandler, ValidationError } from '../middleware/error-handler';

// ---------------------------------------------------------------------------
// Types — structural subset of frontend's PipelineDefinition / ValidationResult.
// Kept loose (no per-node-data discriminators) since structural rules only
// look at edges and node `type`.
// ---------------------------------------------------------------------------

export interface ValidationPipelineNode {
  id: string;
  type: string;
  data?: { type?: string; [k: string]: unknown };
  [k: string]: unknown;
}

export interface ValidationPipelineEdge {
  id: string;
  source: string;
  target: string;
  sourceHandle?: string;
  targetHandle?: string;
  [k: string]: unknown;
}

export interface ValidationPipelineDefinition {
  id: string;
  nodes: ValidationPipelineNode[];
  edges: ValidationPipelineEdge[];
  [k: string]: unknown;
}

export type ValidationSeverity = 'error' | 'warning';

export interface ValidationIssue {
  code: string;
  severity: ValidationSeverity;
  message: string;
  nodeId?: string;
  edgeId?: string;
  field?: string;
}

export interface ValidationResult {
  errors: ValidationIssue[];
  warnings: ValidationIssue[];
  isValid: boolean;
  canPublish: boolean;
}

export interface ValidatePipelineRequest {
  definition: ValidationPipelineDefinition;
}

// ---------------------------------------------------------------------------
// Public validator — pure function, no side effects, mirrors frontend.
// ---------------------------------------------------------------------------

/** Validate a pipeline definition; returns the same shape as the frontend validator. */
export function validatePipelineStructural(
  def: ValidationPipelineDefinition,
): ValidationResult {
  const errors: ValidationIssue[] = [];
  const warnings: ValidationIssue[] = [];

  checkTriggerCount(def.nodes, errors);
  checkCycles(def.nodes, def.edges, errors);

  return {
    errors,
    warnings,
    isValid: errors.length === 0,
    canPublish: errors.length === 0,
  };
}

// ---------------------------------------------------------------------------
// Rule: NO_TRIGGER / MULTIPLE_TRIGGERS — exactly one trigger node required.
// Codes / messages mirror frontend's validatePipeline.ts.
// ---------------------------------------------------------------------------

function checkTriggerCount(
  nodes: ValidationPipelineNode[],
  errors: ValidationIssue[],
): void {
  const triggers = nodes.filter((n) => n.type === 'trigger');
  if (triggers.length === 0) {
    errors.push({
      code: 'NO_TRIGGER',
      severity: 'error',
      message: 'Pipeline must have exactly one Trigger node.',
    });
    return;
  }
  if (triggers.length > 1) {
    for (const trigger of triggers) {
      errors.push({
        code: 'MULTIPLE_TRIGGERS',
        severity: 'error',
        message: 'Pipeline must have exactly one Trigger node.',
        nodeId: trigger.id,
      });
    }
  }
}

// ---------------------------------------------------------------------------
// Rule: CYCLE_DETECTED — iterative WHITE/GRAY/BLACK DFS, mirrors detectCycles.ts.
// ---------------------------------------------------------------------------

function checkCycles(
  nodes: ValidationPipelineNode[],
  edges: ValidationPipelineEdge[],
  errors: ValidationIssue[],
): void {
  const cycleEdgeId = detectCycleEdge(nodes, edges);
  if (cycleEdgeId !== null) {
    errors.push({
      code: 'CYCLE_DETECTED',
      severity: 'error',
      message: 'Pipeline contains a cycle.',
      edgeId: cycleEdgeId,
    });
  }
}

type Color = 0 | 1 | 2; // 0 = WHITE, 1 = GRAY, 2 = BLACK

interface AdjEntry {
  target: string;
  edgeId: string;
}

/** Returns an edge id closing a cycle, or null if the graph is acyclic. O(V+E). */
function detectCycleEdge(
  nodes: ValidationPipelineNode[],
  edges: ValidationPipelineEdge[],
): string | null {
  const adjacency = new Map<string, AdjEntry[]>();
  for (const node of nodes) adjacency.set(node.id, []);
  for (const edge of edges) {
    const list = adjacency.get(edge.source);
    if (list) list.push({ target: edge.target, edgeId: edge.id });
  }

  const color = new Map<string, Color>();
  for (const node of nodes) color.set(node.id, 0);

  for (const startNode of nodes) {
    if (color.get(startNode.id) !== 0) continue;

    const stack: { nodeId: string; index: number }[] = [
      { nodeId: startNode.id, index: 0 },
    ];
    color.set(startNode.id, 1);

    while (stack.length > 0) {
      const frame = stack[stack.length - 1];
      const neighbors = adjacency.get(frame.nodeId) ?? [];

      if (frame.index >= neighbors.length) {
        color.set(frame.nodeId, 2);
        stack.pop();
        continue;
      }

      const next = neighbors[frame.index];
      frame.index += 1;
      const nextColor = color.get(next.target);

      if (nextColor === 1) {
        return next.edgeId;
      }
      if (nextColor === 0) {
        color.set(next.target, 1);
        stack.push({ nodeId: next.target, index: 0 });
      }
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

export const pipelineValidationRouter = Router();

pipelineValidationRouter.post(
  '/',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const body = (req.body ?? {}) as Partial<ValidatePipelineRequest>;
    const def = body.definition;

    if (!def || typeof def !== 'object') {
      throw new ValidationError('definition is required');
    }
    if (!Array.isArray(def.nodes)) {
      throw new ValidationError('definition.nodes must be an array');
    }
    if (!Array.isArray(def.edges)) {
      throw new ValidationError('definition.edges must be an array');
    }

    res.json(validatePipelineStructural(def));
  }),
);
