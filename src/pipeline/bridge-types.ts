// platform-api/src/pipeline/bridge-types.ts
//
// Phase M4.3c — the type surface lives in @connorhoehn/pipeline-runtime
// now. This file re-exports them and keeps the platform-api-local
// singleton (`setPipelineBridge` / `getPipelineBridge`) so existing
// route imports don't change.
//
// The singleton is intentionally NOT in the package — platform-api
// owns the only consumer of this accessor pattern. The historical
// peer (pipeline-worker, decommissioned 2026-05-19) constructed its
// own bridge inline at boot time.

import type { PipelineBridge } from '@connorhoehn/pipeline-runtime';

export type {
  PipelineBridge,
  PipelineBridgeMetrics,
  PipelineRunSnapshot,
  PendingApprovalRow,
  BusEvent,
  ContextPatch,
  PatchResult,
  JsonPatchOperation,
  // v0.3.4 canonical execution types — re-exported so route-layer code
  // can import them from this central bridge-types hub rather than
  // reaching directly into @connorhoehn/pipeline-runtime.
  PipelineRun,
  PipelineRunStatus,
  PipelineStep,
  PipelineStepStatus,
  PipelineContext,
  PipelineResult,
  PipelineApprovalRequest,
  PipelineApprovalDecision,
} from '@connorhoehn/pipeline-runtime';

let bridge: PipelineBridge | null = null;

export function setPipelineBridge(b: PipelineBridge | null): void {
  bridge = b;
}

export function getPipelineBridge(): PipelineBridge | null {
  return bridge;
}
