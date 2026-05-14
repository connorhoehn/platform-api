// platform-api/src/pipeline/bridge-types.ts
//
// Phase M4.3c — the type surface lives in @connorhoehn/pipeline-runtime
// now (shared with pipeline-worker). This file re-exports them and keeps
// the platform-api-local singleton (`setPipelineBridge` /
// `getPipelineBridge`) so existing route imports don't change.
//
// The singleton is intentionally NOT in the package — pipeline-worker
// constructs its own bridge directly inside its boot path; only
// platform-api needs the route-side accessor pattern.

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
} from '@connorhoehn/pipeline-runtime';

let bridge: PipelineBridge | null = null;

export function setPipelineBridge(b: PipelineBridge | null): void {
  bridge = b;
}

export function getPipelineBridge(): PipelineBridge | null {
  return bridge;
}
