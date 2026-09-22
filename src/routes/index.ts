import { Router } from 'express';
import { profilesRouter } from './profiles';
import { socialRouter } from './social';
import { groupsRouter } from './groups';
import { teamsRouter } from './teams';
import { groupMembersRouter } from './group-members';
import { roomsRouter } from './rooms';
import { groupRoomsRouter } from './group-rooms';
import { roomMembersRouter, myRoomsRouter } from './room-members';
import { postsRouter, userPostsRouter } from './posts';
import { commentsRouter } from './comments';
import { postLikesRouter, commentLikesRouter } from './likes';
import { reactionsRouter } from './reactions';
import { activityRouter } from './activity';
import { documentCommentsRouter } from './documentComments';
import { voiceCaptureRouter } from './voiceCapture';
import { sectionReviewsRouter, documentReviewsRouter, myReviewsRouter } from './sectionReviews';
import { sectionItemsRouter, myItemsRouter } from './sectionItems';
import { documentAgentEditRouter } from './documentAgentEdit';
import { documentAutomationsRouter } from './documentAutomations';
import { chatCommandsRouter } from './chatCommands';
import { chatPanelsRouter } from './chatPanels';
import { agentLoopsRouter } from './agentLoops';
import { requireAuth } from '../middleware/auth';
import { callLogRouter } from './callLog';
import { workItemsRouter } from './workItems';
import { documentLifecycleRouter, lifecycleSchemaRouter } from './documentLifecycle';
import {
  documentReviewRequestsRouter,
  sectionReviewRequestsRouter,
} from './documentReviewRequests';
import { documentImportExportRouter } from './documentImportExport';
import { documentRevisionsRouter } from './documentRevisions';
import { deckRouter } from './deck';
import { videoSessionsRouter } from './videoSessions';
import { recordingsRouter } from './recordings';
import {
  pipelineMetricsRouter,
  observabilityRouter,
  pipelineDashboardDataRouter,
} from './pipelineMetrics';
import { pipelineDefinitionsRouter } from './pipelineDefinitions';
import { pipelineStepKindsRouter } from './pipelineStepKinds';
import { pipelineHealthRouter } from './pipelineHealth';
import {
  pipelineTriggersRouter,
  pipelineApprovalsRouter,
  pipelineActiveRunsRouter,
  pipelineCancelRouter,
  pipelinePendingApprovalsRouter,
  pipelineBreakpointRouter,
} from './pipelineTriggers';
import { pipelineReplayRouter } from './pipelineReplay';
import { pipelineRetryRouter, pipelineRunActionsRouter } from './pipelineRetry';
import { pipelineResumeRouter } from './pipelineResume';
import { pipelineBreakpointPatchRouter } from './pipelineBreakpointPatch';
import { pipelineRunAuditRouter, pipelineAuditSearchRouter } from './pipelineAudit';
import { judgmentsRouter } from './judgments';
import { imagesRouter } from './images';
import { pipelineValidationRouter } from './pipelineValidation';
import { pipelineInspectorRouter } from './pipelineInspector';
import { pipelineDLQRouter } from './pipelineDLQ';
import { pipelineLLMStreamRouter } from './pipelineLLMStream';
import { pipelineCostRouter } from './pipelineCost';
import { pipelineLineageRouter } from './pipeline-lineage';
import { pipelineSchemasRouter } from './pipeline-schemas';
import { pipelinePreviewAdjustLayerRouter } from './pipeline-preview-adjust-layer';
import { pipelinePreviewVisionRouter } from './pipeline-preview-vision';
import { documentTypesRouter } from './documentTypes';
import { typedDocumentsRouter } from './typedDocuments';
import { approvalsRouter } from './approvals';
import { createWorkGraphRouter } from './work-graph';
import { workGraphOrchestrator } from '../work-graph/runtime';
import { workGraphComputeSessionsRouter } from './work-graph-compute-sessions';

// Module-shaped pipeline router (Phase A of the distributed-core absorption
// — see .planning/HANDOFF-distributed-core-platform-api-absorption.md). Lives
// under /api/v2/* during the migration so the legacy `/api/pipelines/*`
// surface keeps working unchanged. New routes land here first; they get
// promoted to /api/* once the legacy counterpart is removed.
import { createPipelineRouter } from '../pipeline/http/createPipelineRouter';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { getRunPersistenceService } from '../pipeline/runPersistence';
import { definitionsRepo } from '../pipeline/definitions-repository';
import type { Request } from 'express';
import type {
  AuthContext,
  PipelineRunDetailResponse,
} from '../pipeline/http/contracts';

const router = Router();

// Active now work graph. Authentication is applied at the application mount;
// the router additionally resolves the verified subject + organization into
// server-owned memberships before every operation.
router.use('/work-graph', createWorkGraphRouter(workGraphOrchestrator));
router.use('/work-graph/compute-sessions', workGraphComputeSessionsRouter);

// Before the `/documents/:documentId/...` routers, so `transform` is never read as an id.
// The chat's /doc command → a pipeline run that edits the document as a collaborator.
router.use('/documents', documentAgentEditRouter);
// Doc bots: a scheduled instruction against one document.
router.use('/documents', documentAutomationsRouter);
// Server-backed chat commands: GET /api/chat/commands lists them, POST /api/chat/commands/:trigger/run starts one as a pipeline run.
router.use('/chat/commands', requireAuth, chatCommandsRouter);
// What a chat card's `panel` result opens: GET /api/chat/panels/:kind/:id → the run's final panelData.
router.use('/chat/panels', requireAuth, chatPanelsRouter);
// The chat box as a computer lab: one /agent run's status, stream, prompt and stop.
router.use('/agent-loops', requireAuth, agentLoopsRouter);
router.use('/calls', callLogRouter);
router.use('/profiles', profilesRouter);
router.use('/social', socialRouter);
// Teams and their channels. A channel is a hangout room carrying the
// team's teamId, so `/api/rooms/*` still owns everything about one
// channel; this router owns the team that groups them.
router.use('/teams', teamsRouter);
router.use('/groups', groupsRouter);
router.use('/groups/:groupId', groupMembersRouter);
router.use('/groups/:groupId/rooms', groupRoomsRouter);
router.use('/rooms', myRoomsRouter);
router.use('/rooms', roomsRouter);
router.use('/rooms/:roomId', roomMembersRouter);
router.use('/rooms/:roomId/posts', postsRouter);
router.use('/posts', userPostsRouter);
router.use('/rooms/:roomId/posts/:postId/comments', commentsRouter);
router.use('/rooms/:roomId/posts/:postId/likes', postLikesRouter);
router.use('/rooms/:roomId/posts/:postId/comments/:commentId/likes', commentLikesRouter);
router.use('/rooms/:roomId/posts/:postId', reactionsRouter);
router.use('/activity', activityRouter);
router.use('/documents', documentCommentsRouter);
// Static segment BEFORE any `:documentId` mount — Express resolves mounts in
// registration order, so `/documents/:documentId` would otherwise swallow it.
router.use('/documents/lifecycle-schema', lifecycleSchemaRouter);
router.use('/documents/:documentId/lifecycle', documentLifecycleRouter);
router.use('/documents/:documentId/review-requests', documentReviewRequestsRouter);
router.use('/documents/:documentId/sections/:sectionId/review-requests', sectionReviewRequestsRouter);
router.use('/documents/:documentId/sections/:sectionId/reviews', sectionReviewsRouter);
router.use('/documents/:documentId/reviews', documentReviewsRouter);
router.use('/reviews', myReviewsRouter);
router.use('/documents/:documentId/sections/:sectionId/items', sectionItemsRouter);
router.use('/items', myItemsRouter);
// Work items: the utterance -> proposal -> acceptance lane. Addressed by itemId
// alone (not documentId/sectionId/itemId) because re-attaching an item moves it
// between sections and a URL that encoded the section would break on every move.
// Must precede the '/documents/:documentId' mount below, which would otherwise
// swallow '/work-items' as a sub-path of the import/export router.
router.use('/documents/:documentId/work-items', workItemsRouter);
// A presentation's stored revisions: the .pptx and the per-slide SVG previews
// the deck generator files. Same reason as work-items — it must precede the
// '/documents/:documentId' mount, which would otherwise swallow '/revisions'.
router.use('/documents/:documentId/revisions', documentRevisionsRouter);
// The deck editor's two stateless paths: preview a spec, and derive one from
// real local material. Neither persists — /documents/:id/revisions does that.
router.use('/deck', deckRouter);
router.use('/documents/:documentId', documentImportExportRouter);
// Ambient voice capture — authenticated PCM proxy in front of the
// live-captions sidecar. See routes/voiceCapture.ts.
router.use('/voice-capture', voiceCaptureRouter);
router.use('/video', videoSessionsRouter);
// Recordings — mounted at /api directly so paths are
// /api/recordings, /api/recordings/:id, /api/recordings/:id/playback-token.
router.use('/', recordingsRouter);
// Mount metrics (static segment) BEFORE defs — Express resolves mounts in
// registration order. A hypothetical `/pipelines` mount with a `:pipelineId`
// param would otherwise swallow `/pipelines/metrics`.
router.use('/pipelines/metrics', pipelineMetricsRouter);
// Module dashboard data — pass-through of PipelineModule.getDashboardData()
// (chart definitions, summary KPIs, alerts). Static segment, same precedence
// rule as `/metrics`.
router.use('/pipelines/dashboard-data', pipelineDashboardDataRouter);
router.use('/pipelines/defs', pipelineDefinitionsRouter);
// The catalog of executable step kinds. Static segment, same precedence rule
// as `/metrics` and `/defs`.
router.use('/pipelines/step-kinds', pipelineStepKindsRouter);
// Health introspection — same precedence rule as `/metrics` and `/defs`:
// register the static segment BEFORE any `:pipelineId` mount.
router.use('/pipelines/health', pipelineHealthRouter);
// Validation — POST /api/pipelines/validate. Static segment, must precede
// the `:pipelineId` mount below for the same reason as `/metrics`/`/defs`.
router.use('/pipelines/validate', pipelineValidationRouter);
// Active-runs listing — GET /api/pipelines/runs/active. Static segment,
// must precede the `:pipelineId` mount below so `runs` doesn't get
// interpreted as a pipelineId.
router.use('/pipelines/runs/active', pipelineActiveRunsRouter);
// Pending-approval queue — GET /api/pipelines/approvals. Static segment;
// must precede BOTH the `:pipelineId` mount AND the `:runId/approvals`
// mount below — otherwise `approvals` would be captured as a `:runId`
// param and dispatched to the POST-approval handler (which 405s on GET).
router.use('/pipelines/approvals', pipelinePendingApprovalsRouter);
// T13 inspector (lib-expansion-3) — operator introspection over the
// run-queue. Static segment, must precede the `:pipelineId` mount.
router.use('/pipelines/inspector', pipelineInspectorRouter);
// T9 DLQ (lib-expansion-3) — EventBus dead-letter inspect/redrive. Static
// segment, must precede the `:pipelineId` mount for the same reason.
router.use('/pipelines/dlq', pipelineDLQRouter);
// LLM streaming proxy — POST /api/pipelines/llm/stream. Static segment,
// must precede the `:pipelineId` mount so `llm` doesn't get captured.
router.use('/pipelines/llm/stream', pipelineLLMStreamRouter);
// Output-schema introspection — GET /api/pipeline/output-schemas. Read-only;
// no auth required (schemas are structural metadata, not user data). Static
// path with a singular `pipeline` prefix to distinguish from the plural
// `/pipelines/*` run/definition surface.
router.use('/pipeline/output-schemas', pipelineSchemasRouter);
// Preview routes — ephemeral, no storage or run records. Used by live
// slider panels (adjust-layer) and the vision prompt bench.
router.use('/pipeline/preview/adjust-layer', pipelinePreviewAdjustLayerRouter);
router.use('/pipeline/preview/vision', pipelinePreviewVisionRouter);
// Image lineage DAG — GET /api/pipeline/runs/:runId/image-lineage?pipelineId=<id>.
// Returns per-run image artifact entries with parent edges for the lineage graph.
router.use('/pipeline/runs/:runId/image-lineage', pipelineLineageRouter);
// hub#2225 audit search — GET /api/pipelines/audit/search?actor=&pipelineId=...
// Static segment, must precede the `:pipelineId` mount below for the same
// reason as `/metrics`/`/defs`/`/llm/stream`.
router.use('/pipelines/audit/search', pipelineAuditSearchRouter);
// llm:judge data plane — GET /api/judgments/:id/raw-text + /aggregates.
// Mounted at the top-level so the URL surface stays clean: judgments are
// addressable by their stable judgmentId rather than parent pipeline/run.
router.use('/judgments', judgmentsRouter);
// image:vision-analyze data plane — POST + GET /api/images.
// Mounted at the top-level (same convention as /judgments). Images are
// addressable by their stable imageId rather than parent pipeline/run;
// uploads come in before any pipeline has been triggered.
router.use('/images', imagesRouter);
// Per-pipeline cost aggregation — GET /api/pipelines/:pipelineId/cost.
// Mounted alongside other :pipelineId routes, after static segments.
router.use('/pipelines/:pipelineId/cost', pipelineCostRouter);
// Per-pipeline run trigger. Mounted AFTER the static `/metrics`, `/defs`,
// `/health`, `/validate`, `/runs/active`, and `/approvals` segments so
// their specific paths match first (Express resolves mounts in
// registration order — a `:pipelineId` mount would otherwise swallow them).
router.use('/pipelines/:pipelineId/runs', pipelineTriggersRouter);
// Replay — POST /api/pipelines/:pipelineId/runs/:runId/replay.
router.use('/pipelines/:pipelineId/runs/:runId/replay', pipelineReplayRouter);
// Retry from node — POST /api/pipelines/:pipelineId/runs/:runId/retry/:nodeId.
router.use('/pipelines/:pipelineId/runs/:runId/retry', pipelineRetryRouter);
// hub#2225 — per-run audit timeline. Mounted at the same level as retry/replay
// so the URL stays in shape with /pipelines/:pipelineId/runs/:runId/<verb>.
// Read-only: emits no audit row of its own.
router.use('/pipelines/:pipelineId/runs/:runId/audit', pipelineRunAuditRouter);
// hub#2223 — Resume a failed run from `?fromNodeId=<id>` (re-enters the same
// run via PipelineModule.resumeFromStep, preserving accumulated context).
// Must precede the more general `/pipelines/:pipelineId/runs/:runId` mount
// below so the action segment is matched first.
router.use('/pipelines/:pipelineId/runs/:runId/resume', pipelineResumeRouter);
// Run-level actions (force-fail, etc.) — POST /api/pipelines/:pipelineId/runs/:runId/<action>.
router.use('/pipelines/:pipelineId/runs/:runId', pipelineRunActionsRouter);
// Debug breakpoint resolve — POST /api/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/resolve.
router.use('/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/resolve', pipelineBreakpointRouter);
// hub#2226 edit-and-continue:
//   POST   /api/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/patch
//   DELETE /api/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/patch/:patchId
router.use(
  '/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/patch',
  pipelineBreakpointPatchRouter,
);
// Cancel — POST /api/pipelines/:runId/cancel. Mounted at the same level
// as approvals; both are run-scoped (not pipeline-scoped).
router.use('/pipelines/:runId/cancel', pipelineCancelRouter);
// Approvals — POST /api/pipelines/:runId/approvals (per PIPELINES_PLAN §17.10).
router.use('/pipelines/:runId/approvals', pipelineApprovalsRouter);
// The same definitions at the address people (and curl) reach for first.
// Registered AFTER every other /pipelines mount: a `:pipelineId` GET declared
// earlier swallowed /pipelines/dlq, /pipelines/defs and friends (observed).
router.get('/pipelines', (req, res, next) => { req.url = '/'; pipelineDefinitionsRouter(req, res, next); });
router.get('/pipelines/:pipelineId', (req, res, next) => { req.url = `/${encodeURIComponent(String(req.params.pipelineId))}`; pipelineDefinitionsRouter(req, res, next); });

// ---------------------------------------------------------------------------
// /api/v2 — module-shaped pipeline router. Phase A of the distributed-core
// absorption: same code that will eventually `git mv` into
// distributed-core/src/applications/pipeline/http/. Hosts inject auth +
// persistence + the bridge into the runtime.
// ---------------------------------------------------------------------------
router.use(
  '/v2',
  createPipelineRouter({
    // Cognito-backed when present; falls through to dev-user when SKIP_AUTH
    // is on and no token was attached (matches the legacy handler's posture).
    getAuthContext: (req: Request): AuthContext => {
      const u = (req as Request & { user?: { sub?: string } }).user;
      return { userId: u?.sub ?? 'dev-user' };
    },
    getDefinition: async (userId, pipelineId) =>
      definitionsRepo.get(userId, pipelineId),
    trigger: async (args) => {
      const bridge = getPipelineBridge();
      if (!bridge?.trigger) {
        throw new Error('pipeline bridge not wired');
      }
      // The bridge's trigger surface predates `readonly` modifiers; copy
      // breakpoints into a mutable array so the host stays compatible
      // with both the contracts file (readonly) and the legacy bridge.
      const out = await Promise.resolve(
        bridge.trigger({
          pipelineId: args.pipelineId,
          definition: args.definition,
          triggeredBy: args.triggeredBy,
          triggerPayload: args.triggerPayload,
          breakpoints: args.breakpoints ? [...args.breakpoints] : undefined,
        }),
      );
      return { runId: out.runId };
    },
    registerTrigger: (args) => {
      getRunPersistenceService()?.registerTrigger(args);
    },
    // Live state — runtime's in-memory snapshot. Has full step
    // input/output for runs the bridge is currently executing or
    // recently completed; null once the bridge has evicted the row.
    getLiveRun: async (runId) => {
      const bridge = getPipelineBridge();
      if (!bridge?.getRun) return null;
      const snap = await Promise.resolve(bridge.getRun(runId));
      return (snap as PipelineRunDetailResponse | null) ?? null;
    },
    // Durable fallback. listRuns already does PipelineRunItem →
    // snapshot conversion for the list endpoint; reuse the mapping
    // by filtering its result rather than duplicating the converter.
    // Effectively a small in-memory scan over the most-recent N rows
    // for the pipeline; acceptable at single-node scale.
    getStoredRun: async (pipelineId, runId) => {
      const bridge = getPipelineBridge();
      if (!bridge?.listRuns) return null;
      const out = await Promise.resolve(
        bridge.listRuns(pipelineId, { limit: 100 }),
      );
      const found = (out.runs ?? []).find((r) => r.runId === runId);
      return (found as PipelineRunDetailResponse | undefined) ?? null;
    },
  }),
);
// Observability — GET /api/observability/dashboard, /api/observability/metrics.
router.use('/observability', observabilityRouter);

// Phase 51 Phase A — document type schemas + typed document instances.
router.use('/document-types', documentTypesRouter);
router.use('/typed-documents', typedDocumentsRouter);

// Phase 51 / hub#62 — append-only approval log mirrored from the
// decisions renderer into the approval-workflows DDB table.
router.use('/approvals', approvalsRouter);

export default router;
