// Routes with an id param whose access check is NOT a marked middleware on
// the route (see route-access-guard.test.ts). Each entry says where the check
// lives, or why the route is open to any signed-in person. Adding a route here
// is a claim a reviewer can read; the inventory with the reasoning is
// realtime-examples/docs/design/nfr/access-control-inventory.md (Loop 29c).
//
// Key: 'METHOD /full/path' exactly as the guard prints it.

function each(reason: string, keys: string[]): Record<string, string> {
  return Object.fromEntries(keys.map((k) => [k, reason]));
}

export const ACCESS_ALLOWLIST: Record<string, string> = {
  ...each('public webhook: the owner publishes the path in their own definition; HMAC + replay window when the binding has a secret', [
    'POST /hooks/pipeline/:path',
  ]),
  ...each('in handler: scoped to definitionsRepo.list(caller) plus system pipelines (NFR #174)', [
    'GET /api/capabilities/pipelines/:name',
  ]),
  ...each('in handler: definitions are keyed by the caller (definitionsRepo.get(caller, id)), else a system pipeline', [
    'GET /api/pipelines/defs/:pipelineId',
    'PUT /api/pipelines/defs/:pipelineId',
    'DELETE /api/pipelines/defs/:pipelineId',
    'POST /api/pipelines/defs/:pipelineId/publish',
    'GET /api/pipelines/:pipelineId',
    'POST /api/v2/pipelines/:pipelineId/runs',
    'POST /api/pipelines/:pipelineId/runs/:runId/replay/',
  ]),
  ...each('in handler: trigger runs the caller\'s own (or an inline) definition as the verified caller, pinRequester (#47); the list is filtered to runs the caller may read (NFR #173)', [
    'POST /api/pipelines/:pipelineId/runs/',
    'GET /api/pipelines/:pipelineId/runs/',
  ]),
  ...each('in handler: documentReadGate.assertCanEdit(body.documentId) (#171); a comment-linked run must match that document', [
    'POST /api/pipelines/:pipelineId/runs/:runId/review',
  ]),
  ...each('in handler: hangout rooms assertCanAccess (org + visibility / owner / member), assertOwner for writes; leave goes through /end\'s own-participant check (NFR #175)', [
    'GET /api/rooms/:slug',
    'PATCH /api/rooms/:slug',
    'POST /api/rooms/:slug/archive',
    'GET /api/rooms/:slug/members',
    'POST /api/rooms/:slug/members',
    'DELETE /api/rooms/:slug/members/:userId',
    'POST /api/rooms/:slug/join',
    'POST /api/rooms/:slug/leave',
  ]),
  ...each('OPEN: one global host catalog with no owner field; needs an ownership model, not a gate (inventory, media)', [
    'GET /api/media/catalog/bursts/:burstId',
    'GET /api/media/catalog/files/:fileId',
  ]),
  ...each('in handler: resolveViewer (verified sub + org -> memberships), then authorizeDay / grant owner / capability per read', [
    'GET /api/work-graph/people/:personId/graph',
    'GET /api/work-graph/nodes/:id',
    'GET /api/work-graph/edges/:id',
    'PATCH /api/work-graph/sharing-grants/:id',
    'POST /api/work-graph/transcripts/:id/ask',
    'GET /api/work-graph/previews/:handle',
    'PATCH /api/work-graph/collaboration-requests/:id',
  ]),
  ...each('in handler: session.viewerId === caller, revalidated at use', [
    'GET /api/work-graph/compute-sessions/:personId/:requestKey/:sessionId',
    'POST /api/work-graph/compute-sessions/:personId/:requestKey/:sessionId/use',
    'DELETE /api/work-graph/compute-sessions/:personId/:requestKey/:sessionId',
  ]),
  ...each('in handler: documentReadGate.assertCanEdit (#171); commentId must belong to the document', [
    'POST /api/documents/:documentId/agent-edit',
  ]),
  ...each('in handler: lists / deletes only the caller\'s own automation definitions, bound to this documentId', [
    'GET /api/documents/:documentId/automations',
    'DELETE /api/documents/:documentId/automations/:automationId',
  ]),
  ...each('in handler: the plugin provider gets the caller; the run fallback takes run access or channel membership (NFR #173)', [
    'GET /api/chat/panels/:kind/:id',
  ]),
  ...each('in handler: authorize() — the loop\'s requester or a viewer of its chat channel; shell-url requester only', [
    'GET /api/agent-loops/:runId',
    'GET /api/agent-loops/:runId/files/:fileId',
    'GET /api/agent-loops/:runId/tail',
    'POST /api/agent-loops/:runId/instruct',
    'POST /api/agent-loops/:runId/shell-url',
    'POST /api/agent-loops/:runId/stop',
    'POST /api/agent-loops/:runId/pause',
    'POST /api/agent-loops/:runId/resume',
  ]),
  ...each('in handler: voicemail rows are keyed by the recipient (the caller)', [
    'GET /api/calls/voicemail/:id/audio',
    'POST /api/calls/voicemail/:id/listened',
  ]),
  ...each('public: profiles are visible to signed-in people; a private profile only to its owner (in handler)', [
    'GET /api/profiles/:userId',
  ]),
  ...each('public: the caller is always the follower (key includes the caller)', [
    'POST /api/social/follow/:userId',
    'DELETE /api/social/follow/:userId',
  ]),
  ...each('in handler: org-scoped team load; private teams need membership; owner / member for writes', [
    'GET /api/teams/:teamId',
    'POST /api/teams/:teamId/channels',
    'POST /api/teams/:teamId/members',
    'DELETE /api/teams/:teamId/members/:userId',
    'PATCH /api/teams/:teamId/me',
  ]),
  ...each('in handler: group owner / admin / member / own invite; public groups are open to join and list', [
    'GET /api/groups/:groupId',
    'DELETE /api/groups/:groupId',
    'PATCH /api/groups/:groupId/visibility',
    'POST /api/groups/:groupId/invite',
    'POST /api/groups/:groupId/invitations/:action',
    'POST /api/groups/:groupId/join',
    'DELETE /api/groups/:groupId/leave',
    'GET /api/groups/:groupId/members',
    'POST /api/groups/:groupId/rooms/',
  ]),
  ...each('in handler: legacy rooms — DM rooms refused, group rooms need group membership (NFR #175); leave / members are self / member', [
    'POST /api/rooms/:roomId/join',
    'DELETE /api/rooms/:roomId/leave',
    'GET /api/rooms/:roomId/members',
  ]),
  ...each('in handler: author only, keyed by roomId + postId (+ commentId)', [
    'PUT /api/rooms/:roomId/posts/:postId',
    'DELETE /api/rooms/:roomId/posts/:postId',
    'DELETE /api/rooms/:roomId/posts/:postId/comments/:commentId',
  ]),
  ...each('in handler: documentReadGate.assertCanRead (#154) + author / author-or-owner for changes', [
    'POST /api/documents/:documentId/comments',
    'GET /api/documents/:documentId/comments',
    'PATCH /api/documents/:documentId/comments/:commentId',
    'DELETE /api/documents/:documentId/comments/:commentId',
  ]),
  ...each('in handler: canReadDocument / canEditDocument (#135, #41)', [
    'GET /api/documents/:documentId/revisions/',
    'POST /api/documents/:documentId/revisions/',
    'GET /api/documents/:documentId/revisions/:revisionId/spec',
    'GET /api/documents/:documentId/revisions/:revisionId/file',
    'GET /api/documents/:documentId/revisions/:revisionId/slides',
    'GET /api/documents/:documentId/revisions/:revisionId/slides/:slideId',
  ]),
  ...each('in handler: call-lobby access (lib/lobbyAccess.ts) + own participantId (NFR #175)', [
    'POST /api/video/sessions/:sessionId/join',
    'POST /api/video/sessions/:sessionId/end',
    'POST /api/video/sessions/:sessionId/refresh-token',
    'GET /api/video/sessions/document/:documentId',
    'GET /api/video/sessions/:sessionId/recording',
    'GET /api/video/sessions/by-lobby/:lobbyName',
    'GET /api/video/sessions/:sessionId',
  ]),
  ...each('in handler: requireOwnRecording (recording:read, participant); DELETE also startedBy', [
    'POST /api/recordings/:recordingId/transcribe',
    'PATCH /api/recordings/:recordingId/expiry',
    'GET /api/recordings/:recordingId',
    'POST /api/recordings/:recordingId/playback-token',
    'POST /api/recordings/:recordingId/share',
    'GET /api/recordings/:recordingId/transcript',
    'GET /api/recordings/:recordingId/summary',
    'DELETE /api/recordings/:recordingId',
  ]),
  ...each('public: an unexpired share token is the grant (link holders)', [
    'GET /api/recordings/share/:shareToken/playback',
  ]),
  ...each('public: shared schemas, no content; PUT / DELETE are creator-only in handler (403)', [
    'GET /api/document-types/:typeId',
    'PUT /api/document-types/:typeId',
    'DELETE /api/document-types/:typeId',
    'GET /api/document-types/:typeId/versions',
    'GET /api/document-types/:typeId/versions/:version',
    'GET /api/document-types/:typeId/schema',
  ]),
  ...each('public read: a type\'s records are a shared table; PATCH is creator-only in handler (403)', [
    'GET /api/typed-documents/:documentId',
    'PATCH /api/typed-documents/:documentId',
  ]),
};
