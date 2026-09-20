# Document workspace grants

The assessment workspace consumes the existing document REST workflows. Production
`/api/documents/:documentId/**` requests require a short-lived user document grant,
verified by realtime-modules. This includes comments, review requests, section
reviews/items, lifecycle, agent edits and import/export. The public lifecycle
schema is static and remains readable without a document credential.

Configure the single-tenant service with:

- `DOCUMENT_TENANT_ID`: the tenant this deployment serves, e.g. `assessment`.
- `DOCUMENT_GRANT_ISSUER`, `DOCUMENT_GRANT_KEY_ID`, `DOCUMENT_GRANT_PUBLIC_KEY`:
  the trusted BFF issuer and its Ed25519 public key (escaped PEM newlines accepted).
- `DOCUMENT_SESSION_EPOCH_URL`: trusted BFF session policy endpoint.
- `DOCUMENT_POLICY_SECRET`: credential for that server-to-server endpoint.
- Optional `DOCUMENT_GRANT_AUDIENCE` (default `realtime-documents`).

Use `Authorization: Bearer <grant>`; never substitute a gateway client ID for the
verified subject. Each request verifies the signature, expiry, current session
epoch, tenant, canonical document ID and operation. Epoch lookup failure denies
access. Missing configuration returns 503 in production. Nonproduction preserves
legacy fixture authentication only when no grant configuration is present; set
`DOCUMENT_AUTH_MODE=grants` to require the complete configuration locally too.
`SKIP_AUTH` cannot bypass authentication in production.

Grants are explicit sets, not hierarchical roles: `manage` does not imply `edit`.
GET/HEAD needs `read`; comments and review verdicts need `comment`; content,
ordinary item changes and author lifecycle transitions need `edit`; ownership
claims and review-request administration need `manage`. Author-only comment
mutation, assigned reviewer/assignee checks, owner rules and lifecycle CAS still
apply. Lifecycle `availableActions` is narrowed to the grant's operations.

This is a single-tenant deployment boundary, not a multitenant database migration.
Use canonical opaque document UUIDs and separate deployment storage namespaces.
A document grant cannot authenticate unrelated API routes or cross-document inbox
requests. Those continue to require the application's normal user authentication.
Protected uploads are owned by the gateway and must enforce the same grant scope.

## Comment compatibility

Existing section and anchored comment bodies remain valid. Document-wide roots
now use `POST /api/documents/:id/comments` with:

```json
{"scope":"document","text":"General feedback"}
```

Do not fabricate a section or Yjs anchor for an old document-wide comment. Replies
supply `parentCommentId`; the parent must exist under the same document. Text
updates use `PATCH /api/documents/:id/comments/:commentId` with `{"text":"..."}`
and are restricted to the author. Delete is author-only. Resolve/unresolve keeps
its existing `{"resolved":true}` contract. Text and resolution are separate
requests. Missing comments cannot become phantom rows through a resolve update.

GET comments accepts `sectionId`, `limit` and `cursor`, returning `{comments,
nextCursor?}`. Continue until no `nextCursor`; DynamoDB can return an empty filtered
page with a continuation. Cursors are bound to the requested document. Section
queries use the document partition; repeated section IDs in different documents
cannot expose another document's comments.

Notifications are invalidation/refresh signals. `doc:comment_updated` carries
`{documentId,comment}` on `doc-comments:<id>`, alongside existing added/resolved/
deleted signals. Durable REST reads after reconnect are authoritative. These
signals are not a claim of transactional event delivery.

The assessment BFF remains the compatibility authority for pre-existing comment
IDs, original authors/timestamps and per-actor reactions until an explicit,
verified migration transfers those records. This patch does not replace that data
with new IDs or create a second reactions store.

## Authoritative content locks

`GET /api/documents/:documentId/lifecycle/write-policy` returns
`{documentId, canWrite, version}` with `Cache-Control: no-store`. It requires the
same signed document read grant; `canWrite` additionally requires edit or manage.
The decision uses the existing lifecycle `isLocked` rule and a strongly consistent
DynamoDB read, so an acknowledged approval/finalization is visible to the next
mutation. The gateway checks this endpoint on every edit/restore/metadata/upload
operation and fails closed on unavailable or malformed policy. It does not cache
allow decisions or reinterpret lifecycle status. Reads and idempotent initial
seeding remain available.

REST section/import/agent content writes use the same authority. Explicit lifecycle
transitions, including owner-only reopen, retain their existing state-machine and
compare-and-swap checks. Comments and review responses remain separate operations.
This is per-operation authorization, not a cross-service transaction: an operation
already authorized and in flight may complete while another service transitions.
