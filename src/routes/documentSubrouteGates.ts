// platform-api/src/routes/documentSubrouteGates.ts
//
// Loop 29c: the access gate each sub-router under /api/documents/:documentId
// is mounted behind (routes/index.ts). #135's document:read for anything that
// only looks, #41's document:edit for anything that changes the document —
// except where the established model says a reader writes: a reviewer answers
// a review request or files a review, an addressee acks/answers a callout, a
// participant captures a work item or asks for a verdict (commenter = reader,
// as #154).
import { readOnGet, requireDocumentAccessFor } from '../middleware/resource-access';

export const documentSubrouteGates = {
  /** Lifecycle view/transitions: roles are checked inside; claiming ownership takes edit. */
  lifecycle: requireDocumentAccessFor((req) => (req.method === 'POST' && /^\/ownership\/?$/.test(req.path) ? 'edit' : 'read')),
  /** Review requests (document and section): owner/requester/reviewer roles are checked inside. */
  reviewRequests: requireDocumentAccessFor(() => 'read'),
  /** Section reviews and the document's review list: a reviewer is a reader. */
  reviews: requireDocumentAccessFor(() => 'read'),
  /** Section items (callouts): create/edit/delete take edit; the addressee's ack/answer take read. */
  sectionItems: requireDocumentAccessFor(readOnGet([/^\/[^/]+\/(ack|answer)\/?$/])),
  /** Work items: capture and verdicts are a reader's; propose/applied/reject/reattach/confirm change the document. */
  workItems: requireDocumentAccessFor(readOnGet([/^\/?$/, /^\/[^/]+\/verdict\/?$/])),
  /** Work fields and run drafts: read to look, edit to change or dispatch. */
  work: requireDocumentAccessFor(readOnGet()),
  runDrafts: requireDocumentAccessFor(readOnGet()),
};
