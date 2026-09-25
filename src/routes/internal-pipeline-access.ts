// routes/internal-pipeline-access.ts
//
// The gateway's subscribe-time check for a pipeline run channel
// (realtime-examples NFR #142). `pipeline:run:<id>` carries a run's step
// outputs and checkpoint contexts, so the gateway asks here before it lets a
// socket that did not start the run subscribe to it.
//
//   POST /internal/pipeline-access/authorize
//     { organizationId, userId, runId?, documentId? }
//   → { allow, ownerId?, documentId?, reason? }
//
// Allowed: the run's owner (`triggeredBy.userId`, from the live executor when
// this replica holds the run), or anyone the `document:read` policy lets read
// the run's target document (#135: owner, editor, viewer, member of the
// document's channel). A document id the gateway already knows (a deck-revise
// channel names it; the run's started event carried it) is checked directly.
// Service-signed like /internal/work-graph; the caller is the gateway, the
// user is the socket's verified identity.

import { Router, type Request, type Response } from 'express';
import { GetCommand } from '@aws-sdk/lib-dynamodb';
import { SERVICE_AUTH_HEADER, verifyEnvelope } from '@connorhoehn/service-runtime';
import { canReadDocument, type EditableDocumentRow } from '../deck/documentEditAccess';
import { getPipelineBridge } from '../pipeline/bridge-types';
import { docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';

const router = Router();
const ID = /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$/;
const DOCUMENTS_TABLE = tableName(process.env.DYNAMODB_DOCUMENTS_TABLE || 'crdt-documents');

export interface PipelineAccessDeps {
  /** The live executor's view of a run: who started it and on which document. */
  liveRun(runId: string): Promise<{ ownerId?: string; documentId?: string } | null>;
  documentRow(documentId: string): Promise<EditableDocumentRow | null>;
  /**
   * NFR #172: the run's owner shows it to this user in their work graph — the
   * graph's own decision, so a visible run node is a run you get live.
   */
  sharedThroughGraph?(organizationId: string, userId: string, runId: string, graphs: GraphScope[]): Promise<{ allow: boolean; ownerId?: string }>;
}

export interface GraphScope { personId: string; day: string; timezone: string }

function str(value: unknown): string | undefined {
  return typeof value === 'string' && value ? value : undefined;
}

export const defaultPipelineAccessDeps: PipelineAccessDeps = {
  async liveRun(runId) {
    const bridge = getPipelineBridge() as { getRun?: (id: string) => unknown } | null;
    if (!bridge?.getRun) return null;
    const snap = (await Promise.resolve(bridge.getRun(runId)).catch(() => null)) as Record<string, any> | null;
    if (!snap || typeof snap !== 'object') return null;
    const trigger = (snap.triggeredBy ?? {}) as Record<string, any>;
    const payload = (trigger.payload ?? {}) as Record<string, any>;
    return {
      ownerId: str(trigger.userId),
      documentId: str(payload.documentId) ?? str(snap.context?.documentId),
    };
  },
  async documentRow(documentId) {
    const res = await docClient.send(new GetCommand({ TableName: DOCUMENTS_TABLE, Key: { documentId } }));
    return (res.Item as EditableDocumentRow | undefined) ?? null;
  },
  async sharedThroughGraph(organizationId, userId, runId, graphs) {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { runSharedThroughWorkGraph } = require('../work-graph/runtime') as typeof import('../work-graph/runtime');
    return runSharedThroughWorkGraph(organizationId, userId, runId, graphs);
  },
};

export async function decidePipelineAccess(
  body: { organizationId: string; userId: string; runId?: string; documentId?: string },
  deps: PipelineAccessDeps = defaultPipelineAccessDeps,
): Promise<{ allow: boolean; ownerId?: string; documentId?: string; reason?: string }> {
  const live = body.runId ? await deps.liveRun(body.runId).catch(() => null) : null;
  const ownerId = live?.ownerId;
  if (ownerId && ownerId === body.userId) return { allow: true, ownerId, ...(live?.documentId ? { documentId: live.documentId } : {}) };
  // Loop 29c (NFR #173): a run this replica knows is decided on its OWN
  // document. A different body documentId cannot borrow access: reading
  // document A must not open a run of document B.
  if (live?.documentId && body.documentId && body.documentId !== live.documentId) {
    return { allow: false, ...(ownerId ? { ownerId } : {}), documentId: live.documentId, reason: 'documentId does not match the run' };
  }
  const documentId = live?.documentId ?? body.documentId;
  if (!documentId || !ID.test(documentId)) {
    return { allow: false, ...(ownerId ? { ownerId } : {}), reason: live ? 'not the run owner and the run names no document' : 'run not known here' };
  }
  const row = await deps.documentRow(documentId).catch(() => null);
  if (!row) return { allow: false, ...(ownerId ? { ownerId } : {}), documentId, reason: 'document not found' };
  const decision = await canReadDocument(row, { sub: body.userId, orgId: body.organizationId }, documentId);
  return decision.allow
    ? { allow: true, ...(ownerId ? { ownerId } : {}), documentId }
    : { allow: false, ...(ownerId ? { ownerId } : {}), documentId, reason: decision.reason };
}

const DAY = /^\d{4}-\d{2}-\d{2}$/;
const TIMEZONE = /^[A-Za-z0-9_+\-/]{1,64}$/;
function validGraphs(value: unknown): value is GraphScope[] {
  return Array.isArray(value) && value.length <= 4 && value.every((g) => g && typeof g === 'object'
    && typeof g.personId === 'string' && ID.test(g.personId)
    && typeof g.day === 'string' && DAY.test(g.day)
    && typeof g.timezone === 'string' && TIMEZONE.test(g.timezone)
    && Object.keys(g).every((k) => ['personId', 'day', 'timezone'].includes(k)));
}

function authenticated(req: Request, res: Response): boolean {
  const secret = process.env.SERVICE_AUTH_SECRET?.trim();
  const header = req.header(SERVICE_AUTH_HEADER);
  if (!secret || !header) {
    res.status(401).json({ error: 'SERVICE_AUTH_REQUIRED' });
    return false;
  }
  const result = verifyEnvelope(header, {
    secret,
    allowedServices: [process.env.WORK_GRAPH_GATEWAY_SERVICE_ID?.trim() || 'realtime-examples'],
    payload: req.body as object,
    ttlSec: 30,
    toleranceSec: 5,
  });
  if (!result.ok) {
    res.status(401).json({ error: 'SERVICE_AUTH_INVALID', reason: result.reason });
    return false;
  }
  return true;
}

export function createInternalPipelineAccessRouter(deps: PipelineAccessDeps = defaultPipelineAccessDeps): Router {
  const r = Router();
  r.post('/authorize', async (req, res, next) => {
    try {
      if (!authenticated(req, res)) return;
      const b = req.body as Record<string, unknown>;
      const valid = b && typeof b === 'object'
        && typeof b.organizationId === 'string' && ID.test(b.organizationId)
        && typeof b.userId === 'string' && ID.test(b.userId)
        && (b.runId === undefined || (typeof b.runId === 'string' && ID.test(b.runId)))
        && (b.documentId === undefined || (typeof b.documentId === 'string' && ID.test(b.documentId)))
        && (b.runId !== undefined || b.documentId !== undefined)
        && (b.graphs === undefined || validGraphs(b.graphs))
        && Object.keys(b).every((k) => ['organizationId', 'userId', 'runId', 'documentId', 'graphs'].includes(k));
      if (!valid) {
        res.status(400).json({ error: 'INVALID_REQUEST' });
        return;
      }
      const { graphs, ...body } = b as any;
      const decided = await decidePipelineAccess(body, deps);
      // NFR #172: not the owner, not a reader of its document — does the
      // owner's work graph show it to this user?
      if (!decided.allow && body.runId && deps.sharedThroughGraph) {
        const shared = await deps.sharedThroughGraph(body.organizationId, body.userId, body.runId, graphs ?? []).catch(() => null);
        if (shared?.allow) {
          res.status(200).json({ allow: true, ownerId: shared.ownerId, ...(decided.documentId ? { documentId: decided.documentId } : {}), via: 'work-graph' });
          return;
        }
        if (shared?.ownerId && !decided.ownerId) {
          res.status(200).json({ ...decided, ownerId: shared.ownerId });
          return;
        }
      }
      res.status(200).json(decided);
    } catch (error) {
      next(error);
    }
  });
  return r;
}

router.use(createInternalPipelineAccessRouter());
export { router as internalPipelineAccessRouter };
