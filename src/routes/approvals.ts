// Phase 51 / hub#62 — REST API for the decisions-renderer approval log.
//
// Endpoints:
//   POST /api/approvals                          → log a new approval entry
//   GET  /api/approvals?documentId=<id>          → list entries for a document
//   GET  /api/approvals?status=<approved|...>    → list entries by decision
//
// The renderer-level CRDT state stays the source of truth for live
// editing; this server-side mirror is the audit/query surface.

import { Router, type Request, type Response } from 'express';
import { randomUUID } from 'crypto';
import { approvalRepo } from '../repositories';
import type { ApprovalEntry, ApprovalDecision } from '../repositories';
import { asyncHandler, ValidationError } from '../middleware/error-handler';

export const approvalsRouter = Router();

const VALID_DECISIONS: ApprovalDecision[] = ['approved', 'rejected', 'changes_requested'];

function parseDecision(raw: unknown): ApprovalDecision {
  if (typeof raw !== 'string' || !VALID_DECISIONS.includes(raw as ApprovalDecision)) {
    throw new ValidationError(`decision must be one of: ${VALID_DECISIONS.join(', ')}`);
  }
  return raw as ApprovalDecision;
}

approvalsRouter.post('/', asyncHandler(async (req: Request, res: Response) => {
  const body = (req.body ?? {}) as Record<string, unknown>;

  const documentId = body.documentId;
  if (typeof documentId !== 'string' || !documentId) {
    throw new ValidationError('documentId is required');
  }
  const sectionId = body.sectionId;
  if (typeof sectionId !== 'string' || !sectionId) {
    throw new ValidationError('sectionId is required');
  }
  const decision = parseDecision(body.decision);

  const reviewerId = req.user!.sub;
  const reviewerName = typeof body.reviewerName === 'string' ? body.reviewerName : undefined;
  const comment = typeof body.comment === 'string' ? body.comment : undefined;

  const entry: ApprovalEntry = {
    documentId,
    workflowId: randomUUID(),
    sectionId,
    reviewerId,
    reviewerName,
    decision,
    comment,
    workflowStatus: decision,
    createdAt: new Date().toISOString(),
  };
  await approvalRepo.create(entry);
  res.status(201).json(entry);
}));

approvalsRouter.get('/', asyncHandler(async (req: Request, res: Response) => {
  const documentId = typeof req.query.documentId === 'string' ? req.query.documentId : '';
  const status = typeof req.query.status === 'string' ? req.query.status : '';

  if (documentId) {
    const items = await approvalRepo.listByDocument(documentId);
    res.status(200).json({ items });
    return;
  }
  if (status) {
    const items = await approvalRepo.listByStatus(parseDecision(status));
    res.status(200).json({ items });
    return;
  }
  throw new ValidationError('either documentId or status query parameter is required');
}));
