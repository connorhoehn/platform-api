// platform-api/src/ingest/routes/ingestMapping.ts
//
// REST endpoints for ingest mapping preview (dry-run) + needs-review queue.
//
// Endpoints (mounted at /api/ingest/mapping in routes/index.ts):
//   POST   /preview       — dry-run mapping rules against a file descriptor
//   GET    /review        — list items the LLM mapper flagged below the
//                            confidence threshold (Tier 2 / AGENTIC_ETL_VISION §B)

import { Router, type Request, type Response } from 'express';
import { asyncHandler, ValidationError } from '../../middleware/error-handler';
import { withContext } from '../../lib/logger';
import { matchByRules } from '../mapping/rule-matcher';
import { ingestItemRepo } from '../repositories';
import type { FileDescriptor, MappingRule } from '../types';

/**
 * Confidence threshold below which an LLM-mapped item lands in the
 * needs-review queue. AGENTIC_ETL_VISION §B Tier 2.
 */
const MAPPING_REVIEW_CONFIDENCE_THRESHOLD = 0.7;

const log = withContext({ route: 'ingestMapping' });

export const ingestMappingRouter = Router();

// Preview mapping
ingestMappingRouter.post(
  '/preview',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const body = (req.body ?? {}) as Record<string, unknown>;

    if (!body.file || typeof body.file !== 'object' || Array.isArray(body.file)) {
      throw new ValidationError('file is required and must be an object (FileDescriptor)');
    }
    if (!Array.isArray(body.rules)) {
      throw new ValidationError('rules is required and must be an array of MappingRule[]');
    }

    const file = body.file as FileDescriptor;
    const rules = body.rules as MappingRule[];

    // Validate minimal FileDescriptor shape
    if (typeof file.path !== 'string' || typeof file.name !== 'string') {
      throw new ValidationError('file must include at least path and name fields');
    }

    const result = matchByRules(file, rules);

    log.info(
      { matched: result !== null, ruleId: result?.ruleId ?? null, action: 'mapping.preview' },
      'mapping preview',
    );
    res.json({ result });
  }),
);

// Needs-review queue — items the LLM mapper produced low-confidence matches
// for. Operator decides accept / re-map / discard. The threshold can be
// overridden via `?threshold=<float>` for forensic queries.
ingestMappingRouter.get(
  '/review',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const thresholdParam = typeof req.query.threshold === 'string' ? Number(req.query.threshold) : NaN;
    const threshold = Number.isFinite(thresholdParam) && thresholdParam > 0 && thresholdParam <= 1
      ? thresholdParam
      : MAPPING_REVIEW_CONFIDENCE_THRESHOLD;

    // Pull items currently in the mapping/triggering pipeline that the LLM
    // touched. We scan the two non-terminal statuses where a low-confidence
    // mapping decision is most likely to be sitting.
    const queued = await ingestItemRepo.listByStatus('mapping', 200);
    const triggering = await ingestItemRepo.listByStatus('triggering', 200);
    const all = [...queued, ...triggering];

    const review = all.filter((item) =>
      item.mappingMethod === 'llm-match' &&
      typeof item.mappingConfidence === 'number' &&
      item.mappingConfidence < threshold,
    );

    log.info(
      { threshold, total: all.length, review: review.length, action: 'mapping.review.list' },
      'mapping review queue listed',
    );
    res.json({ items: review, threshold });
  }),
);
