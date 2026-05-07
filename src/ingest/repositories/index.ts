// Singleton instances for ingest repositories — mirrors the pattern in
// social-api/src/repositories/index.ts. Share the same docClient so
// connection pooling is centralized.

import { docClient } from '../../lib/aws-clients';
import { IngestSourceRepository } from './IngestSourceRepository';
import { IngestItemRepository } from './IngestItemRepository';
import { SyncCursorRepository } from './SyncCursorRepository';
import { MappingDecisionRepository } from './MappingDecisionRepository';
import { MappingReviewQueueRepository } from './MappingReviewQueueRepository';
import { DocTypeProposalRepository } from './DocTypeProposalRepository';
import { IngestAuditRepository } from './IngestAuditRepository';

export const ingestSourceRepo = new IngestSourceRepository(docClient);
export const ingestItemRepo = new IngestItemRepository(docClient);
export const syncCursorRepo = new SyncCursorRepository(docClient);
export const mappingDecisionRepo = new MappingDecisionRepository(docClient);
export const mappingReviewQueueRepo = new MappingReviewQueueRepository(docClient);
export const docTypeProposalRepo = new DocTypeProposalRepository(docClient);
export const ingestAuditRepo = new IngestAuditRepository(docClient);

export { IngestSourceRepository } from './IngestSourceRepository';
export { IngestItemRepository } from './IngestItemRepository';
export { SyncCursorRepository } from './SyncCursorRepository';
export { MappingDecisionRepository } from './MappingDecisionRepository';
export { MappingReviewQueueRepository } from './MappingReviewQueueRepository';
export { DocTypeProposalRepository } from './DocTypeProposalRepository';
export { IngestAuditRepository } from './IngestAuditRepository';
