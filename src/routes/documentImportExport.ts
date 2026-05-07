import { Router, Request, Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { GetCommand } from '@aws-sdk/lib-dynamodb';
import { docClient } from '../lib/aws-clients';
import {
  asyncHandler,
  ValidationError,
  NotFoundError,
} from '../middleware/error-handler';
import {
  documentCommentRepo,
  sectionReviewRepo,
  sectionItemRepo,
  documentSectionRepo,
} from '../repositories';
import {
  buildJsonExport,
  buildMarkdownExport,
  type DocumentMeta,
} from '../services/document-exporter';
import {
  applyImport,
  parseJsonImport,
  parseMarkdownSections,
} from '../services/document-importer';
import { tableName } from '../lib/ddb-table-name';

const DOCUMENTS_TABLE = tableName(process.env.DYNAMODB_DOCUMENTS_TABLE || 'crdt-documents');

export const documentImportExportRouter = Router({ mergeParams: true });

async function getDocumentMeta(documentId: string): Promise<DocumentMeta | null> {
  const result = await docClient.send(
    new GetCommand({
      TableName: DOCUMENTS_TABLE,
      Key: { documentId },
    }),
  );
  if (!result.Item) return null;
  const item = result.Item;
  return {
    id: item['documentId'] as string,
    title: (item['title'] as string) ?? 'Untitled',
    type: (item['type'] as string) ?? 'custom',
    status: (item['status'] as string) ?? 'draft',
    createdBy: (item['createdBy'] as string) ?? 'unknown',
    createdAt: (item['createdAt'] as string) ?? new Date().toISOString(),
    updatedAt: (item['updatedAt'] as string) ?? new Date().toISOString(),
    ...(item['icon'] ? { icon: item['icon'] as string } : {}),
    ...(item['description'] ? { description: item['description'] as string } : {}),
  };
}

// ---------------------------------------------------------------------------
// GET /api/documents/:documentId/export?format=json|md
// ---------------------------------------------------------------------------

documentImportExportRouter.get(
  '/export',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { documentId } = req.params;
    const format = (req.query['format'] as string) ?? 'json';

    if (format !== 'json' && format !== 'md') {
      throw new ValidationError('format must be "json" or "md"');
    }

    const meta = await getDocumentMeta(documentId);
    if (!meta) {
      throw new NotFoundError('Document not found');
    }

    const [sections, commentsResult, reviews, items] = await Promise.all([
      documentSectionRepo.getSectionsForDocument(documentId),
      documentCommentRepo.getCommentsForDocument(documentId),
      sectionReviewRepo.getReviewsForDocument(documentId),
      sectionItemRepo.getItemsForDocument(documentId),
    ]);

    const comments = commentsResult.items;

    if (format === 'json') {
      res.status(200).json(
        buildJsonExport({ meta, sections, comments, reviews, items }),
      );
    } else {
      const md = buildMarkdownExport({ meta, sections, comments, reviews, items });
      res.setHeader('Content-Type', 'text/markdown; charset=utf-8');
      res.setHeader(
        'Content-Disposition',
        `attachment; filename="${meta.title.replace(/[^a-zA-Z0-9_-]/g, '_')}.md"`,
      );
      res.status(200).send(md);
    }
  }),
);

// ---------------------------------------------------------------------------
// POST /api/documents/:documentId/import
// ---------------------------------------------------------------------------

documentImportExportRouter.post(
  '/import',
  requireAuth,
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const { documentId } = req.params;
    const { format, content } = req.body as { format?: string; content?: string };

    if (!format || (format !== 'markdown' && format !== 'json')) {
      throw new ValidationError('format must be "markdown" or "json"');
    }
    if (!content || typeof content !== 'string' || !content.trim()) {
      throw new ValidationError('content is required');
    }

    const meta = await getDocumentMeta(documentId);
    if (!meta) {
      throw new NotFoundError('Document not found');
    }

    const parsed =
      format === 'markdown'
        ? parseMarkdownSections(content)
        : parseJsonImport(content);

    const { sectionsCreated, itemsCreated } = await applyImport(
      documentId,
      parsed,
      { sectionRepo: documentSectionRepo, itemRepo: sectionItemRepo },
    );

    res.status(201).json({ documentId, sectionsCreated, itemsCreated });
  }),
);
