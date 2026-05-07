/**
 * DocumentExporter — pure builder functions that shape document data
 * into JSON and Markdown export formats.
 *
 * These functions perform NO DynamoDB access. The caller is responsible
 * for fetching the raw data (meta, sections, comments, items, reviews)
 * and passing it in. This keeps the builders trivially testable without mocks.
 */
import type { DocumentComment } from '../repositories/DocumentCommentRepository';
import type { SectionItemFields } from '../repositories/SectionItemRepository';
import type { SectionReview } from '../repositories/SectionReviewRepository';
import type { DocumentSectionFields } from '../repositories/DocumentSectionRepository';

export interface DocumentMeta {
  id: string;
  title: string;
  type: string;
  status: string;
  createdBy: string;
  createdAt: string;
  updatedAt: string;
  icon?: string;
  description?: string;
}

export interface DocumentExportData {
  meta: DocumentMeta;
  sections: DocumentSectionFields[];
  comments: DocumentComment[];
  reviews: SectionReview[];
  items: SectionItemFields[];
}

function groupBySection<T extends { sectionId: string }>(entries: T[]): Map<string, T[]> {
  const map = new Map<string, T[]>();
  for (const entry of entries) {
    const list = map.get(entry.sectionId) ?? [];
    list.push(entry);
    map.set(entry.sectionId, list);
  }
  return map;
}

/**
 * Build the JSON export envelope. Pure — given the same inputs, always
 * returns the same shape.
 */
export function buildJsonExport(data: DocumentExportData) {
  const { meta, sections, comments, reviews, items } = data;

  const commentsBySection = groupBySection(comments);
  const itemsBySection = groupBySection(items);
  const reviewsBySection = groupBySection(reviews);

  return {
    document: {
      meta: {
        id: meta.id,
        title: meta.title,
        type: meta.type,
        status: meta.status,
        createdBy: meta.createdBy,
        createdAt: meta.createdAt,
        updatedAt: meta.updatedAt,
        ...(meta.icon ? { icon: meta.icon } : {}),
        ...(meta.description ? { description: meta.description } : {}),
      },
      sections: sections.map((s) => ({
        id: s.sectionId,
        title: s.title,
        type: s.type,
        ...(s.sectionType ? { sectionType: s.sectionType } : {}),
        sortOrder: s.sortOrder,
        items: (itemsBySection.get(s.sectionId) ?? []).map((i) => ({
          id: i.itemId,
          text: i.text,
          status: i.status,
          ...(i.assignee ? { assignee: i.assignee } : {}),
          ...(i.priority ? { priority: i.priority } : {}),
          ...(i.dueDate ? { dueDate: i.dueDate } : {}),
          ...(i.category ? { category: i.category } : {}),
          ...(i.notes ? { notes: i.notes } : {}),
          createdAt: i.createdAt,
          updatedAt: i.updatedAt,
        })),
        comments: (commentsBySection.get(s.sectionId) ?? []).map((c) => ({
          id: c.commentId,
          text: c.text,
          userId: c.userId,
          displayName: c.displayName,
          timestamp: c.timestamp,
          ...(c.parentCommentId ? { parentCommentId: c.parentCommentId } : {}),
          ...(c.resolved !== undefined ? { resolved: c.resolved } : {}),
        })),
        reviews: (reviewsBySection.get(s.sectionId) ?? []).map((r) => ({
          userId: r.userId,
          displayName: r.displayName,
          status: r.status,
          timestamp: r.timestamp,
          ...(r.comment ? { comment: r.comment } : {}),
        })),
      })),
    },
  };
}

/**
 * Build the Markdown export string. Pure — given the same inputs,
 * always returns the same string.
 */
export function buildMarkdownExport(data: DocumentExportData): string {
  const { meta, sections, comments, reviews, items } = data;
  const lines: string[] = [];

  lines.push(`# ${meta.title}`);
  lines.push(
    `Status: ${meta.status} | Created by: ${meta.createdBy} | Date: ${meta.createdAt.split('T')[0]}`,
  );
  lines.push('');

  const commentsBySection = groupBySection(comments);
  const itemsBySection = groupBySection(items);
  const reviewsBySection = groupBySection(reviews);

  for (const section of sections) {
    lines.push(`## ${section.title}`);
    lines.push('');

    const sectionItems = itemsBySection.get(section.sectionId) ?? [];
    if (sectionItems.length > 0) {
      lines.push('### Action Items');
      for (const item of sectionItems) {
        const check = item.status === 'done' ? 'x' : ' ';
        const parts: string[] = [];
        if (item.assignee) parts.push(`@${item.assignee}`);
        if (item.priority) parts.push(`priority: ${item.priority}`);
        if (item.dueDate) parts.push(`due: ${item.dueDate}`);
        const suffix = parts.length > 0 ? ` (${parts.join(', ')})` : '';
        lines.push(`- [${check}] ${item.text}${suffix}`);
      }
      lines.push('');
    }

    const sectionComments = commentsBySection.get(section.sectionId) ?? [];
    const topLevel = sectionComments.filter((c) => !c.parentCommentId);
    if (topLevel.length > 0) {
      lines.push('### Comments');
      for (const comment of topLevel) {
        const date = comment.timestamp.split('T')[0];
        lines.push(`> **${comment.displayName}** (${date}): ${comment.text}`);
        const replies = sectionComments.filter(
          (c) => c.parentCommentId === comment.commentId,
        );
        for (const reply of replies) {
          const replyDate = reply.timestamp.split('T')[0];
          lines.push(
            `>   > **${reply.displayName}** (${replyDate}): ${reply.text}`,
          );
        }
      }
      lines.push('');
    }

    const sectionReviews = reviewsBySection.get(section.sectionId) ?? [];
    if (sectionReviews.length > 0) {
      lines.push('### Reviews');
      for (const review of sectionReviews) {
        const date = review.timestamp.split('T')[0];
        const label = review.status.charAt(0).toUpperCase() + review.status.slice(1);
        lines.push(`- ${label} by ${review.displayName} (${date})`);
      }
      lines.push('');
    }
  }

  return lines.join('\n');
}
