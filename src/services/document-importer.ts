/**
 * DocumentImporter — pure parsers + a persistence helper for creating
 * sections/items from an uploaded markdown or JSON document.
 *
 * Design:
 *  - `parseMarkdownSections` / `parseJsonImport` are pure and throw
 *    `ValidationError` on bad input. They do no DynamoDB access.
 *  - `applyImport` performs the DynamoDB writes and is injected with
 *    the two repositories it needs so tests can mock them easily.
 */
import { ValidationError } from '../middleware/error-handler';
import type { DocumentSectionRepository } from '../repositories/DocumentSectionRepository';
import type { SectionItemRepository } from '../repositories/SectionItemRepository';

export interface ParsedItem {
  text: string;
  status: string;
  assignee?: string;
  priority?: string;
  dueDate?: string;
  category?: string;
}

export interface ParsedSection {
  title: string;
  type?: string;
  sectionType?: string;
  items: ParsedItem[];
}

/**
 * Parse a markdown document into a list of sections with action items.
 *
 * Recognised:
 *  - `## Title` — opens a new section
 *  - `- [ ] ...` / `- [x] ...` — adds an action item to the current section
 *  - Trailing `(@user, priority: high)` extracts assignee + priority
 *
 * Throws ValidationError if the input is empty, not a string, or contains
 * no recognisable `## ` section heading.
 */
export function parseMarkdownSections(content: unknown): ParsedSection[] {
  if (typeof content !== 'string' || !content.trim()) {
    throw new ValidationError('markdown content is required');
  }

  const sections: ParsedSection[] = [];
  let current: ParsedSection | null = null;

  for (const line of content.split('\n')) {
    const sectionMatch = line.match(/^##\s+(.+)$/);
    if (sectionMatch) {
      current = { title: sectionMatch[1].trim(), items: [] };
      sections.push(current);
      continue;
    }

    const itemMatch = line.match(/^-\s+\[([ xX])\]\s+(.+)$/);
    if (itemMatch && current) {
      const done = itemMatch[1].toLowerCase() === 'x';
      let text = itemMatch[2].trim();
      let assignee: string | undefined;
      let priority: string | undefined;

      const metaMatch = text.match(/\(([^)]+)\)\s*$/);
      if (metaMatch) {
        text = text.slice(0, text.length - metaMatch[0].length).trim();
        const metaParts = metaMatch[1].split(',').map((p) => p.trim());
        for (const part of metaParts) {
          if (part.startsWith('@')) {
            assignee = part.slice(1);
          } else if (part.toLowerCase().startsWith('priority:')) {
            priority = part.split(':')[1].trim();
          }
        }
      }

      current.items.push({
        text,
        status: done ? 'done' : 'open',
        ...(assignee ? { assignee } : {}),
        ...(priority ? { priority } : {}),
      });
    }
  }

  if (sections.length === 0) {
    throw new ValidationError(
      'markdown content did not contain any section headings',
    );
  }

  return sections;
}

/**
 * Parse + validate a JSON export body into the import-ready shape.
 *
 * Accepts either the raw string (as posted) or a pre-parsed object.
 * Throws ValidationError on invalid JSON or on a missing
 * `document.sections` array.
 */
export function parseJsonImport(content: unknown): ParsedSection[] {
  let data: Record<string, unknown>;

  if (typeof content === 'string') {
    if (!content.trim()) {
      throw new ValidationError('JSON content is required');
    }
    try {
      data = JSON.parse(content) as Record<string, unknown>;
    } catch {
      throw new ValidationError('Invalid JSON content');
    }
  } else if (content && typeof content === 'object') {
    data = content as Record<string, unknown>;
  } else {
    throw new ValidationError('JSON content is required');
  }

  const doc = data['document'] as Record<string, unknown> | undefined;
  if (!doc || typeof doc !== 'object') {
    throw new ValidationError('JSON import is missing required "document" field');
  }

  const rawSections = doc['sections'];
  if (!Array.isArray(rawSections)) {
    throw new ValidationError('JSON import is missing required "document.sections" array');
  }

  return rawSections.map((raw) => {
    const sec = raw as Record<string, unknown>;
    const parsed: ParsedSection = {
      title: (sec['title'] as string) ?? 'Untitled',
      type: (sec['type'] as string) ?? 'text',
      ...(sec['sectionType'] ? { sectionType: sec['sectionType'] as string } : {}),
      items: [],
    };

    const rawItems = (sec['items'] ?? []) as Array<Record<string, unknown>>;
    for (const item of rawItems) {
      parsed.items.push({
        text: (item['text'] as string) ?? '',
        status: (item['status'] as string) ?? 'open',
        ...(item['assignee'] ? { assignee: item['assignee'] as string } : {}),
        ...(item['priority'] ? { priority: item['priority'] as string } : {}),
        ...(item['dueDate'] ? { dueDate: item['dueDate'] as string } : {}),
        ...(item['category'] ? { category: item['category'] as string } : {}),
      });
    }

    return parsed;
  });
}

export interface ApplyImportResult {
  sectionsCreated: number;
  itemsCreated: number;
}

export interface ApplyImportDeps {
  sectionRepo: Pick<DocumentSectionRepository, 'createSection' | 'getSectionsForDocument'>;
  itemRepo: Pick<SectionItemRepository, 'createItem'>;
}

/**
 * Persist parsed sections + items to DynamoDB. sortOrder continues from
 * wherever the existing document's sections end.
 *
 * For markdown imports the caller should pass the default type 'text';
 * for JSON imports each section's own type is carried through.
 */
export async function applyImport(
  documentId: string,
  parsed: ParsedSection[],
  deps: ApplyImportDeps,
  opts: { defaultType?: string } = {},
): Promise<ApplyImportResult> {
  const defaultType = opts.defaultType ?? 'text';
  const existing = await deps.sectionRepo.getSectionsForDocument(documentId);
  let sortOrder =
    existing.length > 0 ? Math.max(...existing.map((s) => s.sortOrder)) + 1 : 0;

  let sectionsCreated = 0;
  let itemsCreated = 0;

  for (const section of parsed) {
    const newSection = await deps.sectionRepo.createSection({
      documentId,
      type: section.type ?? defaultType,
      title: section.title,
      ...(section.sectionType ? { sectionType: section.sectionType } : {}),
      sortOrder: sortOrder++,
    });
    sectionsCreated++;

    for (const item of section.items) {
      await deps.itemRepo.createItem({
        documentId,
        sectionId: newSection.sectionId,
        text: item.text,
        status: item.status,
        ...(item.assignee ? { assignee: item.assignee } : {}),
        ...(item.priority ? { priority: item.priority } : {}),
        ...(item.dueDate ? { dueDate: item.dueDate } : {}),
        ...(item.category ? { category: item.category } : {}),
      });
      itemsCreated++;
    }
  }

  return { sectionsCreated, itemsCreated };
}
