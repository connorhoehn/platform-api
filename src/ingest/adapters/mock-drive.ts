// platform-api/src/ingest/adapters/mock-drive.ts
//
// Mock Google Drive-like adapter with nested folder structure, file metadata
// (tags, owners, shared status), and tag-based filtering. Simulates change
// notifications by allowing files to be modified between sync cycles.

import * as crypto from 'crypto';
import * as path from 'path';
import {
  SourceAdapter,
  SourceConfig,
  SyncCursor,
  FileDescriptor,
  FileContent,
  AdapterCapabilities,
} from '../types';

const MIME_MAP: Record<string, string> = {
  '.json': 'application/json',
  '.csv': 'text/csv',
  '.md': 'text/markdown',
  '.txt': 'text/plain',
  '.pdf': 'application/pdf',
};

function inferMimeType(filePath: string): string {
  const ext = path.extname(filePath).toLowerCase();
  return MIME_MAP[ext] || 'application/octet-stream';
}

function isTextMime(mimeType: string): boolean {
  return mimeType.startsWith('text/') || mimeType === 'application/json';
}

export interface MockDriveFile {
  path: string;          // e.g. "meetings/2026-05-01-standup.md"
  content: string;
  tags?: string[];       // e.g. ["needs-review", "meeting-transcript"]
  owner?: string;
  sharedWith?: string[];
  lastModified?: string; // ISO8601, defaults to now
}

interface StoredFile {
  path: string;
  content: string;
  tags: string[];
  owner: string;
  sharedWith: string[];
  lastModified: string;
}

export class MockDriveAdapter implements SourceAdapter {
  // Mock fixture: declares writeback capability so the dev/demo path can
  // exercise the bidirectional UI. The real GoogleDriveAdapter starts
  // read-only (Agent B promotes it later — see vision doc).
  static readonly capabilities: AdapterCapabilities = {
    readonly: false,
    supportsWriteBack: true,
    requiresOauth: false,
    supportsWebhook: false,
  };

  private files: Map<string, StoredFile> = new Map();

  /**
   * Populate the mock drive with test data.
   * Can be called multiple times; new entries are merged in.
   */
  seed(files: MockDriveFile[]): void {
    for (const file of files) {
      this.files.set(file.path, this.toStored(file));
    }
  }

  // --- Mutation methods (simulate external changes between syncs) ---

  /**
   * Add a new file to the mock drive.
   */
  addFile(file: MockDriveFile): void {
    this.files.set(file.path, this.toStored(file));
  }

  /**
   * Update a file's content and bump its lastModified timestamp.
   */
  updateFile(filePath: string, content: string): void {
    const existing = this.files.get(filePath);
    if (!existing) {
      throw new Error(`MockDriveAdapter: file not found: ${filePath}`);
    }
    existing.content = content;
    existing.lastModified = new Date().toISOString();
  }

  /**
   * Add a tag to a file.
   */
  tagFile(filePath: string, tag: string): void {
    const existing = this.files.get(filePath);
    if (!existing) {
      throw new Error(`MockDriveAdapter: file not found: ${filePath}`);
    }
    if (!existing.tags.includes(tag)) {
      existing.tags.push(tag);
    }
  }

  /**
   * Remove a file from the mock drive.
   */
  removeFile(filePath: string): void {
    this.files.delete(filePath);
  }

  // --- SourceAdapter interface ---

  /**
   * List files matching config filters and cursor.
   *
   * config.folderId — filter to a subtree prefix (e.g. "meetings/")
   * config.tagFilter — only return files with this tag
   * cursor.lastSyncAt — only return files modified after this timestamp
   */
  async *list(config: SourceConfig, cursor: SyncCursor): AsyncGenerator<FileDescriptor> {
    const folderId = config.folderId as string | undefined;
    const tagFilter = config.tagFilter as string | undefined;

    for (const file of this.files.values()) {
      // Filter by folder prefix
      if (folderId && !file.path.startsWith(folderId)) {
        continue;
      }

      // Filter by tag
      if (tagFilter && !file.tags.includes(tagFilter)) {
        continue;
      }

      // Respect cursor — only return files modified after lastSyncAt
      if (cursor.lastSyncAt) {
        const lastSync = new Date(cursor.lastSyncAt).getTime();
        const modified = new Date(file.lastModified).getTime();
        if (modified <= lastSync) {
          continue;
        }
      }

      const hash = crypto.createHash('sha256').update(file.content).digest('hex');
      const name = path.basename(file.path);

      yield {
        path: file.path,
        name,
        hash,
        size: Buffer.byteLength(file.content, 'utf-8'),
        mimeType: inferMimeType(file.path),
        lastModified: file.lastModified,
        tags: file.tags.length > 0 ? [...file.tags] : undefined,
        metadata: {
          owner: file.owner,
          sharedWith: file.sharedWith,
        },
      };
    }
  }

  /**
   * Fetch file content for a given FileDescriptor.
   */
  async fetch(_config: SourceConfig, file: FileDescriptor): Promise<FileContent> {
    const stored = this.files.get(file.path);
    if (!stored) {
      throw new Error(`MockDriveAdapter: file not found: ${file.path}`);
    }

    const data = Buffer.from(stored.content, 'utf-8');
    const mimeType = inferMimeType(file.path);
    const encoding = isTextMime(mimeType) ? 'utf-8' : 'binary';

    const content: FileContent = { data, encoding };

    if (mimeType === 'application/json') {
      content.parsed = JSON.parse(stored.content);
    }

    return content;
  }

  /**
   * Write-back support — stores pushed content at configured pushPath or
   * at the documentId path. Used for Phase 3 bidirectional sync.
   */
  async push(config: SourceConfig, documentId: string, content: FileContent): Promise<void> {
    const pushPath = (config.pushPath as string) ?? documentId;
    const textContent = content.data.toString('utf-8');

    const existing = this.files.get(pushPath);
    if (existing) {
      existing.content = textContent;
      existing.lastModified = new Date().toISOString();
    } else {
      this.files.set(pushPath, {
        path: pushPath,
        content: textContent,
        tags: [],
        owner: 'system',
        sharedWith: [],
        lastModified: new Date().toISOString(),
      });
    }
  }

  // --- Private helpers ---

  private toStored(file: MockDriveFile): StoredFile {
    return {
      path: file.path,
      content: file.content,
      tags: file.tags ?? [],
      owner: file.owner ?? 'unknown',
      sharedWith: file.sharedWith ?? [],
      lastModified: file.lastModified ?? new Date().toISOString(),
    };
  }
}

/** Singleton instance for convenience in tests. */
export const mockDriveAdapter = new MockDriveAdapter();
