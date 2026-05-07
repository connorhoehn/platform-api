import * as fs from 'fs/promises';
import * as path from 'path';
import * as crypto from 'crypto';
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

export class MockLocalAdapter implements SourceAdapter {
  static readonly capabilities: AdapterCapabilities = {
    readonly: true,
    supportsWriteBack: false,
    requiresOauth: false,
    supportsWebhook: false,
  };

  async *list(config: SourceConfig, cursor: SyncCursor): AsyncGenerator<FileDescriptor> {
    const basePath = config.basePath as string;
    const files = await this.scanDir(basePath, basePath);

    for (const file of files) {
      if (cursor.lastSyncAt) {
        const lastSync = new Date(cursor.lastSyncAt).getTime();
        const modified = new Date(file.lastModified).getTime();
        if (modified <= lastSync) continue;
      }
      yield file;
    }
  }

  async fetch(config: SourceConfig, file: FileDescriptor): Promise<FileContent> {
    const basePath = config.basePath as string;
    const fullPath = path.join(basePath, file.path);
    const data = await fs.readFile(fullPath);
    const mimeType = inferMimeType(file.path);
    const encoding = isTextMime(mimeType) ? 'utf-8' : 'binary';

    const content: FileContent = { data, encoding };

    if (mimeType === 'application/json') {
      content.parsed = JSON.parse(data.toString('utf-8'));
    }

    return content;
  }

  private async scanDir(basePath: string, currentPath: string): Promise<FileDescriptor[]> {
    const results: FileDescriptor[] = [];
    const entries = await fs.readdir(currentPath, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = path.join(currentPath, entry.name);

      if (entry.isDirectory()) {
        const nested = await this.scanDir(basePath, fullPath);
        results.push(...nested);
      } else if (entry.isFile()) {
        const stat = await fs.stat(fullPath);
        const data = await fs.readFile(fullPath);
        const hash = crypto.createHash('sha256').update(data).digest('hex');
        const relativePath = path.relative(basePath, fullPath);

        results.push({
          path: relativePath,
          name: entry.name,
          hash,
          size: stat.size,
          mimeType: inferMimeType(entry.name),
          lastModified: stat.mtime.toISOString(),
        });
      }
    }

    return results;
  }
}
