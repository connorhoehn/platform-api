import { google } from 'googleapis';
import type { drive_v3 } from 'googleapis';
import type {
  SourceAdapter,
  SourceConfig,
  SyncCursor,
  FileDescriptor,
  FileContent,
  AdapterCapabilities,
} from '../types';

const GOOGLE_DOC_MIME = 'application/vnd.google-apps.document';
const GOOGLE_SHEET_MIME = 'application/vnd.google-apps.spreadsheet';

const EXPORT_MIME: Record<string, string> = {
  [GOOGLE_DOC_MIME]: 'text/plain',
  [GOOGLE_SHEET_MIME]: 'text/csv',
};

const MIME_MAP: Record<string, string> = {
  '.json': 'application/json',
  '.csv': 'text/csv',
  '.md': 'text/markdown',
  '.txt': 'text/plain',
  '.pdf': 'application/pdf',
};

function inferMimeType(name: string): string {
  const dotIdx = name.lastIndexOf('.');
  if (dotIdx === -1) return 'application/octet-stream';
  const ext = name.slice(dotIdx).toLowerCase();
  return MIME_MAP[ext] ?? 'application/octet-stream';
}

function isTextMime(mimeType: string): boolean {
  return mimeType.startsWith('text/') || mimeType === 'application/json';
}

/**
 * Real Google Drive adapter — wraps googleapis drive v3.
 *
 * SourceConfig fields:
 *   clientId       (string, required) — OAuth2 client ID
 *   clientSecret   (string, required) — OAuth2 client secret
 *   refreshToken   (string, required) — long-lived OAuth2 refresh token
 *   folderId       (string, optional) — Drive folder ID to scope listing
 *   query          (string, optional) — extra files.list query clause
 *
 * Change detection: Drive's "md5Checksum" field (present on binary files)
 * is used as the hash when available. For Google Workspace files (Docs,
 * Sheets) which have no checksum, the file's "modifiedTime" is hashed.
 *
 * SyncCursor.checkpoint stores the Drive nextPageToken so a paused listing
 * can resume without re-scanning from the top.
 *
 * Incremental sync: when cursor.lastSyncAt is set, the query appends
 * `modifiedTime > '<ISO>'` so only changed files are returned.
 *
 * Google Workspace files are exported via files.export (Docs → plain text,
 * Sheets → CSV). Binary Drive files are fetched with files.get?alt=media.
 */
export class GoogleDriveAdapter implements SourceAdapter {
  // Read-only at v1 — Agent B promotes this to bidirectional with the
  // revisions API (see GoogleDriveWriteBackDriver). Drive uses OAuth2
  // (client id/secret + refresh token) for all calls, so requiresOauth
  // is true.
  static readonly capabilities: AdapterCapabilities = {
    readonly: true,
    supportsWriteBack: false,
    requiresOauth: true,
    supportsWebhook: false,
  };

  // Return type inferred (googleapis' own OAuth2): 'google-auth-library'
  // was a phantom dependency — never declared, resolved only via hoisting —
  // and its type identity diverges from googleapis-common's nested copy.
  private buildAuth(config: SourceConfig) {
    const auth = new google.auth.OAuth2(
      config.clientId as string,
      config.clientSecret as string,
    );
    auth.setCredentials({ refresh_token: config.refreshToken as string });
    return auth;
  }

  private buildDrive(config: SourceConfig): drive_v3.Drive {
    const auth = this.buildAuth(config);
    return google.drive({ version: 'v3', auth });
  }

  private buildQuery(config: SourceConfig, cursor: SyncCursor): string {
    const parts: string[] = ["trashed = false"];

    const folderId = config.folderId as string | undefined;
    if (folderId) {
      parts.push(`'${folderId}' in parents`);
    }

    const extraQuery = config.query as string | undefined;
    if (extraQuery) {
      parts.push(extraQuery);
    }

    if (cursor.lastSyncAt) {
      parts.push(`modifiedTime > '${cursor.lastSyncAt}'`);
    }

    return parts.join(' and ');
  }

  async *list(config: SourceConfig, cursor: SyncCursor): AsyncGenerator<FileDescriptor> {
    const drive = this.buildDrive(config);
    const q = this.buildQuery(config, cursor);

    let pageToken: string | undefined =
      cursor.checkpoint as string | undefined;

    do {
      const response = await drive.files.list({
        q,
        pageSize: 100,
        pageToken: pageToken ?? undefined,
        fields: 'nextPageToken, files(id, name, mimeType, size, md5Checksum, modifiedTime, parents)',
      });

      for (const file of response.data.files ?? []) {
        if (!file.id || !file.name) continue;

        const mimeType = file.mimeType ?? inferMimeType(file.name);
        const isGoogleDoc = mimeType in EXPORT_MIME;

        // Use md5Checksum when available; fall back to modifiedTime hash
        const hash = file.md5Checksum ?? `modtime:${file.modifiedTime ?? ''}`;

        const descriptor: FileDescriptor = {
          path: file.id,                      // Drive IDs are the canonical path
          name: file.name,
          hash,
          size: file.size ? parseInt(file.size, 10) : 0,
          mimeType: isGoogleDoc ? EXPORT_MIME[mimeType]! : mimeType,
          lastModified: file.modifiedTime ?? new Date().toISOString(),
          metadata: { driveId: file.id, driveMimeType: mimeType },
        };

        yield descriptor;
      }

      pageToken = response.data.nextPageToken ?? undefined;
    } while (pageToken);
  }

  async fetch(config: SourceConfig, file: FileDescriptor): Promise<FileContent> {
    const drive = this.buildDrive(config);
    const driveId = (file.metadata?.driveId as string | undefined) ?? file.path;
    const driveMimeType = file.metadata?.driveMimeType as string | undefined;

    let data: Buffer;
    let finalMimeType: string;

    if (driveMimeType && driveMimeType in EXPORT_MIME) {
      // Google Workspace file — export as text
      const exportMime = EXPORT_MIME[driveMimeType]!;
      const response = await drive.files.export(
        { fileId: driveId, mimeType: exportMime },
        { responseType: 'arraybuffer' },
      );
      data = Buffer.from(response.data as ArrayBuffer);
      finalMimeType = exportMime;
    } else {
      // Binary Drive file — download directly
      const response = await drive.files.get(
        { fileId: driveId, alt: 'media' },
        { responseType: 'arraybuffer' },
      );
      data = Buffer.from(response.data as ArrayBuffer);
      finalMimeType = driveMimeType ?? file.mimeType ?? 'application/octet-stream';
    }

    const encoding = isTextMime(finalMimeType) ? 'utf-8' : 'binary';
    const content: FileContent = { data, encoding };

    if (finalMimeType === 'application/json') {
      content.parsed = JSON.parse(data.toString('utf-8'));
    }

    return content;
  }
}
