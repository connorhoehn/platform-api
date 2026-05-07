// social-api/src/ingest/adapters/google-drive-writeback.ts
//
// Google Drive write-back driver — peer of GoogleDriveAdapter.
//
// Implements the `WriteBackDriver` contract for the bidirectional
// sync loop described in AGENTIC_ETL_VISION.md (section D).
//
// =====================================================================
// OAuth scope requirement
// =====================================================================
// The read-side adapter is happy with the narrow `drive.readonly` or
// `drive.file` scope. Write-back is NOT — `drive.file` only grants
// access to files the *app itself* created or that the user explicitly
// opened with the app via the picker, which excludes the typical
// "operator paste a folder ID, sync everything in it" flow.
//
// Operators pointing at a pre-existing folder MUST grant the broader
//   https://www.googleapis.com/auth/drive
// scope when minting the refresh token. If the refresh token only has
// `drive.file`, calls to `files.update` against a file the app didn't
// create will return HTTP 403 with reason `insufficientFilePermissions`
// or `insufficientPermissions`. We surface that as a `missing_scope`
// error rather than a generic auth failure so the operator UI can
// prompt for re-consent.
// =====================================================================

import { google } from 'googleapis';
import type { OAuth2Client } from 'google-auth-library';
import type { drive_v3 } from 'googleapis';
import { Readable } from 'stream';
import type { SourceConfig } from '../types';
import type {
  WriteBackDriver,
  WriteBackChange,
  WriteBackResult,
  ResolvedChange,
} from './writeback-types';

/** Drive error reasons that mean "the token doesn't have write scope". */
const MISSING_SCOPE_REASONS = new Set([
  'insufficientFilePermissions',
  'insufficientPermissions',
  'forbidden',
]);

interface DriveApiError {
  code?: number;
  errors?: Array<{ reason?: string; message?: string }>;
  message?: string;
  response?: {
    status?: number;
    data?: { error?: { errors?: Array<{ reason?: string }>; message?: string } };
  };
}

function classifyDriveError(err: unknown): {
  code: 'missing_scope' | 'auth_failed' | 'not_found' | 'rate_limited' | 'unknown';
  retriable: boolean;
  message: string;
} {
  const e = err as DriveApiError;
  const status = e.code ?? e.response?.status;
  const reasons: string[] = [];
  for (const r of e.errors ?? []) if (r.reason) reasons.push(r.reason);
  for (const r of e.response?.data?.error?.errors ?? []) if (r.reason) reasons.push(r.reason);
  const message = e.message ?? e.response?.data?.error?.message ?? 'Drive API error';

  if (status === 403 && reasons.some((r) => MISSING_SCOPE_REASONS.has(r))) {
    return {
      code: 'missing_scope',
      retriable: false,
      message:
        `Drive write rejected (${reasons.join(',')}): the OAuth refresh ` +
        `token lacks the 'https://www.googleapis.com/auth/drive' scope. ` +
        `Re-mint the token with full Drive scope.`,
    };
  }
  if (status === 401) return { code: 'auth_failed', retriable: false, message };
  if (status === 404) return { code: 'not_found', retriable: false, message };
  if (status === 429) return { code: 'rate_limited', retriable: true, message };
  return { code: 'unknown', retriable: false, message };
}

/**
 * Bidirectional Google Drive driver.
 *
 * Conflict model: Drive exposes a `headRevisionId` on every file. We
 * read it before writing; if it doesn't match the change's
 * `baseRevision`, we report a conflict instead of clobbering.
 *
 * Note: Google Workspace files (Docs, Sheets, Slides) don't accept
 * raw media uploads — they need a conversion step. v1 only supports
 * write-back to *binary* Drive files (PDFs, plain text, JSON, CSV
 * stored as files). Writing to Docs is deferred; mapping engine
 * decides whether a typed-doc edit can flow back at all.
 */
export class GoogleDriveWriteBackDriver implements WriteBackDriver {
  private buildAuth(config: SourceConfig): OAuth2Client {
    const auth = new google.auth.OAuth2(
      config.clientId as string,
      config.clientSecret as string,
    );
    auth.setCredentials({ refresh_token: config.refreshToken as string });
    return auth;
  }

  private buildDrive(config: SourceConfig): drive_v3.Drive {
    return google.drive({ version: 'v3', auth: this.buildAuth(config) });
  }

  async push(
    config: SourceConfig,
    change: WriteBackChange,
  ): Promise<WriteBackResult> {
    const drive = this.buildDrive(config);
    const fileId = change.sourcePath;

    // 1. Read current headRevisionId for conflict detection.
    let remoteRevision: string;
    try {
      const meta = await drive.files.get({
        fileId,
        fields: 'headRevisionId, mimeType',
      });
      const headRev = meta.data.headRevisionId;
      if (!headRev) {
        // Workspace files (Docs/Sheets) lack headRevisionId — refuse
        // for v1 rather than silently proceed.
        return {
          status: 'error',
          sourcePath: fileId,
          code: 'unknown',
          message:
            'File has no headRevisionId (Google Workspace file). ' +
            'Write-back to Docs/Sheets is not supported in v1.',
          retriable: false,
        };
      }
      remoteRevision = headRev;
    } catch (err) {
      const classified = classifyDriveError(err);
      return { status: 'error', sourcePath: fileId, ...classified };
    }

    // 2. Detect conflict.
    if (change.baseRevision !== null && change.baseRevision !== remoteRevision) {
      let remoteContent: Buffer | undefined;
      try {
        const dl = await drive.files.get(
          { fileId, alt: 'media' },
          { responseType: 'arraybuffer' },
        );
        remoteContent = Buffer.from(dl.data as ArrayBuffer);
      } catch {
        // Best-effort; conflict UI can re-fetch if needed.
      }
      return {
        status: 'conflict',
        sourcePath: fileId,
        expectedRevision: change.baseRevision,
        remoteRevision,
        remoteContent,
      };
    }

    // 3. Push the update via files.update with a media stream.
    try {
      const updated = await drive.files.update({
        fileId,
        media: {
          mimeType: change.mimeType,
          body: Readable.from(change.content),
        },
        fields: 'id, headRevisionId',
      });
      const newRevision = updated.data.headRevisionId;
      if (!newRevision) {
        return {
          status: 'error',
          sourcePath: fileId,
          code: 'unknown',
          message: 'files.update succeeded but returned no headRevisionId',
          retriable: false,
        };
      }
      return {
        status: 'ok',
        sourcePath: fileId,
        newRevision,
        timestamp: new Date().toISOString(),
      };
    } catch (err) {
      const classified = classifyDriveError(err);
      return { status: 'error', sourcePath: fileId, ...classified };
    }
  }

  /**
   * v1 conflict resolution: surface to the operator. The default
   * implementation is "keep-local" — the operator UI overrides this
   * with explicit decisions. Auto-merge is intentionally not done in
   * the driver; it would require source-specific 3-way diff logic the
   * vision doc defers to Phase 2.
   */
  async resolveConflict(
    local: WriteBackChange,
    remote: { revision: string; content: Buffer; mimeType: string },
  ): Promise<ResolvedChange> {
    return {
      resolved: { ...local, baseRevision: remote.revision },
      decision: 'keep-local',
    };
  }
}
