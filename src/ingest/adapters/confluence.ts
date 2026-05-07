// platform-api/src/ingest/adapters/confluence.ts
//
// Confluence connector — Tier 2 source adapter + write-back driver pair.
//
// Auth:
//   API token (no OAuth dance). The token + the user's email address are
//   sent as HTTP basic auth, per Atlassian's documented contract for
//   Confluence Cloud API tokens.
//
// Page-id ↔ documentId mapping strategy:
//   The Confluence numeric page id is used as the canonical FileDescriptor
//   `path`. The document title becomes `name`, and `metadata.pageId` keeps
//   the id explicit for write-back code paths. The current `version.number`
//   is also stashed in `metadata.versionNumber` so the WriteBackDriver can
//   read it back when the doc is edited inside the platform — this lets us
//   detect optimistic-concurrency conflicts on push without an extra GET.

import type {
  SourceAdapter,
  SourceConfig,
  SyncCursor,
  FileDescriptor,
  FileContent,
} from '../types';

// ─── Types ────────────────────────────────────────────────────────────────

/**
 * Required adapter / driver config for Confluence.
 *
 * SourceConfig fields:
 *   baseUrl       (string, required) — e.g. "https://acme.atlassian.net/wiki"
 *   email         (string, required) — Atlassian account email (basic auth user)
 *   apiToken      (string, required) — Atlassian API token (basic auth pass)
 *   spaceKey      (string, optional) — restrict listing to this space
 *   rootPageId    (string, optional) — restrict listing to descendants of this page
 *   pageSize      (number, optional) — page-size for `?limit=` parameter (default 50)
 *
 * Exactly one of `spaceKey` or `rootPageId` should be set; if both are set,
 * `rootPageId` wins (page-tree traversal).
 */
export interface ConfluenceConfig extends SourceConfig {
  baseUrl: string;
  email: string;
  apiToken: string;
  spaceKey?: string;
  rootPageId?: string;
  pageSize?: number;
}

/**
 * Minimal write-back driver shape, mirroring the vision doc:
 *
 *   WriteBackDriver:  push(documentId, change) → ack
 *
 * Agent A is formalising the canonical interface; this local definition
 * matches the documented signature and will be reconciled at integration
 * time. Kept minimal so we don't fork the upstream contract.
 */
export interface WriteBackChange {
  /** New body content, in storage format (XHTML) or plain text. */
  body: string;
  /** Optional new title. If absent, the existing title is preserved. */
  title?: string;
  /**
   * Version number the editor was looking at when the change was authored.
   * If this doesn't match the current `version.number` on the server, the
   * driver returns a `WriteBackConflict` instead of overwriting.
   */
  baseVersion: number;
  /** Optional content representation (default 'storage'). */
  representation?: 'storage' | 'wiki' | 'editor' | 'view';
}

export interface WriteBackAck {
  ok: true;
  documentId: string;
  newVersion: number;
}

export interface WriteBackConflict {
  ok: false;
  reason: 'version-conflict';
  documentId: string;
  /** Version the caller's edit was based on. */
  baseVersion: number;
  /** Version currently on the server. */
  remoteVersion: number;
  /** Server's current body so the operator UI can render a diff. */
  remoteBody?: string;
  remoteTitle?: string;
}

export type WriteBackResult = WriteBackAck | WriteBackConflict;

export interface WriteBackDriver<C = SourceConfig> {
  push(config: C, documentId: string, change: WriteBackChange): Promise<WriteBackResult>;
}

// ─── Helpers ──────────────────────────────────────────────────────────────

interface ConfluencePageRef {
  id: string;
  title: string;
  version?: { number?: number };
  body?: { storage?: { value?: string } };
}

interface ConfluenceListResponse {
  results: ConfluencePageRef[];
  _links?: { next?: string; base?: string };
  size?: number;
}

function authHeader(config: ConfluenceConfig): string {
  const token = Buffer.from(`${config.email}:${config.apiToken}`).toString('base64');
  return `Basic ${token}`;
}

function trimTrailingSlash(url: string): string {
  return url.endsWith('/') ? url.slice(0, -1) : url;
}

function buildInitialUrl(config: ConfluenceConfig): string {
  const base = trimTrailingSlash(config.baseUrl);
  const limit = config.pageSize ?? 50;
  if (config.rootPageId) {
    // Descendant traversal — Confluence Cloud REST: /content/{id}/descendant/page
    const params = new URLSearchParams({
      limit: String(limit),
      expand: 'version,body.storage',
    });
    return `${base}/rest/api/content/${encodeURIComponent(config.rootPageId)}/descendant/page?${params.toString()}`;
  }
  if (config.spaceKey) {
    const params = new URLSearchParams({
      type: 'page',
      spaceKey: config.spaceKey,
      limit: String(limit),
      expand: 'version,body.storage',
    });
    return `${base}/rest/api/content?${params.toString()}`;
  }
  throw new Error('ConfluenceAdapter: must specify spaceKey or rootPageId');
}

/**
 * Confluence's pagination uses `_links.next` which is a path relative to
 * `_links.base` (e.g. "/rest/api/content?cursor=…"). Resolve it against
 * the configured base URL.
 */
function resolveNextLink(config: ConfluenceConfig, response: ConfluenceListResponse): string | null {
  const next = response._links?.next;
  if (!next) return null;
  if (next.startsWith('http://') || next.startsWith('https://')) return next;
  // _links.base is the API root (e.g. https://x.atlassian.net/wiki) and
  // _links.next is a path beginning with /rest/...
  const base = response._links?.base ?? trimTrailingSlash(config.baseUrl);
  return `${trimTrailingSlash(base)}${next}`;
}

function pageToDescriptor(page: ConfluencePageRef, baseUrl: string): FileDescriptor {
  const versionNumber = page.version?.number ?? 0;
  const body = page.body?.storage?.value ?? '';
  // Use version+id as the change-detection hash so a re-publish (which bumps
  // version.number) is detected even when title/body bytes round-trip identically.
  const hash = `v${versionNumber}:${page.id}`;
  return {
    path: page.id,
    name: page.title,
    hash,
    size: Buffer.byteLength(body, 'utf-8'),
    mimeType: 'text/html',
    lastModified: new Date().toISOString(),
    metadata: {
      pageId: page.id,
      versionNumber,
      baseUrl: trimTrailingSlash(baseUrl),
    },
  };
}

// ─── Source adapter ───────────────────────────────────────────────────────

/**
 * Confluence read-side adapter.
 *
 * Listing semantics:
 *   - When `spaceKey` is set, lists every page in that space.
 *   - When `rootPageId` is set, walks the descendant page-tree from that root.
 *   - Pagination follows Confluence's `_links.next` field. The full next-URL
 *     is stored in `cursor.checkpoint` so a crashed sync resumes mid-walk
 *     without re-scanning from the top.
 *
 * Change detection: hash combines `version.number` + page id. Confluence
 * does not expose a content checksum and `body.storage.value` is non-stable
 * across edits that don't change the rendered output, so version is the
 * authoritative signal.
 */
export class ConfluenceSourceAdapter implements SourceAdapter {
  // Allow tests to inject a fetch mock.
  constructor(private readonly httpFetch: typeof fetch = fetch) {}

  async *list(config: SourceConfig, cursor: SyncCursor): AsyncGenerator<FileDescriptor> {
    const cfg = config as ConfluenceConfig;
    let nextUrl: string | null =
      (cursor.checkpoint as string | undefined) ?? buildInitialUrl(cfg);

    while (nextUrl) {
      const response = await this.httpFetch(nextUrl, {
        method: 'GET',
        headers: {
          Authorization: authHeader(cfg),
          Accept: 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error(
          `ConfluenceAdapter.list failed: ${response.status} ${response.statusText}`,
        );
      }

      const body = (await response.json()) as ConfluenceListResponse;
      for (const page of body.results ?? []) {
        if (!page.id || !page.title) continue;
        yield pageToDescriptor(page, cfg.baseUrl);
      }

      nextUrl = resolveNextLink(cfg, body);
    }
  }

  async fetch(config: SourceConfig, file: FileDescriptor): Promise<FileContent> {
    const cfg = config as ConfluenceConfig;
    const base = trimTrailingSlash(cfg.baseUrl);
    const pageId = (file.metadata?.pageId as string | undefined) ?? file.path;
    const url = `${base}/rest/api/content/${encodeURIComponent(pageId)}?expand=version,body.storage`;

    const response = await this.httpFetch(url, {
      method: 'GET',
      headers: {
        Authorization: authHeader(cfg),
        Accept: 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(
        `ConfluenceAdapter.fetch failed: ${response.status} ${response.statusText}`,
      );
    }

    const page = (await response.json()) as ConfluencePageRef;
    const value = page.body?.storage?.value ?? '';
    const data = Buffer.from(value, 'utf-8');
    return {
      data,
      encoding: 'utf-8',
      parsed: {
        pageId: page.id,
        title: page.title,
        versionNumber: page.version?.number ?? 0,
        body: value,
      },
    };
  }
}

// ─── Write-back driver ────────────────────────────────────────────────────

/**
 * Confluence write-back driver. Implements the `WriteBackDriver` contract
 * by calling Confluence's `PUT /rest/api/content/{id}` endpoint.
 *
 * Optimistic concurrency:
 *   The Confluence update API requires `version.number` to be **one greater
 *   than the page's current version** — passing an older version triggers a
 *   409 Conflict. On a 409 we fetch the current version + body and return
 *   a `WriteBackConflict` so the operator surface can show a dual-pane diff
 *   instead of clobbering the remote edit.
 */
export class ConfluenceWriteBackDriver implements WriteBackDriver<ConfluenceConfig> {
  constructor(private readonly httpFetch: typeof fetch = fetch) {}

  async push(
    config: ConfluenceConfig,
    documentId: string,
    change: WriteBackChange,
  ): Promise<WriteBackResult> {
    const base = trimTrailingSlash(config.baseUrl);
    const url = `${base}/rest/api/content/${encodeURIComponent(documentId)}`;

    // Fetch existing title if the caller didn't supply one — Confluence's
    // PUT requires `title` even for body-only edits.
    const existingTitle = await this.peekTitle(config, documentId);
    const title = change.title ?? existingTitle;

    const newVersion = change.baseVersion + 1;
    const payload = {
      id: documentId,
      type: 'page',
      title,
      version: { number: newVersion },
      body: {
        storage: {
          value: change.body,
          representation: change.representation ?? 'storage',
        },
      },
    };

    const response = await this.httpFetch(url, {
      method: 'PUT',
      headers: {
        Authorization: authHeader(config),
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    });

    if (response.status === 409) {
      // Re-read remote so the conflict envelope carries the operator-side info.
      const remote = await this.readPage(config, documentId);
      return {
        ok: false,
        reason: 'version-conflict',
        documentId,
        baseVersion: change.baseVersion,
        remoteVersion: remote?.version?.number ?? change.baseVersion + 1,
        remoteBody: remote?.body?.storage?.value,
        remoteTitle: remote?.title,
      };
    }

    if (!response.ok) {
      throw new Error(
        `ConfluenceWriteBackDriver.push failed: ${response.status} ${response.statusText}`,
      );
    }

    const updated = (await response.json()) as ConfluencePageRef;
    return {
      ok: true,
      documentId,
      newVersion: updated.version?.number ?? newVersion,
    };
  }

  private async readPage(
    config: ConfluenceConfig,
    documentId: string,
  ): Promise<ConfluencePageRef | null> {
    const base = trimTrailingSlash(config.baseUrl);
    const url = `${base}/rest/api/content/${encodeURIComponent(documentId)}?expand=version,body.storage`;
    const res = await this.httpFetch(url, {
      method: 'GET',
      headers: {
        Authorization: authHeader(config),
        Accept: 'application/json',
      },
    });
    if (!res.ok) return null;
    return (await res.json()) as ConfluencePageRef;
  }

  private async peekTitle(config: ConfluenceConfig, documentId: string): Promise<string> {
    const page = await this.readPage(config, documentId);
    return page?.title ?? '';
  }
}
