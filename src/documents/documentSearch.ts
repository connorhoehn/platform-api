/**
 * Document search for the Documents "All documents" page (realtime-examples
 * NFR #230, documents-detail PLAN WS-L lane L2).
 *
 * The page used to fetch every document the gateway lists and filter the
 * titles in the browser. That cannot rank by what a document SAYS, and it
 * shows titles the viewer's `document:read` (#135) would refuse. This module
 * answers one question on the server: which documents may this person read
 * that match these words, best first?
 *
 * ## Data path
 *
 * - Rows: one projected Scan of `crdt-documents` (140 rows locally, one page),
 *   shared by every request for `ROWS_TTL_MS` and single-flighted, so a burst
 *   of keystrokes costs one scan. Past the TTL the old rows still answer while
 *   one background load replaces them (stale-while-revalidate), so no
 *   reader's request waits on a refresh — only a replica's first request does.
 * - Folders: one Query on the org's `crdt-document-folders` partition (the
 *   explorer's own read shape), same TTL.
 * - Words: a page's text lives only in its Y.Doc snapshot. `readDocumentText`
 *   (shared with the #209 excerpt) costs one key-only Query per document to
 *   find the newest version and decodes each version once per process. The
 *   probe is remembered for `TEXT_TTL_MS`, so a body edit reaches search
 *   within that window. Decks and diagrams are not probed (no page body).
 * - Access: every candidate goes through `canReadDocument` — owner, editor,
 *   viewer, or channel member — with the membership lookup memoised for
 *   `MEMBER_TTL_MS` (one read per distinct channel, not per document). A document the
 *   viewer may not read is never ranked, counted or faceted.
 *
 * Each replica holds its own caches; nothing here is shared state, so two
 * replicas can disagree for at most the TTLs above.
 */

import { QueryCommand, ScanCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { tableName } from '../lib/ddb-table-name';
import { readDocumentText, type DocumentText } from '../work-graph/sources/documentExcerpt';
import { canReadDocument, isChannelMember, type ChannelMemberLookup } from '../deck/documentEditAccess';

const DOCUMENTS_TABLE = tableName(process.env.DYNAMODB_DOCUMENTS_TABLE || 'crdt-documents');
const FOLDERS_TABLE = tableName(process.env.DYNAMODB_DOCUMENT_FOLDERS_TABLE || 'crdt-document-folders');

export const ROWS_TTL_MS = 10_000;
export const TEXT_TTL_MS = 30_000;
/**
 * A channel-membership answer is reused this long, across requests: a removed
 * member stops seeing a shared document in search within this window plus one
 * refresh (the #135 read gate keeps its per-document answers 60 s).
 */
export const MEMBER_TTL_MS = 15_000;
const PROBE_CONCURRENCY = 8;
const ACCESS_CONCURRENCY = 16;

export type SearchKind = 'page' | 'presentation' | 'diagram';
export type SearchSort = 'relevance' | 'updated' | 'name';
export type SearchMatch = 'title' | 'summary' | 'body';

export interface DocumentRow {
  documentId: string;
  title?: string;
  type?: string;
  createdBy?: string;
  createdByName?: string;
  editors?: unknown;
  viewers?: unknown;
  channel?: string;
  description?: string;
  updatedAt?: string;
  createdAt?: string;
  organizationId?: string;
}

export interface FolderInfo {
  id: string;
  name: string;
  parentFolderId: string | null;
}

export interface SearchFilters {
  kind?: SearchKind[];
  owner?: string[];
  /** A folder id (its sub-folders included), or `unfiled`. */
  folder?: string;
  /** Changed at or after this epoch ms. */
  since?: number;
}

export interface SearchRequest {
  q: string;
  sort: SearchSort;
  filters: SearchFilters;
  offset: number;
  limit: number;
}

export interface SearchResultItem {
  id: string;
  title: string;
  kind: SearchKind;
  type: string;
  summary: string | null;
  folderId: string | null;
  folderName: string | null;
  ownerId: string | null;
  ownerName: string | null;
  updatedAt: string | null;
  /** The best field the words were found in; absent with no query. */
  match?: SearchMatch;
  score?: number;
}

export interface SearchResponse {
  query: string;
  sort: SearchSort;
  /** Matching documents across all pages. */
  total: number;
  /** Documents the viewer may read, before the query and filters. */
  libraryTotal: number;
  items: SearchResultItem[];
  nextCursor: string | null;
  facets: {
    kinds: Array<{ kind: SearchKind; count: number }>;
    owners: Array<{ id: string; name: string; count: number }>;
    folders: Array<FolderInfo & { count: number }>;
  };
  timings: { rowsMs: number; accessMs: number; textMs: number; rankMs: number };
}

export function kindOf(type: string | undefined): SearchKind {
  const t = (type ?? '').toLowerCase();
  if (t.includes('present') || t.includes('deck') || t.includes('slide')) return 'presentation';
  if (t.includes('diagram') || t.includes('whiteboard')) return 'diagram';
  return 'page';
}

/**
 * The row's description when an author wrote one. Seeds and old create paths
 * filled it with the type's folder name ("Project documents") or "<title>
 * presentation"; those say nothing, and the page's opening sentence is better.
 */
export function meaningfulDescription(row: Pick<DocumentRow, 'description' | 'title' | 'type'>): string | null {
  const d = row.description?.trim();
  if (!d) return null;
  const lower = d.toLowerCase();
  const title = (row.title ?? '').trim().toLowerCase();
  if (lower === 'project documents' || lower === title || lower === `${title} presentation` || lower === `${title} ${(row.type ?? '').toLowerCase()}`) return null;
  return d;
}

export function tokenize(q: string): string[] {
  return [...new Set(q.toLowerCase().split(/[^\p{L}\p{N}]+/u).filter((t) => t.length > 0))].slice(0, 8);
}

/** Does `token` start a word in `hay` (so "rel" finds "release" but not "unreliable")? */
function hasWordPrefix(hay: string, token: string): boolean {
  let from = 0;
  for (;;) {
    const at = hay.indexOf(token, from);
    if (at < 0) return false;
    if (at === 0 || !/[\p{L}\p{N}]/u.test(hay[at - 1]!)) return true;
    from = at + 1;
  }
}

/** Enough to drop an archived title match below a live one, not below a body match. */
export const ARCHIVE_PENALTY = 150;

const FIELD_WEIGHT: Record<SearchMatch, number> = { title: 100, summary: 30, body: 10 };

/**
 * Relevance: every word must appear somewhere (AND). Each word scores its best
 * field — title 100, summary 30, body 10 — and the title earns a bonus for
 * being the whole query (+200) or starting with it (+80). Ties are broken by
 * recency outside this function. Null = not a match.
 */
export function scoreDocument(
  fields: { title: string; summary: string; body: string },
  tokens: readonly string[],
  phrase: string,
): { score: number; match: SearchMatch } | null {
  if (!tokens.length) return null;
  const title = fields.title.toLowerCase();
  const summary = fields.summary.toLowerCase();
  const body = fields.body.toLowerCase();
  let score = 0;
  let best: SearchMatch = 'body';
  for (const token of tokens) {
    const field: SearchMatch | null = hasWordPrefix(title, token) ? 'title'
      : hasWordPrefix(summary, token) ? 'summary'
        : hasWordPrefix(body, token) ? 'body' : null;
    if (!field) return null;
    score += FIELD_WEIGHT[field];
    if (FIELD_WEIGHT[field] > FIELD_WEIGHT[best]) best = field;
  }
  const p = phrase.trim().toLowerCase();
  if (p && title === p) score += 200;
  else if (p && title.startsWith(p)) score += 80;
  else if (p && tokens.length > 1 && summary.includes(p)) score += 20;
  return { score, match: best };
}

export function encodeCursor(offset: number): string {
  return Buffer.from(`o:${offset}`).toString('base64url');
}

export function decodeCursor(cursor: string | undefined): number {
  if (!cursor) return 0;
  const m = /^o:(\d{1,6})$/.exec(Buffer.from(cursor, 'base64url').toString());
  return m ? Number(m[1]) : 0;
}

async function mapLimit<T, R>(items: readonly T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const out = new Array<R>(items.length);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const i = next++;
      out[i] = await fn(items[i]!);
    }
  }));
  return out;
}

function timeOf(iso: string | undefined): number {
  const t = iso ? Date.parse(iso) : NaN;
  return Number.isFinite(t) ? t : 0;
}

export interface DocumentSearchDeps {
  scanRows: () => Promise<DocumentRow[]>;
  listFolders: (orgId: string) => Promise<{ folders: FolderInfo[]; placement: Map<string, string | null> }>;
  readText: (documentId: string, title: string) => Promise<DocumentText>;
  memberLookup: ChannelMemberLookup;
  now: () => number;
}

export function ddbSearchDeps(client: DynamoDBDocumentClient): DocumentSearchDeps {
  return {
    scanRows: async () => {
      const rows: DocumentRow[] = [];
      let start: Record<string, unknown> | undefined;
      do {
        const page = await client.send(new ScanCommand({
          TableName: DOCUMENTS_TABLE,
          ProjectionExpression: 'documentId, title, #t, createdBy, createdByName, editors, viewers, channel, description, updatedAt, createdAt, organizationId',
          ExpressionAttributeNames: { '#t': 'type' },
          ExclusiveStartKey: start,
        }));
        rows.push(...((page.Items ?? []) as DocumentRow[]));
        start = page.LastEvaluatedKey as Record<string, unknown> | undefined;
      } while (start);
      return rows;
    },
    listFolders: async (orgId) => {
      const folders: FolderInfo[] = [];
      const placement = new Map<string, string | null>();
      let start: Record<string, unknown> | undefined;
      do {
        const page = await client.send(new QueryCommand({
          TableName: FOLDERS_TABLE,
          KeyConditionExpression: 'orgId = :o',
          ExpressionAttributeValues: { ':o': orgId },
          ExclusiveStartKey: start,
        }));
        for (const item of page.Items ?? []) {
          const key = String(item.itemId ?? '');
          if (key.startsWith('folder#')) {
            folders.push({ id: key.slice(7), name: String(item.name ?? ''), parentFolderId: (item.parentFolderId as string | null | undefined) ?? null });
          } else if (key.startsWith('doc#')) {
            placement.set(key.slice(4), (item.folderId as string | null | undefined) ?? null);
          }
        }
        start = page.LastEvaluatedKey as Record<string, unknown> | undefined;
      } while (start);
      return { folders, placement };
    },
    readText: (documentId, title) => readDocumentText(client, documentId, title),
    memberLookup: isChannelMember,
    now: () => Date.now(),
  };
}

interface Cached<T> { at: number; value?: T; pending?: Promise<T> }

/**
 * One in-flight load per key. A fresh value (younger than `ttl`) is reused; a
 * stale one is still answered at once while one background load replaces it
 * (stale-while-revalidate), so a refresh never lands on a reader's request —
 * only the very first load of a key does. A failed refresh keeps the old value.
 */
function cachedLoad<T>(store: Map<string, Cached<T>>, key: string, ttl: number, now: number, load: () => Promise<T>): Promise<T> {
  const hit = store.get(key);
  if (hit?.value !== undefined && now - hit.at < ttl) return Promise.resolve(hit.value);
  const start = () => {
    const pending = load().then(
      (value) => { store.set(key, { at: now, value }); return value; },
      (err) => { const cur = store.get(key); if (cur?.value !== undefined) store.set(key, { at: cur.at, value: cur.value }); else store.delete(key); throw err; },
    );
    store.set(key, { ...(hit ?? { at: 0 }), pending });
    return pending;
  };
  if (hit?.value !== undefined) {
    if (!hit.pending) start().catch(() => undefined);
    return Promise.resolve(hit.value);
  }
  return hit?.pending ?? start();
}

export class DocumentSearchIndex {
  private readonly rows = new Map<string, Cached<DocumentRow[]>>();
  private readonly folders = new Map<string, Cached<{ folders: FolderInfo[]; placement: Map<string, string | null> }>>();
  private readonly texts = new Map<string, Cached<DocumentText>>();
  private readonly members = new Map<string, Cached<boolean>>();

  constructor(private readonly deps: DocumentSearchDeps) {}

  private text(row: DocumentRow): Promise<DocumentText> {
    const title = row.title ?? '';
    return cachedLoad(this.texts, `${row.documentId}\u0000${title}`, TEXT_TTL_MS, this.deps.now(), () => this.deps.readText(row.documentId, title))
      .catch(() => ({ timestamp: null, excerpt: null, text: '' }));
  }

  async search(user: { sub?: string; orgId?: string }, req: SearchRequest): Promise<SearchResponse> {
    const t0 = this.deps.now();
    const orgId = user.orgId || 'default';
    const [rows, tree] = await Promise.all([
      cachedLoad(this.rows, 'all', ROWS_TTL_MS, t0, () => this.deps.scanRows()),
      cachedLoad(this.folders, orgId, ROWS_TTL_MS, t0, () => this.deps.listFolders(orgId)),
    ]);
    const t1 = this.deps.now();

    // Access first: nothing below sees a row the viewer may not read.
    const lookup: ChannelMemberLookup = (channel, userId) =>
      cachedLoad(this.members, `${channel}\u0000${userId}`, MEMBER_TTL_MS, t0, () => this.deps.memberLookup(channel, userId));
    const inOrg = rows.filter((r) => r.documentId && (!r.organizationId || !user.orgId || r.organizationId === user.orgId));
    const allowed = await mapLimit(inOrg, ACCESS_CONCURRENCY, async (r) => (await canReadDocument(r, user, r.documentId, lookup)).allow);
    const readable = inOrg.filter((_r, i) => allowed[i]);
    const t2 = this.deps.now();

    const folderById = new Map(tree.folders.map((f) => [f.id, f]));
    const folderOf = (id: string) => tree.placement.get(id) ?? null;
    const underFolder = (docFolder: string | null, target: string): boolean => {
      if (target === 'unfiled') return !docFolder;
      for (let f = docFolder, guard = 0; f && guard < 32; f = folderById.get(f)?.parentFolderId ?? null, guard++) {
        if (f === target) return true;
      }
      return false;
    };

    // A document filed under a folder named "Archive" (at any depth) is an old
    // copy someone put away: at equal relevance it ranks below a live one.
    const archived = (id: string): boolean => {
      for (let f = folderOf(id), guard = 0; f && guard < 32; f = folderById.get(f)?.parentFolderId ?? null, guard++) {
        if (folderById.get(f)?.name.trim().toLowerCase() === 'archive') return true;
      }
      return false;
    };

    const { kind, owner, folder, since } = req.filters;
    const filtered = readable.filter((r) =>
      (!kind?.length || kind.includes(kindOf(r.type)))
      && (!owner?.length || owner.includes(r.createdBy ?? ''))
      && (!folder || underFolder(folderOf(r.documentId), folder))
      && (!since || timeOf(r.updatedAt) >= since));

    // Words: probed only when a query needs the body, or for the page shown.
    const tokens = tokenize(req.q);
    const needsBody = (r: DocumentRow) => kindOf(r.type) === 'page';
    let ranked: Array<{ row: DocumentRow; score?: number; match?: SearchMatch; text?: DocumentText }>;
    if (tokens.length) {
      const texts = await mapLimit(filtered, PROBE_CONCURRENCY, (r) => (needsBody(r) ? this.text(r) : Promise.resolve(undefined)));
      ranked = [];
      filtered.forEach((row, i) => {
        const text = texts[i];
        const summary = [text?.excerpt ?? '', row.description ?? ''].join('\n');
        const hit = scoreDocument({ title: row.title ?? '', summary, body: text?.text ?? '' }, tokens, req.q);
        if (hit) ranked.push({ row, ...hit, score: hit.score - (archived(row.documentId) ? ARCHIVE_PENALTY : 0), text });
      });
    } else {
      ranked = filtered.map((row) => ({ row }));
    }
    const t3 = this.deps.now();

    const byUpdated = (a: { row: DocumentRow }, b: { row: DocumentRow }) => timeOf(b.row.updatedAt) - timeOf(a.row.updatedAt);
    const byName = (a: { row: DocumentRow }, b: { row: DocumentRow }) => (a.row.title ?? '').localeCompare(b.row.title ?? '', undefined, { sensitivity: 'base' });
    if (req.sort === 'name') ranked.sort((a, b) => byName(a, b) || byUpdated(a, b));
    else if (req.sort === 'relevance' && tokens.length) ranked.sort((a, b) => (b.score ?? 0) - (a.score ?? 0) || byUpdated(a, b));
    else ranked.sort((a, b) => byUpdated(a, b) || byName(a, b));

    const pageRows = ranked.slice(req.offset, req.offset + req.limit);
    // The shown page's summaries (a query already read them).
    const pageTexts = await mapLimit(pageRows, PROBE_CONCURRENCY, (e) => (e.text || !needsBody(e.row) ? Promise.resolve(e.text) : this.text(e.row)));
    const items: SearchResultItem[] = pageRows.map((e, i) => {
      const row = e.row;
      const folderId = folderOf(row.documentId);
      const excerpt = pageTexts[i]?.excerpt ?? null;
      return {
        id: row.documentId,
        title: row.title || 'Untitled',
        kind: kindOf(row.type),
        type: row.type ?? '',
        summary: meaningfulDescription(row) ?? excerpt ?? null,
        folderId,
        folderName: folderId ? folderById.get(folderId)?.name ?? null : null,
        ownerId: row.createdBy ?? null,
        ownerName: row.createdByName ?? null,
        updatedAt: row.updatedAt ?? null,
        ...(e.match ? { match: e.match, score: e.score } : {}),
      };
    });
    const t4 = this.deps.now();

    const kinds = new Map<SearchKind, number>();
    const owners = new Map<string, { name: string; count: number }>();
    const folderCounts = new Map<string, number>();
    for (const r of readable) {
      const k = kindOf(r.type);
      kinds.set(k, (kinds.get(k) ?? 0) + 1);
      const o = r.createdBy ?? '';
      if (o) {
        const cur = owners.get(o) ?? { name: r.createdByName || o, count: 0 };
        cur.count += 1;
        if (r.createdByName) cur.name = r.createdByName;
        owners.set(o, cur);
      }
      for (let f = folderOf(r.documentId), guard = 0; f && guard < 32; f = folderById.get(f)?.parentFolderId ?? null, guard++) {
        folderCounts.set(f, (folderCounts.get(f) ?? 0) + 1);
      }
    }

    const end = req.offset + req.limit;
    return {
      query: req.q,
      sort: req.sort,
      total: ranked.length,
      libraryTotal: readable.length,
      items,
      nextCursor: end < ranked.length ? encodeCursor(end) : null,
      facets: {
        kinds: [...kinds].map(([k, count]) => ({ kind: k, count })),
        owners: [...owners].map(([id, v]) => ({ id, name: v.name, count: v.count })).sort((a, b) => b.count - a.count),
        folders: tree.folders.filter((f) => folderCounts.has(f.id)).map((f) => ({ ...f, count: folderCounts.get(f.id)! })),
      },
      timings: { rowsMs: t1 - t0, accessMs: t2 - t1, textMs: t3 - t2 + (t4 - t3), rankMs: 0 },
    };
  }
}
