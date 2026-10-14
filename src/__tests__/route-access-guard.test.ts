// Loop 29c (realtime-examples access sweep): no route that takes an id can
// silently skip an access check.
//
// Walks the real Express app (createApp) and, for every route whose path has a
// `:param`, requires ONE of:
//   - a middleware on its path marked with `accessPolicy`
//     (src/middleware/resource-access.ts: requireDocumentRead / requireDocumentEdit /
//     requireChannelMember, or anything wrapped with `markAccess`), or a known
//     named guard (requireAdmin, requireRoomMembership); or
//   - an entry in ACCESS_ALLOWLIST below saying where the check lives instead
//     (in the handler, in a module-level `router.use`, a shared secret, or
//     "scoped by the caller's own id", or why it is public).
//
// A new id route fails this test until it does one of those. A stale
// allowlist entry (the route is gone, or is now guarded) fails too, so the
// list cannot rot. The inventory with the reasoning is
// realtime-examples/docs/design/nfr/access-control-inventory.md.
//
// Body-supplied ids cannot be seen from the router; the inventory covers them,
// and the fixed ones use the same marked middlewares (`bodyId(...)`).

import { createApp } from '../app';
import { ACCESS_ALLOWLIST } from './route-access-allowlist';

const KNOWN_GUARDS = new Set(['requireAdmin', 'requireRoomMembership']);

interface Mid { name: string; policy?: string; matches(path: string): boolean }
interface RouteInfo { key: string; path: string; mids: Mid[] }
/** A router-level `use(fn)` that hides its routes (a lazily built router): the walk cannot see past it. */
interface OpaqueMount { key: string; path: string; mids: Mid[] }

const PARAM = /\(\?:(?:\\\/)?\(\[\^\\?\/\]\+\?\)\)/g;

/** A mount layer's regexp back to its path string (`/documents/:documentId`). */
function mountPath(layer: { regexp?: RegExp & { fast_slash?: boolean }; keys?: Array<{ name: string }> }): string {
  if (!layer.regexp || layer.regexp.fast_slash) return '';
  const keys = (layer.keys ?? []).map((k) => k.name);
  let i = 0;
  const src = layer.regexp.source
    .replace(/^\^/, '')
    .replace(/\\\/\?\(\?=\\\/\|\$\)$/, '')
    .replace(PARAM, (m) => `${m.startsWith('(?:\\/') ? '/' : ''}:${keys[i++] ?? 'p'}`);
  return src.replace(/\\\//g, '/').replace(/\\\./g, '.').replace(/\\-/g, '-');
}

function prefixMatcher(prefix: string): (path: string) => boolean {
  if (!prefix) return () => true;
  const re = new RegExp(`^${prefix.split('/').map((seg) => (seg.startsWith(':') ? '[^/]+' : seg.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))).join('/')}(?:/|$)`);
  return (path) => re.test(path);
}

function collect(stack: any[], prefix: string, inherited: Mid[], out: RouteInfo[], opaque: OpaqueMount[] = []): void {
  const mids = [...inherited];
  for (const layer of stack) {
    if (layer.route) {
      const methods = Object.keys(layer.route.methods).filter((m) => m !== '_all').map((m) => m.toUpperCase());
      const path = `${prefix}${layer.route.path}`;
      // Only layers BEFORE the final handler count: a guard placed after the
      // handler has already let it run.
      const own: Mid[] = layer.route.stack.slice(0, -1).map((l: any) => ({ name: l.handle?.name ?? l.name, policy: l.handle?.accessPolicy, matches: () => true }));
      for (const method of methods.length ? methods : ['ALL']) out.push({ key: `${method} ${path}`, path, mids: [...mids, ...own] });
    } else if (layer.handle?.stack) {
      collect(layer.handle.stack, prefix + mountPath(layer), mids, out, opaque);
    } else {
      const full = prefix + mountPath(layer);
      const name = layer.handle?.name ?? layer.name;
      if (layer.regexp?.fast_slash && (!name || name === '<anonymous>') && !layer.handle?.accessPolicy && /\/:[A-Za-z]/.test(prefix)) {
        opaque.push({ key: `USE ${prefix}`, path: prefix, mids: [...mids] });
      }
      mids.push({ name: layer.handle?.name ?? layer.name, policy: layer.handle?.accessPolicy, matches: prefixMatcher(full) });
    }
  }
}

function walkApp(): { routes: RouteInfo[]; opaque: OpaqueMount[] } {
  const app = createApp() as unknown as { _router: { stack: unknown[] } };
  const out: RouteInfo[] = [];
  const opaque: OpaqueMount[] = [];
  collect(app._router.stack, '', [], out, opaque);
  return { routes: out, opaque };
}

function guarded(r: { path: string; mids: Mid[] }): boolean {
  return r.mids.some((m) => m.matches(r.path) && (m.policy !== undefined || KNOWN_GUARDS.has(m.name)));
}

describe('route access guard', () => {
  const { routes: all, opaque } = walkApp();
  const idRoutes = all.filter((r) => /\/:[A-Za-z]/.test(r.path));

  it('walks the whole app', () => {
    expect(all.length).toBeGreaterThan(200);
    expect(idRoutes.length).toBeGreaterThan(100);
  });

  it('every route with an id param is behind an access middleware or allowlisted with a reason', () => {
    const naked = idRoutes.filter((r) => !guarded(r) && !ACCESS_ALLOWLIST[r.key]).map((r) => r.key);
    expect(naked).toEqual([]);
  });

  it('a lazily built router under an id path sits behind an access middleware (its routes are invisible to this walk)', () => {
    const naked = opaque.filter((m) => !guarded(m) && !ACCESS_ALLOWLIST[m.key]).map((m) => m.key);
    expect(naked).toEqual([]);
  });

  it('every allowlist entry names a live, unguarded route and gives a reason', () => {
    const byKey = new Map<string, { path: string; mids: Mid[] }>([...all, ...opaque].map((r) => [r.key, r]));
    const stale = Object.entries(ACCESS_ALLOWLIST)
      .filter(([key, reason]) => !byKey.has(key) || guarded(byKey.get(key)!) || reason.trim().length < 12)
      .map(([key]) => key);
    expect(stale).toEqual([]);
  });

  it('the fixed Loop 29c routes carry their policy at registration', () => {
    const policies = (key: string) => {
      const r = [...all, ...opaque].find((x) => x.key === key);
      return r ? r.mids.filter((m) => m.policy && m.matches(r.path)).map((m) => m.policy) : null;
    };
    expect(policies('POST /api/documents/:documentId/transform-run')).toEqual(['document:edit', 'document:read']);
    expect(policies('POST /api/documents/:documentId/import')).toEqual(['document:edit']);
    expect(policies('GET /api/documents/:documentId/export')).toEqual(['document:read']);
    for (const key of [
      'GET /api/documents/:documentId/lifecycle/',
      'PATCH /api/documents/:documentId/sections/:sectionId/items/:itemId',
      'POST /api/documents/:documentId/work-items/:itemId/propose',
      'USE /api/documents/:documentId/run-drafts',
      'USE /api/documents/:documentId/work',
    ]) expect([key, policies(key)]).toEqual([key, ['document:read|edit']]);
    expect(policies('POST /api/chat/commands/:trigger/run')).toEqual(['chat-command:document+sources']);
    expect(policies('POST /api/deck/revise-run')).toEqual(['deck:sources-read']);
    // P1 2026-10-14: the two recording-era routes guard at registration.
    expect(policies('POST /api/video/sessions/:sessionId/call-binding')).toEqual(['call-lobby:own-media-participant']);
    expect(policies('GET /api/recordings/:recordingId/status')).toEqual(['recording:read+lobby-authority']);
  });
});
