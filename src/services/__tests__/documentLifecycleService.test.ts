/**
 * Unit tests for ownership resolution.
 *
 * The route tests exercise the same logic through HTTP; these hit `rolesFor`
 * and `loadContext` directly, because `rolesFor` is also consumed by
 * `documentReviewRequests.ts` (where a spurious 'owner' role lets a caller
 * appoint reviewers), and that path deserves coverage that does not depend on
 * the lifecycle routes.
 *
 * The property under test throughout: 'owner' is granted ONLY against a
 * positively resolved owner id. Neither the absence of a record nor a failure
 * to read one may ever produce it.
 */

const lifecycleRows = new Map<string, Record<string, unknown>>();
const typedDocs = new Map<string, { createdBy: string }>();
let typedDocLookupError: Error | null = null;

const fakeLifecycleRepo = {
  async get(documentId: string): Promise<Record<string, unknown> | null> {
    return lifecycleRows.get(documentId) ?? null;
  },
  async initialize(
    documentId: string,
    ownerId: string,
    status = 'draft',
  ): Promise<Record<string, unknown> | null> {
    if (lifecycleRows.has(documentId)) return null;
    const row = {
      documentId, status, ownerId,
      createdAt: 'now', updatedAt: 'now', updatedBy: ownerId,
      version: 0, history: [],
    };
    lifecycleRows.set(documentId, row);
    return row;
  },
};

jest.mock('../../repositories', () => ({
  get documentLifecycleRepo() { return fakeLifecycleRepo; },
  documentReviewRequestRepo: {
    getRequestsForDocument: jest.fn().mockResolvedValue([]),
  },
  typedDocumentRepo: {
    async get(documentId: string): Promise<{ createdBy: string } | null> {
      if (typedDocLookupError) throw typedDocLookupError;
      return typedDocs.get(documentId) ?? null;
    },
  },
}));

jest.mock('../../lib/event-catalog', () => ({
  catalog: { declare: jest.fn(), publishSignal: jest.fn() },
  publishSignal: jest.fn().mockResolvedValue(undefined),
  publishDurable: jest.fn().mockResolvedValue(undefined),
  publishWithOutbox: jest.fn().mockResolvedValue(undefined),
  connectCatalog: jest.fn().mockResolvedValue(undefined),
  disconnectCatalog: jest.fn().mockResolvedValue(undefined),
}));

import {
  loadContext,
  rolesFor,
  claimOwnership,
  toView,
  toPublicView,
  availableActionsFor,
  type LifecycleContext,
} from '../documentLifecycleService';
import { lifecycleErrorCode } from '../../lib/document-lifecycle';

const DOC = 'doc-1';

function seedRow(overrides: Record<string, unknown> = {}): void {
  lifecycleRows.set(DOC, {
    documentId: DOC, status: 'draft', ownerId: 'alice',
    createdAt: 'now', updatedAt: 'now', updatedBy: 'alice',
    version: 1, history: [], ...overrides,
  });
}

beforeEach(() => {
  lifecycleRows.clear();
  typedDocs.clear();
  typedDocLookupError = null;
});

// ---------------------------------------------------------------------------

describe('loadContext — owner resolution is three-valued', () => {
  it('resolved from the lifecycle row', async () => {
    seedRow();
    const ctx = await loadContext(DOC);
    expect(ctx.owner).toEqual({ state: 'resolved', ownerId: 'alice', source: 'lifecycle' });
    expect(ctx.ownerId).toBe('alice');
  });

  it('resolved from the typed-document creator when no row exists', async () => {
    typedDocs.set(DOC, { createdBy: 'alice' });
    const ctx = await loadContext(DOC);
    expect(ctx.owner).toEqual({ state: 'resolved', ownerId: 'alice', source: 'creator' });
  });

  it('absent when the creator read SUCCEEDS and finds nothing', async () => {
    const ctx = await loadContext(DOC);
    expect(ctx.owner.state).toBe('absent');
    expect(ctx.ownerId).toBeNull();
  });

  it('unavailable when the creator read THROWS — never absent', async () => {
    typedDocLookupError = new Error('ddb unavailable');
    const ctx = await loadContext(DOC);
    expect(ctx.owner.state).toBe('unavailable');
    // The distinction is the whole point: a failure must not read as "unowned".
    expect(ctx.owner.state).not.toBe('absent');
    expect(ctx.ownerId).toBeNull();
  });

  it('does not consult the creator at all once a row records an owner', async () => {
    seedRow();
    typedDocLookupError = new Error('ddb unavailable');
    const ctx = await loadContext(DOC);
    expect(ctx.owner).toEqual({ state: 'resolved', ownerId: 'alice', source: 'lifecycle' });
  });

  it('treats an empty-string ownerId on the row as no owner, not as owner ""', async () => {
    seedRow({ ownerId: '' });
    const ctx = await loadContext(DOC);
    expect(ctx.owner.state).toBe('absent');
  });
});

describe('rolesFor', () => {
  const ownerless = (state: 'absent' | 'unavailable'): LifecycleContext =>
    ({
      documentId: DOC,
      record: null,
      status: 'draft',
      owner: state === 'absent' ? { state } : { state, reason: 'boom' },
      ownerId: null,
      requests: [],
      outstanding: [],
    }) as LifecycleContext;

  it('grants owner to the resolved owner', async () => {
    seedRow();
    expect(rolesFor(await loadContext(DOC), 'alice')).toEqual(['owner']);
  });

  it('denies owner to everyone else', async () => {
    seedRow();
    expect(rolesFor(await loadContext(DOC), 'mallory')).toEqual([]);
  });

  it('grants NOTHING on an absent owner', () => {
    expect(rolesFor(ownerless('absent'), 'mallory')).toEqual([]);
    expect(rolesFor(ownerless('absent'), 'alice')).toEqual([]);
  });

  it('grants NOTHING on an unavailable owner', () => {
    expect(rolesFor(ownerless('unavailable'), 'mallory')).toEqual([]);
    expect(rolesFor(ownerless('unavailable'), 'alice')).toEqual([]);
  });

  it('still grants reviewer independently of ownership', () => {
    const ctx = {
      ...ownerless('unavailable'),
      requests: [{ reviewerId: 'bob', state: 'pending' }],
    } as unknown as LifecycleContext;
    expect(rolesFor(ctx, 'bob')).toEqual(['reviewer']);
  });

  it('offers no available actions when nobody owns the document', () => {
    expect(availableActionsFor(ownerless('absent'), 'mallory')).toEqual([]);
    expect(availableActionsFor(ownerless('unavailable'), 'mallory')).toEqual([]);
  });
});

describe('claimOwnership', () => {
  it('records the claimant as owner on a genuinely ownerless document', async () => {
    const result = await claimOwnership({ documentId: DOC, userId: 'alice' });
    expect(result.claimed).toBe(true);
    expect(lifecycleRows.get(DOC)!['ownerId']).toBe('alice');
    expect(rolesFor(await loadContext(DOC), 'alice')).toEqual(['owner']);
  });

  it('refuses rather than grants when the owner cannot be determined', async () => {
    typedDocLookupError = new Error('ddb unavailable');
    await expect(claimOwnership({ documentId: DOC, userId: 'mallory' })).rejects.toMatchObject({
      status: 503,
      code: 'owner_lookup_unavailable',
    });
    expect(lifecycleRows.has(DOC)).toBe(false);
  });

  it('refuses a takeover of an owned document', async () => {
    seedRow();
    const err = await claimOwnership({ documentId: DOC, userId: 'mallory' }).catch((e) => e);
    expect(lifecycleErrorCode(err)).toBe('owner_already_established');
    expect(lifecycleRows.get(DOC)!['ownerId']).toBe('alice');
  });

  it('is a no-op for the existing owner', async () => {
    seedRow();
    const result = await claimOwnership({ documentId: DOC, userId: 'alice' });
    expect(result.claimed).toBe(false);
    expect(lifecycleRows.get(DOC)!['version']).toBe(1);
  });

  it('loses a concurrent claim safely rather than overwriting the winner', async () => {
    // `initialize` returns null when a row already exists; simulate the racing
    // writer landing between our read and our write.
    const ctx = await loadContext(DOC);
    seedRow({ ownerId: 'alice' });
    const err = await claimOwnership({ documentId: DOC, userId: 'mallory', ctx }).catch((e) => e);
    expect(lifecycleErrorCode(err)).toBe('owner_already_established');
    expect(lifecycleRows.get(DOC)!['ownerId']).toBe('alice');
  });

  it('treats a lost race won by ourselves as success, not a conflict', async () => {
    const ctx = await loadContext(DOC);
    seedRow({ ownerId: 'alice' });
    const result = await claimOwnership({ documentId: DOC, userId: 'alice', ctx });
    expect(result.claimed).toBe(false);
  });
});

describe('toPublicView redacts what toView exposes', () => {
  beforeEach(() => {
    seedRow({
      status: 'in_review',
      updatedBy: 'alice',
      lastReason: 'needs a rollback plan',
      history: [{ from: 'draft', to: 'in_review', action: 'submit_for_review', actor: 'alice', at: 'now' }],
    });
  });

  it('keeps the state', async () => {
    const ctx = await loadContext(DOC);
    const pub = toPublicView(ctx);
    expect(pub.status).toBe('in_review');
    expect(pub.version).toBe(1);
    expect(pub.implicit).toBe(false);
  });

  it('drops every field that names a person or carries free text', async () => {
    const ctx = await loadContext(DOC);
    const pub = toPublicView(ctx) as Record<string, unknown>;
    for (const field of ['history', 'ownerId', 'updatedBy', 'lastReason']) {
      expect(pub[field]).toBeUndefined();
    }
    expect(JSON.stringify(pub)).not.toMatch(/alice|rollback/);
    // ...all of which the authenticated view still carries.
    const full = toView(ctx) as Record<string, unknown>;
    expect(full['ownerId']).toBe('alice');
    expect(full['history']).toHaveLength(1);
  });
});
