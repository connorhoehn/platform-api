/**
 * Exhaustive tests for the document lifecycle transition table.
 *
 * "Exhaustive" is literal: every (from, to) pair and every (from, action)
 * pair is asserted, so adding a state or a rule without updating the expected
 * sets fails here rather than silently widening what the API permits.
 */

import {
  transition,
  normalizeStatus,
  isLegacyStatus,
  isLocked,
  isLegalTransition,
  findRule,
  actionsFrom,
  availableActions,
  isKnownAction,
  isTerminalStatus,
  assertTerminalStatesHaveNoExit,
  TRANSITIONS,
  DOCUMENT_STATUSES,
  LIFECYCLE_ACTIONS,
  type DocumentStatus,
  type LifecycleAction,
  type ActorRole,
  type OutstandingReview,
} from '../document-lifecycle';
import {
  ConflictError,
  ForbiddenError,
  ValidationError,
} from '../../middleware/error-handler';

// ---------------------------------------------------------------------------
// The expected table, written out independently of the implementation.
// ---------------------------------------------------------------------------

const LEGAL_PAIRS: ReadonlyArray<[DocumentStatus, DocumentStatus]> = [
  ['draft', 'in_review'],
  ['in_review', 'draft'],
  ['in_review', 'approved'],
  ['in_review', 'changes_requested'],
  ['changes_requested', 'draft'],
  ['changes_requested', 'in_review'],
  ['approved', 'draft'],
  ['approved', 'finalized'],
  // Nothing leaves 'finalized'. Its absence from this list is what every
  // `finalized -> *` case in the exhaustive pair sweep below asserts against.
];

/** (from, action) → who may drive it. Everything absent is illegal. */
const LEGAL_MOVES: ReadonlyArray<{
  from: DocumentStatus;
  action: LifecycleAction;
  to: DocumentStatus;
  roles: ActorRole[];
}> = [
  { from: 'draft', action: 'submit_for_review', to: 'in_review', roles: ['owner'] },
  { from: 'in_review', action: 'withdraw', to: 'draft', roles: ['owner'] },
  { from: 'in_review', action: 'approve', to: 'approved', roles: ['reviewer'] },
  { from: 'in_review', action: 'request_changes', to: 'changes_requested', roles: ['reviewer'] },
  { from: 'changes_requested', action: 'revise', to: 'draft', roles: ['owner'] },
  { from: 'changes_requested', action: 'submit_for_review', to: 'in_review', roles: ['owner'] },
  { from: 'approved', action: 'reopen', to: 'draft', roles: ['owner'] },
  { from: 'approved', action: 'finalize', to: 'finalized', roles: ['owner'] },
];

const ALL_ROLES: ActorRole[] = ['owner', 'reviewer'];

function pairKey(from: string, to: string): string {
  return `${from}->${to}`;
}
function moveKey(from: string, action: string): string {
  return `${from}:${action}`;
}

const LEGAL_PAIR_KEYS = new Set(LEGAL_PAIRS.map(([f, t]) => pairKey(f, t)));
const LEGAL_MOVE_KEYS = new Set(LEGAL_MOVES.map((m) => moveKey(m.from, m.action)));

describe('transition table integrity', () => {
  it('has exactly the expected number of rules', () => {
    expect(TRANSITIONS).toHaveLength(LEGAL_MOVES.length);
  });

  it('every rule matches the independently-declared expectation', () => {
    for (const rule of TRANSITIONS) {
      const expected = LEGAL_MOVES.find(
        (m) => m.from === rule.from && m.action === rule.action,
      );
      expect(expected).toBeDefined();
      expect(rule.to).toBe(expected!.to);
      expect([...rule.allowedRoles].sort()).toEqual([...expected!.roles].sort());
    }
  });

  it('exactly one rule can reach approved, and it requires all reviews', () => {
    const toApproved = TRANSITIONS.filter((r) => r.to === 'approved');
    expect(toApproved).toHaveLength(1);
    expect(toApproved[0]!.requiresAllReviews).toBe(true);
    expect(toApproved[0]!.allowedRoles).toEqual(['reviewer']);
  });

  it('every rule leaving approved is owner-only', () => {
    // This is the "anyone can unlock a finalized doc" bug, encoded as a test.
    // There are two exits now (reopen and finalize) and BOTH are owner-only,
    // which is the property that actually mattered — the original single-rule
    // count was a proxy for it.
    const fromApproved = TRANSITIONS.filter((r) => r.from === 'approved');
    expect(fromApproved.map((r) => r.action).sort()).toEqual(['finalize', 'reopen']);
    for (const rule of fromApproved) {
      expect(rule.allowedRoles).toEqual(['owner']);
    }
  });

  it('no rule is a self-transition', () => {
    expect(TRANSITIONS.filter((r) => r.from === r.to)).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// The terminal state
// ---------------------------------------------------------------------------

describe('finalized is terminal', () => {
  it('no rule leaves it, for any action', () => {
    expect(TRANSITIONS.filter((r) => r.from === 'finalized')).toHaveLength(0);
    expect(actionsFrom('finalized')).toEqual([]);
  });

  it('offers no action to any combination of roles', () => {
    const roleSets: ActorRole[][] = [[], ['owner'], ['reviewer'], ['owner', 'reviewer']];
    for (const roles of roleSets) {
      expect(availableActions('finalized', roles)).toEqual([]);
    }
  });

  it('refuses every action in the vocabulary, even from an all-roles actor', () => {
    for (const action of LIFECYCLE_ACTIONS) {
      expect(() =>
        transition('finalized', action, { roles: ALL_ROLES, outstandingReviews: [] }),
      ).toThrow(ConflictError);
    }
  });

  it('explains that it is permanent rather than a missing permission', () => {
    // The wording is load-bearing: a bare "no actions are legal" reads as
    // something the caller might fix by asking someone.
    expect(() => transition('finalized', 'reopen', { roles: ALL_ROLES })).toThrow(
      /terminal state.*for any user/s,
    );
    expect(() => transition('finalized', 'reopen', { roles: ALL_ROLES })).toThrow(
      /not a permission you are missing/,
    );
  });

  it('is reported as terminal, and the other states are not', () => {
    expect(isTerminalStatus('finalized')).toBe(true);
    for (const s of DOCUMENT_STATUSES.filter((x) => x !== 'finalized')) {
      expect(isTerminalStatus(s)).toBe(false);
    }
  });

  it('locks the document, like approved but permanently', () => {
    expect(isLocked('finalized')).toBe(true);
    expect(isLocked('approved')).toBe(true);
    // The lock says nothing about reversibility; only isTerminalStatus does.
    expect(isTerminalStatus('approved')).toBe(false);
  });

  it('the runtime guard agrees with the table', () => {
    expect(() => assertTerminalStatesHaveNoExit()).not.toThrow();
  });
});

describe('reaching finalized takes two different actors', () => {
  // The point of the owner/reviewer split on approve vs finalize: no single
  // role can drive a document from draft to a state nobody can undo.
  it('an owner alone cannot approve, so cannot reach finalized alone', () => {
    expect(() =>
      transition('in_review', 'approve', { roles: ['owner'], outstandingReviews: [] }),
    ).toThrow(ForbiddenError);
  });

  it('a reviewer alone cannot finalize an approved document', () => {
    expect(() =>
      transition('approved', 'finalize', { roles: ['reviewer'], outstandingReviews: [] }),
    ).toThrow(ForbiddenError);
  });

  it('a bystander certainly cannot', () => {
    expect(() =>
      transition('approved', 'finalize', { roles: [], outstandingReviews: [] }),
    ).toThrow(ForbiddenError);
  });

  it('the owner can, once a reviewer has approved', () => {
    const result = transition('approved', 'finalize', {
      roles: ['owner'],
      outstandingReviews: [],
    });
    expect(result.to).toBe('finalized');
    expect(result.action).toBe('finalize');
  });
});

describe('finalize is gated on outstanding reviews', () => {
  const pending: OutstandingReview[] = [
    { requestKey: 'r-1', reviewerId: 'grace', state: 'pending' },
  ];

  it('refuses with 409 while a review request is outstanding', () => {
    // The deliberate change from the old warn-not-block behaviour: with no
    // unlock, finalizing over a pending request makes it permanently
    // unanswerable, so the override is not offered at all.
    expect(() =>
      transition('approved', 'finalize', {
        roles: ['owner'],
        outstandingReviews: pending,
      }),
    ).toThrow(ConflictError);
  });

  it('names the blocking reviewer', () => {
    expect(() =>
      transition('approved', 'finalize', {
        roles: ['owner'],
        outstandingReviews: pending,
      }),
    ).toThrow(/grace/);
  });

  it('the rule carries requiresAllReviews, so the gate is not incidental', () => {
    const rule = findRule('approved', 'finalize')!;
    expect(rule.requiresAllReviews).toBe(true);
  });

  it('reopen is NOT gated on reviews — only the one-way door is', () => {
    expect(() =>
      transition('approved', 'reopen', {
        roles: ['owner'],
        outstandingReviews: pending,
      }),
    ).not.toThrow();
  });

  it('forbidden still beats outstanding-reviews', () => {
    expect(() =>
      transition('approved', 'finalize', {
        roles: ['reviewer'],
        outstandingReviews: pending,
      }),
    ).toThrow(ForbiddenError);
  });
});

describe('isLegalTransition — every (from, to) pair', () => {
  const pairs: Array<[DocumentStatus, DocumentStatus]> = [];
  for (const from of DOCUMENT_STATUSES) {
    for (const to of DOCUMENT_STATUSES) pairs.push([from, to]);
  }

  it('covers all 25 pairs (5 states squared)', () => {
    expect(pairs).toHaveLength(25);
    expect(LEGAL_PAIR_KEYS.size).toBe(8);
  });

  it.each(pairs)('%s -> %s', (from, to) => {
    expect(isLegalTransition(from, to)).toBe(LEGAL_PAIR_KEYS.has(pairKey(from, to)));
  });
});

describe('transition() — every (from, action) pair', () => {
  const combos: Array<[DocumentStatus, LifecycleAction]> = [];
  for (const from of DOCUMENT_STATUSES) {
    for (const action of LIFECYCLE_ACTIONS) combos.push([from, action]);
  }

  it('covers all 35 combinations (5 states x 7 actions)', () => {
    expect(combos).toHaveLength(35);
  });

  it.each(combos)(
    'from %s, action %s',
    (from, action) => {
      const legal = LEGAL_MOVE_KEYS.has(moveKey(from, action));
      // Give the actor every role and zero outstanding reviews, so the ONLY
      // thing under test here is whether the move exists in the table.
      const call = (): unknown =>
        transition(from, action, { roles: ALL_ROLES, outstandingReviews: [] });

      if (legal) {
        const expected = LEGAL_MOVES.find((m) => m.from === from && m.action === action)!;
        const result = transition(from, action, {
          roles: ALL_ROLES,
          outstandingReviews: [],
        });
        expect(result.from).toBe(from);
        expect(result.to).toBe(expected.to);
        expect(result.action).toBe(action);
      } else {
        expect(call).toThrow(ConflictError);
      }
    },
  );

  it('an illegal transition explains which actions are legal instead', () => {
    expect(() =>
      transition('draft', 'approve', { roles: ALL_ROLES }),
    ).toThrow(/Cannot 'approve' a document in state 'draft'.*submit_for_review/s);
  });

  // 'finalize' used to be this file's example of an unknown action. It is a
  // real action now, so the example moved — and the pair below pins the new
  // fact directly, because "finalize is in the vocabulary" is exactly the
  // thing a careless revert would undo while leaving every other test green.
  it('rejects an action outside the vocabulary with a 400, not a 409', () => {
    expect(() => transition('draft', 'publish', { roles: ALL_ROLES })).toThrow(
      ValidationError,
    );
    expect(() => transition('draft', 'publish', { roles: ALL_ROLES })).toThrow(
      /Unknown lifecycle action/,
    );
  });

  it('isKnownAction gates the vocabulary', () => {
    for (const a of LIFECYCLE_ACTIONS) expect(isKnownAction(a)).toBe(true);
    expect(isKnownAction('publish')).toBe(false);
    expect(isKnownAction('')).toBe(false);
  });

  it('finalize IS part of the vocabulary', () => {
    expect(isKnownAction('finalize')).toBe(true);
    expect(LIFECYCLE_ACTIONS).toContain('finalize');
  });

  it('a finalize attempt from a non-approved state is a 409, not a 400', () => {
    // The distinction matters: 400 would say "no such action", which would
    // send the caller looking for a typo instead of at the document's state.
    expect(() => transition('draft', 'finalize', { roles: ALL_ROLES })).toThrow(
      ConflictError,
    );
  });
});

describe('authorization — every legal move against every actor', () => {
  const roleSets: Array<{ label: string; roles: ActorRole[] }> = [
    { label: 'bystander (no roles)', roles: [] },
    { label: 'owner only', roles: ['owner'] },
    { label: 'reviewer only', roles: ['reviewer'] },
    { label: 'owner + reviewer', roles: ['owner', 'reviewer'] },
  ];

  for (const move of LEGAL_MOVES) {
    for (const set of roleSets) {
      const permitted = move.roles.some((r) => set.roles.includes(r));
      it(`${move.from} --${move.action}--> ${move.to} by ${set.label} is ${
        permitted ? 'allowed' : 'FORBIDDEN'
      }`, () => {
        const call = (): unknown =>
          transition(move.from, move.action, {
            roles: set.roles,
            outstandingReviews: [],
          });
        if (permitted) {
          const result = transition(move.from, move.action, {
            roles: set.roles,
            outstandingReviews: [],
          });
          expect(result.to).toBe(move.to);
        } else {
          expect(call).toThrow(ForbiddenError);
        }
      });
    }
  }

  it('a bystander cannot reopen an approved document', () => {
    // THE BUG: previously any caller could unlock a finalized doc.
    expect(() =>
      transition('approved', 'reopen', { roles: [] }),
    ).toThrow(ForbiddenError);
  });

  it('a designated reviewer cannot reopen an approved document either', () => {
    expect(() =>
      transition('approved', 'reopen', { roles: ['reviewer'] }),
    ).toThrow(ForbiddenError);
  });

  it('a legacy `final` document is equally protected from a bystander', () => {
    expect(() => transition('final', 'reopen', { roles: [] })).toThrow(ForbiddenError);
  });

  it('the owner CAN reopen an approved document', () => {
    const result = transition('approved', 'reopen', { roles: ['owner'] });
    expect(result.to).toBe('draft');
  });

  it('an owner who is not a reviewer cannot approve their own document', () => {
    expect(() =>
      transition('in_review', 'approve', { roles: ['owner'], outstandingReviews: [] }),
    ).toThrow(ForbiddenError);
  });

  it('illegal beats forbidden: a bystander approving a draft gets 409, not 403', () => {
    // draft -> approve is not in the table at all, so the shape of the failure
    // is "impossible", not "not you".
    expect(() => transition('draft', 'approve', { roles: [] })).toThrow(ConflictError);
  });
});

describe('approve requires every outstanding required review to be satisfied', () => {
  const outstanding = (reviewerId: string, state = 'pending'): OutstandingReview => ({
    requestKey: `_doc:${reviewerId}`,
    reviewerId,
    state,
  });

  it('approves when nothing is outstanding', () => {
    const result = transition('in_review', 'approve', {
      roles: ['reviewer'],
      outstandingReviews: [],
    });
    expect(result.to).toBe('approved');
  });

  it('approves when outstandingReviews is omitted entirely', () => {
    const result = transition('in_review', 'approve', { roles: ['reviewer'] });
    expect(result.to).toBe('approved');
  });

  it('rejects with 409 when one required review is still pending', () => {
    expect(() =>
      transition('in_review', 'approve', {
        roles: ['reviewer'],
        outstandingReviews: [outstanding('bob')],
      }),
    ).toThrow(ConflictError);
  });

  it('names the blocking reviewers in the 409', () => {
    expect(() =>
      transition('in_review', 'approve', {
        roles: ['reviewer'],
        outstandingReviews: [outstanding('bob'), outstanding('carol')],
      }),
    ).toThrow(/2 required review requests still outstanding \(bob, carol\)/);
  });

  it('pluralizes correctly for a single outstanding review', () => {
    expect(() =>
      transition('in_review', 'approve', {
        roles: ['reviewer'],
        outstandingReviews: [outstanding('bob')],
      }),
    ).toThrow(/1 required review request still outstanding/);
  });

  it('a changes_requested review counts as outstanding, not satisfied', () => {
    expect(() =>
      transition('in_review', 'approve', {
        roles: ['reviewer'],
        outstandingReviews: [outstanding('bob', 'changes_requested')],
      }),
    ).toThrow(ConflictError);
  });

  it('does NOT gate request_changes on outstanding reviews', () => {
    const result = transition('in_review', 'request_changes', {
      roles: ['reviewer'],
      outstandingReviews: [outstanding('bob'), outstanding('carol')],
    });
    expect(result.to).toBe('changes_requested');
  });

  it('does NOT gate withdraw on outstanding reviews', () => {
    const result = transition('in_review', 'withdraw', {
      roles: ['owner'],
      outstandingReviews: [outstanding('bob')],
    });
    expect(result.to).toBe('draft');
  });

  it('forbidden beats outstanding-reviews: wrong actor gets 403', () => {
    expect(() =>
      transition('in_review', 'approve', {
        roles: ['owner'],
        outstandingReviews: [outstanding('bob')],
      }),
    ).toThrow(ForbiddenError);
  });
});

describe('legacy `final` status', () => {
  it('normalizes to approved on read', () => {
    expect(normalizeStatus('final')).toBe('approved');
  });

  it('is flagged for migration', () => {
    expect(isLegacyStatus('final')).toBe(true);
    expect(isLegacyStatus('approved')).toBe(false);
  });

  it('behaves exactly like approved in the transition table', () => {
    const fromLegacy = transition('final', 'reopen', { roles: ['owner'] });
    const fromCurrent = transition('approved', 'reopen', { roles: ['owner'] });
    expect(fromLegacy.from).toBe('approved');
    expect(fromLegacy.to).toBe(fromCurrent.to);
  });

  it('reports migratedLegacyStatus so the write path can rewrite it', () => {
    expect(transition('final', 'reopen', { roles: ['owner'] }).migratedLegacyStatus).toBe(true);
    expect(transition('approved', 'reopen', { roles: ['owner'] }).migratedLegacyStatus).toBe(false);
  });

  it('rejects the same illegal moves as approved', () => {
    expect(() => transition('final', 'approve', { roles: ['reviewer'] })).toThrow(ConflictError);
    expect(() => transition('final', 'submit_for_review', { roles: ['owner'] })).toThrow(ConflictError);
  });

  it('locks the document', () => {
    expect(isLocked('final')).toBe(true);
    expect(isLocked('approved')).toBe(true);
    expect(isLocked('finalized')).toBe(true);
    expect(isLocked('draft')).toBe(false);
    expect(isLocked('in_review')).toBe(false);
    expect(isLocked('changes_requested')).toBe(false);
  });

  // -------------------------------------------------------------------------
  // The one that would be easy to get wrong
  // -------------------------------------------------------------------------

  it('does NOT normalize to the new terminal state, despite the spelling', () => {
    // 'final' (legacy stored value) and 'finalized' (terminal state) differ by
    // three characters and mean opposite things about reversibility. Mapping
    // the old string onto the new state would silently make an existing set of
    // documents permanently unreopenable on the strength of a value written
    // when nothing on the server meant anything by it.
    expect(normalizeStatus('final')).toBe('approved');
    expect(normalizeStatus('final')).not.toBe('finalized');
  });

  it('leaves a legacy document reopenable', () => {
    expect(isTerminalStatus('final')).toBe(false);
    expect(actionsFrom(normalizeStatus('final'))).toContain('reopen');
    expect(() => transition('final', 'reopen', { roles: ['owner'] })).not.toThrow();
  });

  it('a legacy document can still be finalized, but only through the real rule', () => {
    // It is `approved`, so `finalize` is legal from it — for the owner, with
    // no outstanding reviews. That is the migration path: not inherited,
    // transitioned.
    const result = transition('final', 'finalize', {
      roles: ['owner'],
      outstandingReviews: [],
    });
    expect(result.from).toBe('approved');
    expect(result.to).toBe('finalized');
    // Still flagged, so the write path rewrites the stored 'final' as it goes.
    expect(result.migratedLegacyStatus).toBe(true);
  });

  it('and a non-owner still cannot', () => {
    expect(() =>
      transition('final', 'finalize', { roles: ['reviewer'], outstandingReviews: [] }),
    ).toThrow(ForbiddenError);
  });
});

describe('normalizeStatus fallbacks', () => {
  it.each([
    [undefined, 'draft'],
    [null, 'draft'],
    ['', 'draft'],
    ['review', 'draft'], // the status the old code declared but never set
    ['nonsense', 'draft'],
    ['draft', 'draft'],
    ['in_review', 'in_review'],
    ['changes_requested', 'changes_requested'],
    ['approved', 'approved'],
    ['final', 'approved'],
  ])('normalizeStatus(%p) === %p', (input, expected) => {
    expect(normalizeStatus(input as string | null | undefined)).toBe(expected);
  });
});

describe('introspection helpers (drive frontend button state)', () => {
  it('actionsFrom lists every action legal from a state', () => {
    expect(actionsFrom('draft').sort()).toEqual(['submit_for_review']);
    expect(actionsFrom('in_review').sort()).toEqual(
      ['approve', 'request_changes', 'withdraw'].sort(),
    );
    expect(actionsFrom('changes_requested').sort()).toEqual(
      ['revise', 'submit_for_review'].sort(),
    );
    expect(actionsFrom('approved').sort()).toEqual(['finalize', 'reopen']);
    expect(actionsFrom('finalized')).toEqual([]);
  });

  it('availableActions filters by the actor roles', () => {
    expect(availableActions('in_review', ['owner'])).toEqual(['withdraw']);
    expect(availableActions('in_review', ['reviewer']).sort()).toEqual(
      ['approve', 'request_changes'].sort(),
    );
    expect(availableActions('in_review', []).length).toBe(0);
    expect(availableActions('approved', ['reviewer']).length).toBe(0);
    expect(availableActions('approved', ['owner']).sort()).toEqual(['finalize', 'reopen']);
    // Terminal: empty for everyone, which is what the FE renders as "closed".
    expect(availableActions('finalized', ['owner'])).toEqual([]);
  });

  it('findRule returns undefined for illegal moves', () => {
    expect(findRule('draft', 'approve')).toBeUndefined();
    expect(findRule('draft', 'submit_for_review')).toBeDefined();
  });
});
