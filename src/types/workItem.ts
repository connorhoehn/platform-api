// platform-api/src/types/workItem.ts
//
// The work item: an utterance that has been turned into something a human can
// act on, and the AI proposal that answers it.
//
//     utterance -> work item -> AI proposal -> human acceptance -> the edit lands
//
// A work item is NOT a new table. It is a section item (see
// SectionItemRepository) with provenance attached. That was a deliberate choice
// over the two alternatives:
//
//   * A new table would have re-implemented assignee/priority/dueDate/status,
//     the `rejected` terminal state, accept-with-notes, reject-with-reason and
//     `GET /api/items/mine` — all of which section items already have and all of
//     which a work item needs.
//   * Review requests CANNOT carry it: their key is
//     `${sectionId|_doc}:${reviewerId}`, which permits exactly one row per
//     (section, reviewer). Two remarks about the same section by the same person
//     would overwrite each other.
//
// So the shape below is additive: every field here is optional, a hand-created
// task is a work item with no provenance, and nothing that reads section items
// today needs to change.

/**
 * Where the work item came from.
 *
 * Structurally compatible with realtime-modules' published `ContextFrame`
 * (client/voice/contextFrame.ts) — the capture path hands us one verbatim and we
 * store it verbatim. It is re-declared rather than imported because platform-api
 * does not depend on the browser package, and because a stored frame must keep
 * its shape even if the client contract later gains fields.
 *
 * The one rule the frame exists to enforce, restated here because this is where
 * it gets persisted: RULES PICK THE TARGET, THE MODEL ONLY PICKS THE ACTION.
 * Nothing downstream of this type may re-derive `sectionId` from `utterance`.
 */
export interface CapturedContext {
  entityType: string;
  entityId: string;
  documentId?: string;
  sectionId?: string;
  anchor: {
    relPos?: string;
    sectionId?: string;
    quote?: string;
  };
  tier: 'selection' | 'section' | 'viewport' | 'route' | 'none';
  confidence: 'exact' | 'high' | 'medium' | 'low' | 'none';
  contextSplit: boolean;
  autoAttach: boolean;
  reason: string;
  t0_ms: number;
  t1_ms: number;
  endTier?: string;
  endSectionId?: string;
  capturedBy?: string;
}

/**
 * What we are allowed to DO with the item, derived from the context frame's own
 * confidence — never from a model, and never fanned out.
 *
 *   high        attach to the target section; everyone sees it
 *   medium      attach, but only the speaker sees it until they confirm
 *   low         do not attach at all; sits in the inbox with ranked candidates
 *
 * The failure mode this forbids is Fireflies': on an ambiguous match it attaches
 * to EVERY candidate, which is worse than doing nothing because the wrong
 * attachments are indistinguishable from the right one. Here ambiguity produces
 * exactly one row, in the inbox, addressed to nobody's document.
 */
export type ConfidenceTier = 'high' | 'medium' | 'low';

/** Sentinel section for items that are not attached to any section yet. */
export const INBOX_SECTION_ID = '_inbox';

export type AttachState =
  /** Attached at the tier the context frame chose. */
  | 'attached'
  /** Attached but visible only to the speaker until they confirm. */
  | 'author_only'
  /** Not attached; awaiting a human choice from `candidates`. */
  | 'inbox'
  /**
   * The original anchor no longer resolved and we re-found the target by
   * matching `anchor.quote`. ALWAYS surfaced as a badge: a silently re-homed
   * anchor is a wrong attachment wearing a right one's clothes.
   */
  | 'healed';

/** A ranked target offered for a low-confidence item. Never auto-applied. */
export interface AttachCandidate {
  sectionId: string;
  sectionTitle?: string;
  /** 0..1. Ordering only — it is not a probability and is never a threshold. */
  score: number;
  reason: string;
}

/** One non-destructive move, appended and never overwritten. */
export interface ReattachEvent {
  at: string;
  by: string;
  fromSectionId: string;
  toSectionId: string;
  /** 'human' when someone chose it, 'heal' when the quote rung re-found it. */
  via: 'human' | 'heal';
}

// ---------------------------------------------------------------------------
// Proposals
// ---------------------------------------------------------------------------

/**
 * A typed change to a SCALAR the document holds outside its prose.
 *
 * The document is three substrates, and suggestion marks only cover one of them:
 *
 *   prose          ydoc.getXmlFragment('section:<id>')  -> marks work
 *   document meta  ydoc.getMap('meta')                  -> Y.Map scalar
 *   section titles ydoc.getArray('sections')            -> Y.Map scalar
 *
 * The operator's own example — "update this document to have a different
 * header" — is the second one. A ProseMirror mark cannot express "this Y.Map key
 * should hold a different string", so scalars get this instead.
 *
 * `from` is not documentation. It is the COMPARE half of a compare-and-set: the
 * value the proposal was drafted against. If someone renamed the header in the
 * minutes between the remark and the acceptance, applying `to` unconditionally
 * would silently destroy their edit. Applying it conditionally fails loudly and
 * the human re-drafts. Blind last-write-wins is exactly the bug a CRDT is
 * supposed to have made impossible, and re-introducing it at the application
 * layer would be an own goal.
 */
export interface FieldChange {
  /** `meta.title` | `meta.<key>` | `sections.<sectionId>.title` */
  path: string;
  /** Expected current value. Mismatch => stale => refuse. */
  from: unknown;
  /** Proposed value. */
  to: unknown;
}

/**
 * A prose edit, expressed as find-and-replace rather than positions.
 *
 * There is NO server-side Y.Doc anywhere in this system — the gateway relays
 * CRDT bytes opaquely and never materialises a document. So the drafting side
 * cannot produce ProseMirror positions, because it has no ProseMirror. What it
 * can produce is a verbatim quote from the text it was shown, and the text that
 * should stand in its place. A live browser editor resolves the quote to a real
 * range at apply time.
 *
 * An empty `replacement` is a deletion. An empty `anchorQuote` is an append.
 */
export interface ProseEdit {
  anchorQuote: string;
  replacement: string;
}

export interface ProseProposal {
  kind: 'prose';
  proposalId: string;
  sectionId: string;
  edits: ProseEdit[];
  rationale?: string;
}

export interface FieldProposal {
  kind: 'field';
  proposalId: string;
  change: FieldChange;
  rationale?: string;
}

export type Proposal = ProseProposal | FieldProposal;

export type ProposalStatus =
  /** No proposal has been drafted for this item. */
  | 'none'
  /** A draft is being generated. */
  | 'drafting'
  /** Drafted and awaiting a human decision. */
  | 'ready'
  /** A human accepted it AND the edit landed in the document. */
  | 'applied'
  /** A human rejected it. */
  | 'rejected'
  /** Drafting failed, or the apply hit a stale value and was refused. */
  | 'failed';

/**
 * Receipt that the edit actually landed.
 *
 * This exists because of the anti-pattern this whole feature is a correction
 * for: the existing doc-type proposals queue records a "promote" decision and
 * never applies anything, so its accepted rows are indistinguishable from its
 * pending ones except by a status string that means nothing. Acceptance here is
 * only recorded AFTER the mutation, and it carries evidence of what mutated.
 */
export interface AppliedReceipt {
  at: string;
  by: string;
  /** What the document held immediately before the write. */
  observedFrom?: unknown;
  /** What it holds now. */
  wrote?: unknown;
  /** Prose: the suggestion identity whose marks were accepted. */
  suggestionKey?: string;
  /** How many prose edits resolved and applied. */
  editsApplied?: number;
}

/** Provenance + proposal, merged onto SectionItemFields. All optional. */
export interface WorkItemProvenance {
  source?: 'human' | 'voice';
  /** Verbatim transcript. Never paraphrased, never re-derived. */
  utterance?: string;
  utteranceId?: string;
  captureId?: string;
  capturedContext?: CapturedContext;
  confidenceTier?: ConfidenceTier;
  attachState?: AttachState;
  candidates?: AttachCandidate[];
  reattachHistory?: ReattachEvent[];
  /** Only this user may see the item while attachState is 'author_only'. */
  createdBy?: string;

  proposal?: Proposal;
  proposalStatus?: ProposalStatus;
  proposalError?: string;
  applied?: AppliedReceipt;
  rejectedReason?: string;
}

/**
 * Map a captured context frame onto the tier that decides what we may do.
 *
 * Pure, total, and the ONLY place the mapping lives. Note that `contextSplit`
 * and `autoAttach` dominate: a remark spoken while the screen moved is low
 * confidence regardless of how specific its start rung was, because we know the
 * speaker was looking at two things and we do not know which one they meant.
 */
export function tierFromContext(ctx: CapturedContext): ConfidenceTier {
  if (!ctx.autoAttach || ctx.contextSplit) return 'low';
  if (!ctx.sectionId) return 'low';
  if (ctx.confidence === 'exact' || ctx.confidence === 'high') return 'high';
  if (ctx.confidence === 'medium') return 'medium';
  return 'low';
}

/** The attach state a fresh item gets. Never fans out — exactly one target. */
export function attachStateForTier(tier: ConfidenceTier): AttachState {
  if (tier === 'high') return 'attached';
  if (tier === 'medium') return 'author_only';
  return 'inbox';
}
