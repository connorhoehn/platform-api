// The card a chat command leaves behind.
//
// A slash command in a conversation (/doc, /summarize, /diagram) becomes a
// pipeline run, and the conversation keeps a CARD for it — a chat message
// whose metadata names the run, so the thread shows the run moving and, when
// there is a page behind it, opens that page. Recording pipelines post the
// same card from their lobby; this is the helper for a run that already
// knows its channel.
import { postChatMessage, editChatMessage, readChatHistory, PIPELINE_IDENTITY, ChatRefusedError, describeError, type ChatHistoryMessage, type ServiceIdentity } from '../gateway/GatewayServiceClient';
import log from '../lib/logger';

export interface RunCardInput {
  channel: string | null | undefined;
  runId: string;
  pipelineId: string;
  /** The slash command, e.g. `/summarize`. */
  command: string;
  /** What the card says after the command. */
  instruction: string;
  /** Extra metadata for the card (a `documentId` lets it open the page). */
  extra?: Record<string, unknown>;
  /**
   * The run context, when a step posts. A run started from the chat client
   * carries `clientPostsCard: true` (the dispatcher merges it into every
   * trigger payload): the client already posted a card with the run id, which
   * updates live and opens the `result` — a second one from the step would be
   * a duplicate. Runs started with no client (an API caller, a lobby) do not
   * carry it and the step's card is the only one.
   */
  context?: Record<string, unknown> | null;
  /**
   * The person the run is for. Read from `context.requestedBy` /
   * `requestedByName` when absent. On a direct message the card is posted AS
   * them — see `runCardIdentity`.
   */
  requester?: { userId: string; displayName?: string } | null;
}

/** What happened to a card post: skipped, posted (and as whom), or refused/failed with the gateway's reason. */
export interface RunCardPostOutcome {
  posted: boolean;
  /** `no-channel` or `client-posts`: nothing was attempted, and that is not a failure. */
  skipped?: 'no-channel' | 'client-posts';
  /** Who the message was sent as. */
  as?: 'person' | 'pipeline';
  /** Set when a post was attempted and did not land: the gateway's refusal or the timeout, verbatim. */
  error?: string;
  code?: string;
}

/** A step's `postCard` may be an older boolean-returning stand-in (tests). */
export function runCardOutcomeOf(result: boolean | RunCardPostOutcome | undefined): RunCardPostOutcome {
  return typeof result === 'object' && result ? result : { posted: result === true };
}

/** Step output fields for a card post: `posted`, plus `cardError`/`cardErrorCode` when it failed. */
export function runCardOutputOf(outcome: RunCardPostOutcome): Record<string, unknown> {
  return {
    posted: outcome.posted,
    ...(outcome.error ? { cardError: outcome.error } : {}),
    ...(outcome.code ? { cardErrorCode: outcome.code } : {}),
  };
}

const isMemberAddressedDm = (channel: string): boolean => channel.startsWith('chat:dm:');

/**
 * Who posts the card. A member-addressed direct message (`chat:dm:a:b`) admits
 * only its two members — the gateway's `ChatService._checkDmMembership` has no
 * service bypass — so the pipeline's own identity is refused there with
 * CHAT_DM_FORBIDDEN (NFR #42). The card goes in as the person who asked, which
 * is also exactly what the chat client does when it posts the card itself
 * (`clientPostsCard`). The metadata still names the pipeline as the agent.
 * Everywhere else the pipeline posts as itself, as before.
 */
export function runCardIdentity(input: RunCardInput): ServiceIdentity | undefined {
  if (!input.channel || !isMemberAddressedDm(input.channel)) return undefined;
  const ctx = (input.context ?? {}) as Record<string, unknown>;
  const userId = input.requester?.userId ?? str(ctx.requestedBy);
  if (!userId || userId.startsWith('agent:')) return undefined;
  const displayName = input.requester?.displayName ?? str(ctx.requestedByName) ?? userId;
  return { userId, displayName, kind: 'person' };
}

/** True when the run context says the chat client posted the card itself. */
export function clientPostsCard(context: unknown): boolean {
  return !!context && typeof context === 'object' && (context as Record<string, unknown>).clientPostsCard === true;
}

/** The card's metadata — what the chat message carries, and what a step returns even when it does not post. */
export function runCardMetadata(input: RunCardInput): Record<string, unknown> {
  return {
    kind: 'agent-run', runId: input.runId, pipelineId: input.pipelineId, command: input.command, instruction: input.instruction,
    agentName: PIPELINE_IDENTITY.displayName, agentUserId: PIPELINE_IDENTITY.userId,
    ...(input.extra ?? {}),
  };
}

/**
 * Post the run's card into the conversation. Never throws: a missing channel is
 * a skip, and a refusal or gateway failure comes back as `error` (logged at
 * error level) so the step can put it on the run. Skipped when the run's
 * context says the client posted the card — see `RunCardInput.context`.
 */
export async function postRunCard(input: RunCardInput): Promise<RunCardPostOutcome> {
  if (!input.channel) return { posted: false, skipped: 'no-channel' };
  if (clientPostsCard(input.context)) {
    log.debug({ channel: input.channel, runId: input.runId }, 'run card left to the client that started the run');
    return { posted: false, skipped: 'client-posts' };
  }
  const identity = runCardIdentity(input);
  const as = identity ? 'person' : 'pipeline';
  try {
    await postChatMessage({
      channel: input.channel,
      message: `${input.command} ${input.instruction}`,
      metadata: runCardMetadata(input),
      ...(identity ? { identity } : {}),
    });
    return { posted: true, as };
  } catch (e) {
    const error = describeError(e);
    const code = e instanceof ChatRefusedError ? e.code : undefined;
    log.error({ err: error, code, channel: input.channel, runId: input.runId, pipelineId: input.pipelineId, as }, 'could not post the run card into the conversation');
    return { posted: false, as, error, ...(code ? { code } : {}) };
  }
}

/** `postRunCard`, as a boolean. */
export async function postRunCardToChannel(input: RunCardInput): Promise<boolean> {
  return (await postRunCard(input)).posted;
}

// ─── The run card becomes the document card ─────────────────────────────
//
// A run that produced a document leaves ONE message in the conversation: the
// run's own card, turned into the conversation's document card
// (`kind: 'document'` — "<title> · Created by <name> · Open") with a `run`
// summary under it, so "Open run" still reaches the run. A second message
// saying the same thing is what the thread used to get, and the run card sat
// beside it saying "Received".
//
// The card is edited AS ITS AUTHOR, because only the author may edit it: the
// requester when the chat client posted it (clientPostsCard — and every DM
// card), the pipeline when a step did. When no card is found (a run started
// from an API caller) the document card is posted instead.

export interface RunDocumentCardInput {
  channel: string | null | undefined;
  /** Every id the card may carry: the loop id the client was given, and the executor's. */
  runIds: string[];
  pipelineId: string;
  documentId: string;
  title: string;
  icon?: string;
  requester: { userId: string; displayName?: string };
  /** The one line under the card: what was asked and for whom. */
  summary?: string;
  /**
   * Post a document card when no run card is found (default true). A repair of
   * an old run passes false: a card it cannot find (older than the history
   * window) is not a reason to add a second one.
   */
  postIfNoCard?: boolean;
}

export interface RunDocumentCardOutcome {
  /**
   * `promoted`: the run card became the document card; `posted`: a new
   * document card; `already`: the card already is this document's card.
   */
  outcome: 'promoted' | 'posted' | 'already' | 'skipped' | 'failed';
  messageId?: string;
  error?: string;
}

export interface RunDocumentCardDeps {
  readHistory?: typeof readChatHistory;
  edit?: typeof editChatMessage;
  post?: typeof postChatMessage;
}

/** The metadata of a conversation's document card, with the run that made it. */
export function runDocumentCardMetadata(input: RunDocumentCardInput): Record<string, unknown> {
  return {
    kind: 'document',
    documentId: input.documentId,
    title: input.title,
    ...(input.icon ? { icon: input.icon } : {}),
    // The card says "Created by <createdBy>" and wants a name, never an id
    // (DocumentActivityCard); the id rides beside it.
    createdBy: input.requester.displayName ?? input.requester.userId,
    createdById: input.requester.userId,
    displayName: input.requester.displayName ?? input.requester.userId,
    run: { runId: input.runIds[0], pipelineId: input.pipelineId, ...(input.summary ? { summary: input.summary } : {}) },
  };
}

/** The newest card in `messages` naming one of `runIds` (its own `runId`, or a document card's `run.runId`). */
export function findRunCard(messages: ChatHistoryMessage[], runIds: string[]): ChatHistoryMessage | undefined {
  const ids = new Set(runIds.filter(Boolean));
  return [...messages].reverse().find((m) => {
    const md = m.metadata ?? {};
    const run = md.run && typeof md.run === 'object' ? (md.run as Record<string, unknown>).runId : undefined;
    return typeof m.id === 'string' && (ids.has(md.runId as string) || ids.has(run as string)) && (md.kind === 'agent-run' || md.kind === 'document');
  });
}

/** True when `card` already is the document card for `documentId`. */
export function isDocumentCardFor(card: ChatHistoryMessage | undefined, documentId: string): boolean {
  return card?.metadata?.kind === 'document' && card.metadata.documentId === documentId;
}

/** Turn the run's card into the document card, or post one. Never throws. */
export async function announceRunDocument(input: RunDocumentCardInput, deps: RunDocumentCardDeps = {}): Promise<RunDocumentCardOutcome> {
  if (!input.channel) return { outcome: 'skipped' };
  const channel = input.channel;
  const readHistory = deps.readHistory ?? readChatHistory;
  const edit = deps.edit ?? editChatMessage;
  const post = deps.post ?? postChatMessage;
  const requester: ServiceIdentity = {
    userId: input.requester.userId,
    displayName: input.requester.displayName ?? input.requester.userId,
    kind: 'person',
  };
  const metadata = runDocumentCardMetadata(input);
  const message = `${requester.displayName} created ${input.title || 'a document'}`;
  try {
    const history = await readHistory({ channel, identity: requester });
    const own = history.find((m) => typeof m.id === 'string' && isDocumentCardFor(m, input.documentId));
    if (own?.id) return { outcome: 'already', messageId: own.id };
    const card = findRunCard(history, input.runIds);
    // A card that is already ANOTHER document's (a run with two pages) keeps
    // it: the second page gets its own card below.
    if (card?.id && card.metadata?.kind === 'agent-run') {
      const author = card.userId === requester.userId ? requester : card.userId === PIPELINE_IDENTITY.userId ? PIPELINE_IDENTITY : null;
      if (author) {
        await edit({ channel, messageId: card.id, message, metadata, identity: author });
        // The service does not ack an edit; read it back so "promoted" is a fact.
        const after = (await readHistory({ channel, identity: requester })).find((m) => m.id === card.id);
        if (after?.metadata?.kind === 'document' && after.metadata.documentId === input.documentId) {
          return { outcome: 'promoted', messageId: card.id };
        }
        log.warn({ channel, messageId: card.id, documentId: input.documentId }, 'run card edit did not land; posting the document card instead');
      }
    }
    // No card to turn (or it could not be turned): the document card on its own
    // — unless the caller is repairing an old run, which never adds a card.
    if (input.postIfNoCard === false) return { outcome: 'skipped' };
    await post({ channel, message, metadata, ...(isMemberAddressedDm(channel) ? { identity: requester } : {}) });
    return { outcome: 'posted' };
  } catch (e) {
    const error = describeError(e);
    log.error({ err: error, channel, documentId: input.documentId }, 'could not put the run\'s document into the conversation');
    return { outcome: 'failed', error };
  }
}

export const str = (v: unknown): string | undefined => (typeof v === 'string' && v ? v : undefined);
