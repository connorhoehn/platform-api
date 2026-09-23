// The card a chat command leaves behind.
//
// A slash command in a conversation (/doc, /summarize, /diagram) becomes a
// pipeline run, and the conversation keeps a CARD for it — a chat message
// whose metadata names the run, so the thread shows the run moving and, when
// there is a page behind it, opens that page. Recording pipelines post the
// same card from their lobby; this is the helper for a run that already
// knows its channel.
import { postChatMessage, PIPELINE_IDENTITY, ChatRefusedError, describeError, type ServiceIdentity } from '../gateway/GatewayServiceClient';
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

export const str = (v: unknown): string | undefined => (typeof v === 'string' && v ? v : undefined);
