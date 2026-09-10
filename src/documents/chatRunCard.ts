// The card a chat command leaves behind.
//
// A slash command in a conversation (/doc, /summarize, /diagram) becomes a
// pipeline run, and the conversation keeps a CARD for it — a chat message
// whose metadata names the run, so the thread shows the run moving and, when
// there is a page behind it, opens that page. Recording pipelines post the
// same card from their lobby; this is the helper for a run that already
// knows its channel.
import { postChatMessage, PIPELINE_IDENTITY } from '../gateway/GatewayServiceClient';
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
 * Post the run's card into the conversation. Never fatal: a missing channel or
 * a gateway hiccup is reported, not thrown. Skipped (false) when the run's
 * context says the client posted the card — see `RunCardInput.context`.
 */
export async function postRunCardToChannel(input: RunCardInput): Promise<boolean> {
  if (!input.channel) return false;
  if (clientPostsCard(input.context)) {
    log.debug({ channel: input.channel, runId: input.runId }, 'run card left to the client that started the run');
    return false;
  }
  try {
    await postChatMessage({
      channel: input.channel,
      message: `${input.command} ${input.instruction}`,
      metadata: runCardMetadata(input),
    });
    return true;
  } catch (e) {
    log.warn({ err: e instanceof Error ? e.message : String(e), channel: input.channel, runId: input.runId }, 'could not post the run card into the conversation');
    return false;
  }
}

export const str = (v: unknown): string | undefined => (typeof v === 'string' && v ? v : undefined);
