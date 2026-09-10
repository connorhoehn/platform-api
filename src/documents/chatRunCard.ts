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
}

/** Post the run's card into the conversation. Never fatal: a missing channel or a gateway hiccup is reported, not thrown. */
export async function postRunCardToChannel(input: RunCardInput): Promise<boolean> {
  if (!input.channel) return false;
  try {
    await postChatMessage({
      channel: input.channel,
      message: `${input.command} ${input.instruction}`,
      metadata: {
        kind: 'agent-run', runId: input.runId, pipelineId: input.pipelineId, command: input.command, instruction: input.instruction,
        agentName: PIPELINE_IDENTITY.displayName, agentUserId: PIPELINE_IDENTITY.userId,
        ...(input.extra ?? {}),
      },
    });
    return true;
  } catch (e) {
    log.warn({ err: e instanceof Error ? e.message : String(e), channel: input.channel, runId: input.runId }, 'could not post the run card into the conversation');
    return false;
  }
}

export const str = (v: unknown): string | undefined => (typeof v === 'string' && v ? v : undefined);
