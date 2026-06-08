/**
 * Subscribes to `transcription:transcript.ready` (published by the
 * transcription-worker container after it writes transcript.json) and
 * generates a markdown meeting-notes summary via the existing LLM
 * abstraction.
 *
 * Flow per delivery:
 *   1. Receive {recordingId, callId, transcriptKey, transcriptUrl, ...}.
 *   2. Find the recording row (need callId for the DDB UpdateCommand).
 *   3. Flip transcriptStatus → 'processing'.
 *   4. Fetch the transcript JSON via LVS's segments endpoint (proxied
 *      blob fetch).
 *   5. Render the segments into prompt-friendly speaker-tagged text.
 *   6. Call createLLMClient().stream() with the COE-template-style
 *      system prompt + speaker-tagged transcript user prompt.
 *   7. Persist:
 *        transcriptStatus → 'ready'
 *        transcriptKey    → from payload
 *        summaryMarkdown  → LLM response (inline in DDB; <400 KB cap)
 *        transcriptGeneratedAt → ISO now
 *   8. Done — no further events emitted; the FE polls
 *      GET /api/recordings/:id/transcript and /summary to surface state.
 *
 * Failure policy: re-throws so the durable transport sees the failure
 * and redelivers. After 3 attempts the MQ redrive policy moves the
 * message to DLQ; we mark status='failed' before the throw so the
 * FE knows to render an error chip instead of an indefinite spinner.
 */

import type { EventEnvelope, Subscription } from '@connorhoehn/event-catalog';
import { catalog } from '../lib/event-catalog';
import { callRecordingRepo } from '../repositories';
import { createLLMClient } from '../pipeline/createLLMClient';
import { callSfu } from '../lib/sfu-breaker';
import { VIDEO_API_URL } from '../lib/video-auth';
import log from '../lib/logger';

interface TranscriptReadyPayload {
  recordingId: string;
  callId: string;
  transcriptKey: string;
  transcriptUrl?: string;
  durationMs: number;
  speakerCount: number;
  languages: string[];
}

interface TranscriptSegment {
  startMs: number;
  endMs: number;
  speakerId: string;
  speakerLabel: string;
  text: string;
}

interface TranscriptDoc {
  recordingId: string;
  callId: string;
  channelArn: string;
  durationMs: number;
  speakerCount: number;
  languages: string[];
  segments: TranscriptSegment[];
}

const SUMMARY_SYSTEM_PROMPT = `You are an organizational knowledge analyst. \
You will be given a multi-speaker meeting transcript with speaker labels \
and timestamps. Produce a concise, well-structured markdown summary suitable \
for participants reviewing the call later.

Output STRICTLY this markdown structure (omit a section only if its content \
would be empty after a genuine scan of the transcript):

## Summary
A 2-4 sentence overview of what the meeting was about and the outcome.

## Topics Discussed
- Bullet list of distinct topics, each with a one-line summary.

## Key Decisions
- Bullet list of decisions reached during the call. Quote the deciding \
  speaker by their label when the decision is attributable.

## Action Items
| Owner | Action | Deadline |
|-------|--------|----------|
| Name  | What they committed to do | When (or "TBD") |

## Open Questions
- Anything raised but not resolved. Cite the asker by speaker label.

## Speaking Time
- speakerLabel: approximate minutes spoken (rounded). Skip speakers with \
  <5% of total talk time.

Rules:
- Use the speakerLabel strings verbatim — do not rename participants.
- Quote sparingly. Paraphrase technical content.
- If a section has no content, omit the section heading entirely.
- Do not add a preamble or signature. Start directly with "## Summary".`;

function formatTranscriptForPrompt(doc: TranscriptDoc): string {
  const totalMin = Math.round(doc.durationMs / 60000);
  const header =
    `<recording duration="${totalMin} min" speakers="${doc.speakerCount}" ` +
    `languages="${(doc.languages || []).join(',') || 'en'}">\n`;
  const lines: string[] = [header];
  for (const s of doc.segments) {
    const min = Math.floor(s.startMs / 60000);
    const sec = Math.floor((s.startMs % 60000) / 1000);
    const ts = `${String(min).padStart(2, '0')}:${String(sec).padStart(2, '0')}`;
    lines.push(`[${ts}] ${s.speakerLabel}: ${s.text}`);
  }
  lines.push('</recording>');
  return lines.join('\n');
}

async function fetchTranscript(channelArn: string, recordingId: string): Promise<TranscriptDoc> {
  // Same URL shape the worker used to write the file — LVS resolves
  // /api/channels/<arn>/recordings/<id>/segments/<file> against the
  // recording dir, which is where the worker wrote transcript.json.
  const url =
    `${VIDEO_API_URL}/api/channels/${encodeURIComponent(channelArn)}` +
    `/recordings/${encodeURIComponent(recordingId)}/segments/transcript.json`;
  const r = await callSfu(url, {
    method: 'GET',
    headers: { Authorization: `Bearer ${process.env.LVS_ADMIN_TOKEN ?? 'admin'}` },
  });
  if (!r.ok) {
    throw new Error(`transcript fetch ${url} → ${r.status}`);
  }
  return (await r.json()) as TranscriptDoc;
}

async function generateSummary(transcriptText: string): Promise<string> {
  const client = createLLMClient();
  // claude-sonnet-4-6 default per the existing pipeline conventions.
  // maxTokens 4096 — typical summary output is 1-3 KB markdown but
  // long meetings may need more; 4096 is the COE template precedent.
  let response = '';
  for await (const chunk of client.stream(
    'claude-sonnet-4-6',
    SUMMARY_SYSTEM_PROMPT,
    transcriptText,
    { maxTokens: 4096 },
  )) {
    if (chunk.done) {
      response = chunk.response;
      break;
    }
    // We don't surface intermediate tokens — the consumer is async, not
    // user-facing real-time. Collecting the full response on done is
    // simpler than accumulating tokens.
  }
  if (!response.trim()) {
    throw new Error('LLM returned empty response');
  }
  return response.trim();
}

export interface StartSummaryConsumerOptions {
  _catalog?: { subscribe: typeof catalog.subscribe };
}

export function startTranscriptionSummaryConsumer(
  opts: StartSummaryConsumerOptions = {},
): { stop: () => Promise<void> } {
  const cat = opts._catalog ?? catalog;

  const subscription: Subscription = cat.subscribe<TranscriptReadyPayload>(
    'transcription:transcript.ready',
    async (envelope: EventEnvelope<TranscriptReadyPayload>) => {
      const { recordingId, callId, transcriptKey } = envelope.payload;
      log.info({ recordingId, callId }, '[transcription-summary] received transcript.ready');

      // Need channelArn for the LVS fetch URL — look up the row first.
      const detail = await callRecordingRepo.getRecording(callId, recordingId);
      if (!detail) {
        // The recording row doesn't exist. Could be a race where
        // recording.completed lost to a TTL sweep; nothing we can do
        // here. Throw to redeliver — on the off chance it's a transient
        // read failure, redelivery covers it. After DLQ the operator
        // can investigate.
        throw new Error(`recording ${callId}/${recordingId} not found`);
      }
      const channelArn = detail.channelArn;

      try {
        await callRecordingRepo.updateTranscriptStatus(callId, recordingId, {
          status: 'processing',
          transcriptKey,
        });

        const doc = await fetchTranscript(channelArn, recordingId);
        if (!doc.segments || doc.segments.length === 0) {
          // No-audible-legs case from the worker. Mark ready with an
          // empty summary rather than failing — the FE renders a
          // "no transcript available" badge.
          await callRecordingRepo.updateTranscriptStatus(callId, recordingId, {
            status: 'ready',
            transcriptKey,
            summaryMarkdown: '_No audible speech was captured in this recording._',
            transcriptGeneratedAt: new Date().toISOString(),
          });
          log.info({ recordingId }, '[transcription-summary] no segments — marked ready with placeholder');
          return;
        }

        const promptText = formatTranscriptForPrompt(doc);
        const summary = await generateSummary(promptText);

        await callRecordingRepo.updateTranscriptStatus(callId, recordingId, {
          status: 'ready',
          transcriptKey,
          summaryMarkdown: summary,
          transcriptGeneratedAt: new Date().toISOString(),
        });

        log.info(
          {
            recordingId,
            segmentCount: doc.segments.length,
            speakerCount: doc.speakerCount,
            summaryChars: summary.length,
          },
          '[transcription-summary] ready',
        );
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        log.error(
          { err: message, recordingId, callId },
          '[transcription-summary] failed; will redeliver',
        );
        // Mark failed only after redelivery budget exhausted — but the
        // catalog envelope doesn't expose dlq-imminent reliably. For
        // MVP, flip to failed every time and let success overwrite it
        // on a redelivery that does succeed.
        try {
          await callRecordingRepo.updateTranscriptStatus(callId, recordingId, {
            status: 'failed',
            transcriptError: message,
          });
        } catch {
          // Swallow secondary failure — the throw below is the
          // load-bearing signal to MQ.
        }
        throw err;
      }
    },
  );

  return {
    async stop() {
      try {
        await subscription.unsubscribe();
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        log.warn({ err: message }, '[transcription-summary-consumer] unsubscribe failed');
      }
    },
  };
}
