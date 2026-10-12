// Native processing events are wake-ups only. Every response re-reads the
// participant-authorized recording; event payloads never reach the browser.
import { createHash } from 'crypto';
import type { CallRecording } from '../repositories/CallRecordingRepository';

export interface RecordingEvent { payload?: unknown }
export interface RecordingEventSource {
  subscribeAll(listener: (event: RecordingEvent) => Promise<void>): string;
  unsubscribe(id: string): void;
}
export function recordingStatus(row: CallRecording) {
  const status = {
    recordingId: row.recordingId, callId: row.callId, lobbyName: row.lobbyName,
    composeStatus: row.composeStatus ?? null, transcriptStatus: row.transcriptStatus ?? null,
    summaryStatus: row.summaryStatus ?? null, processingAttemptId: row.processingAttemptId ?? null,
    summaryRunId: row.summaryRunId ?? null, transcriptionRequested: row.transcriptionRequested ?? false,
  };
  return { ...status, revision: createHash('sha256').update(JSON.stringify(status)).digest('hex') };
}
export function matchesRecordingEvent(event: RecordingEvent, row: CallRecording): boolean {
  const p = event.payload;
  if (!p || typeof p !== 'object') return false;
  const payload = p as Record<string, unknown>;
  if (row.summaryRunId && payload.runId === row.summaryRunId) return true;
  return [payload, payload.context, payload.output, payload.triggerPayload].some(value => {
    if (!value || typeof value !== 'object') return false;
    const identity = value as Record<string, unknown>;
    return identity.recordingId === row.recordingId && identity.callId === row.callId;
  });
}

/** Subscribe before the second read to close the read/subscribe race. A
 * bounded resync also covers lost events and unavailable native bus transport. */
export async function waitForRecordingChange(options: {
  source: RecordingEventSource | null; row: CallRecording; revision: string;
  read: () => Promise<CallRecording>; signal: AbortSignal; timeoutMs?: number;
}): Promise<void> {
  const { source, row, revision, read, signal } = options;
  if (signal.aborted) return;
  let unsubscribe: (() => void) | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let finish: () => void = () => {};
  const ready = new Promise<void>(resolve => { finish = resolve; });
  signal.addEventListener('abort', finish, { once: true });
  try {
    if (source) {
      const id = source.subscribeAll(async event => { if (matchesRecordingEvent(event, row)) finish(); });
      unsubscribe = () => source.unsubscribe(id);
    }
    timer = setTimeout(finish, options.timeoutMs ?? 25_000);
    if (signal.aborted || recordingStatus(await read()).revision !== revision) finish();
    await ready;
  } finally {
    clearTimeout(timer); signal.removeEventListener('abort', finish); unsubscribe?.();
  }
}
