import { callRecordingRepo } from '../repositories';

/** The native executor has exhausted summary retries. No transcript is rewritten. */
export async function settleRecordingSummaryFailure(input: {
  runId: string;
  context: unknown;
  error: string;
}): Promise<boolean> {
  if (!input.context || typeof input.context !== 'object') return false;
  const ctx = input.context as { callId?: unknown; recordingId?: unknown };
  if (typeof ctx.callId !== 'string' || !ctx.callId || typeof ctx.recordingId !== 'string' || !ctx.recordingId) return false;
  return callRecordingRepo.failPendingSummary(ctx.callId, ctx.recordingId, input.runId, input.error);
}
