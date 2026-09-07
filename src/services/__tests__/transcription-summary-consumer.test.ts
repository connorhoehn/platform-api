// A failed SUMMARY must not report a failed TRANSCRIPT.
//
// The consumer does two jobs in one handler: fetch a transcript the worker
// has already written, then ask an LLM to summarise it. Those failed into
// one field. So a summary call that died for want of credentials wrote
// `transcriptStatus: 'failed'` onto a row whose `transcriptKey` the very
// same handler had stamped seconds earlier — and the UI reported the
// transcription as failed for a transcript that served 200 the moment the
// status was flipped by hand. Observed live: transcribed in 1.4s,
// summarised never, reported as a transcription failure.
//
// These tests pin the split: anything up to and including the transcript
// fetch is a transcript failure; everything after it is a summary failure
// and leaves the transcript ready with its key intact.

import { describe, it, expect, jest, beforeEach } from '@jest/globals';
import type { EventEnvelope } from '@connorhoehn/event-catalog';

// --- mocks ------------------------------------------------------------------

const mockGetRecording = jest.fn();
const mockUpdateTranscriptStatus = jest.fn();
const mockListReactions = jest.fn();
const mockCallSfu = jest.fn();
const mockStream = jest.fn();

jest.mock('../../repositories', () => ({
  callRecordingRepo: {
    getRecording: (...a: unknown[]) => mockGetRecording(...a),
    updateTranscriptStatus: (...a: unknown[]) => mockUpdateTranscriptStatus(...a),
  },
  callReactionRepo: {
    listByLobby: (...a: unknown[]) => mockListReactions(...a),
  },
}));

jest.mock('../../lib/event-catalog', () => ({
  catalog: { subscribe: jest.fn(), publish: jest.fn(async () => undefined) },
}));

jest.mock('../../lib/sfu-breaker', () => ({
  callSfu: (...a: unknown[]) => mockCallSfu(...a),
}));

jest.mock('../../lib/video-auth', () => ({ VIDEO_API_URL: 'https://video.test' }));

jest.mock('../../pipeline/createLLMClient', () => ({
  createLLMClient: () => ({
    provider: 'test',
    stream: (...a: unknown[]) => mockStream(...a),
  }),
}));

import { startTranscriptionSummaryConsumer } from '../transcription-summary-consumer';

// --- helpers ----------------------------------------------------------------

type Handler = (e: EventEnvelope<Record<string, unknown>>) => Promise<void>;

/** Start the consumer and hand back the handler it registered. */
function handlerUnderTest(): Handler {
  let captured: Handler | undefined;
  const cat = {
    subscribe: ((_type: string, fn: Handler) => {
      captured = fn;
      return { unsubscribe: async () => undefined };
    }) as never,
    publish: (async () => undefined) as never,
  };
  startTranscriptionSummaryConsumer({
    _catalog: cat,
    _dispatch: (async () => ({ fired: [], failed: [] })) as never,
  });
  if (!captured) throw new Error('consumer registered no handler');
  return captured;
}

const envelope = {
  payload: {
    recordingId: 'rec-1',
    callId: 'call-1',
    transcriptKey: 'arn_1/rec-1/transcript.json',
    durationMs: 60_000,
    speakerCount: 2,
    languages: ['en'],
  },
} as unknown as EventEnvelope<Record<string, unknown>>;

const ROW = {
  callId: 'call-1',
  recordingId: 'rec-1',
  channelArn: 'arn:1',
  lobbyName: 'dm:a:b',
  orgId: 'org-1',
  startedAt: '2026-09-06T00:00:00.000Z',
  endedAt: '2026-09-06T00:01:00.000Z',
};

const TRANSCRIPT = {
  recordingId: 'rec-1',
  callId: 'call-1',
  channelArn: 'arn:1',
  durationMs: 60_000,
  speakerCount: 1,
  languages: ['en'],
  segments: [{ startMs: 0, endMs: 1_000, speakerId: 's1', speakerLabel: 'Bob', text: 'hello' }],
};

/** The last patch handed to updateTranscriptStatus. */
function lastPatch(): Record<string, unknown> {
  const calls = mockUpdateTranscriptStatus.mock.calls;
  return calls[calls.length - 1]![2] as Record<string, unknown>;
}

beforeEach(() => {
  mockGetRecording.mockReset().mockResolvedValue(ROW as never);
  mockUpdateTranscriptStatus.mockReset().mockResolvedValue(undefined as never);
  mockListReactions.mockReset().mockResolvedValue([] as never);
  mockCallSfu.mockReset();
  mockStream.mockReset();
});

// --- tests ------------------------------------------------------------------

describe('summary failure vs transcript failure', () => {
  it('an LLM failure leaves the transcript ready and fails only the summary', async () => {
    mockCallSfu.mockResolvedValue({ ok: true, status: 200, json: async () => TRANSCRIPT } as never);
    mockStream.mockImplementation(() => {
      throw new Error('ANTHROPIC_API_KEY not set');
    });

    await expect(handlerUnderTest()(envelope)).rejects.toThrow('ANTHROPIC_API_KEY');

    const patch = lastPatch();
    // The transcript exists — we fetched and parsed it in this very
    // handler — so this is what the row must say.
    expect(patch.status).toBe('ready');
    expect(patch.transcriptKey).toBe('arn_1/rec-1/transcript.json');
    expect(patch.summaryStatus).toBe('failed');
    expect(String(patch.summaryError)).toContain('ANTHROPIC_API_KEY');
    // And crucially, nothing wrote a transcript error.
    expect(patch.transcriptError).toBeUndefined();
  });

  it('an empty LLM response is a summary failure, not a transcript failure', async () => {
    mockCallSfu.mockResolvedValue({ ok: true, status: 200, json: async () => TRANSCRIPT } as never);
    mockStream.mockImplementation(async function* () {
      yield { done: true, response: '   ' };
    } as never);

    await expect(handlerUnderTest()(envelope)).rejects.toThrow();
    const patch = lastPatch();
    expect(patch.status).toBe('ready');
    expect(patch.summaryStatus).toBe('failed');
  });

  it('a transcript fetch failure IS a transcript failure', async () => {
    mockCallSfu.mockResolvedValue({ ok: false, status: 404, json: async () => ({}) } as never);

    await expect(handlerUnderTest()(envelope)).rejects.toThrow('404');

    const patch = lastPatch();
    expect(patch.status).toBe('failed');
    expect(String(patch.transcriptError)).toContain('404');
    // No summary was attempted, so no summary verdict is invented.
    expect(patch.summaryStatus).toBeUndefined();
  });

  it('the happy path settles both axes and clears any earlier error', async () => {
    mockCallSfu.mockResolvedValue({ ok: true, status: 200, json: async () => TRANSCRIPT } as never);
    mockStream.mockImplementation(async function* () {
      yield { done: true, response: '## Summary\nThey said hello.' };
    } as never);

    await handlerUnderTest()(envelope);

    const patch = lastPatch();
    expect(patch.status).toBe('ready');
    expect(patch.summaryStatus).toBe('ready');
    expect(patch.summaryMarkdown).toContain('They said hello.');
    // A redelivery that finally succeeds must not leave the previous
    // attempt's error next to a 'ready' status. `null` REMOVEs the field.
    expect(patch.summaryError).toBeNull();
    expect(patch.transcriptError).toBeNull();
  });

  it('a silent recording is ready on both axes, not failed on either', async () => {
    mockCallSfu.mockResolvedValue({
      ok: true, status: 200, json: async () => ({ ...TRANSCRIPT, segments: [] }),
    } as never);

    await handlerUnderTest()(envelope);

    const patch = lastPatch();
    expect(patch.status).toBe('ready');
    expect(patch.summaryStatus).toBe('ready');
    expect(String(patch.summaryMarkdown)).toContain('No audible speech');
    expect(mockStream).not.toHaveBeenCalled();
  });

  it('the first write marks the summary pending rather than leaving it absent', async () => {
    mockCallSfu.mockResolvedValue({ ok: true, status: 200, json: async () => TRANSCRIPT } as never);
    mockStream.mockImplementation(async function* () {
      yield { done: true, response: '## Summary\nok' };
    } as never);

    await handlerUnderTest()(envelope);

    const first = mockUpdateTranscriptStatus.mock.calls[0]![2] as Record<string, unknown>;
    expect(first.status).toBe('processing');
    expect(first.summaryStatus).toBe('pending');
  });
});
