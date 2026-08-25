// Unit tests for mergeReactionsIntoTranscript — pure transcript+reaction
// merge used by the transcript-serving route and the summary consumer.

import { mergeReactionsIntoTranscript } from '../transcript-reactions';

const STARTED_AT = '2026-08-25T10:00:00.000Z';

function seg(startMs: number, text: string, speakerLabel = 'Alice') {
  return {
    startMs,
    endMs: startMs + 5000,
    speakerId: 'u-alice',
    speakerLabel,
    text,
  };
}

describe('mergeReactionsIntoTranscript', () => {
  it('injects reactions at the right offset, sorted into the segment stream', () => {
    const doc = {
      recordingId: 'rec-1',
      segments: [seg(0, 'hello'), seg(60000, 'one minute in'), seg(120000, 'two minutes in')],
    };
    const reactions = [
      // 90s after recording start → lands between segments 2 and 3
      { userId: 'u-connor', displayName: 'Connor', emoji: '👍', at: '2026-08-25T10:01:30.000Z' },
    ];

    const merged = mergeReactionsIntoTranscript(doc, reactions, STARTED_AT);

    expect(merged.segments).toHaveLength(4);
    expect(merged.segments.map((s) => s.startMs)).toEqual([0, 60000, 90000, 120000]);
    const reaction = merged.segments[2];
    expect(reaction).toEqual({
      startMs: 90000,
      endMs: 91000,
      speakerId: 'u-connor',
      speakerLabel: 'Connor',
      text: 'reacted with 👍',
      kind: 'reaction',
    });
    // Original doc untouched (new doc returned).
    expect(doc.segments).toHaveLength(3);
    expect(merged.recordingId).toBe('rec-1');
  });

  it('clamps reactions stamped before recording start to offset 0', () => {
    const doc = { segments: [seg(0, 'hello')] };
    const reactions = [
      { userId: 'u-1', displayName: 'Early Bird', emoji: '🎉', at: '2026-08-25T09:59:58.000Z' },
    ];

    const merged = mergeReactionsIntoTranscript(doc, reactions, STARTED_AT);

    const reaction = merged.segments.find((s) => s.kind === 'reaction');
    expect(reaction).toBeDefined();
    expect(reaction!.startMs).toBe(0);
    expect(reaction!.endMs).toBe(1000);
  });

  it('returns identical segments when there are no reactions', () => {
    const doc = { segments: [seg(0, 'a'), seg(30000, 'b'), seg(60000, 'c')] };

    const merged = mergeReactionsIntoTranscript(doc, [], STARTED_AT);

    expect(merged.segments).toEqual(doc.segments);
    expect(merged.segments).not.toBe(doc.segments); // still a new array
  });

  it('marks injected segments with kind: "reaction" and leaves speech segments unflagged', () => {
    const doc = { segments: [seg(0, 'speech')] };
    const merged = mergeReactionsIntoTranscript(
      doc,
      [{ userId: 'u-2', emoji: '🔥', at: '2026-08-25T10:00:05.000Z' }],
      STARTED_AT,
    );

    const kinds = merged.segments.map((s) => s.kind);
    expect(kinds).toEqual([undefined, 'reaction']);
  });

  it('falls back to userId then "Someone" for the speaker label, and "reaction" for the speaker id', () => {
    const doc = { segments: [] as ReturnType<typeof seg>[] };
    const merged = mergeReactionsIntoTranscript(
      doc,
      [
        { userId: 'u-9', emoji: '👏', at: '2026-08-25T10:00:01.000Z' }, // no displayName
        { userId: null, emoji: '👀', at: '2026-08-25T10:00:02.000Z' }, // anonymous
      ],
      STARTED_AT,
    );

    expect(merged.segments[0].speakerLabel).toBe('u-9');
    expect(merged.segments[0].speakerId).toBe('u-9');
    expect(merged.segments[1].speakerLabel).toBe('Someone');
    expect(merged.segments[1].speakerId).toBe('reaction');
  });

  it('skips reactions with an unparseable timestamp', () => {
    const doc = { segments: [seg(0, 'speech')] };
    const merged = mergeReactionsIntoTranscript(
      doc,
      [{ userId: 'u-1', emoji: '💥', at: 'not-a-date' }],
      STARTED_AT,
    );
    expect(merged.segments).toHaveLength(1);
    expect(merged.segments[0].text).toBe('speech');
  });

  it('is stable: a reaction at the same startMs as a speech segment sorts after it', () => {
    const doc = { segments: [seg(60000, 'same instant')] };
    const merged = mergeReactionsIntoTranscript(
      doc,
      [{ userId: 'u-1', displayName: 'C', emoji: '👍', at: '2026-08-25T10:01:00.000Z' }],
      STARTED_AT,
    );
    expect(merged.segments.map((s) => s.kind)).toEqual([undefined, 'reaction']);
  });
});
