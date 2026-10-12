import type { CallRecording } from '../../repositories/CallRecordingRepository';
import { recordingStatus, waitForRecordingChange, type RecordingEvent, type RecordingEventSource } from '../recordingStatus';
const row = { callId: 'call-one', recordingId: 'recording-one', lobbyName: 'orgiq:initiative:one',
  composeStatus: 'ok', transcriptStatus: 'processing', summaryRunId: 'run-one', processingAttemptId: 'generation-one' } as CallRecording;
function source() {
  const listeners = new Map<string, (event: RecordingEvent) => Promise<void>>();
  const bus: RecordingEventSource = { subscribeAll: listener => { listeners.set('one', listener); return 'one'; }, unsubscribe: id => { listeners.delete(id); } };
  return { bus, listeners, emit: async (payload: unknown) => { for (const listener of listeners.values()) await listener({ payload }); } };
}
it('projects only processing identity and states, never private content', () => {
  const status = recordingStatus({ ...row, summaryMarkdown: 'private summary', transcriptJson: 'private transcript', participants: [{ userId: 'private-person' }] } as CallRecording);
  expect(JSON.stringify(status)).not.toContain('private');
  expect(status.revision).toMatch(/^[a-f0-9]{64}$/);
  expect(recordingStatus({ ...row, summaryStatus: 'ready' }).revision).not.toBe(status.revision);
});
it('wakes on the owning native run, ignores other recordings and unsubscribes', async () => {
  const h = source(); let settled = false;
  const pending = waitForRecordingChange({ source: h.bus, row, revision: recordingStatus(row).revision,
    read: async () => row, signal: new AbortController().signal, timeoutMs: 1000 }).then(() => { settled = true; });
  await h.emit({ runId: 'other', output: { recordingId: 'other', callId: row.callId } });
  expect(settled).toBe(false);
  await h.emit({ runId: row.summaryRunId }); await pending;
  expect(h.listeners.size).toBe(0);
});
it('recognizes a new generation from its exact call and recording output', async () => {
  const h = source();
  const pending = waitForRecordingChange({ source: h.bus, row, revision: recordingStatus(row).revision,
    read: async () => row, signal: new AbortController().signal });
  await h.emit({ runId: 'new', context: { recordingId: row.recordingId, callId: row.callId, processingAttemptId: 'new' } });
  await pending; expect(h.listeners.size).toBe(0);
});
it('closes the subscribe/read race without waiting for another event', async () => {
  const h = source();
  await waitForRecordingChange({ source: h.bus, row, revision: recordingStatus(row).revision,
    read: async () => ({ ...row, transcriptStatus: 'ready' }), signal: new AbortController().signal });
  expect(h.listeners.size).toBe(0);
});
it('releases timers and subscription on disconnect, read failure and bounded resync', async () => {
  const h = source(), abort = new AbortController();
  const pending = waitForRecordingChange({ source: h.bus, row, revision: recordingStatus(row).revision, read: async () => row, signal: abort.signal });
  abort.abort(); await pending; expect(h.listeners.size).toBe(0);
  await expect(waitForRecordingChange({ source: h.bus, row, revision: recordingStatus(row).revision,
    read: async () => { throw new Error('revoked'); }, signal: new AbortController().signal })).rejects.toThrow('revoked');
  expect(h.listeners.size).toBe(0);
  await waitForRecordingChange({ source: h.bus, row, revision: recordingStatus(row).revision, read: async () => row, signal: new AbortController().signal, timeoutMs: 5 });
  expect(h.listeners.size).toBe(0);
});

it.each(['abort', 'timeout'])('releases a subscription on %s even when the recheck is stalled', async reason => {
  const h = source(), abort = new AbortController();
  let rejectRead!: (error: Error) => void;
  const stalled = new Promise<CallRecording>((_resolve, reject) => { rejectRead = reject; });
  const pending = waitForRecordingChange({ source: h.bus, row, revision: recordingStatus(row).revision,
    read: () => stalled, signal: abort.signal, timeoutMs: 5 });
  if (reason === 'abort') abort.abort();
  await pending;
  expect(h.listeners.size).toBe(0);
  // A late dependency rejection must still have an attached handler.
  rejectRead(new Error('late read failure'));
  await Promise.resolve();
});
