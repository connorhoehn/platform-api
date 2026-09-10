import { decideRecording, callCardRecordingFields, isRecordingReason, dmLobbyMembers } from '../recordingDecision';

const profiles = (flags: Record<string, boolean | undefined>) =>
  async (id: string) => (id in flags ? { recordCalls: flags[id] } : null);

describe('decideRecording', () => {
  it('records a DM only with everyone opted in', async () => {
    await expect(decideRecording({ lobbyName: 'dm:a:b', requested: null, callerId: 'a', getProfile: profiles({ a: true, b: true }) }))
      .resolves.toEqual({ enabled: true, reason: 'dm-both-opted-in' });
    await expect(decideRecording({ lobbyName: 'dm:a:b:c', requested: null, callerId: 'a', getProfile: profiles({ a: true, b: true }) }))
      .resolves.toEqual({ enabled: false, reason: 'dm-not-opted-in:c' });
    await expect(decideRecording({ lobbyName: 'dm:a:b:c', requested: null, callerId: 'a', getProfile: profiles({}) }))
      .resolves.toEqual({ enabled: false, reason: 'dm-not-opted-in:a,b,c' });
  });

  it('honours an explicit off even when everyone opted in', async () => {
    await expect(decideRecording({ lobbyName: 'dm:a:b', requested: { enabled: false }, callerId: 'a', getProfile: profiles({ a: true, b: true }) }))
      .resolves.toEqual({ enabled: false, reason: 'dm-not-opted-in:a' });
  });

  it('survives a profile lookup that throws', async () => {
    await expect(decideRecording({ lobbyName: 'dm:a:b', requested: null, callerId: 'a', getProfile: async () => { throw new Error('ddb down'); } }))
      .resolves.toEqual({ enabled: false, reason: 'dm-not-opted-in:a,b' });
  });

  it('takes a room at its word, reason included', async () => {
    const getProfile = jest.fn();
    await expect(decideRecording({ lobbyName: 'room:x', requested: { enabled: true, reason: 'room-setting-on' }, callerId: 'a', getProfile }))
      .resolves.toEqual({ enabled: true, reason: 'room-setting-on' });
    await expect(decideRecording({ lobbyName: 'room:x', requested: { enabled: false }, callerId: 'a', getProfile }))
      .resolves.toEqual({ enabled: false, reason: 'room-setting-off' });
    expect(getProfile).not.toHaveBeenCalled();
  });

  it('ignores a reason it does not know', async () => {
    await expect(decideRecording({ lobbyName: 'room:x', requested: { enabled: true, reason: 'because' }, callerId: 'a', getProfile: profiles({}) }))
      .resolves.toEqual({ enabled: true, reason: 'default-on' });
  });

  it('defaults everything else on', async () => {
    await expect(decideRecording({ lobbyName: '6ea7c8fd-1234-4abc-8def-0123456789ab', requested: null, callerId: 'a', getProfile: profiles({}) }))
      .resolves.toEqual({ enabled: true, reason: 'default-on' });
  });
});

describe('helpers', () => {
  it('names dm members and nobody else', () => {
    expect(dmLobbyMembers('dm:a:b')).toEqual(['a', 'b']);
    expect(dmLobbyMembers('dmg:abc')).toBeNull();
    expect(dmLobbyMembers('dm:a')).toBeNull();
  });
  it('knows the vocabulary, parameterised form included', () => {
    expect(isRecordingReason('room-setting-off')).toBe(true);
    expect(isRecordingReason('dm-not-opted-in:a,b')).toBe(true);
    expect(isRecordingReason('nope')).toBe(false);
  });
  it('shapes the card fields as a tri-state', () => {
    expect(callCardRecordingFields(null)).toEqual({});
    expect(callCardRecordingFields({ enabled: true, reason: 'default-on' })).toEqual({ recorded: true });
    expect(callCardRecordingFields({ enabled: false, reason: 'room-setting-off' })).toEqual({ recorded: false, notRecordedReason: 'room-setting-off' });
  });
});
