import { isGovernedRecordingLobby, resolveRecordedCallBinding, type RecordedCallWindow } from '../recording-call-binding';
import type { VideoSessionRecord } from '../../repositories/VideoSessionStore';

const start = '2026-10-10T10:00:00.000Z', end = '2026-10-10T10:01:00.000Z';
const recording: RecordedCallWindow = { channelArn: 'channel-one', startedAt: start, endedAt: end,
  tracks: [{ participantId: 'opaque-seat-a', tokenParticipantId: 'opaque-seat-a', userId: 'actor-a' },
    { participantId: 'opaque-seat-b', tokenParticipantId: 'opaque-seat-b', userId: 'actor-b' }] };
function session(): VideoSessionRecord {
  const row: VideoSessionRecord = { documentId: 'orgiq:initiative:p-one', sessionId: 'media-one', channelArn: recording.channelArn,
    orgId: 'tenant-one', startedAt: start, endedAt: end, status: 'ended', startedBy: 'actor-a', streamKey: '', participants: [], participantHistory: {} };
  for (const [pid, uid, client] of [['opaque-seat-a', 'actor-a', 'client-a'], ['opaque-seat-b', 'actor-b', 'client-b']]) {
    row.participantHistory![pid] = { participantId: pid, userId: uid, clientId: client, displayName: uid,
      joinedAt: start, leftAt: end, callBinding: { callId: 'canonical-one', clientId: client, userId: uid, tenant: row.orgId!, epoch: 2,
        sessionId: row.sessionId, channelArn: row.channelArn, lobbyName: row.documentId, boundAt: start } };
  }
  return row;
}

it('binds the canonical adopted call after Leave from exact native recorded seats', () => {
  const row = session();
  expect(resolveRecordedCallBinding([row], recording)).toEqual({ status: 'bound', callId: 'canonical-one', session: row });
  expect(resolveRecordedCallBinding([row, structuredClone(row)], recording)).toMatchObject({ status: 'bound' });
});

it('does not select a newer lobby/session or an arbitrary supplied user prefix', () => {
  const actual = session(), future = session();
  future.sessionId = 'future'; future.startedAt = '2026-10-10T11:00:00Z'; future.endedAt = '2026-10-10T11:01:00Z';
  const older = session(); older.sessionId = 'previous'; older.endedAt = '2026-10-10T09:00:00Z';
  expect(resolveRecordedCallBinding([future, older, actual], recording)).toMatchObject({ status: 'bound', callId: 'canonical-one', session: { sessionId: 'media-one' } });
  const misleading = structuredClone(recording); misleading.tracks[0].participantId = 'actor-a-looking-unknown'; misleading.tracks[0].tokenParticipantId = 'actor-a-looking-unknown';
  expect(resolveRecordedCallBinding([actual], misleading)).toMatchObject({ status: 'unresolved' });
});

it.each([
  ['tenant', (r: VideoSessionRecord) => { r.participantHistory!['opaque-seat-a'].callBinding!.tenant = 'other-tenant'; }],
  ['session', (r: VideoSessionRecord) => { r.participantHistory!['opaque-seat-a'].callBinding!.sessionId = 'other-session'; }],
  ['channel', (r: VideoSessionRecord) => { r.participantHistory!['opaque-seat-a'].callBinding!.channelArn = 'other-channel'; }],
  ['lobby', (r: VideoSessionRecord) => { r.participantHistory!['opaque-seat-a'].callBinding!.lobbyName = 'orgiq:initiative:other'; }],
  ['user', (r: VideoSessionRecord) => { r.participantHistory!['opaque-seat-a'].callBinding!.userId = 'other-actor'; }],
  ['client', (r: VideoSessionRecord) => { r.participantHistory!['opaque-seat-a'].callBinding!.clientId = 'other-tab'; }],
  ['recorded seat key', (r: VideoSessionRecord) => { r.participantHistory!['opaque-seat-a'].participantId = 'other-seat'; }],
  ['negative epoch', (r: VideoSessionRecord) => { r.participantHistory!['opaque-seat-a'].callBinding!.epoch = -1; }],
  ['fractional epoch', (r: VideoSessionRecord) => { r.participantHistory!['opaque-seat-a'].callBinding!.epoch = 1.2; }],
  ['actor epoch', (r: VideoSessionRecord) => { r.participantHistory!['opaque-seat-a'].callBinding!.actor = { sub: 'admin', epoch: -1 }; }],
  ['missing binding', (r: VideoSessionRecord) => { delete r.participantHistory!['opaque-seat-a'].callBinding; }],
  ['missing client', (r: VideoSessionRecord) => { delete r.participantHistory!['opaque-seat-a'].clientId; }],
  ['missing tenant', (r: VideoSessionRecord) => { delete r.orgId; }],
  ['invalid tenant', (r: VideoSessionRecord) => { r.orgId = 'tenant|other'; }],
  ['left before recording', (r: VideoSessionRecord) => { r.participantHistory!['opaque-seat-a'].leftAt = '2026-10-10T09:59:59Z'; }],
  ['joined after recording', (r: VideoSessionRecord) => { r.participantHistory!['opaque-seat-a'].joinedAt = '2026-10-10T10:02:00Z'; }],
  ['bound after recording', (r: VideoSessionRecord) => { r.participantHistory!['opaque-seat-a'].callBinding!.boundAt = '2026-10-10T10:02:00Z'; }],
  ['bound before join', (r: VideoSessionRecord) => { r.participantHistory!['opaque-seat-a'].callBinding!.boundAt = '2026-10-10T09:00:00Z'; }],
  ['bound after leaving', (r: VideoSessionRecord) => { r.participantHistory!['opaque-seat-a'].leftAt = start; r.participantHistory!['opaque-seat-a'].callBinding!.boundAt = '2026-10-10T10:00:30Z'; }],
] as const)('rejects inconsistent %s provenance', (_label, change) => {
  const row = session(); change(row);
  expect(resolveRecordedCallBinding([row], recording)).toMatchObject({ status: 'unresolved' });
});

it('requires trusted recorded userId to agree with exact attendance when present', () => {
  expect(resolveRecordedCallBinding([session()], { ...recording, tracks: [{ participantId: 'opaque-seat-a', tokenParticipantId: 'opaque-seat-a', userId: 'impostor' }] }))
    .toEqual({ status: 'unresolved', reason: 'unbound-recorded-participant' });
  expect(resolveRecordedCallBinding([session()], { ...recording, tracks: [{ participantId: 'opaque-seat-a', tokenParticipantId: 'opaque-seat-a' }] }))
    .toEqual({ status: 'unresolved', reason: 'unbound-recorded-participant' });
  expect(resolveRecordedCallBinding([session()], { ...recording, tracks: [{ participantId: 'opaque-seat-a', tokenParticipantId: 'opaque-seat-a', userId: '' }] }))
    .toEqual({ status: 'unresolved', reason: 'unbound-recorded-participant' });
});

it('never canonically associates an absent, null or fabricated signed-seat field from a same-user query pid', () => {
  for (const track of [
    { participantId: 'opaque-seat-a', userId: 'actor-a' },
    { participantId: 'opaque-seat-a', tokenParticipantId: null, userId: 'actor-a' },
    { participantId: 'opaque-seat-a', tokenParticipantId: 'another-seat', userId: 'actor-a' },
  ]) expect(resolveRecordedCallBinding([session()], { ...recording, tracks: [track] })).toMatchObject({ status: 'unresolved' });
});

it('leaves an unbound screen leg unresolved even when its signed user matches a camera', () => {
  expect(resolveRecordedCallBinding([session()], { ...recording, tracks: [...recording.tracks,
    { participantId: 'opaque-seat-a:screen', userId: 'actor-a', kind: 'video' }] }))
    .toEqual({ status: 'unresolved', reason: 'missing-recorded-participant' });
});

it('binds two cameras and a screen only through the native verified JWT parent seat', () => {
  const withScreen = { ...recording, tracks: [...recording.tracks,
    { participantId: 'opaque-screen-routing-source', tokenParticipantId: 'opaque-seat-a', userId: 'actor-a', kind: 'video' }] };
  expect(resolveRecordedCallBinding([session()], withScreen)).toMatchObject({ status: 'bound', callId: 'canonical-one' });
  expect(resolveRecordedCallBinding([session()], { ...withScreen, tracks: [...recording.tracks,
    { participantId: 'opaque-seat-a:screen', tokenParticipantId: 'opaque-seat-b', userId: 'actor-a' }] }))
    .toMatchObject({ status: 'unresolved' });
  expect(resolveRecordedCallBinding([session()], { ...withScreen, tracks: [...recording.tracks,
    { participantId: 'opaque-seat-a', tokenParticipantId: 'not-an-authenticated-seat', userId: 'actor-a' }] }))
    .toMatchObject({ status: 'unresolved' });
  expect(resolveRecordedCallBinding([session()], { ...withScreen, tracks: [...recording.tracks,
    { participantId: 'opaque-seat-a', tokenParticipantId: '', userId: 'actor-a' }] }))
    .toMatchObject({ status: 'unresolved', reason: 'missing-recorded-participant' });
  expect(resolveRecordedCallBinding([session()], { ...withScreen, tracks: [...recording.tracks,
    { participantId: 'opaque-screen-routing-source', tokenParticipantId: 'opaque-seat-a' }] }))
    .toMatchObject({ status: 'unresolved', reason: 'unbound-recorded-participant' });
});

it('rejects concurrent mixed calls and duplicated exact seat histories with conflicting canonical ids', () => {
  const mixed = session(); mixed.participantHistory!['opaque-seat-b'].callBinding!.callId = 'canonical-two';
  expect(resolveRecordedCallBinding([mixed], recording)).toEqual({ status: 'unresolved', reason: 'mixed-call-bindings' });
  const duplicate = session(); duplicate.sessionId = 'media-two';
  for (const participant of Object.values(duplicate.participantHistory!)) {
    participant.callBinding!.sessionId = duplicate.sessionId; participant.callBinding!.callId = 'canonical-two';
  }
  expect(resolveRecordedCallBinding([session(), duplicate], recording)).toEqual({ status: 'unresolved', reason: 'ambiguous-sessions' });
  mixed.sessionId = 'media-mixed';
  for (const participant of Object.values(mixed.participantHistory!)) participant.callBinding!.sessionId = mixed.sessionId;
  expect(resolveRecordedCallBinding([session(), mixed], recording)).toEqual({ status: 'unresolved', reason: 'ambiguous-sessions' });
});

it('requires actual window and recorded seat evidence and never expands legacy roster-only media', () => {
  expect(resolveRecordedCallBinding([session()], { ...recording, startedAt: undefined })).toMatchObject({ status: 'unresolved', reason: 'missing-recording-window' });
  expect(resolveRecordedCallBinding([session()], { ...recording, endedAt: start, startedAt: end })).toMatchObject({ status: 'unresolved', reason: 'missing-recording-window' });
  for (const tracks of [[], [{ userId: 'actor-a' }], [{ participantId: '' }]]) {
    expect(resolveRecordedCallBinding([session()], { ...recording, tracks })).toMatchObject({ status: 'unresolved', reason: 'missing-recorded-participant' });
  }
});

it('protects both governed tenant families while retaining explicit legacy compatibility elsewhere', () => {
  for (const lobby of ['orgiq:initiative:p-one', 'assessment:team:TEAM-005', 'assessment:doc:one']) expect(isGovernedRecordingLobby(lobby)).toBe(true);
  for (const lobby of ['dm:a:b', 'room:a', '']) expect(isGovernedRecordingLobby(lobby)).toBe(false);
});
