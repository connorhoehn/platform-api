import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { MemoryVideoSessionStore } from '../MemoryVideoSessionStore';
import { VideoSessionRepository } from '../VideoSessionRepository';
import { VideoCallBindingConflict, type VideoSessionRecord, type VideoParticipantCallBinding } from '../VideoSessionStore';
const binding: VideoParticipantCallBinding = { callId: 'call-one', clientId: 'client-one', userId: 'alice', tenant: 'orgiq', epoch: 2, sessionId: 'sess-one', channelArn: 'arn-one', lobbyName: 'orgiq:initiative:p-observability', boundAt: '2026-10-10T12:01:00Z' };
const participant = { participantId: 'pid-one', userId: 'alice', clientId: 'client-one', displayName: 'Alice', joinedAt: '2026-10-10T12:00:00Z' };
const row = (): VideoSessionRecord => ({ documentId: binding.lobbyName, sessionId: binding.sessionId, channelArn: binding.channelArn, streamKey: '', orgId: 'orgiq', startedBy: 'alice', status: 'active', startedAt: participant.joinedAt, participants: [participant], participantHistory: { 'pid-one': { ...participant } } });
const bind = (repo: MemoryVideoSessionStore | VideoSessionRepository, value = binding) => repo.bindCallParticipant(binding.lobbyName, binding.sessionId, 'pid-one', binding.channelArn, value);
test('native memory stores exact participant binding once, preserves first stamp and survives leave', async () => {
  const repo = new MemoryVideoSessionStore(); await repo.createSession(row());
  expect(await bind(repo)).toEqual(binding);
  expect(await bind(repo, { ...binding, boundAt: '2026-10-10T12:02:00Z' })).toEqual(binding);
  await expect(bind(repo, { ...binding, callId: 'call-other' })).rejects.toBeInstanceOf(VideoCallBindingConflict);
  await repo.removeParticipant(binding.lobbyName, binding.sessionId, 'pid-one');
  expect(await bind(repo)).toBeNull();
  expect((await repo.getSession(binding.lobbyName, binding.sessionId))?.participantHistory?.['pid-one']?.callBinding).toEqual(binding);
});
test.each(['ended', 'different-user', 'different-client', 'missing-history', 'left', 'expired', 'channel', 'tenant'])('native memory refuses %s without changing attendance', async kind => {
  const rec = row();
  if (kind === 'ended') rec.status = 'ended';
  if (kind === 'different-user') rec.participants = [{ ...participant, userId: 'bob' }];
  if (kind === 'different-client') rec.participants = [{ ...participant, clientId: 'other' }];
  if (kind === 'missing-history') rec.participantHistory = {};
  if (kind === 'left') rec.participantHistory!['pid-one']!.leftAt = '2026-10-10T12:01:00Z';
  if (kind === 'expired') rec.ttl = 1;
  if (kind === 'channel') rec.channelArn = 'other';
  if (kind === 'tenant') rec.orgId = 'other';
  const repo = new MemoryVideoSessionStore(); await repo.createSession(rec); expect(await bind(repo)).toBeNull();
});
test('DDB write fences the exact live roster, identity, tenant/channel, TTL and previously absent binding', async () => {
  const send = jest.fn().mockResolvedValueOnce({ Item: row() }).mockResolvedValue({});
  const repo = new VideoSessionRepository({ send } as unknown as DynamoDBDocumentClient);
  expect(await bind(repo)).toEqual(binding);
  const get = send.mock.calls[0]![0].input; expect(get.ConsistentRead).toBe(true);
  const update = send.mock.calls[1]![0].input;
  expect(update.ConditionExpression).toContain('participants = :live');
  expect(update.ConditionExpression).toContain('attribute_not_exists(#history.#pid.#binding)');
  expect(update.ConditionExpression).toContain('#ttl > :now');
  expect(update.ExpressionAttributeValues[':binding']).toEqual(binding);
});
test('DDB concurrent leave is reread and refused after failed condition', async () => {
  const gone = row(); gone.participants = []; gone.participantHistory!['pid-one']!.leftAt = '2026-10-10T12:01:00Z';
  const send = jest.fn().mockResolvedValueOnce({ Item: row() }).mockRejectedValueOnce({ name: 'ConditionalCheckFailedException' }).mockResolvedValueOnce({ Item: gone });
  expect(await bind(new VideoSessionRepository({ send } as unknown as DynamoDBDocumentClient))).toBeNull(); expect(send).toHaveBeenCalledTimes(3);
});
test('DDB concurrent conflicting bind cannot overwrite prior participant call', async () => {
  const changed = row(); changed.participantHistory!['pid-one']!.callBinding = { ...binding, callId: 'call-other' };
  const send = jest.fn().mockResolvedValueOnce({ Item: row() }).mockRejectedValueOnce({ name: 'ConditionalCheckFailedException' }).mockResolvedValueOnce({ Item: changed });
  await expect(bind(new VideoSessionRepository({ send } as unknown as DynamoDBDocumentClient))).rejects.toBeInstanceOf(VideoCallBindingConflict);
});
test('channel lookup traverses filtered empty pages consistently; refuses repeated cursor', async () => {
  const send = jest.fn().mockResolvedValueOnce({ Items: [], LastEvaluatedKey: { documentId: 'x', sessionId: 'x' } }).mockResolvedValueOnce({ Items: [row()] });
  const repo = new VideoSessionRepository({ send } as unknown as DynamoDBDocumentClient);
  expect(await repo.findByChannelArn('arn-one')).toEqual([row()]);
  expect(send.mock.calls.every(([command]) => command.input.ConsistentRead === true)).toBe(true);
  expect(send.mock.calls[1]![0].input.ExclusiveStartKey).toEqual({ documentId: 'x', sessionId: 'x' });
  send.mockReset().mockResolvedValue({ Items: [], LastEvaluatedKey: { documentId: 'x', sessionId: 'x' } });
  await expect(repo.findByChannelArn('arn-one')).rejects.toThrow('did not advance');
});
