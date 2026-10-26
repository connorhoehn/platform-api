import cases from './fixtures/hangout-plan-cases.json';
import { ValidationError } from '../../middleware/error-handler.js';
import { readHangoutPlan } from '../hangout-plan';
import { resolveHangoutMedia } from '../hangout-media';
import type { HangoutRoom } from '../../repositories/HangoutRoomRepository';
const plan = { expectedVersion: 0, mode: 'assigned', allowReturnToMain: true, rooms: [{ id: 'design', name: 'Design', members: ['alice'] }] };
test('saved room identities and assignments survive a round trip; server owns audit fields', () => {
  expect(readHangoutPlan({ ...plan, updatedBy: 'mallory' }, 'owner')).toMatchObject({ version: 1, updatedBy: 'owner', rooms: plan.rooms });
});
test.each([
  { ...plan, expectedVersion: -1 },
  { ...plan, rooms: [plan.rooms[0], { id: 'build', name: 'Build', members: ['alice'] }] },
  { ...plan, rooms: Array(51).fill(plan.rooms[0]) },
  { ...plan, rooms: [{ id: 'design~breakout~forged', name: 'X', members: [] }] },
  { ...plan, allowReturnToMain: 'yes' },
])('rejects invalid or ambiguous saved plans', input => { expect(() => readHangoutPlan(input, 'owner')).toThrow(); });
test('a temporary media room resolves the persistent parent and inherits its recording and join configuration', async () => {
  const read = jest.fn(async () => ({ roomId: 'parent-1', state: 'active', settings: { audioOnly: true, recordingEnabled: false } } as HangoutRoom));
  expect(await resolveHangoutMedia('social:room:review~breakout~temporary', read)).toMatchObject({ orgId: 'social', slug: 'review', roomId: 'parent-1', breakoutRoomId: 'temporary', audioOnly: true, recordingEnabled: false });
  expect(read).toHaveBeenCalledWith('social', 'review');
  await expect(resolveHangoutMedia('social:room:missing', async () => null)).rejects.toThrow('Hangout not available');
});

// The same corpus runs in the owner library and the BFF adapter.
test.each(cases)('shared plan contract: $name', ({ input, valid }) => {
  jest.useFakeTimers().setSystemTime(new Date('2026-10-26T12:00:00Z'));
  try {
    if (valid) {
      const value = readHangoutPlan(input, 'authenticated-owner');
      expect(value.version).toBe((input as { expectedVersion: number }).expectedVersion + 1);
      expect(value.updatedBy).toBe('authenticated-owner');
      expect(value.updatedAt).toBe('2026-10-26T12:00:00.000Z');
      expect(value).not.toHaveProperty('expectedVersion');
    } else expect(() => readHangoutPlan(input, 'owner')).toThrow(ValidationError);
  } finally { jest.useRealTimers(); }
});
