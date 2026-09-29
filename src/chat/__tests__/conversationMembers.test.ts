// Who a conversation's document is shared with: both DM names, a room's
// current members, a hashed group DM's peers — the requester always, agents never.
jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import { GetCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { conversationMembers, dmNames } from '../conversationMembers';

describe('conversationMembers', () => {
  it('reads a DM\'s members off its name', async () => {
    const send = jest.fn();
    await expect(conversationMembers('chat:dm:dev-dave:dev-eve', 'dev-dave', send)).resolves.toEqual(['dev-dave', 'dev-eve']);
    expect(send).not.toHaveBeenCalled();
  });

  it('does not take a DM\'s names as a grant when the requester is not one of them', async () => {
    await expect(conversationMembers('chat:dm:dev-eve:dev-frank', 'dev-dave', jest.fn())).resolves.toEqual(['dev-dave']);
  });

  it('reads a room\'s current members, skipping removed people and agents', async () => {
    const send = jest.fn(async (command: unknown) => {
      expect(command).toBeInstanceOf(QueryCommand);
      expect((command as QueryCommand).input).toMatchObject({ TableName: 'test-chat-members', ExpressionAttributeValues: { ':c': 'room:eng' } });
      return {
        Items: [
          { channel: 'room:eng', userId: 'dev-eve' },
          { channel: 'room:eng', userId: 'dev-frank', removedAt: '2026-09-01T00:00:00Z' },
          { channel: 'room:eng', userId: 'agent:pipeline' },
          { channel: 'room:eng', userId: 'dev-grace' },
        ],
      };
    });
    await expect(conversationMembers('room:eng', 'dev-dave', send)).resolves.toEqual(['dev-dave', 'dev-eve', 'dev-grace']);
  });

  it('reads a hashed group DM from the requester\'s conversation row', async () => {
    const send = jest.fn(async (command: unknown) => {
      expect(command).toBeInstanceOf(GetCommand);
      return { Item: { peers: JSON.stringify(['dev-dave', 'dev-eve', 'dev-frank']) } };
    });
    await expect(conversationMembers('chat:dmg:abc', 'dev-dave', send)).resolves.toEqual(['dev-dave', 'dev-eve', 'dev-frank']);
  });

  it('falls back to the requester when the table cannot be read', async () => {
    const send = jest.fn(async () => { throw new Error('ddb down'); });
    await expect(conversationMembers('room:eng', 'dev-dave', send)).resolves.toEqual(['dev-dave']);
  });

  it('parses member-addressed DM names only', () => {
    expect(dmNames('chat:dm:a:b')).toEqual(['a', 'b']);
    expect(dmNames('chat:dm:a:')).toBeNull();
    expect(dmNames('room:x')).toBeNull();
  });
});
