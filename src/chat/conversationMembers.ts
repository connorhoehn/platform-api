// Who is in a conversation, for sharing something the conversation made.
//
// A document an /agent loop writes for a conversation belongs to everyone in
// it, not only to the person who typed the command: both people in a DM, every
// member of a room. This answers "who are they" from the same sources the
// read gates already trust (documentEditAccess.ts), so the editors a document
// is created with and the people the gates would admit are the same set.
//
//   chat:dm:<a>:<b>[:<c>…]  the names ARE the members (the gateway derives DM
//                           membership from the name — NFR #151 / #155).
//   chat:dmg:<hash>         a hashed group DM carries no names: the requester's
//                           `chat-conversations` row lists its `peers`.
//   anything else (rooms)   current `chat-members` rows (no `removedAt`).
//
// The requester is always included, and agent identities never are: a
// document is shared with people.

import { GetCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import log from '../lib/logger';

const CHAT_MEMBERS_TABLE = tableName(process.env.DYNAMODB_CHAT_MEMBERS_TABLE || 'chat-members');
const CHAT_CONVERSATIONS_TABLE = tableName(process.env.DYNAMODB_CHAT_CONVERSATIONS_TABLE || 'chat-conversations');

/** A cap, so a huge room does not turn into a row nobody can write. */
export const MAX_CONVERSATION_EDITORS = 200;

type Send = (command: unknown) => Promise<unknown>;

/** The ids a member-addressed DM names, or null for anything else (or a malformed name). */
export function dmNames(channel: string): string[] | null {
  if (!channel.startsWith('chat:dm:')) return null;
  const names = channel.slice('chat:dm:'.length).split(':');
  return names.length && names.every(Boolean) ? names : null;
}

const isPerson = (id: unknown): id is string => typeof id === 'string' && !!id && !id.startsWith('agent:');

/**
 * Every person in `channel`, requester first. Never throws: an unreadable
 * table leaves the requester alone (the document is still theirs), logged.
 */
export async function conversationMembers(
  channel: string,
  requester: string,
  send: Send = (command) => docClient.send(command as never) as Promise<unknown>,
): Promise<string[]> {
  const out = new Set<string>(isPerson(requester) ? [requester] : []);
  if (!channel) return [...out];
  try {
    const names = dmNames(channel);
    if (names) {
      // The name is a grant only when the requester is one of the names.
      if (names.includes(requester)) for (const name of names) if (isPerson(name)) out.add(name);
    } else if (channel.startsWith('chat:dmg:')) {
      const res = await send(new GetCommand({ TableName: CHAT_CONVERSATIONS_TABLE, Key: { userId: requester, channel } })) as { Item?: Record<string, unknown> };
      const raw = res.Item?.peers;
      const peers = typeof raw === 'string' ? JSON.parse(raw) as unknown : raw;
      if (Array.isArray(peers) && peers.includes(requester)) for (const peer of peers) if (isPerson(peer)) out.add(peer);
    } else {
      let key: Record<string, unknown> | undefined;
      do {
        const res = await send(new QueryCommand({
          TableName: CHAT_MEMBERS_TABLE,
          KeyConditionExpression: '#c = :c',
          ExpressionAttributeNames: { '#c': 'channel' },
          ExpressionAttributeValues: { ':c': channel },
          ...(key ? { ExclusiveStartKey: key } : {}),
        })) as { Items?: Array<Record<string, unknown>>; LastEvaluatedKey?: Record<string, unknown> };
        for (const row of res.Items ?? []) if (row.removedAt === undefined && isPerson(row.userId)) out.add(row.userId);
        key = res.LastEvaluatedKey;
      } while (key && out.size < MAX_CONVERSATION_EDITORS);
    }
  } catch (err) {
    log.warn({ channel, err: err instanceof Error ? err.message : String(err) }, 'conversation members could not be read; sharing with the requester only');
  }
  return [...out].slice(0, MAX_CONVERSATION_EDITORS);
}
