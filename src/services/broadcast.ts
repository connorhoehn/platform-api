/**
 * BroadcastService — publishes social events to the WebSocket gateway via Redis.
 *
 * The gateway (src/core/message-router.js) subscribes to `websocket:route:{channelId}`.
 * When it receives a `channel_message` envelope, it delivers `message` to all local
 * WebSocket clients that have joined that channel via a chat.join action.
 *
 * Design decisions:
 * - targetNodes read from `websocket:channel:{channelId}:nodes` (Redis SET maintained by gateway)
 * - If targetNodes is empty: no WS clients are subscribed → skip publish (no-op)
 * - If Redis is unavailable: log warning and return — social writes are the primary operation
 * - Singleton exported as `broadcastService` — imported by route handlers
 */
import { createClient, RedisClientType } from 'redis';

const REDIS_URL = `redis://${process.env.REDIS_ENDPOINT ?? 'redis'}:${process.env.REDIS_PORT ?? '6379'}`;

type SocialEventType =
  | 'social:post'
  | 'social:comment'
  | 'social:like'
  | 'social:member_joined'
  | 'social:member_left'
  | 'doc:comment_added'
  | 'doc:comment_resolved'
  | 'doc:comment_deleted';

class BroadcastService {
  private client: RedisClientType | null = null;
  private connecting = false;
  private unavailable = false;

  private async getClient(): Promise<RedisClientType | null> {
    if (this.client?.isReady) return this.client;
    if (this.connecting || this.unavailable) return null;

    this.connecting = true;
    try {
      const c = createClient({
        url: REDIS_URL,
        socket: { reconnectStrategy: false },
      }) as RedisClientType;
      c.on('error', (err: Error) => {
        console.warn('[broadcast] Redis unavailable:', err.message);
        this.client = null;
        this.connecting = false;
        this.unavailable = true;
      });
      await c.connect();
      this.client = c;
      this.connecting = false;
      return c;
    } catch (err) {
      console.warn('[broadcast] Redis connect failed:', (err as Error).message);
      this.client = null;
      this.connecting = false;
      this.unavailable = true;
      return null;
    }
  }

  /**
   * Emit a social event to all WebSocket clients subscribed to channelId.
   *
   * @param channelId  The room's channelId (from social-rooms DynamoDB item.channelId)
   * @param eventType  The social event type string
   * @param payload    The event data (post/comment/like/member object)
   */
  async emit(
    channelId: string,
    eventType: SocialEventType,
    payload: Record<string, unknown>
  ): Promise<void> {
    const redis = await this.getClient();
    if (!redis) {
      console.warn(`[broadcast] Redis unavailable, skipping ${eventType} on channel ${channelId}`);
      return;
    }

    try {
      // Look up which gateway nodes have clients subscribed to this channel
      // Key maintained by gateway node-manager.js: `websocket:channel:${channel}:nodes`
      const nodesKey = `websocket:channel:${channelId}:nodes`;
      const targetNodes = await redis.sMembers(nodesKey);

      if (targetNodes.length === 0) {
        // No WebSocket clients are subscribed to this channel — nothing to deliver
        return;
      }

      // Build the channel_message envelope that gateway handleChannelMessage expects
      const envelope = {
        type: 'channel_message',
        channel: channelId,
        message: {
          type: eventType,
          channel: channelId,
          payload,
          timestamp: new Date().toISOString(),
        },
        excludeClientId: null,
        fromNode: 'social-api',
        seq: 0,
        timestamp: new Date().toISOString(),
        targetNodes,
      };

      await redis.publish(`websocket:route:${channelId}`, JSON.stringify(envelope));
    } catch (err) {
      // Non-fatal — social writes succeed even if broadcast fails
      console.warn(`[broadcast] emit failed for ${eventType} on channel ${channelId}:`, (err as Error).message);
    }
  }
}

// Singleton — shared across all route handlers in the same process
export const broadcastService = new BroadcastService();
