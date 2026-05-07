# Event Catalog — Project Handoff

## What This Is

A unified event registry and transport layer. All inter-service communication goes through this project instead of services invoking Redis directly. It wraps two transports — Redis pub/sub (ephemeral/real-time) and message-queue (durable/at-least-once) — behind a single catalog with schema enforcement, metrics, and an admin UI.

## Why

- platform-api currently publishes to Redis with ad-hoc channel strings and no visibility into what's flowing
- No way to see all event types, their schemas, who produces them, or who consumes them
- No observability on the ephemeral path (Redis pub/sub is fire-and-forget with no metrics)
- Decoupling services later requires a contract layer — this is it

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  event-catalog                        │
│                                                      │
│  ┌────────────┐   ┌────────────┐   ┌─────────────┐ │
│  │  Registry  │   │  Signal    │   │  Durable    │ │
│  │  (schemas, │   │  (Redis    │   │  (message-  │ │
│  │   metrics, │   │   pub/sub) │   │   queue)    │ │
│  │   topology)│   │            │   │             │ │
│  └─────┬──────┘   └─────┬──────┘   └──────┬──────┘ │
│        │                 │                  │        │
│  ┌─────┴─────────────────┴──────────────────┴─────┐ │
│  │              Unified Client SDK                  │ │
│  │  publish(event, payload) / subscribe(event, fn) │ │
│  └──────────────────────────────────────────────────┘ │
│                                                      │
│  ┌──────────────────────────────────────────────────┐ │
│  │              Admin API + UI                       │ │
│  │  GET /events, /events/:name/stats, /topology     │ │
│  └──────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
   ┌──────────┐                  ┌─────────────┐
   │  Redis   │                  │ message-queue│
   │ (docker) │                  │  (SQS-like) │
   └──────────┘                  └─────────────┘
```

## Dependencies

- **Redis** — Docker container, ephemeral transport for real-time signals
- **message-queue** (`~/projects/message-queue`) — durable transport, `file:../message-queue` or HTTP client against its API
- **ui-components** (`~/projects/ui-components`) — component library for the admin UI (optional, can use directly or build standalone)

## Core Concepts

### Event Declaration

Every event type must be declared before it can be published. Declaration includes:

```typescript
catalog.declare({
  name: 'social:post',
  schema: { /* JSON Schema for payload */ },
  transport: 'signal',        // 'signal' (Redis) or 'durable' (message-queue)
  producer: 'platform-api',
  description: 'New post created in a social room',
});
```

### Transport Selection

The caller doesn't choose Redis vs. message-queue at publish time — the declaration determines it:

| Transport | Semantics | Use Case |
|-----------|-----------|----------|
| `signal` | At-most-once, sub-10ms, no replay | Cursors, presence, typing indicators, real-time broadcasts |
| `durable` | At-least-once, retry, DLQ, replay | Pipeline events, outbox drain, webhooks, anything that can't be dropped |

### Client SDK (library mode)

```typescript
import { EventCatalog } from 'event-catalog';

const catalog = new EventCatalog({
  redis: { url: 'redis://localhost:6379' },
  messageQueue: { url: 'http://localhost:3004' },
  serviceName: 'platform-api',
  registryUrl: 'http://localhost:3010',  // self, or remote catalog instance
});

// Declare at startup
catalog.declare({ name: 'social:post', transport: 'signal', schema: PostSchema });
catalog.declare({ name: 'pipeline:run:completed', transport: 'durable', schema: RunCompletedSchema });

// Publish
await catalog.publish('social:post', { roomId, post, userId });

// Subscribe
catalog.subscribe('social:post', (event) => { /* handler */ });
```

### Server Mode

Runs as a standalone service exposing:

1. **Registry API** — CRUD for event declarations, schema validation
2. **Admin API** — metrics, topology, event inspection
3. **Admin UI** — built-in dashboard (like message-queue's `src/admin/`)

## Admin API Surface

```
GET    /api/events                    — list all declared events
GET    /api/events/:name              — event detail (schema, producers, subscribers)
GET    /api/events/:name/stats        — publish rate, last seen, error count, subscriber lag
GET    /api/events/:name/history      — last N messages (signal: ring buffer, durable: from queue)
POST   /api/events                    — manually declare an event (services do this at startup)
DELETE /api/events/:name              — remove a declaration

GET    /api/topology                  — graph of services → events → subscribers
GET    /api/health                    — catalog health + transport connectivity

GET    /api/transports/signal/channels  — raw Redis channel list with stats
GET    /api/transports/durable/queues   — proxied from message-queue's admin API
```

## Admin UI Views

1. **Event list** — all declared events, filterable by transport/producer/status
2. **Event detail** — schema (rendered), live metrics chart, recent messages (inspectable)
3. **Topology** — directed graph: service nodes → event edges → subscriber nodes
4. **Transport health** — Redis connection status, message-queue queue depths, error rates

## What platform-api Currently Publishes (migration targets)

| Current Code | Channel/Pattern | New Event Name | Transport |
|---|---|---|---|
| `BroadcastService.emit()` | `websocket:route:{channelId}` | `social:post`, `social:comment`, `social:like`, `social:member_joined`, `social:member_left`, `doc:comment_added`, `doc:comment_resolved`, `doc:comment_deleted` | signal |
| `RedisIngestEventBus.emit()` | `ingest:events` | `ingest:progress` | signal |
| `publishWithOutbox()` | DynamoDB outbox table | `social.post.created`, `social.reaction.created`, etc. | durable |
| distributed-core EventBus | In-process only | `pipeline:run:started`, `pipeline:run:completed`, `pipeline:run:failed` | durable |

Note: distributed-core's EventBus stays in-process for pipeline execution. But the *externally visible* lifecycle events (run started/completed/failed) should also be published through event-catalog so other services can subscribe.

## Key Design Decisions to Make

1. **Schema format** — JSON Schema? TypeScript types compiled to JSON Schema? Keep it simple for v1.
2. **Signal channel mapping** — does `social:post` map 1:1 to a Redis channel, or does the catalog fan out to per-room channels (like BroadcastService does today with `websocket:route:{channelId}`)? Probably: the catalog publishes to a logical channel, and a routing layer (gateway-side) handles per-connection fan-out.
3. **Registration persistence** — DynamoDB? Or reconstructed from service startups (ephemeral, in-memory)? In-memory is simpler for v1; services re-register on restart.
4. **Ring buffer for signal history** — Redis Streams (XADD/XRANGE with MAXLEN) gives you last-N inspection for debugging without building custom storage.
5. **Auth** — internal service-to-service for v1, no auth. Add mTLS or shared secret later.

## Project Structure (suggested)

```
event-catalog/
├── src/
│   ├── index.ts              — library exports (client SDK)
│   ├── client/               — EventCatalog client class
│   │   ├── index.ts
│   │   ├── signal.ts         — Redis pub/sub wrapper
│   │   └── durable.ts        — message-queue client wrapper
│   ├── registry/             — event declaration store
│   │   ├── index.ts
│   │   └── types.ts
│   ├── server/               — standalone server mode
│   │   ├── index.ts
│   │   ├── routes.ts         — admin API
│   │   └── metrics.ts        — per-event counters, rates
│   ├── admin/                — admin UI (Vite app, same pattern as message-queue)
│   │   ├── src/
│   │   ├── index.html
│   │   └── vite.config.ts
│   └── types/                — shared types
│       └── index.ts
├── docker-compose.yml        — Redis + message-queue for local dev
├── package.json
└── tsconfig.json
```

## How This Connects to websocket-gateway

Today: platform-api → Redis pub/sub → websocket-gateway subscribes to `websocket:route:*`

After: platform-api → event-catalog (signal transport) → Redis pub/sub → websocket-gateway subscribes

The gateway becomes a subscriber through event-catalog's client SDK. It registers: "I subscribe to `social:*`" and the catalog routes those signals via Redis. The gateway doesn't change much — it still gets messages from Redis — but now the catalog knows about the subscription and can show it in the topology view.

## How This Connects to message-queue

event-catalog's durable transport is a thin client around message-queue's existing API. When you `catalog.publish('pipeline:run:completed', payload)` with transport `durable`:

1. Catalog validates payload against schema
2. Catalog calls message-queue's `SendMessage` API with the event serialized
3. Subscribers poll/receive from message-queue (consumer groups work as-is)

The catalog adds: schema validation, metrics counting, and registration metadata. message-queue stays the dumb durable pipe.

## Success Criteria

- [ ] Any service can `catalog.declare()` + `catalog.publish()` + `catalog.subscribe()` without importing `redis` directly
- [ ] Admin UI shows all events, their schemas, live throughput, and which services are connected
- [ ] platform-api's BroadcastService and RedisIngestEventBus are replaced with catalog client calls
- [ ] Adding a new event type to any service automatically appears in the catalog UI
- [ ] No `redis.publish()` calls remain outside of event-catalog's internals
