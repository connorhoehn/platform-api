# Video Integration — platform-api + live-video-streaming

## How Video Sessions Work Post-Extraction

Video sessions are an application-level concept that platform-api manages on top of the raw video infrastructure provided by live-video-streaming. The separation is intentional:

- **live-video-streaming** — thin infrastructure service managing IVS channels and participant tokens. No knowledge of documents, users, or sessions.
- **platform-api** — composes those primitives into "sessions" tied to documents, tracks participants, persists lifecycle state in DynamoDB.

The frontend never talks to live-video-streaming directly. All video operations go through platform-api, which orchestrates the underlying channel lifecycle.

## Primitives Called on live-video-streaming

platform-api makes three HTTP calls to live-video-streaming (configured via `VIDEO_API_URL` env var):

### 1. Create Channel

```
POST {VIDEO_API_URL}/api/channels
Content-Type: application/json

{ "name": "<channelName>" }

Response 201:
{
  "arn": "arn:aws:ivs:...:channel/<id>",
  "name": "<channelName>",
  "streamKey": "<streamKey>",
  "ingestEndpoint": "<rtmps-endpoint>",
  "playbackUrl": "<hls-url>"
}
```

Called when creating a new video session. The channel name is set to the `documentId` (or a random UUID if no document context).

### 2. Mint Participant Token

```
POST {VIDEO_API_URL}/api/channels/{arn}/participant-tokens
Authorization: Bearer <streamKey>
Content-Type: application/json

{ "role": "publisher", "ttlSeconds": 3600 }

Response 200:
{
  "token": "<participant-token>",
  "participantId": "<participant-id>",
  "expiresAt": "<iso-timestamp>"
}
```

Called when a user joins an existing session. The `streamKey` from the session record is used as Bearer auth. The returned token is what the frontend uses to connect to the IVS stage.

### 3. Delete Channel (Teardown)

```
DELETE {VIDEO_API_URL}/api/channels/{arn}

Response 200: {}
```

Called when a session ends. Tears down the underlying IVS channel and stops any active streams.

## Frontend Flow

```
Frontend (useHangoutEmbed)          platform-api              live-video-streaming
         |                               |                            |
         |-- POST /api/video/sessions -->|                            |
         |   { documentId, displayName } |-- POST /api/channels ----->|
         |                               |<-- { arn, streamKey } -----|
         |                               |   [persist session to DDB] |
         |<-- { sessionId } ------------|                            |
         |                               |                            |
         |-- POST /sessions/:id/join --->|                            |
         |   { documentId, displayName } |-- POST /channels/:arn/     |
         |                               |   participant-tokens ----->|
         |                               |<-- { token, participantId }|
         |<-- { token, participantId } --|                            |
         |                               |                            |
         | [stage.join(token, endpoint)] |                            |
         |   (IVS Real-Time SDK)         |                            |
         |                               |                            |
         |-- POST /sessions/:id/end ---->|                            |
         |   { documentId }              |-- DELETE /channels/:arn --->|
         |                               |<-- 200 -------------------|
         |                               |   [update session in DDB]  |
         |<-- 200 ----------------------|                            |
```

### Key frontend details:

1. **`useHangoutEmbed` hook** — manages the session lifecycle from the React side
2. After receiving the participant token, the frontend calls `stage.join(token, endpoint)` using the IVS Real-Time SDK directly against the IVS infrastructure (not through platform-api or live-video-streaming)
3. The `/end` endpoint uses `optionalAuth` so `sendBeacon()` works on tab close (no auth header in beacon requests)

## Schema: VideoSessionRecord

Persisted in DynamoDB table `{prefix}document-video-sessions`.

| Field | Type | Key | Description |
|-------|------|-----|-------------|
| `documentId` | string | PK | Document this session is attached to |
| `sessionId` | string | SK | Unique session identifier (UUID) |
| `channelArn` | string | | IVS channel ARN from live-video-streaming |
| `streamKey` | string | | Channel stream key (used as Bearer token for participant-token minting) |
| `status` | `'active' \| 'ended'` | | Session lifecycle state |
| `startedAt` | string (ISO) | | When the session was created |
| `endedAt` | string (ISO) | | When the session was ended (absent while active) |
| `startedBy` | string | | User ID of the session creator |
| `participants` | array | | List of participants who have joined |
| `participants[].userId` | string | | Participant's user ID |
| `participants[].displayName` | string | | Display name at time of join |
| `participants[].joinedAt` | string (ISO) | | When this participant joined |
| `transcriptStatus` | `'pending' \| 'processing' \| 'available' \| 'failed'` | | Transcript generation state (future) |
| `transcript` | string | | Full transcript text (future) |
| `aiSummary` | string | | AI-generated session summary (future) |

### Access patterns:

- **Get session by document + sessionId** — direct key lookup (PK + SK)
- **List sessions for document** — query on PK (`documentId`), sorted by `startedAt` descending
- **Get single session** — requires both `documentId` (PK) and `sessionId` (SK) since sessionId alone is not unique across documents

## What live-video-streaming Needs to Be Running

For video sessions to work end-to-end:

1. **live-video-streaming service must be running** and accessible at the URL configured in `VIDEO_API_URL`
2. **IVS resources** — live-video-streaming manages the AWS IVS (Interactive Video Service) resources. In local dev, it can run with mocked IVS responses.
3. **Network connectivity** — platform-api must be able to reach live-video-streaming over HTTP

### Local dev setup:

```bash
# In the live-video-streaming project directory:
npm run dev   # Starts on its default port (typically 3002)

# Then set VIDEO_API_URL when starting platform-api:
VIDEO_API_URL=http://localhost:3002 npm run dev
```

### Without live-video-streaming:

If `VIDEO_API_URL` is empty or the service is unreachable:
- `POST /api/video/sessions` will fail with a 500 error
- `POST /api/video/sessions/:id/join` will fail when trying to mint tokens
- `POST /api/video/sessions/:id/end` will fail when trying to delete the channel
- `GET` routes (list/get sessions) will still work since they only read from DynamoDB

### Production:

In production, live-video-streaming runs as a separate pod/service in the same cluster. `VIDEO_API_URL` is set to the internal Kubernetes service DNS name (e.g., `http://live-video-streaming:3002`).
