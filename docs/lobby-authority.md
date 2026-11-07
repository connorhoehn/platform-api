# Lobby authority for OrgIQ realtime tokens

`lobbyAccess` (`src/lib/lobbyAccess.ts`) decides whether a caller may use a
call lobby: open or join its video session, read its sessions, recordings,
reactions or captions. For an OrgIQ realtime token (`authKind: 'orgiq-hs256'`)
it applies only a prefix rule. The lobby must be `<tenant>:<rest>` for a tenant
the token lists, and a `<tenant>:dm:<ids>` lobby only for a listed id.

That rule cannot know whether this person may use *that* initiative
(`orgiq:initiative:<id>`), assessment team (`assessment:team:<id>`), pipeline
run (`pipelines:run:<id>`) or private Social channel (`social:ch:<slug>`). It
also cannot tell whether the name is a lobby family at all. That authority
belongs to the OrgIQ BFF (aws-agentcore `middleware/`): its gateway
`lobbyGuard` plus fresh directory and page reads.

## Mechanism

With `LOBBY_AUTHORITY_REQUIRED=true`, every lobby decision for an OrgIQ token
runs as follows:

1. The prefix rule runs as before. A refusal here never reaches the BFF.
2. platform-api then sends `POST LOBBY_AUTHORITY_URL` with the caller's own
   `Authorization: Bearer <realtime token>` and `{ "lobbyName": "<name>" }`.
3. It admits the caller only on HTTP 200 `{ "allowed": true }`. Every other
   outcome refuses with the usual 404: 403, 401 (the token expired or the epoch
   was retired), 404 (a BFF without the route), 5xx, a 3-second timeout, a
   malformed body, or `LOBBY_AUTHORITY_URL` unset.
4. A lobby under one of the BFF's tenant prefixes (`LOBBY_AUTHORITY_TENANTS`,
   default `orgiq,assessment,social,pipelines`) is refused to every non-OrgIQ
   caller. Without this, a Cognito or SKIP_AUTH dev bearer naming
   `orgiq:initiative:<id>` would fall through to the open ad-hoc rule.

The answer is cached per request and lobby, so an assert followed by a list
filter asks only once. Nothing is cached across requests.

The BFF answers with the same call admission that a `join` frame gets on its
`/ws` (`authorizeLiveGatewayCall`). It re-reads the directory (epoch, grants
and masquerade actor) and checks the family's REST read gate, Social
StartCall and membership, and the page resource.

### Why a callback rather than lobby grants in the token

Carrying `lobbies: [{name, exp}]` in the realtime token was the first design
considered. It was not chosen for three reasons:

- The OrgIQ portal uses one realtime token for the whole page, and the gateway
  socket shares it. A changed token reconnects that socket, so per-lobby
  re-minting would drop the live connection.
- The call library (`useConversationCall`'s `platformApi.getIdToken()`) asks
  for a token without a lobby argument. A lobby-scoped token would need new
  client plumbing in the library and in the call host.
- A grant stays valid until it expires, even after a revocation. The callback
  is fresh on every request.

The cost is one BFF round trip per lobby decision, which only happens on
session create, join and read calls. The BFF must also be reachable from
platform-api. That holds already, because the BFF serves the gateway these
calls depend on.

## Rollout (two independently deployable halves)

1. **Deploy the BFF first.** It adds `POST /api/realtime/lobby-access`. The
   route is inert until someone calls it.
2. **Then point platform-api at it.** Set `LOBBY_AUTHORITY_URL` to a URL that
   platform-api's tasks can reach. The check is on by default (since the
   6 November 2026 security review), so without a URL every OrgIQ call lobby
   is refused.

Reversing that order refuses every OrgIQ call lobby, because the BFF answers
404 until step 1 lands. `LOBBY_AUTHORITY_REQUIRED=false` turns the check off,
for local development without the BFF only.

| Env | Default | Meaning |
|-----|---------|---------|
| `LOBBY_AUTHORITY_REQUIRED` | on | anything but `false` keeps steps 2–4 on |
| `LOBBY_AUTHORITY_URL` | — | The BFF route, e.g. `http://host.docker.internal:3001/api/realtime/lobby-access` |
| `LOBBY_AUTHORITY_TENANTS` | `orgiq,assessment,social,pipelines` | Prefixes that only an OrgIQ token, with BFF admission, can reach |

Tests: `src/lib/__tests__/lobbyAuthority.test.ts`.
