# Recording processing status

`GET /api/recordings/:recordingId/status` returns a whitelisted processing projection and a SHA-256 revision. It never returns transcript, summary text, participants or native pipeline payloads. An `after` revision holds the request for up to 25 seconds until the existing native pipeline EventBus wakes it or the resync deadline expires.

The route verifies authentication and recording participation before subscribing, subscribes before its second read to close the race, then verifies authentication again and reads current recording authority before responding. OrgIQ identities also require a fresh owning-BFF lobby decision, including original source context for Assessment documents. Expired credentials and revoked participation/source access return no status. Disconnects and failed reads release subscriptions and timers.

The portal uses these wake-ups to recheck its existing authorized metadata reader without clearing the card. Hidden tabs pause, actor/scope changes abort, requests have deadlines and observation remains finite. Its five-second polling remains a fallback when the native bus, event endpoint or network is unavailable. The exact call association is discovered through the existing participant index before monitoring begins.

Verification (October 11, 2026 Eastern): 12 focused checks pass, including an actual distributed-core EventBus publication waking an authenticated HTTP request. Its pub/sub transport and recording/host authorities are isolated fixtures. The existing recording-route suite also passed (43 cases); source typecheck, build and OpenAPI coverage pass. This is not a new live SFU call or primary-service activation. The user explicitly requested preservation of the current memory-backed application session.
