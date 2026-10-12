# Recording processing status

`GET /api/recordings/:recordingId/status` returns a whitelisted processing projection and a SHA-256 revision. It never returns transcript, summary text, participants or native pipeline payloads. An `after` revision holds the request for up to 25 seconds until the existing native pipeline EventBus wakes it or the resync deadline expires.

The route verifies authentication and recording participation before subscribing, subscribes before its second read to close the race, then verifies authentication again and reads current recording authority before responding. OrgIQ identities also require a fresh owning-BFF lobby decision, including original source context for Assessment documents. Expired credentials and revoked participation/source access return no status. Disconnects and failed reads release subscriptions and timers.

The portal uses these wake-ups to recheck its existing authorized metadata reader without clearing the card. Hidden tabs pause, actor/scope changes abort, requests have deadlines and observation remains finite. Its five-second polling remains a fallback when the native bus, event endpoint or network is unavailable. The exact call association is discovered through the existing participant index before monitoring begins.

Verification (October 11, 2026 Eastern): 12 focused checks pass, including an actual distributed-core EventBus publication waking an authenticated HTTP request. Its pub/sub transport and recording/host authorities are isolated fixtures. The existing recording-route suite also passed (43 cases); source typecheck, build and OpenAPI coverage pass. This is not a new live SFU call or primary-service activation. The user explicitly requested preservation of the current memory-backed application session.


## Live native acceptance

`scripts/meeting-live-acceptance.cjs` connects real browser RTP capture, native
recording/composition, the signed completion webhook, native finalize and
transcription executors, Whisper, Codex subscription summarization, DynamoDB
artifacts, and authenticated EventBus-driven status reads in one isolated run.
It also checks both recorded participants can read the exact saved summary,
refuses a nonparticipant, and probes the downloaded MP4 for audio and video.
Composition must report `ok`, retain both input tracks, and preserve their
native signed media seat identities.

The harness requires explicit `MEETING_ACCEPTANCE_OPT_IN=owned-native-services`,
`MEETING_ACCEPTANCE_RESOURCES_JSON`, `MEETING_ACCEPTANCE_PRIVATE_CONFIG`,
`MEETING_ACCEPTANCE_OUTPUT`, `MEETING_ACCEPTANCE_PLAYWRIGHT_MODULE`, and
`MEETING_ACCEPTANCE_MEDIA_ROOT`. The resource file must declare owned loopback
DynamoDB/Redis services named `meeting-finalize-native-*` or
`meeting-pipeline-native-*`. The private configuration's `env` supplies the
owned SFU endpoints/admin/service/webhook credentials, the speech endpoint, and
the authenticated local Codex bridge. `NODE_ID` must start with `meeting-live-`
and match the SFU's live health response before any session is created.

Use a dedicated SFU, Redis, DynamoDB, media directory, and signing keys. Build
platform-api before starting `scripts/codex-summary-server.cjs`. The media root
contains `initiative/u-vera.wav` and `initiative/u-nora.wav`; the browser pairs
that prepared speech with animated canvases, using no physical devices.
Artifacts and sanitized receipts are written under the supplied output directory;
private transcript/summary bodies and logs must not be published accidentally.
The harness deletes only its uniquely prefixed DynamoDB tables and closes its
browser and HTTP servers. Its caller owns shutting down the dedicated SFU,
bridge, port forwards, and containers after preserving the recording/evidence.

Gateway acceptance and host directory authorization are explicit fixtures.
Production Raft/RunPersistence bootstrap, the application's rendered post-call
card, and application search are separate acceptance boundaries. This harness
does not activate or reset the primary application.
