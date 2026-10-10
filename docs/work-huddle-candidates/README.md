# Staged local recording follow-up

This source patch preserves the tested recording access/timestamp work prepared on October 10, 2026. It is a candidate, **not activated** in the local service. Apply only during a coordinated idle window after preserving sessions, signing keys, original media, and current service configuration.

The patch applies to this checkout with `git apply --check docs/work-huddle-candidates/recording-access-and-timestamps.patch`. No environment files, recordings, or credentials are included. Platform and LVS recording access changes must be integrated together; do not activate only one side.

Private runtime evidence remains in the aws-agentcore checkout under `docs/demos/work-huddles/evidence/2026-10-10/`. Recording access verification covered 98 platform checks, 38 native access checks, and 14 native UI checks. Whole native UI build still has pre-existing dependency/type errors. Real transcript regeneration and service activation are not claimed by this handoff.


## Isolated automatic completion verification — October 10, 2026

[The automatic proof](automatic-finalize-20261010/automatic-proof.json) starts at the actual HMAC-verified completion webhook, with no recording-row seed or manual transcription call. Real native DynamoDB repositories, the native executor/EventBus and default child trigger produce eleven real Whisper-timed segments and a genuine Codex subscription summary. Duplicate completion retains the exact ready row and makes no second media read, transcription or summary request. Both recorded synthetic participants read the same result; a nonparticipant is denied. The immutable input media retains its original hash.

[Native lifecycle receipts](automatic-finalize-20261010/native-lifecycle.json), [Dynamo transaction checks](automatic-finalize-20261010/actual-dynamo-transactions.json), [source verification](automatic-finalize-20261010/source-verification.json) and [owned cleanup](automatic-finalize-20261010/resource-cleanup.json) preserve the boundaries. Raw transcripts, summaries, row bodies, WAL and private bridge configuration remain outside the repository. The first failed run used an isolated speech launch without the existing cached ASR dependency path; it made no model call and was retained privately before that launch was corrected.

This is isolated evidence, not activation of a primary service or a real browser/SFU recording. Accepted-seat provenance and composed-media transport are explicit fixtures; production Raft/RunPersistence bootstrap is not exercised. Native pipeline lifecycle events are observed, but **Work chat cards remain absent** because the native lobby mapper does not yet map `orgiq:initiative:*`. Gateway card delivery and production UI realtime processing notifications are therefore not claimed; scoped post-call status polling and its UI fixtures are separate checks.

Assessment document recording association now requires every signed recorded seat to agree on the same server-verified exact source, tenant and lobby. Missing or mixed source context leaves that association absent while preserving an independently valid call binding. The authoritative primary row supplies list metadata, and native document recording reads recheck the current source authority. Historical recordings are not guessed or backfilled. [Backend verification](automatic-finalize-20261010/document-backend-verification.json) and source verification record the focused tests; no live document call or source mutation is claimed.
