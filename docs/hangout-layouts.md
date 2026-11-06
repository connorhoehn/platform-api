# Saved hangout layouts and call activity

A hangout is persistent configuration. Its saved `plan` contains a version,
room IDs/names, assigned user IDs, join mode and return-to-main preference. A
call instantiates that layout as live breakouts; adding or moving rooms during
a call does not write the saved plan. Presence and media sessions are separate
from assignment.

`PATCH /api/rooms/:slug` accepts `plan.expectedVersion`. The repository writes
conditionally against the current version (or its absence on the first save),
returning 409 after a concurrent change. Plans allow 50 rooms and 500 unique
assignments; a person can be assigned to one saved room. Private-room assignments
must already have access. Settings patches preserve omitted settings and reject
invalid types. Changing defaults does not rewrite an existing call's media.

Video-session creation resolves the parent hangout, including breakout lobby
names, and records the parent identity and selected room. Recording defaults
follow the parent hangout through the existing media/recording decision path.
Clients still enforce camera-off entry through the shared call hook. The local
acceptance uses the native SFU; AWS IVS acceptance remains separate.

`GET /api/rooms/:slug/activity` authorizes the parent room first. It queries a
bounded page of 20 session pointers, reads exact session keys, checks the parent
organization and room again, and returns recorded participation. Live activity
comes from the call, not a saved assignment or an unfinished historical leg.
Pointers follow the current seven-day session retention; no backfill is implied.
The in-memory video store is not durable historical storage.

Dialogue summaries require attendance, matching lobby/session identities and
current recording access through `resolveForUser` and `recording:read`. Removing
a recording participant or deleting its recording also removes summary access
through this view. No summary is synthesized when processing is unavailable.
