# Staged local recording follow-up

This source patch preserves the tested recording access/timestamp work prepared on October 10, 2026. It is a candidate, **not activated** in the local service. Apply only during a coordinated idle window after preserving sessions, signing keys, original media, and current service configuration.

The patch applies to this checkout with `git apply --check docs/work-huddle-candidates/recording-access-and-timestamps.patch`. No environment files, recordings, or credentials are included. Platform and LVS recording access changes must be integrated together; do not activate only one side.

Private runtime evidence remains in the aws-agentcore checkout under `docs/demos/work-huddles/evidence/2026-10-10/`. Recording access verification covered 98 platform checks, 38 native access checks, and 14 native UI checks. Whole native UI build still has pre-existing dependency/type errors. Real transcript regeneration and service activation are not claimed by this handoff.
