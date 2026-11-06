# Saved hangout plan contract

Platform-api and the AgentCore BFF consume the structural contract from
`@connorhoehn/realtime-modules/hangout-rooms`. The host adapter retains
the authenticated actor, clock and HTTP error mapping. Assignment is separate
from private-room membership, attendance and IVS session state.

The shared parser accepts an `expectedVersion`, normalizes room names, validates
50 rooms / 500 unique assignments and rejects an increment beyond the safe
integer range. Caller-supplied audit fields do not survive parsing. The route
checks ownership and private-room access; `HangoutRoomRepository.updateRoom`
compares the previous version in the actual DynamoDB condition. The first save
requires no existing plan. A stale save cannot change accompanying room fields.

`src/lib/__tests__/fixtures/hangout-plan-cases.json` is the shared test corpus and
must stay byte-identical to the copies in realtime-modules and the AgentCore BFF.
`HangoutPlan.dynamo.test.ts` uses two independent DynamoDB clients to race first
and subsequent saves and checks that missing/cross-tenant rows are not created.
Run it against an owned local DynamoDB instance with
`HANGOUT_PLAN_ACCEPTANCE_ENDPOINT=http://127.0.0.1:<port>` and a unique
`DDB_TABLE_PREFIX=<unique>_`; it creates and deletes only its namespaced table.
