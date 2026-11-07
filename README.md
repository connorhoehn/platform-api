# @connorhoehn/platform-api

An Express and TypeScript service that sits in front of a realtime collaboration
platform. It handles call and hangout lobby admission, recordings and their
processing state, document workspace grants, pipeline execution, social features
and chat-command plugins, and stores its data in DynamoDB. It listens on port 3001
by default and exposes `GET /health` for readiness checks.

## What's in the repo

- `src/` - the service. Routes live in `src/routes`, pipeline execution in
  `src/pipeline`, documents and decks in `src/documents` and `src/deck`,
  recordings in `src/recordings`, DynamoDB repositories in `src/repositories`,
  background services in `src/services`, and shared helpers in `src/lib`.
- `test/` - Jest tests that sit outside `src/`, including chaos and Raft
  integration scenarios. Most unit tests sit next to the code in `__tests__` folders.
- `openapi/` - OpenAPI descriptions for the documents, pipelines, social and
  video APIs. `npm run lint:openapi` fails when a route and its description drift.
- `plugins/` - chat-command plugin guide and a runnable example plugin.
- `scripts/` - operational and acceptance scripts, including backfills and
  the OpenAPI coverage lint.
- `observability/` - Grafana panel definitions.
- `docs/` - operator guides and API contract notes (see Documentation below).
- `Dockerfile` - multi-stage image build for the service.
- `dist/` - compiler output from `npm run build`; generated, not committed.

## Getting started

Prerequisites:

- Node.js 20 or newer and npm.
- SSH access to GitHub. Several dependencies are installed from private
  repositories by git reference.
- Redis and DynamoDB Local for development. The `dev` script expects Redis on
  `localhost` and DynamoDB Local at `http://localhost:8000`.

Commands (all defined in `package.json`):

```bash
npm install            # install dependencies
npm run build          # compile TypeScript to dist/
npm run dev            # run from source with auto-restart and local development settings
npm start              # run the compiled service (node dist/index.js)
npm test               # run the Jest suite
npm run typecheck      # type-check without emitting files
npm run lint           # lint src/ and test/
npm run lint:fix       # lint and apply automatic fixes
npm run lint:openapi   # check routes against the OpenAPI descriptions
```

`npm run dev` sets its own local values, including `SKIP_AUTH=true` so requests
need no Cognito token. `SKIP_AUTH` takes effect only beside `INSECURE_DEV_MODE=true`
and never with `NODE_ENV=production`; set without it, the service refuses to start.
The image sets `NODE_ENV=production`, so a local container stack must set
`NODE_ENV=development` too.

Common environment variables:

- `PORT` - HTTP port (default `3001`).
- `REDIS_ENDPOINT` - Redis host used for event pub/sub and coordination.
- `LOCALSTACK_ENDPOINT` or `DYNAMODB_ENDPOINT` - DynamoDB endpoint for local runs.
- `DDB_TABLE_PREFIX` - prefix added to table names, for sharing one DynamoDB instance.
- `AWS_REGION`, `COGNITO_REGION`, `COGNITO_USER_POOL_ID` - region and user pool used to verify tokens.
- `COGNITO_CLIENT_ID` - the app client(s), comma-separated, whose ID tokens are accepted.
  Unset refuses every Cognito token.
- `CORS_ALLOWED_ORIGINS` - comma-separated origins allowed to call with credentials.
  Unset allows none (loopback dev servers only in insecure dev mode).
- `ADMIN_TOKEN` / `VIDEO_ADMIN_TOKEN` - the admin bearer; admin routes answer 503 without it.
  `VIDEO_ADMIN_TOKEN` (or `LVS_ADMIN_TOKEN`) is also the bearer for the video service;
  nothing falls back to a default.
- `MESSAGE_QUEUE_URL` - message queue used for durable event delivery.
- `VIDEO_API_URL` - the video streaming service that backs call sessions.
- `VIDEO_SERVICE_TOKEN` - required for calls. The bearer for the video service's
  `/api/sessions` routes; it must equal the video service's `LVS_SERVICE_TOKEN`.
  Unset refuses every session create, join, refresh and end with a 503 before any
  request is sent.
- `VIDEO_OPS_TOKEN` (or `LVS_OPS_TOKEN`) - the bearer for webhook registration
  (`/api/event-subscriptions`); falls back to `VIDEO_ADMIN_TOKEN` when the video
  service has no `OPS_TOKEN`.
- Video service side: it needs `LVS_SERVICE_TOKEN`, `ADMIN_TOKEN` (also the dashboard
  subscribe) and `LVS_WEBHOOK_ALLOWED_HOSTS` (the host of `LVS_WEBHOOK_BOOTSTRAP_URL`
  when it is private or loopback). Recording media URLs this API returns carry a
  short-lived `?token=` playback token minted per channel with the admin token.
- `PLATFORM_RUNTIME_PROFILE` - `local-ephemeral` (default) or `durable`; the durable
  profile refuses to start when required providers are missing.

Feature-specific variables are described in the guides under `docs/`.

To build the container image, run this from the directory that contains the
`platform-api/` checkout. It forwards your SSH agent so `npm ci` can fetch the
private dependencies:

```bash
DOCKER_BUILDKIT=1 docker build --ssh default -f platform-api/Dockerfile -t platform-api .
```

## Related repositories

This service depends on:

- `connorhoehn/realtime-modules` - realtime gateway, call and document building blocks.
- `connorhoehn/service-runtime` - signed service-to-service requests, logging and health helpers.
- `connorhoehn/pipeline-runtime` - pipeline contracts, the run bridge and approval state machine.
- `connorhoehnslalom/event-catalog` - the event registry and durable event delivery.

It is used by:

- `connorhoehnslalom/aws-agentcore` - runs this service in its local development stack.
- `connorhoehn/realtime-examples` - example applications built on the realtime platform.

## Documentation

- [Document workspace grants](docs/document-workspace-grants.md) - grant-based access to document routes, comments and write locks.
- [Lobby authority](docs/lobby-authority.md) - how call lobby admission asks the owning backend, and how to roll it out.
- [Recording processing](docs/recording-status.md) - the endpoint that reports recording processing, and the live acceptance run.
- [Hangout layouts](docs/hangout-layouts.md) - saved hangout plans, breakouts and call activity.
- [Hangout plan contract](docs/hangout-plan-contract.md) - validation and versioned writes for saved plans.
- [Judge tables](docs/judge-PROVISIONING.md) - DynamoDB tables for the judge pipeline step.
- [Memory tables](docs/memory-PROVISIONING.md) - DynamoDB table for the pipeline memory step.
- [Vision step](docs/vision-PROVISIONING.md) - setup and tables for the image vision step.
- [Chat-command plugins](plugins/README.md) - writing and enabling plugins.
- [Chaos scenarios](test/chaos/CHAOS-SCENARIOS.md) and [Raft read-path tests](test/integration/raft/READ-PATH-TESTS.md) - failure-injection and read-path test specifications.
- [Changelog](CHANGELOG.md)
