// Local-dev-only DynamoDB table-name prefix (see
// .planning/SHARED-SERVICES-MIGRATION.md). When multiple project agents
// share one DynamoDB-local instance via the agent-hub shared services,
// each project sets DDB_TABLE_PREFIX to namespace its tables and avoid
// collisions. Production leaves the env var unset; this helper returns
// the bare name unchanged in that case.

export const tableName = (base: string): string =>
  `${process.env.DDB_TABLE_PREFIX ?? ''}${base}`;
