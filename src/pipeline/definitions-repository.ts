// social-api/src/pipeline/definitions-repository.ts
//
// Wave 1 of the pipeline-definitions persistence migration.
// Replaces the in-memory `stubStore` in routes/pipelineDefinitions.ts with a
// DynamoDB-backed store. Wave 2 will wire the route handlers to call into
// this repository.
//
// Storage shape (table `pipeline-definitions`):
//   PK: userId (S)
//   SK: pipelineId (S)
//   Item:
//     {
//       userId,
//       pipelineId,
//       definition: <full PipelineDefinition>,
//       updatedAt: <ISO string>,
//     }
//
// The route file deliberately keeps an untyped `unknown` view of the
// definition (see TYPES_SYNC.md) so the frontend's `PipelineDefinition`
// type can evolve independently. We mirror that here: `PipelineDefinition`
// is the structural minimum (`id: string`) plus arbitrary extra fields.
//
// All methods are scoped to `userId` so callers cannot accidentally read or
// mutate another user's pipelines.
//
// NOTE: this file is intentionally NOT wired up yet — the route still uses
// the in-memory `stubStore`. Wave 2 will swap it in behind a feature flag.

import {
  DynamoDBDocumentClient,
  QueryCommand,
  ScanCommand,
} from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from '../repositories/BaseRepository';
import { docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';

/**
 * Structural minimum of a pipeline definition. Mirrors the route file's
 * loose contract — anything else is preserved opaquely.
 */
export interface PipelineDefinition {
  id: string;
  [key: string]: unknown;
}

/** Persisted item shape. */
export interface PipelineDefinitionItem {
  userId: string;
  pipelineId: string;
  definition: PipelineDefinition;
  updatedAt: string;
}

const PIPELINE_DEFINITIONS_TABLE = tableName('pipeline-definitions');

export class DefinitionsRepository {
  private store: BaseRepository;

  constructor(private docClient: DynamoDBDocumentClient) {
    this.store = new BaseRepository(PIPELINE_DEFINITIONS_TABLE, docClient);
  }

  /** Fetch a single pipeline definition for a user. Returns null if absent. */
  async get(
    userId: string,
    pipelineId: string,
  ): Promise<PipelineDefinition | null> {
    const item = await this.store.getItem<PipelineDefinitionItem>({
      userId,
      pipelineId,
    });
    return item ? item.definition : null;
  }

  /**
   * List every pipeline definition owned by `userId`. Order is whatever
   * DynamoDB returns by SK (pipelineId) — callers should not rely on it.
   */
  async list(userId: string): Promise<PipelineDefinition[]> {
    const result = await this.docClient.send(
      new QueryCommand({
        TableName: PIPELINE_DEFINITIONS_TABLE,
        KeyConditionExpression: 'userId = :uid',
        ExpressionAttributeValues: { ':uid': userId },
      }),
    );
    const items = (result.Items ?? []) as PipelineDefinitionItem[];
    return items.map((it) => it.definition);
  }

  /**
   * Scan-style enumeration of EVERY pipeline definition across ALL users.
   *
   * Used by cross-user surfaces that must see the complete corpus on each
   * tick:
   *   - the schedule evaluator in `src/index.ts` (ticks once per minute)
   *   - the public webhook router in `src/routes/pipelineWebhooks.ts`
   *     (matches inbound POSTs to a `webhookPath` attached to any user's
   *     definition)
   *
   * Both consumers wrap this in a 60-second in-memory cache so the Scan
   * runs at most once per minute regardless of webhook QPS. Pagination is
   * handled here so the caller never has to think about
   * `LastEvaluatedKey` — at the volumes we expect (single-digit thousands
   * of definitions), one or two Scan pages cover the whole table.
   *
   * NOTE: Scan is O(table) and bills per item read. If the definition
   * count grows past ~10k, swap to a GSI keyed on a constant attribute
   * (e.g. `entityType = "pipeline"`) and Query that instead — same
   * shape, lower cost.
   */
  async listAll(): Promise<PipelineDefinition[]> {
    const out: PipelineDefinition[] = [];
    let lastKey: Record<string, unknown> | undefined;
    do {
      const result = await this.docClient.send(
        new ScanCommand({
          TableName: PIPELINE_DEFINITIONS_TABLE,
          ExclusiveStartKey: lastKey,
        }),
      );
      const items = (result.Items ?? []) as PipelineDefinitionItem[];
      for (const it of items) {
        if (it && it.definition) out.push(it.definition);
      }
      lastKey = result.LastEvaluatedKey as
        | Record<string, unknown>
        | undefined;
    } while (lastKey);
    return out;
  }

  /**
   * Upsert a pipeline definition. `def.id` becomes the SK; callers MUST
   * validate that `def.id === pipelineId` upstream (the route already does).
   */
  async put(userId: string, def: PipelineDefinition): Promise<void> {
    const item: PipelineDefinitionItem = {
      userId,
      pipelineId: def.id,
      definition: def,
      updatedAt: new Date().toISOString(),
    };
    return this.store.putItem(item as unknown as Record<string, unknown>);
  }

  /** Remove a pipeline definition. No-op if absent. */
  async delete(userId: string, pipelineId: string): Promise<void> {
    return this.store.deleteItem({ userId, pipelineId });
  }
}

/** Singleton instance — shares the same docClient as the other repositories. */
export const definitionsRepo = new DefinitionsRepository(docClient);
