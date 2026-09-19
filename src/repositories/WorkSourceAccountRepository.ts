import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';
import type { WorkSourceKind } from '@connorhoehn/realtime-modules/work-graph';
import { tableName } from '../lib/ddb-table-name';

const WORK_GRAPH_TABLE = tableName('work-graph');
const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:@/-]*$/;

export interface WorkSourceAccountMapping {
  source: WorkSourceKind;
  producerServiceId: string;
  producerOrganizationId: string;
  sourceSubject: string;
  organizationId: string;
  actorId: string;
  revision: number;
  updatedAt: string;
}

export class WorkSourceAccountConflictError extends Error {
  constructor(readonly expectedRevision: number | null) {
    super('Work source account mapping no longer has the expected revision.');
    this.name = 'WorkSourceAccountConflictError';
  }
}

function validId(value: string): boolean {
  return value.length > 0 && value.length <= 128 && ID_PATTERN.test(value);
}

function partition(source: WorkSourceKind, producerOrganizationId: string): string {
  return `WGACCOUNT#${source}#${producerOrganizationId}`;
}

function sortKey(sourceSubject: string): string {
  return `SUBJECT#${sourceSubject}`;
}

function conditionalFailure(error: unknown): boolean {
  return (error as { name?: string })?.name === 'ConditionalCheckFailedException';
}

function parseMapping(value: unknown): WorkSourceAccountMapping | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const item = value as Record<string, unknown>;
  const mapping = item.mapping;
  if (!mapping || typeof mapping !== 'object' || Array.isArray(mapping)) return null;
  const candidate = mapping as Record<string, unknown>;
  const source = candidate.source;
  if (!['cloud-compute', 'local-compute', 'document', 'pipeline', 'conversation', 'meeting'].includes(String(source))) return null;
  const stringFields = [
    candidate.producerServiceId,
    candidate.producerOrganizationId,
    candidate.sourceSubject,
    candidate.organizationId,
    candidate.actorId,
  ];
  if (!stringFields.every((field) => typeof field === 'string' && validId(field))) return null;
  if (!Number.isSafeInteger(candidate.revision) || Number(candidate.revision) < 1) return null;
  if (typeof candidate.updatedAt !== 'string' || Number.isNaN(Date.parse(candidate.updatedAt))) return null;
  const parsed = candidate as unknown as WorkSourceAccountMapping;
  if (item.PK !== partition(parsed.source, parsed.producerOrganizationId)
    || item.SK !== sortKey(parsed.sourceSubject)
    || item.entityType !== 'work-source-account') return null;
  return { ...parsed };
}

/**
 * Server-owned join from a producer's authenticated subject to a platform
 * actor. Ingest never trusts actorId or organizationId copied into an event.
 */
export class WorkSourceAccountRepository {
  constructor(
    private readonly docClient: DynamoDBDocumentClient,
    private readonly table: string = WORK_GRAPH_TABLE,
  ) {}

  async resolve(
    source: WorkSourceKind,
    producerServiceId: string,
    producerOrganizationId: string,
    sourceSubject: string,
  ): Promise<Pick<WorkSourceAccountMapping, 'organizationId' | 'actorId'> | null> {
    if (![producerServiceId, producerOrganizationId, sourceSubject].every(validId)) return null;
    const result = await this.docClient.send(new GetCommand({
      TableName: this.table,
      Key: { PK: partition(source, producerOrganizationId), SK: sortKey(sourceSubject) },
      ConsistentRead: true,
    }));
    const mapping = parseMapping(result.Item);
    if (!mapping
      || mapping.source !== source
      || mapping.producerServiceId !== producerServiceId
      || mapping.producerOrganizationId !== producerOrganizationId
      || mapping.sourceSubject !== sourceSubject) return null;
    return { organizationId: mapping.organizationId, actorId: mapping.actorId };
  }

  async put(mapping: WorkSourceAccountMapping, expectedRevision: number | null): Promise<void> {
    if (!parseMapping({
      PK: partition(mapping.source, mapping.producerOrganizationId),
      SK: sortKey(mapping.sourceSubject),
      entityType: 'work-source-account',
      mapping,
    })) throw new RangeError('Invalid work source account mapping.');
    if (expectedRevision === null ? mapping.revision !== 1 : mapping.revision !== expectedRevision + 1) {
      throw new RangeError('Work source account mapping revision must advance exactly once.');
    }
    try {
      await this.docClient.send(new PutCommand({
        TableName: this.table,
        Item: {
          PK: partition(mapping.source, mapping.producerOrganizationId),
          SK: sortKey(mapping.sourceSubject),
          entityType: 'work-source-account',
          mapping,
        },
        ConditionExpression: expectedRevision === null
          ? 'attribute_not_exists(PK) AND attribute_not_exists(SK)'
          : 'attribute_exists(PK) AND attribute_exists(SK) AND #mapping.#revision = :expected',
        ...(expectedRevision === null ? {} : {
          ExpressionAttributeNames: { '#mapping': 'mapping', '#revision': 'revision' },
          ExpressionAttributeValues: { ':expected': expectedRevision },
        }),
      }));
    } catch (error) {
      if (conditionalFailure(error)) throw new WorkSourceAccountConflictError(expectedRevision);
      throw error;
    }
  }
}
