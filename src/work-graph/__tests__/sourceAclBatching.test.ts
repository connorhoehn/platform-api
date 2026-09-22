// Authorizing a day's graph reads the source rows behind every node. It used
// to read them one node at a time — and for a run-backed node that read is a
// FILTERED SCAN, which walks the table to find one row. A hundred-node day
// therefore issued hundreds of sequential scans and took tens of seconds.
//
// These tests pin the read count, not the wall clock: one query per table per
// pass, whatever the graph's size.

jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import { BatchGetCommand, ScanCommand, GetCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DynamoSourceAclAdapter } from '../runtime';
import type { WorkAclTarget, WorkGraphPolicyScope } from '../policy';
import type { StoredWorkNode } from '../../repositories/WorkGraphRepository';

const scope: WorkGraphPolicyScope = {
  organizationId: 'dev-org',
  viewerId: 'dev-frank',
  personId: 'dev-frank',
  day: '2026-09-22',
  timezone: 'America/New_York',
  policyRevision: 'policy-7',
};

function node(id: string, kind: StoredWorkNode['kind'], source: StoredWorkNode['sourceRef']['source'], resourceId: string): WorkAclTarget {
  return {
    kind: 'node',
    id,
    policyRef: `${source}:${kind}`,
    sourceRef: { source, resourceId },
    node: {
      id,
      organizationId: 'dev-org',
      actorId: 'dev-frank',
      kind,
      sourceRef: { source, resourceId },
      policyRef: `${source}:${kind}`,
      title: id,
      status: 'completed',
      startedAt: '2026-09-22T13:00:00.000Z',
      updatedAt: '2026-09-22T13:00:00.000Z',
      sourceEventId: `event-${id}`,
      revision: 1,
    },
  } as unknown as WorkAclTarget;
}

interface Counts { scan: number; batchGet: number; get: number }

function client(rows: Record<string, unknown>[]): { client: DynamoDBDocumentClient; counts: Counts } {
  const counts: Counts = { scan: 0, batchGet: 0, get: 0 };
  const send = async (command: unknown) => {
    if (command instanceof ScanCommand) {
      counts.scan += 1;
      return { Items: rows };
    }
    if (command instanceof BatchGetCommand) {
      counts.batchGet += 1;
      const input = (command as unknown as { input: { RequestItems: Record<string, { Keys: Array<{ documentId: string }> }> } }).input;
      const table = Object.keys(input.RequestItems)[0]!;
      const wanted = new Set(input.RequestItems[table]!.Keys.map((key) => key.documentId));
      return { Responses: { [table]: rows.filter((row) => wanted.has(String(row.documentId))) } };
    }
    if (command instanceof GetCommand) {
      counts.get += 1;
      // Conversation membership.
      return { Item: { channel: 'c', userId: 'dev-frank' } };
    }
    return {};
  };
  return { client: { send } as unknown as DynamoDBDocumentClient, counts };
}

describe('source ACL authorization reads', () => {
  it('authorizes a run, its steps and its workspaces with one scan', async () => {
    const rows = [
      { runId: 'run-9', pipelineId: 'p', triggeredBy: 'dev-frank' },
      { runId: 'run-12', pipelineId: 'p', triggeredBy: 'dev-frank' },
    ];
    const { client: ddb, counts } = client(rows);
    const targets = [
      node('n1', 'run', 'pipeline', 'run-9'),
      node('n2', 'run', 'pipeline', 'run-9::sources'),
      node('n3', 'run', 'pipeline', 'run-9::deck'),
      node('n4', 'run', 'pipeline', 'run-9::session-a'),
      node('n5', 'run', 'pipeline', 'run-12'),
      node('n6', 'run', 'pipeline', 'run-12::deck'),
    ];

    const decisions = await new DynamoSourceAclAdapter('pipeline', ddb).authorizeDisclosure(scope, targets);
    expect(counts.scan).toBe(1);
    expect([...decisions.values()].every((decision) => decision.decision === 'allow')).toBe(true);
    expect(decisions.get('n3')?.maximumDisclosure).toBe('details');
  });

  it('still refuses a run no row has, and one two rows both claim', async () => {
    const { client: ddb } = client([
      { runId: 'run-twice', pipelineId: 'p', triggeredBy: 'dev-frank' },
      { runId: 'run-twice', pipelineId: 'p2', triggeredBy: 'dev-frank' },
    ]);
    const decisions = await new DynamoSourceAclAdapter('pipeline', ddb).authorizeDisclosure(scope, [
      node('n1', 'run', 'pipeline', 'run-twice'),
      node('n2', 'run', 'pipeline', 'run-missing'),
    ]);
    expect(decisions.get('n1')?.decision).toBe('deny');
    expect(decisions.get('n2')?.decision).toBe('deny');
  });

  it('reads every document of a day in one batch, and each channel once', async () => {
    const rows = [
      { documentId: 'doc-a', createdBy: 'dev-frank' },
      { documentId: 'doc-b', createdBy: 'dev-connor', channel: 'c' },
    ];
    const { client: ddb, counts } = client(rows);
    const targets = [
      node('d1', 'document', 'document', 'doc-a'),
      node('d2', 'change', 'document', 'doc-a:v1'),
      node('d3', 'change', 'document', 'doc-a:v2'),
      node('d4', 'document', 'document', 'doc-b'),
      node('d5', 'change', 'document', 'doc-b:v1'),
    ];

    const decisions = await new DynamoSourceAclAdapter('document', ddb).authorizeDisclosure(scope, targets);
    expect(counts.batchGet).toBe(1);
    // `doc-b` is someone else's, shared through one channel: asked once for the
    // owner and once for the viewer, not once per revision node.
    expect(counts.get).toBeLessThanOrEqual(2);
    expect(decisions.get('d1')?.decision).toBe('allow');
    expect(decisions.get('d3')?.maximumDisclosure).toBe('details');
  });

  it('authorizes a meeting, its recording and its transcript with one scan', async () => {
    const rows = [{
      callId: 'call-1',
      recordingId: 'rec-1',
      transcriptId: 'tr-1',
      startedBy: 'dev-frank',
      participants: [{ userId: 'dev-frank' }],
    }];
    const { client: ddb, counts } = client(rows);
    const decisions = await new DynamoSourceAclAdapter('meeting', ddb).authorizeDisclosure(scope, [
      node('m1', 'meeting', 'meeting', 'meeting:call-1'),
      node('m2', 'transcript', 'meeting', 'transcript:tr-1'),
    ]);
    expect(counts.scan).toBe(1);
    expect(decisions.get('m1')?.decision).toBe('allow');
    expect(decisions.get('m2')?.capabilities).toEqual(['view-transcript']);
  });
});
