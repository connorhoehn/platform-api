// social-api/src/pipeline/__tests__/PipelineRaftStateMachine.test.ts
//
// Unit tests for `PipelineRaftStateMachine` — verifies the IRaftStateMachine
// contract (apply / applyBatch / lastAppliedIndex / lastAppliedTerm /
// exportSnapshot / importSnapshot) plus the per-command idempotency +
// terminal-state-sticky semantics.
//
// We do NOT spin up a `RaftNode` here — the SM is a pure data structure
// when isolated from the consensus engine, which is exactly the surface
// we want to pin.

import {
  PipelineRaftStateMachine,
  defaultPipelineRaftClusterConfig,
  snapshotFromJSON,
  snapshotToJSON,
  type PipelineRunCommand,
  type PipelineRaftSnapshotState,
} from '../PipelineRaftStateMachine';
import type { RaftLogEntry } from 'distributed-core';

function entry<T>(index: number, term: number, command: T): RaftLogEntry<T> {
  return { index, term, command };
}

describe('PipelineRaftStateMachine', () => {
  const NODE = 'node-A';

  describe('apply()', () => {
    test('run-started creates a slot in "running"', () => {
      const sm = new PipelineRaftStateMachine(defaultPipelineRaftClusterConfig(NODE));
      const result = sm.apply(
        entry<PipelineRunCommand>(1, 1, {
          type: 'run-started',
          runId: 'r-1',
          pipelineId: 'p-1',
          pipelineVersion: 3,
          ownerNodeId: NODE,
          startedAt: '2026-04-29T10:00:00Z',
        }),
      );

      expect(result).toEqual({ runId: 'r-1', status: 'running', applied: 'created' });
      expect(sm.lastAppliedIndex).toBe(1);
      expect(sm.lastAppliedTerm).toBe(1);
      const slot = sm.getRun('r-1');
      expect(slot).toBeDefined();
      expect(slot!.status).toBe('running');
      expect(slot!.pipelineId).toBe('p-1');
      expect(slot!.pipelineVersion).toBe(3);
      expect(slot!.ownerNodeId).toBe(NODE);
      expect(slot!.lastEvent).toBe('run-started');
      expect(slot!.lastAppliedIndex).toBe(1);
    });

    test('replaying the SAME index is a no-op (idempotency)', () => {
      const sm = new PipelineRaftStateMachine(defaultPipelineRaftClusterConfig(NODE));
      const cmd: PipelineRunCommand = {
        type: 'run-started',
        runId: 'r-2',
        pipelineId: 'p-2',
        pipelineVersion: 1,
        ownerNodeId: NODE,
        startedAt: '2026-04-29T10:00:00Z',
      };
      sm.apply(entry(5, 1, cmd));
      const replay = sm.apply(entry(5, 1, cmd));

      // apply() returns TResult | null — null only for built-ins. Our
      // user commands always echo a result. The non-null assertion
      // documents the contract for the reader.
      expect(replay).not.toBeNull();
      expect(replay!.applied).toBe('idempotent-skip');
      expect(sm.size()).toBe(1);
      expect(sm.lastAppliedIndex).toBe(5);
    });

    test('run-updated transitions through awaiting_approval → running', () => {
      const sm = new PipelineRaftStateMachine(defaultPipelineRaftClusterConfig(NODE));
      sm.apply(
        entry<PipelineRunCommand>(1, 1, {
          type: 'run-started',
          runId: 'r-3',
          pipelineId: 'p-3',
          pipelineVersion: 1,
          ownerNodeId: NODE,
          startedAt: '2026-04-29T10:00:00Z',
        }),
      );

      const r1 = sm.apply(
        entry<PipelineRunCommand>(2, 1, {
          type: 'run-updated',
          runId: 'r-3',
          status: 'awaiting_approval',
          eventName: 'pipeline:run:awaiting_approval',
          at: '2026-04-29T10:01:00Z',
        }),
      );
      expect(r1).not.toBeNull();
      expect(r1!.applied).toBe('updated');
      expect(sm.getRun('r-3')!.status).toBe('awaiting_approval');

      const r2 = sm.apply(
        entry<PipelineRunCommand>(3, 1, {
          type: 'run-updated',
          runId: 'r-3',
          status: 'running',
          eventName: 'pipeline:run:resumed',
          at: '2026-04-29T10:02:00Z',
        }),
      );
      expect(r2).not.toBeNull();
      expect(r2!.applied).toBe('updated');
      expect(sm.getRun('r-3')!.status).toBe('running');
    });

    test('run-terminated is sticky — subsequent updates do NOT transition out of terminal', () => {
      const sm = new PipelineRaftStateMachine(defaultPipelineRaftClusterConfig(NODE));
      sm.apply(
        entry<PipelineRunCommand>(1, 1, {
          type: 'run-started',
          runId: 'r-4',
          pipelineId: 'p-4',
          pipelineVersion: 1,
          ownerNodeId: NODE,
          startedAt: '2026-04-29T10:00:00Z',
        }),
      );
      sm.apply(
        entry<PipelineRunCommand>(2, 1, {
          type: 'run-terminated',
          runId: 'r-4',
          status: 'completed',
          at: '2026-04-29T10:05:00Z',
        }),
      );
      expect(sm.getRun('r-4')!.status).toBe('completed');

      // A later 'run-updated' command with status='running' must NOT
      // overwrite the terminal status.
      const stuck = sm.apply(
        entry<PipelineRunCommand>(3, 1, {
          type: 'run-updated',
          runId: 'r-4',
          status: 'running',
          eventName: 'spurious-update',
          at: '2026-04-29T10:06:00Z',
        }),
      );
      expect(stuck).not.toBeNull();
      expect(stuck!.applied).toBe('idempotent-skip');
      expect(sm.getRun('r-4')!.status).toBe('completed');

      // A second terminate is also rejected (terminal is sticky to the
      // FIRST terminal status).
      const second = sm.apply(
        entry<PipelineRunCommand>(4, 1, {
          type: 'run-terminated',
          runId: 'r-4',
          status: 'failed',
          at: '2026-04-29T10:07:00Z',
        }),
      );
      expect(second).not.toBeNull();
      expect(second!.applied).toBe('idempotent-skip');
      expect(sm.getRun('r-4')!.status).toBe('completed');
    });

    test('run-checkpointed records the latest checkpoint blob', () => {
      const sm = new PipelineRaftStateMachine(defaultPipelineRaftClusterConfig(NODE));
      sm.apply(
        entry<PipelineRunCommand>(1, 1, {
          type: 'run-started',
          runId: 'r-5',
          pipelineId: 'p-5',
          pipelineVersion: 1,
          ownerNodeId: NODE,
          startedAt: '2026-04-29T10:00:00Z',
        }),
      );
      sm.apply(
        entry<PipelineRunCommand>(2, 1, {
          type: 'run-checkpointed',
          runId: 'r-5',
          checkpoint: { currentStepIds: ['step-3'], emitted: 7 },
          at: '2026-04-29T10:01:00Z',
        }),
      );
      expect(sm.getRun('r-5')!.checkpoint).toEqual({
        currentStepIds: ['step-3'],
        emitted: 7,
      });

      sm.apply(
        entry<PipelineRunCommand>(3, 1, {
          type: 'run-checkpointed',
          runId: 'r-5',
          checkpoint: { currentStepIds: ['step-4'], emitted: 12 },
          at: '2026-04-29T10:02:00Z',
        }),
      );
      expect(sm.getRun('r-5')!.checkpoint).toEqual({
        currentStepIds: ['step-4'],
        emitted: 12,
      });
    });

    test('emits domain events on each command type', () => {
      const sm = new PipelineRaftStateMachine(defaultPipelineRaftClusterConfig(NODE));
      const events: string[] = [];
      sm.on('pipeline:run:started', () => events.push('started'));
      sm.on('pipeline:run:updated', () => events.push('updated'));
      sm.on('pipeline:run:checkpointed', () => events.push('checkpointed'));
      sm.on('pipeline:run:terminated', () => events.push('terminated'));

      sm.apply(
        entry<PipelineRunCommand>(1, 1, {
          type: 'run-started',
          runId: 'r-6',
          pipelineId: 'p-6',
          pipelineVersion: 1,
          ownerNodeId: NODE,
          startedAt: '2026-04-29T10:00:00Z',
        }),
      );
      sm.apply(
        entry<PipelineRunCommand>(2, 1, {
          type: 'run-updated',
          runId: 'r-6',
          status: 'awaiting_approval',
          eventName: 'pipeline:run:awaiting_approval',
          at: '2026-04-29T10:01:00Z',
        }),
      );
      sm.apply(
        entry<PipelineRunCommand>(3, 1, {
          type: 'run-checkpointed',
          runId: 'r-6',
          checkpoint: { foo: 1 },
          at: '2026-04-29T10:02:00Z',
        }),
      );
      sm.apply(
        entry<PipelineRunCommand>(4, 1, {
          type: 'run-terminated',
          runId: 'r-6',
          status: 'completed',
          at: '2026-04-29T10:03:00Z',
        }),
      );

      expect(events).toEqual(['started', 'updated', 'checkpointed', 'terminated']);
    });

    test('built-in "noop" command updates lastAppliedIndex without affecting runs', () => {
      const sm = new PipelineRaftStateMachine(defaultPipelineRaftClusterConfig(NODE));
      // Cast through unknown — the IRaftStateMachine contract accepts the
      // built-in command shape via the base class, which intercepts before
      // the typed `applyCommand`.
      const noop = { type: 'noop' } as unknown as PipelineRunCommand;
      sm.apply(entry(7, 2, noop));
      expect(sm.lastAppliedIndex).toBe(7);
      expect(sm.lastAppliedTerm).toBe(2);
      expect(sm.size()).toBe(0);
    });

    test('built-in "config-change" command updates the cluster config', () => {
      const sm = new PipelineRaftStateMachine(defaultPipelineRaftClusterConfig(NODE));
      const configChange = {
        type: 'config-change',
        config: { members: ['node-A', 'node-B', 'node-C'] },
      } as unknown as PipelineRunCommand;
      sm.apply(entry(11, 3, configChange));
      expect(sm.clusterConfig.members).toEqual(['node-A', 'node-B', 'node-C']);
      expect(sm.lastAppliedIndex).toBe(11);
      expect(sm.lastAppliedTerm).toBe(3);
    });
  });

  describe('applyBatch()', () => {
    test('replays a sequence of entries and ends in the right state', () => {
      const sm = new PipelineRaftStateMachine(defaultPipelineRaftClusterConfig(NODE));
      const entries: RaftLogEntry<PipelineRunCommand>[] = [
        entry(1, 1, {
          type: 'run-started',
          runId: 'r-batch-1',
          pipelineId: 'p-batch',
          pipelineVersion: 1,
          ownerNodeId: NODE,
          startedAt: '2026-04-29T10:00:00Z',
        }),
        entry(2, 1, {
          type: 'run-checkpointed',
          runId: 'r-batch-1',
          checkpoint: { step: 'a' },
          at: '2026-04-29T10:01:00Z',
        }),
        entry(3, 1, {
          type: 'run-terminated',
          runId: 'r-batch-1',
          status: 'completed',
          at: '2026-04-29T10:02:00Z',
        }),
      ];
      sm.applyBatch(entries);
      expect(sm.lastAppliedIndex).toBe(3);
      expect(sm.getRun('r-batch-1')!.status).toBe('completed');
      expect(sm.getRun('r-batch-1')!.checkpoint).toEqual({ step: 'a' });
    });
  });

  describe('exportSnapshot / importSnapshot', () => {
    test('roundtrips through exportRaftSnapshot + JSON', () => {
      const sm1 = new PipelineRaftStateMachine(defaultPipelineRaftClusterConfig(NODE));
      sm1.apply(
        entry<PipelineRunCommand>(1, 1, {
          type: 'run-started',
          runId: 'r-snap',
          pipelineId: 'p-snap',
          pipelineVersion: 2,
          ownerNodeId: NODE,
          startedAt: '2026-04-29T10:00:00Z',
        }),
      );
      sm1.apply(
        entry<PipelineRunCommand>(2, 1, {
          type: 'run-checkpointed',
          runId: 'r-snap',
          checkpoint: { phase: 'mid' },
          at: '2026-04-29T10:01:00Z',
        }),
      );

      // exportPipelineRaftSnapshot() narrows the envelope's `state` to
      // PipelineRaftSnapshotState — the base's `exportRaftSnapshot()`
      // defaults to RaftSnapshot<EntitySnapshot>, which doesn't fit our
      // user state without a cast.
      const envelope = sm1.exportPipelineRaftSnapshot();
      expect(envelope.lastIncludedIndex).toBe(2);
      expect(envelope.lastIncludedTerm).toBe(1);
      const userState: PipelineRaftSnapshotState = envelope.state;
      expect(userState.version).toBe(1);
      expect(userState.runs['r-snap']).toBeDefined();

      // JSON roundtrip — proves the SM state is JSON-serializable.
      const json = snapshotToJSON(envelope);
      const parsed = snapshotFromJSON(json);

      const sm2 = new PipelineRaftStateMachine(defaultPipelineRaftClusterConfig('node-Z'));
      sm2.importPipelineRaftSnapshot(parsed);

      expect(sm2.lastAppliedIndex).toBe(2);
      expect(sm2.lastAppliedTerm).toBe(1);
      expect(sm2.getRun('r-snap')).toEqual(sm1.getRun('r-snap'));
    });

    test('importSnapshot rejects a payload with the wrong shape', () => {
      const sm = new PipelineRaftStateMachine(defaultPipelineRaftClusterConfig(NODE));
      // Cast through `unknown` — we're DELIBERATELY feeding a malformed
      // payload to verify the version-check guard, so the type
      // mismatch is the point of the test.
      const bad = {
        lastIncludedIndex: 0,
        lastIncludedTerm: 0,
        state: { version: 999, runs: {} },
        clusterConfig: { members: [NODE] },
        sessions: {},
      } as unknown as Parameters<typeof sm.importPipelineRaftSnapshot>[0];
      expect(() => sm.importPipelineRaftSnapshot(bad)).toThrow(
        /not a PipelineRaftSnapshotState/,
      );
    });
  });

  describe('IRaftStateMachine contract', () => {
    test('exposes lastAppliedIndex / lastAppliedTerm / clusterConfig getters', () => {
      const sm = new PipelineRaftStateMachine(defaultPipelineRaftClusterConfig(NODE));
      expect(sm.lastAppliedIndex).toBe(0);
      expect(sm.lastAppliedTerm).toBe(0);
      expect(sm.clusterConfig.members).toEqual([NODE]);
    });
  });
});
