// The example instruction — "create a meeting with the platform team and
// create a ppt" — has to become nodes with capabilities behind them, not agent
// steps that describe the work.

import {
  agendaLoopNodeId, agentLoopNodeId, buildAgentLoopDefinition, deckLoopNodeId, meetingLoopNodeId,
  type AgentLoopRequest, type LoopPlan, type LoopStep,
} from '../agentLoopPlanner';
import { MEETING_STEP_KIND } from '../../loop/meetingStepHandler';
import { AGENDA_STEP_KIND } from '../../loop/agendaStepHandler';
import { DECK_STEP_KIND } from '../../deck/DeckStepHandler';

const step = (id: string, kind: LoopStep['kind'], title: string, dependsOn: string[] = []): LoopStep => ({
  id, kind, title, prompt: `do ${title}`, dependsOn, done: `${title} is done`,
});

const request = (instruction: string): AgentLoopRequest => ({
  instruction,
  hints: [],
  contextBudgetTokens: 200_000,
  model: 'sonnet',
  chatChannel: 'chat:room:eng',
  requestedBy: 'dev-connor',
  requestedByName: 'Connor',
});

const plan: LoopPlan = {
  steps: [
    step('s1', 'meeting', 'Platform sync'),
    step('s2', 'agenda', 'Platform sync agenda', ['s1']),
    step('s3', 'deck', 'Sprint review deck', ['s2']),
    step('s4', 'other', 'Send it round', ['s3']),
  ],
} as LoopPlan;

function nodesOf(definition: Record<string, unknown>): Array<{ id: string; type: string; data: Record<string, unknown> }> {
  return definition.nodes as Array<{ id: string; type: string; data: Record<string, unknown> }>;
}

describe('a plan becomes nodes with capabilities behind them', () => {
  const definition = buildAgentLoopDefinition(plan, request('create a meeting with the platform team and create a ppt'));
  const nodes = nodesOf(definition);

  it('gives a meeting step one real node', () => {
    const node = nodes.find((candidate) => candidate.id === meetingLoopNodeId('s1'))!;
    expect(node.type).toBe(MEETING_STEP_KIND);
    expect(node.data).toMatchObject({ type: MEETING_STEP_KIND, title: 'Platform sync', loop: { kind: 'meeting', stepId: 's1' } });
  });

  it('gives an agenda step a draft and a write', () => {
    const draft = nodes.find((candidate) => candidate.id === agentLoopNodeId('s2'))!;
    const write = nodes.find((candidate) => candidate.id === agendaLoopNodeId('s2'))!;
    expect(draft.type).toBe('agent');
    expect(String(draft.data.prompt)).toContain('You draft a meeting agenda');
    expect(draft.data.label).toBe('Draft the agenda for Platform sync agenda');
    expect(write.type).toBe(AGENDA_STEP_KIND);
    expect(write.data).toMatchObject({ agendaFrom: `$.steps.${agentLoopNodeId('s2')}.text`, loop: { kind: 'agenda', role: 'write' } });
  });

  it('leaves the deck pair and the ordinary step as they were', () => {
    expect(nodes.find((candidate) => candidate.id === deckLoopNodeId('s3'))!.type).toBe(DECK_STEP_KIND);
    expect(nodes.find((candidate) => candidate.id === agentLoopNodeId('s4'))!.type).toBe('agent');
  });

  it('chains every node in order', () => {
    expect(nodes.map((node) => node.type)).toEqual([
      'trigger', MEETING_STEP_KIND, 'agent', AGENDA_STEP_KIND, 'agent', DECK_STEP_KIND, 'agent',
    ]);
    const edges = definition.edges as Array<{ source: string; target: string }>;
    expect(edges).toHaveLength(nodes.length - 1);
    expect(edges[0]).toMatchObject({ source: 'trigger', target: meetingLoopNodeId('s1') });
  });
});

describe('the fixture loop runs every kind with no model', () => {
  const nodes = nodesOf(buildAgentLoopDefinition(plan, request('[deck-fixture] the whole loop, locally')));

  it('pins an agenda on the agenda node', () => {
    const write = nodes.find((candidate) => candidate.id === agendaLoopNodeId('s2'))!;
    const agenda = write.data.agenda as { title: string; items: unknown[] };
    expect(agenda.title).toContain('Fixture agenda');
    expect(agenda.items.length).toBeGreaterThan(0);
  });

  it('still pins a spec on the deck node', () => {
    expect(nodes.find((candidate) => candidate.id === deckLoopNodeId('s3'))!.data.spec).toBeDefined();
  });

  it('pins the meeting’s inputs, so nothing about it depends on reading a sentence', () => {
    const node = nodes.find((candidate) => candidate.id === meetingLoopNodeId('s1'))!;
    expect(node.data).toMatchObject({ proposeInMinutes: 24 * 60 });
    expect(node.data.spec).toBeUndefined();
  });

  it('pins the team the instruction names, when it names one', () => {
    const nodesWithTeam = nodesOf(buildAgentLoopDefinition(plan, request('[deck-fixture] create a meeting with the platform team and a ppt')));
    expect(nodesWithTeam.find((candidate) => candidate.id === meetingLoopNodeId('s1'))!.data.team).toBe('platform');
  });
});
