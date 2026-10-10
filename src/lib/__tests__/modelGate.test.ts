// The model gate every caller goes through (realtime-examples NFR Loop 39):
// an open breaker fails fast with the reason, each purpose has its own pool,
// identical input shares one call, and a planner that finds the model
// unavailable fails its loop instead of falling back. No Redis here; the
// fleet-wide check is the Loop 39 2-replica run in docs/design/nfr.
jest.mock('../redis-client', () => ({ getRedisCacheClient: async () => null, getRedisClient: async () => null }));
import { FixtureLLMClient } from 'distributed-core/applications/pipeline';
import { gatedAgentClient, gatedLLMClient, gateStream, resetModelGate, withModelGate } from '../modelGate';
import { forceBreaker, ModelUnavailableError, resetModelHealth, runModelProbe } from '../modelHealth';
import { resetModelSlots } from '../../deck/modelSlots';
import { planAgentLoop, type AgentLoopRequest } from '../../chat/agentLoopPlanner';
import type { AgentClient, AgentEvent } from '../../pipeline/agent/types';

const tick = (ms: number) => new Promise((r) => setTimeout(r, ms));
const collect = async <E>(it: AsyncIterable<E>) => { const out: E[] = []; for await (const e of it) out.push(e); return out; };

describe('model gate', () => {
  const env = { ...process.env };
  beforeEach(async () => { resetModelHealth(); resetModelSlots(); resetModelGate(); await forceBreaker(null); });
  afterEach(() => { process.env = { ...env }; });

  it('fails fast with "Model unavailable: <reason>" while the breaker is forced open, without calling the model', async () => {
    await forceBreaker('test: forced');
    let called = false;
    const started = Date.now();
    await expect(withModelGate('verdict', async () => { called = true; return 1; })).rejects.toThrow('Model unavailable: test: forced');
    expect(Date.now() - started).toBeLessThan(1000);
    expect(called).toBe(false);
    const llm = gatedLLMClient(new FixtureLLMClient(['hi']), 'drafter');
    await expect(collect(llm.stream('m', '', 'p'))).rejects.toBeInstanceOf(ModelUnavailableError);
  });

  it('forwards an explicit provider scope while retaining the global operator stop', async () => {
    process.env.PIPELINE_LLM_PROVIDER = 'claude-code';
    await runModelProbe({ env: { PIPELINE_LLM_PROVIDER: 'claude-code' }, readCredentials: async () => null,
      lookup: async () => undefined, connect: async () => undefined });
    const client = gatedLLMClient(new FixtureLLMClient(['summary']), 'pipeline', { breakerScope: 'codex' });
    expect((await collect(client.stream('', '', 'speech'))).at(-1)).toMatchObject({ response: 'summary' });
    await forceBreaker('operator stop');
    await expect(collect(client.stream('', '', 'speech'))).rejects.toThrow('operator stop');
  });

  it('streams through unchanged when closed', async () => {
    const llm = gatedLLMClient(new FixtureLLMClient(['hey']), 'pipeline');
    const chunks = await collect(llm.stream('m', '', 'p'));
    expect(chunks.at(-1)).toMatchObject({ done: true, response: 'hey' });
  });

  it('keeps pools apart: a full deck pool does not hold up the planner', async () => {
    process.env.DECK_MODEL_CONCURRENCY = '1';
    process.env.MODEL_CONCURRENCY_PLANNER = '1';
    let release!: () => void;
    const deck = withModelGate('deck', () => new Promise<string>((r) => { release = () => r('deck'); }));
    await tick(10);
    const deckQueued = withModelGate('deck', async () => 'deck-2');
    const planner = await withModelGate('planner', async () => 'planned');
    expect(planner).toBe('planned');
    release();
    await expect(deck).resolves.toBe('deck');
    await expect(deckQueued).resolves.toBe('deck-2');
  });

  it('caps each pool at its limit', async () => {
    process.env.MODEL_CONCURRENCY_VERDICT = '2';
    let inFlight = 0; let peak = 0;
    await Promise.all([0, 1, 2, 3, 4].map(() => withModelGate('verdict', async () => {
      inFlight += 1; peak = Math.max(peak, inFlight); await tick(15); inFlight -= 1;
    })));
    expect(peak).toBe(2);
  });

  it('shares one call between identical requests (single-flight)', async () => {
    let calls = 0;
    const inner: AgentClient = {
      provider: 'test',
      async *run() { calls += 1; await tick(20); yield { type: 'done', text: 'plan', turns: 1, tokensIn: 0, tokensOut: 0, model: 'test', stopReason: 'complete', toolCalls: [] } satisfies AgentEvent; },
    };
    const client = gatedAgentClient(inner, 'planner', { singleFlight: true });
    const [a, b] = await Promise.all([collect(client.run('same')), collect(client.run('same'))]);
    expect(calls).toBe(1);
    expect(a).toEqual(b);
    await collect(client.run('different'));
    expect(calls).toBe(2);
  });

  it('the planner reports the model as unavailable instead of a single-step fallback', async () => {
    await forceBreaker('test: forced');
    const inner: AgentClient = { provider: 'test', async *run() { yield { type: 'done', text: '{}', turns: 1, tokensIn: 0, tokensOut: 0, model: 'test', stopReason: 'complete', toolCalls: [] } satisfies AgentEvent; } };
    const request = { instruction: 'make a plan', hints: [], sources: [], chatChannel: 'c', requestedBy: 'u' } as unknown as AgentLoopRequest;
    const plan = await planAgentLoop(request, gatedAgentClient(inner, 'planner'));
    expect(plan.unavailable).toBe('Model unavailable: test: forced');
    expect(plan.source).toBe('fallback');
  });

  it('does not count a slot wait against the breaker', async () => {
    process.env.MODEL_CONCURRENCY_OTHER = '1';
    process.env.MODEL_SLOT_WAIT_MS_OTHER = '20';
    let release!: () => void;
    const hold = withModelGate('other', () => new Promise<void>((r) => { release = r; }));
    await tick(5);
    await expect(collect(gateStream('other', async function* () { yield 1; }))).rejects.toThrow('no model slot');
    release();
    await hold;
    await expect(withModelGate('other', async () => 'ok')).resolves.toBe('ok');
  });
});
