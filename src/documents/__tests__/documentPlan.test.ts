import type { LLMClient, LLMChunk, LLMStreamOptions, StepHandlerHelpers } from 'distributed-core/applications/pipeline';
import { createDocumentPlanHandler, DOCUMENT_PLAN_KIND } from '../documentAgentPipeline';

/** An LLM that answers with what it was given, and records how it was asked. */
function fakeLLM(answer: string, opts: { hang?: boolean } = {}) {
  const calls: Array<{ model: string; prompt: string; opts: LLMStreamOptions }> = [];
  const client: LLMClient = {
    provider: 'fixture',
    stream(model: string, _system: string, prompt: string, o: LLMStreamOptions = {}): AsyncIterable<LLMChunk> {
      calls.push({ model, prompt, opts: o });
      return {
        async *[Symbol.asyncIterator]() {
          if (opts.hang) {
            await new Promise<void>((_, reject) => o.signal?.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError'))));
          }
          for (const ch of answer) yield { done: false, token: ch } as LLMChunk;
          yield { done: true, response: answer, tokensIn: 10, tokensOut: answer.length } as LLMChunk;
        },
      };
    },
  } as unknown as LLMClient;
  return { client, calls };
}

// document:plan never touches the helpers; the StepHandler contract still takes them.
const helpers = { runId: 'run-1', stepId: 'plan', emit: async () => undefined } as unknown as StepHandlerHelpers;
const node = (data: Record<string, unknown>) => ({ id: 'plan', type: DOCUMENT_PLAN_KIND, position: { x: 0, y: 0 }, data: { type: DOCUMENT_PLAN_KIND, ...data } }) as never;

describe('document:plan', () => {
  it('asks once, at temperature 0, with the outline interpolated, and returns the text', async () => {
    const { client, calls } = fakeLLM('[{"op":"setTitle","text":"Q3 plan"}]');
    const h = createDocumentPlanHandler(client);
    const out = await h.execute(node({ prompt: 'Doc:\n{{ context.documentOutline }}\nDo: {{ context.instruction }}', model: 'haiku', temperature: 0 }), {
      documentId: 'd1', documentOutline: 'title: Untitled\n1. hello', instruction: 'add a title',
    }, helpers);
    expect(out.status).toBe('completed');
    expect((out as { output: { text: string; temperature: number } }).output.text).toContain('setTitle');
    expect(calls).toHaveLength(1);
    expect(calls[0].model).toBe('haiku');
    expect(calls[0].opts.temperature).toBe(0);
    expect(calls[0].opts.maxTokens).toBe(1024);
    expect(calls[0].prompt).toContain('1. hello');
    expect(calls[0].prompt).toContain('add a title');
  });

  it('stops at its own bound and says so, rather than waiting on the model', async () => {
    const { client } = fakeLLM('never', { hang: true });
    const h = createDocumentPlanHandler(client);
    const out = await h.execute(node({ prompt: 'x', model: 'haiku', timeoutMs: 50 }), {}, helpers);
    expect(out.status).toBe('failed');
    expect((out as { error: string }).error).toMatch(/timeout after 50ms/);
  });
});
