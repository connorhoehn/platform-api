import { buildAutomationDefinition, automationOf, nextRunAt, validateAutomationInput, DOCUMENT_AUTOMATION_PREFIX } from '../documentAutomations';

describe('document automations', () => {
  const input = { documentId: 'doc-1234567890', instruction: 'Add a summary at the top', cron: '0 9 * * 1-5', userId: 'u-carol', userName: 'Carol' };

  test('is a published pipeline with a schedule trigger and the document pinned to its runs', () => {
    const def = buildAutomationDefinition(input);
    expect(String(def.id).startsWith(DOCUMENT_AUTOMATION_PREFIX)).toBe(true);
    expect(def.status).toBe('published');
    const trigger = (def.nodes as Array<{ id: string; data: Record<string, unknown> }>).find((n) => n.id === 'trigger')!;
    expect(trigger.data).toMatchObject({ triggerType: 'schedule', schedule: '0 9 * * 1-5' });
    expect(def.triggerBinding).toMatchObject({ event: 'schedule', schedule: '0 9 * * 1-5', payload: { documentId: 'doc-1234567890', instruction: 'Add a summary at the top', requestedBy: 'u-carol' } });
    // The rest of the chain is the /doc chain, in suggest mode.
    const ids = (def.nodes as Array<{ id: string }>).map((n) => n.id);
    expect(ids).toEqual(['trigger', 'read', 'plan', 'review', 'approve', 'apply']);
    expect(automationOf(def)).toMatchObject({ documentId: 'doc-1234567890', instruction: 'Add a summary at the top', cron: '0 9 * * 1-5', createdBy: 'u-carol' });
    expect(automationOf({ id: 'other', nodes: [] })).toBeNull();
  });

  test('says when it fires next', () => {
    const from = new Date('2026-09-15T08:30:00');
    expect(nextRunAt('0 9 * * *', from)).toBe(new Date('2026-09-15T09:00:00').toISOString());
    expect(nextRunAt('*/5 * * * *', from)).toBe(new Date('2026-09-15T08:35:00').toISOString());
    expect(nextRunAt('not a cron', from)).toBeNull();
  });

  test('refuses what it cannot run', () => {
    expect(validateAutomationInput({ instruction: '', cron: '* * * * *' })).toEqual({ error: 'instruction is required' });
    expect(validateAutomationInput({ instruction: 'x', cron: 'every day' })).toMatchObject({ error: expect.stringContaining('5-field') });
    expect(validateAutomationInput({ instruction: ' tidy ', cron: '0 9 * * *' })).toEqual({ instruction: 'tidy', cron: '0 9 * * *' });
  });
});
