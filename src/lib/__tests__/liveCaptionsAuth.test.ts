// The live-captions sidecar requires X-Live-Captions-Secret on every POST.
const mockWarn = jest.fn();
jest.mock('../logger', () => ({
  __esModule: true,
  default: { info: jest.fn(), warn: (...a: unknown[]) => mockWarn(...a), error: jest.fn(), debug: jest.fn() },
}));

import { liveCaptionsAuthHeaders, LIVE_CAPTIONS_SECRET_HEADER, __resetLiveCaptionsAuthWarningForTests } from '../liveCaptionsAuth';

const saved = process.env.LIVE_CAPTIONS_SHARED_SECRET;
beforeEach(() => {
  mockWarn.mockReset();
  __resetLiveCaptionsAuthWarningForTests();
});
afterAll(() => {
  if (saved === undefined) delete process.env.LIVE_CAPTIONS_SHARED_SECRET; else process.env.LIVE_CAPTIONS_SHARED_SECRET = saved;
});

describe('liveCaptionsAuthHeaders', () => {
  it('sends the configured secret in X-Live-Captions-Secret', () => {
    process.env.LIVE_CAPTIONS_SHARED_SECRET = 'cap-secret';
    expect(LIVE_CAPTIONS_SECRET_HEADER).toBe('X-Live-Captions-Secret');
    expect(liveCaptionsAuthHeaders()).toEqual({ 'X-Live-Captions-Secret': 'cap-secret' });
    expect(mockWarn).not.toHaveBeenCalled();
  });

  it('trims surrounding whitespace from the configured value', () => {
    process.env.LIVE_CAPTIONS_SHARED_SECRET = '  cap-secret\n';
    expect(liveCaptionsAuthHeaders()).toEqual({ 'X-Live-Captions-Secret': 'cap-secret' });
  });

  it('sends no header when unset or blank, and warns once naming the variable', () => {
    delete process.env.LIVE_CAPTIONS_SHARED_SECRET;
    expect(liveCaptionsAuthHeaders()).toEqual({});
    process.env.LIVE_CAPTIONS_SHARED_SECRET = '   ';
    expect(liveCaptionsAuthHeaders()).toEqual({});
    expect(mockWarn).toHaveBeenCalledTimes(1);
    expect(JSON.stringify(mockWarn.mock.calls[0])).toContain('LIVE_CAPTIONS_SHARED_SECRET');
  });

  it('never writes the secret to the log', () => {
    process.env.LIVE_CAPTIONS_SHARED_SECRET = 'cap-secret';
    liveCaptionsAuthHeaders();
    delete process.env.LIVE_CAPTIONS_SHARED_SECRET;
    liveCaptionsAuthHeaders();
    expect(JSON.stringify(mockWarn.mock.calls)).not.toContain('cap-secret');
  });
});
