import { describeError } from '../GatewayServiceClient';

describe('describeError', () => {
  test('names an AggregateError by its parts, since its own message is empty', () => {
    const agg = new AggregateError([new Error('connect ECONNREFUSED ::1:18080'), new Error('connect ECONNREFUSED 127.0.0.1:18080')]);
    expect(agg.message).toBe('');
    expect(describeError(agg)).toBe('connect ECONNREFUSED ::1:18080; connect ECONNREFUSED 127.0.0.1:18080');
  });
  test('falls back to the code, then the name, for an Error with no message', () => {
    const e = Object.assign(new Error(''), { code: 'ECONNRESET' });
    expect(describeError(e)).toBe('ECONNRESET');
    expect(describeError(new TypeError(''))).toBe('TypeError');
  });
});
