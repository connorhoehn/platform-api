import { distinctHeading } from '../distinctHeading';

describe('distinctHeading (NFR #93)', () => {
  it('returns the heading only when it names something the title does not', () => {
    expect(distinctHeading('Audit E2E', 'audit-e2e-1790150600')).toBe('Audit E2E');
    expect(distinctHeading('Audit E2E', 'audit-e2e')).toBeUndefined();
    expect(distinctHeading('  ', 'x')).toBeUndefined();
    expect(distinctHeading(undefined, 'x')).toBeUndefined();
    expect(distinctHeading('x'.repeat(200), 'y')).toHaveLength(120);
  });
});
