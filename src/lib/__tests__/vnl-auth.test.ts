// social-api/src/lib/__tests__/vnl-auth.test.ts
//
// Tests for getVnlAuthToken: caching, missing env var error, no-token error,
// Cognito call wiring.
//
// Strategy: stable mock shared across resetModules() calls, env vars set
// in beforeEach so each requireFresh() sees the right config.

// Stable mock function referenced by the factory so it persists across resetModules().
const mockSend = jest.fn();

jest.mock('@aws-sdk/client-cognito-identity-provider', () => ({
    CognitoIdentityProviderClient: jest.fn().mockImplementation(() => ({ send: mockSend })),
    InitiateAuthCommand: jest.fn().mockImplementation((input) => ({ input })),
}));

beforeEach(() => {
    jest.resetModules();
    mockSend.mockReset();
    process.env.VNL_COGNITO_USER_POOL_CLIENT_ID = 'client-id';
    process.env.VNL_SERVICE_USERNAME = 'svc-user';
    process.env.VNL_SERVICE_PASSWORD = 'svc-pass';
    delete process.env.VNL_API_URL;
});

afterEach(() => {
    delete process.env.VNL_COGNITO_USER_POOL_CLIENT_ID;
    delete process.env.VNL_SERVICE_USERNAME;
    delete process.env.VNL_SERVICE_PASSWORD;
});

function requireFresh() {
    return require('../vnl-auth') as typeof import('../vnl-auth');
}

// ─── happy path ───────────────────────────────────────────────────────────

describe('getVnlAuthToken — happy path', () => {
    test('returns the IdToken from Cognito', async () => {
        mockSend.mockResolvedValue({ AuthenticationResult: { IdToken: 'id-token-abc', ExpiresIn: 3600 } });
        const { getVnlAuthToken } = requireFresh();
        const token = await getVnlAuthToken();
        expect(token).toBe('id-token-abc');
    });

    test('caches the token — calls Cognito only once for two successive requests', async () => {
        mockSend.mockResolvedValue({ AuthenticationResult: { IdToken: 'cached-token', ExpiresIn: 3600 } });
        const { getVnlAuthToken } = requireFresh();
        await getVnlAuthToken();
        await getVnlAuthToken();
        expect(mockSend).toHaveBeenCalledTimes(1);
    });

    test('passes CLIENT_ID and credentials to Cognito', async () => {
        mockSend.mockResolvedValue({ AuthenticationResult: { IdToken: 'tok', ExpiresIn: 3600 } });
        const { getVnlAuthToken } = requireFresh();
        await getVnlAuthToken();
        const call = mockSend.mock.calls[0][0];
        expect(call.input.ClientId).toBe('client-id');
        expect(call.input.AuthParameters.USERNAME).toBe('svc-user');
        expect(call.input.AuthParameters.PASSWORD).toBe('svc-pass');
    });
});

// ─── missing env vars ─────────────────────────────────────────────────────

describe('getVnlAuthToken — missing env vars', () => {
    test('throws when VNL_COGNITO_USER_POOL_CLIENT_ID is missing', async () => {
        delete process.env.VNL_COGNITO_USER_POOL_CLIENT_ID;
        const { getVnlAuthToken } = requireFresh();
        await expect(getVnlAuthToken()).rejects.toThrow('VNL auth not configured');
    });

    test('throws when VNL_SERVICE_USERNAME is missing', async () => {
        delete process.env.VNL_SERVICE_USERNAME;
        const { getVnlAuthToken } = requireFresh();
        await expect(getVnlAuthToken()).rejects.toThrow('VNL auth not configured');
    });

    test('throws when VNL_SERVICE_PASSWORD is missing', async () => {
        delete process.env.VNL_SERVICE_PASSWORD;
        const { getVnlAuthToken } = requireFresh();
        await expect(getVnlAuthToken()).rejects.toThrow('VNL auth not configured');
    });
});

// ─── Cognito response errors ───────────────────────────────────────────────

describe('getVnlAuthToken — Cognito response errors', () => {
    test('throws when Cognito returns no IdToken', async () => {
        mockSend.mockResolvedValue({ AuthenticationResult: {} });
        const { getVnlAuthToken } = requireFresh();
        await expect(getVnlAuthToken()).rejects.toThrow('no IdToken returned');
    });

    test('propagates Cognito client errors', async () => {
        mockSend.mockRejectedValue(new Error('network failure'));
        const { getVnlAuthToken } = requireFresh();
        await expect(getVnlAuthToken()).rejects.toThrow('network failure');
    });
});

// ─── VNL_API_URL export ────────────────────────────────────────────────────

describe('VNL_API_URL export', () => {
    test('equals VNL_API_URL env var when set', () => {
        process.env.VNL_API_URL = 'https://api.example.com/prod';
        const { VNL_API_URL } = requireFresh();
        expect(VNL_API_URL).toBe('https://api.example.com/prod');
    });

    test('defaults to empty string when VNL_API_URL not set', () => {
        const { VNL_API_URL } = requireFresh();
        expect(VNL_API_URL).toBe('');
    });
});

export {};
