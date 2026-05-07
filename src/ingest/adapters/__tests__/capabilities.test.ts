// social-api/src/ingest/adapters/__tests__/capabilities.test.ts
//
// Per-adapter capability declaration assertions. Each adapter exposes a
// static `capabilities` block that the engine and operator UI introspect
// without instantiating the class — these tests pin the declared shape
// so accidental changes (e.g. flipping requiresOauth) fail loudly.

import { MockLocalAdapter } from '../mock-local';
import { MockWebhookAdapter } from '../mock-webhook';
import { MockDriveAdapter } from '../mock-drive';
import { S3Adapter } from '../s3';
import { GoogleDriveAdapter } from '../google-drive';
import type { AdapterCapabilities } from '../../types';

describe('adapter capabilities', () => {
    test('MockLocalAdapter — read-only filesystem, no oauth, no webhook', () => {
        const expected: AdapterCapabilities = {
            readonly: true,
            supportsWriteBack: false,
            requiresOauth: false,
            supportsWebhook: false,
        };
        expect(MockLocalAdapter.capabilities).toEqual(expected);
    });

    test('MockWebhookAdapter — read-only push surface, webhook-driven', () => {
        const expected: AdapterCapabilities = {
            readonly: true,
            supportsWriteBack: false,
            requiresOauth: false,
            supportsWebhook: true,
        };
        expect(MockWebhookAdapter.capabilities).toEqual(expected);
    });

    test('MockDriveAdapter — bidirectional fixture for dev/demo, no oauth', () => {
        const expected: AdapterCapabilities = {
            readonly: false,
            supportsWriteBack: true,
            requiresOauth: false,
            supportsWebhook: false,
        };
        expect(MockDriveAdapter.capabilities).toEqual(expected);
    });

    test('S3Adapter — read-only at v1, AWS creds (not oauth)', () => {
        const expected: AdapterCapabilities = {
            readonly: true,
            supportsWriteBack: false,
            requiresOauth: false,
            supportsWebhook: false,
        };
        expect(S3Adapter.capabilities).toEqual(expected);
    });

    test('GoogleDriveAdapter — read-only at v1, requires oauth', () => {
        const expected: AdapterCapabilities = {
            readonly: true,
            supportsWriteBack: false,
            requiresOauth: true,
            supportsWebhook: false,
        };
        expect(GoogleDriveAdapter.capabilities).toEqual(expected);
    });
});
