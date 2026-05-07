// social-api/src/ingest/adapters/__tests__/adapter-factory.test.ts
//
// Tests for createAdapter factory: correct adapter class per source type,
// error on unsupported type.

import { createAdapter } from '../adapter';
import { MockLocalAdapter } from '../mock-local';
import { MockWebhookAdapter } from '../mock-webhook';
import { S3Adapter } from '../s3';
import { GoogleDriveAdapter } from '../google-drive';
import type { IngestSource } from '../../types';

function makeSource(type: string): IngestSource {
    return {
        sourceId: 'src-1',
        name: 'Test Source',
        type,
        config: {},
        createdBy: 'test',
        createdAt: '2026-01-01T00:00:00Z',
        updatedAt: '2026-01-01T00:00:00Z',
    } as unknown as IngestSource;
}

describe('createAdapter', () => {
    test('returns MockLocalAdapter for type="local"', () => {
        expect(createAdapter(makeSource('local'))).toBeInstanceOf(MockLocalAdapter);
    });

    test('returns MockWebhookAdapter for type="webhook"', () => {
        expect(createAdapter(makeSource('webhook'))).toBeInstanceOf(MockWebhookAdapter);
    });

    test('returns S3Adapter for type="s3"', () => {
        expect(createAdapter(makeSource('s3'))).toBeInstanceOf(S3Adapter);
    });

    test('returns GoogleDriveAdapter for type="google-drive"', () => {
        expect(createAdapter(makeSource('google-drive'))).toBeInstanceOf(GoogleDriveAdapter);
    });

    test('throws for unsupported type', () => {
        expect(() => createAdapter(makeSource('confluence'))).toThrow('Unsupported source type: confluence');
    });

    test('each call returns a new instance', () => {
        const a = createAdapter(makeSource('local'));
        const b = createAdapter(makeSource('local'));
        expect(a).not.toBe(b);
    });
});
