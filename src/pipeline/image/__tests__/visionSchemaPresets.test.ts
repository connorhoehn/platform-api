// platform-api/src/pipeline/image/__tests__/visionSchemaPresets.test.ts
//
// Round-trip tests for buildPresetSchema. Verifies the JSON Schema shape
// (required fields, enum from labels, array constraints) for each preset.

import {
  buildPresetSchema,
  type VisionSchemaPreset,
} from '../visionSchemaPresets';

// ---------------------------------------------------------------------------
// caption-only
// ---------------------------------------------------------------------------

describe('buildPresetSchema — caption-only', () => {
  it('returns undefined', () => {
    expect(buildPresetSchema('caption-only')).toBeUndefined();
  });

  it('returns undefined even when options are supplied', () => {
    expect(buildPresetSchema('caption-only', { labels: ['a'], maxTags: 5 })).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// classification-enum
// ---------------------------------------------------------------------------

describe('buildPresetSchema — classification-enum', () => {
  it('produces required fields: label, confidence, caption', () => {
    const schema = buildPresetSchema('classification-enum', {
      labels: ['cat', 'dog', 'bird'],
    });
    expect(schema).toBeDefined();
    expect(schema!['required']).toEqual(
      expect.arrayContaining(['label', 'confidence', 'caption']),
    );
  });

  it('embeds the label enum', () => {
    const labels = ['sunny', 'cloudy', 'rainy'];
    const schema = buildPresetSchema('classification-enum', { labels });
    const props = schema!['properties'] as Record<string, unknown>;
    const labelProp = props['label'] as Record<string, unknown>;
    expect(labelProp['enum']).toEqual(labels);
  });

  it('confidence property has min 0 max 1', () => {
    const schema = buildPresetSchema('classification-enum', { labels: ['x'] });
    const props = schema!['properties'] as Record<string, unknown>;
    const conf = props['confidence'] as Record<string, unknown>;
    expect(conf['minimum']).toBe(0);
    expect(conf['maximum']).toBe(1);
  });

  it('includes optional rationale field in properties', () => {
    const schema = buildPresetSchema('classification-enum', { labels: ['a'] });
    const props = schema!['properties'] as Record<string, unknown>;
    expect(props['rationale']).toBeDefined();
  });

  it('works with an empty labels array', () => {
    const schema = buildPresetSchema('classification-enum', { labels: [] });
    expect(schema).toBeDefined();
    const props = schema!['properties'] as Record<string, unknown>;
    const labelProp = props['label'] as Record<string, unknown>;
    expect(labelProp['enum']).toEqual([]);
  });

  it('works when options are omitted', () => {
    const schema = buildPresetSchema('classification-enum');
    expect(schema).toBeDefined();
    const props = schema!['properties'] as Record<string, unknown>;
    const labelProp = props['label'] as Record<string, unknown>;
    expect(labelProp['enum']).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// multi-label-tags
// ---------------------------------------------------------------------------

describe('buildPresetSchema — multi-label-tags', () => {
  it('produces required fields: tags, caption', () => {
    const schema = buildPresetSchema('multi-label-tags');
    expect(schema!['required']).toEqual(expect.arrayContaining(['tags', 'caption']));
  });

  it('uses default maxItems of 10 when maxTags is omitted', () => {
    const schema = buildPresetSchema('multi-label-tags');
    const props = schema!['properties'] as Record<string, unknown>;
    const tags = props['tags'] as Record<string, unknown>;
    expect(tags['maxItems']).toBe(10);
  });

  it('respects the maxTags option', () => {
    const schema = buildPresetSchema('multi-label-tags', { maxTags: 5 });
    const props = schema!['properties'] as Record<string, unknown>;
    const tags = props['tags'] as Record<string, unknown>;
    expect(tags['maxItems']).toBe(5);
  });

  it('tag items have tag + confidence required', () => {
    const schema = buildPresetSchema('multi-label-tags');
    const props = schema!['properties'] as Record<string, unknown>;
    const tags = props['tags'] as Record<string, unknown>;
    const items = tags['items'] as Record<string, unknown>;
    expect(items['required']).toEqual(expect.arrayContaining(['tag', 'confidence']));
  });

  it('confidence in tag items has min 0 max 1', () => {
    const schema = buildPresetSchema('multi-label-tags');
    const props = schema!['properties'] as Record<string, unknown>;
    const tags = props['tags'] as Record<string, unknown>;
    const items = tags['items'] as Record<string, unknown>;
    const itemProps = items['properties'] as Record<string, unknown>;
    const conf = itemProps['confidence'] as Record<string, unknown>;
    expect(conf['minimum']).toBe(0);
    expect(conf['maximum']).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// ocr-records
// ---------------------------------------------------------------------------

describe('buildPresetSchema — ocr-records', () => {
  it('produces required fields: fullText, lines, caption', () => {
    const schema = buildPresetSchema('ocr-records');
    expect(schema!['required']).toEqual(
      expect.arrayContaining(['fullText', 'lines', 'caption']),
    );
  });

  it('lines items have text + confidence required', () => {
    const schema = buildPresetSchema('ocr-records');
    const props = schema!['properties'] as Record<string, unknown>;
    const lines = props['lines'] as Record<string, unknown>;
    const items = lines['items'] as Record<string, unknown>;
    expect(items['required']).toEqual(expect.arrayContaining(['text', 'confidence']));
  });

  it('lines items include an optional region object with x/y/width/height', () => {
    const schema = buildPresetSchema('ocr-records');
    const props = schema!['properties'] as Record<string, unknown>;
    const lines = props['lines'] as Record<string, unknown>;
    const items = lines['items'] as Record<string, unknown>;
    const itemProps = items['properties'] as Record<string, unknown>;
    const region = itemProps['region'] as Record<string, unknown>;
    const regionProps = region['properties'] as Record<string, unknown>;
    expect(Object.keys(regionProps)).toEqual(
      expect.arrayContaining(['x', 'y', 'width', 'height']),
    );
  });

  it('includes optional language field', () => {
    const schema = buildPresetSchema('ocr-records');
    const props = schema!['properties'] as Record<string, unknown>;
    expect(props['language']).toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// detection-regions
// ---------------------------------------------------------------------------

describe('buildPresetSchema — detection-regions', () => {
  it('produces required fields: regions, caption', () => {
    const schema = buildPresetSchema('detection-regions');
    expect(schema!['required']).toEqual(
      expect.arrayContaining(['regions', 'caption']),
    );
  });

  it('regions items have label, confidence, bbox required', () => {
    const schema = buildPresetSchema('detection-regions');
    const props = schema!['properties'] as Record<string, unknown>;
    const regions = props['regions'] as Record<string, unknown>;
    const items = regions['items'] as Record<string, unknown>;
    expect(items['required']).toEqual(
      expect.arrayContaining(['label', 'confidence', 'bbox']),
    );
  });

  it('bbox has x, y, width, height required', () => {
    const schema = buildPresetSchema('detection-regions');
    const props = schema!['properties'] as Record<string, unknown>;
    const regions = props['regions'] as Record<string, unknown>;
    const items = regions['items'] as Record<string, unknown>;
    const itemProps = items['properties'] as Record<string, unknown>;
    const bbox = itemProps['bbox'] as Record<string, unknown>;
    expect(bbox['required']).toEqual(
      expect.arrayContaining(['x', 'y', 'width', 'height']),
    );
  });

  it('confidence in region items has min 0 max 1', () => {
    const schema = buildPresetSchema('detection-regions');
    const props = schema!['properties'] as Record<string, unknown>;
    const regions = props['regions'] as Record<string, unknown>;
    const items = regions['items'] as Record<string, unknown>;
    const itemProps = items['properties'] as Record<string, unknown>;
    const conf = itemProps['confidence'] as Record<string, unknown>;
    expect(conf['minimum']).toBe(0);
    expect(conf['maximum']).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Exhaustiveness guard
// ---------------------------------------------------------------------------

describe('buildPresetSchema — unknown preset', () => {
  it('throws for an unknown preset value', () => {
    expect(() =>
      buildPresetSchema('nonexistent' as VisionSchemaPreset),
    ).toThrow(/Unknown VisionSchemaPreset/);
  });
});
