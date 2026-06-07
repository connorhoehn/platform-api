// platform-api/src/pipeline/image/__tests__/visionSchemaPresets.test.ts
//
// Round-trip tests for buildPresetSchema. Verifies the JSON Schema shape
// (required fields, enum from labels, array constraints) for each preset.

import {
  buildPresetSchema,
  getPresetDefaultPrompt,
  PRESET_DEFAULT_PROMPTS,
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
// face-detection
// ---------------------------------------------------------------------------

describe('buildPresetSchema — face-detection', () => {
  it('produces required field: faces', () => {
    const schema = buildPresetSchema('face-detection');
    expect(schema!['required']).toEqual(expect.arrayContaining(['faces']));
  });

  it('face items require bbox + confidence', () => {
    const schema = buildPresetSchema('face-detection');
    const props = schema!['properties'] as Record<string, unknown>;
    const faces = props['faces'] as Record<string, unknown>;
    const items = faces['items'] as Record<string, unknown>;
    expect(items['required']).toEqual(expect.arrayContaining(['bbox', 'confidence']));
  });

  it('bbox uses normalized [0,1] coordinates', () => {
    const schema = buildPresetSchema('face-detection');
    const props = schema!['properties'] as Record<string, unknown>;
    const faces = props['faces'] as Record<string, unknown>;
    const items = faces['items'] as Record<string, unknown>;
    const itemProps = items['properties'] as Record<string, unknown>;
    const bbox = itemProps['bbox'] as Record<string, unknown>;
    const bboxProps = bbox['properties'] as Record<string, unknown>;
    const x = bboxProps['x'] as Record<string, unknown>;
    expect(x['minimum']).toBe(0);
    expect(x['maximum']).toBe(1);
  });

  it('exposes optional age, expression, gaze attributes', () => {
    const schema = buildPresetSchema('face-detection');
    const props = schema!['properties'] as Record<string, unknown>;
    const faces = props['faces'] as Record<string, unknown>;
    const items = faces['items'] as Record<string, unknown>;
    const itemProps = items['properties'] as Record<string, unknown>;
    expect(itemProps['age']).toBeDefined();
    expect(itemProps['expression']).toBeDefined();
    expect(itemProps['gaze']).toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// color-palette
// ---------------------------------------------------------------------------

describe('buildPresetSchema — color-palette', () => {
  it('produces required fields: dominant, temperature', () => {
    const schema = buildPresetSchema('color-palette');
    expect(schema!['required']).toEqual(
      expect.arrayContaining(['dominant', 'temperature']),
    );
  });

  it('dominant is an array capped at 5 items', () => {
    const schema = buildPresetSchema('color-palette');
    const props = schema!['properties'] as Record<string, unknown>;
    const dominant = props['dominant'] as Record<string, unknown>;
    expect(dominant['type']).toBe('array');
    expect(dominant['maxItems']).toBe(5);
  });

  it('dominant items require hex + percentage', () => {
    const schema = buildPresetSchema('color-palette');
    const props = schema!['properties'] as Record<string, unknown>;
    const dominant = props['dominant'] as Record<string, unknown>;
    const items = dominant['items'] as Record<string, unknown>;
    expect(items['required']).toEqual(expect.arrayContaining(['hex', 'percentage']));
  });

  it('temperature is an enum of warm/cool/neutral', () => {
    const schema = buildPresetSchema('color-palette');
    const props = schema!['properties'] as Record<string, unknown>;
    const temp = props['temperature'] as Record<string, unknown>;
    expect(temp['enum']).toEqual(expect.arrayContaining(['warm', 'cool', 'neutral']));
  });
});

// ---------------------------------------------------------------------------
// image-quality-rating
// ---------------------------------------------------------------------------

describe('buildPresetSchema — image-quality-rating', () => {
  it('produces all required scoring fields + rationale', () => {
    const schema = buildPresetSchema('image-quality-rating');
    expect(schema!['required']).toEqual(
      expect.arrayContaining([
        'focus',
        'exposure',
        'composition',
        'overallScore',
        'rationale',
      ]),
    );
  });

  it('all numeric score fields are integers 0-10', () => {
    const schema = buildPresetSchema('image-quality-rating');
    const props = schema!['properties'] as Record<string, unknown>;
    for (const key of ['focus', 'exposure', 'composition', 'overallScore']) {
      const field = props[key] as Record<string, unknown>;
      expect(field['type']).toBe('integer');
      expect(field['minimum']).toBe(0);
      expect(field['maximum']).toBe(10);
    }
  });

  it('rationale is a string field', () => {
    const schema = buildPresetSchema('image-quality-rating');
    const props = schema!['properties'] as Record<string, unknown>;
    const rationale = props['rationale'] as Record<string, unknown>;
    expect(rationale['type']).toBe('string');
  });
});

// ---------------------------------------------------------------------------
// Preset default prompts
// ---------------------------------------------------------------------------

describe('PRESET_DEFAULT_PROMPTS', () => {
  it('has an entry for every preset', () => {
    const presets: VisionSchemaPreset[] = [
      'caption-only',
      'classification-enum',
      'multi-label-tags',
      'ocr-records',
      'detection-regions',
      'face-detection',
      'color-palette',
      'image-quality-rating',
    ];
    for (const p of presets) {
      expect(PRESET_DEFAULT_PROMPTS[p]).toBeTruthy();
      expect(getPresetDefaultPrompt(p)).toBe(PRESET_DEFAULT_PROMPTS[p]);
    }
  });

  it('face-detection prompt restricts to human faces', () => {
    expect(getPresetDefaultPrompt('face-detection')).toMatch(/human faces/i);
    expect(getPresetDefaultPrompt('face-detection')).toMatch(/Do NOT detect non-human/);
  });

  it('color-palette prompt asks for hex codes and temperature', () => {
    const prompt = getPresetDefaultPrompt('color-palette');
    expect(prompt).toMatch(/hex/i);
    expect(prompt).toMatch(/warm\/cool\/neutral/);
  });

  it('image-quality-rating prompt mentions 30/30/40 weighting', () => {
    expect(getPresetDefaultPrompt('image-quality-rating')).toMatch(/30\/30\/40/);
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
