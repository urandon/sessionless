import Ajv from '@redocly/ajv/dist/2020.js';
import { describe, expect, it } from 'vitest';
import openapi from '../api/openapi.json';
import type { components } from '../api/generated';

type Page = components['schemas']['ComputeChoicesPageV1'];

function choice(value: Page): Page['choices'][number] {
  const entry = value.choices[0];
  if (!entry) throw new Error('compute-choice test fixture requires one entry');
  return entry;
}

// Existing locked OpenAPI generator dependency; no new download or production
// validator. This executes schema conditionals, not just TypeScript compilation.
// Go additionally enforces byte limits, duplicate IDs, source-time comparisons,
// label curation boundaries and input compatibility. No route is mounted here.
const ajv = new Ajv({
  strict: false,
  formats: { 'date-time': /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$/ },
});
ajv.addSchema({ $id: 'sessionless-compute-contract', components: openapi.components });
const validate = ajv.compile<Page>({
  $ref: 'sessionless-compute-contract#/components/schemas/ComputeChoicesPageV1',
});

function page(): Page {
  return {
    version: 1,
    session_id: 'session-1',
    read_at: '2026-10-10T12:00:00Z',
    input_kind: 'text',
    registry_revision: '1'.repeat(64),
    coverage: 'complete',
    choices: [
      {
        choice_id: 'choice-1',
        choice_revision: '2'.repeat(64),
        model_label: 'Reviewed model',
        provider_label: 'Reviewed provider',
        placement: 'attached',
        state: 'eligible',
        observed_at: '2026-10-10T11:59:00Z',
        valid_until: '2026-10-10T12:01:00Z',
        freshness: 'current',
        disclosure_revision: '3'.repeat(64),
        disclosure: {
          data_class: 'private',
          input_kinds: ['text'],
          provider_processing: true,
          host_operator_access: true,
          fallback: 'deny',
          text: 'The selected provider processes the entire admitted conversation context and new input as private data. The execution host operator can technically access admitted data. Only the listed input kinds are supported. No alternate resource or provider is selected automatically.',
        },
        capacity: {
          state: 'available',
          observed_at: '2026-10-10T11:59:00Z',
          valid_until: '2026-10-10T12:01:00Z',
          freshness: 'current',
        },
      },
    ],
  };
}

describe('unmounted closed compute-choice OpenAPI contract', () => {
  it.each(['模型 😀', 'café e\u0301', 'model\u00a0name'])(
    'accepts reviewed ordinary Unicode display copy %s',
    (label) => {
      const value = page();
      choice(value).model_label = label;
      choice(value).provider_label = label;
      expect(validate(value), JSON.stringify(validate.errors)).toBe(true);
    },
  );

  it('accepts typed public observations without manufacturing execution authority', () => {
    const value = page();
    expect(validate(value), JSON.stringify(validate.errors)).toBe(true);
    expect(Object.keys(choice(value))).not.toContain('worker_id');
    choice(value).placement = 'managed';
    expect(validate(value), JSON.stringify(validate.errors)).toBe(true);
  });

  it('keeps missing required evidence unknown without invented windows or quantities', () => {
    const value = page();
    const entry = choice(value);
    entry.state = 'unknown';
    entry.reason_code = 'unknown';
    delete entry.observed_at;
    delete entry.valid_until;
    delete entry.freshness;
    entry.capacity = { state: 'unknown' };
    expect(validate(value), JSON.stringify(validate.errors)).toBe(true);
    expect(JSON.stringify(entry.capacity)).toBe('{"state":"unknown"}');
  });

  it('supports empty continuation and complete pages without inventory totals', () => {
    const value = page();
    value.choices = [];
    expect(validate(value)).toBe(true);
    value.coverage = 'page';
    value.next_cursor = 'A'.repeat(164);
    expect(validate(value)).toBe(true); // Shape only; MAC/scope are server-owned.
    value.coverage = 'complete';
    expect(validate(value)).toBe(false);
  });

  it('accepts all closed known reasons with truthful shape', () => {
    for (const reason of openapi.components.schemas.ComputeChoiceV1.properties.reason_code.enum) {
      const value = page();
      const entry = choice(value);
      entry.state = 'unavailable';
      entry.reason_code = reason as NonNullable<typeof entry.reason_code>;
      if (reason === 'evidence_stale') {
        entry.freshness = 'expired';
      }
      expect(validate(value), `${reason}: ${JSON.stringify(validate.errors)}`).toBe(true);
    }
  });

  it.each([
    ['newline label', (v: Page) => Reflect.set(choice(v), 'model_label', 'model\nsecret')],
    ['bidi label', (v: Page) => Reflect.set(choice(v), 'model_label', 'model\u202esecret')],
    ['leading space label', (v: Page) => Reflect.set(choice(v), 'model_label', ' model')],
    ['trailing space label', (v: Page) => Reflect.set(choice(v), 'model_label', 'model ')],
    ['tab label', (v: Page) => Reflect.set(choice(v), 'model_label', 'model\tname')],
    ['format label', (v: Page) => Reflect.set(choice(v), 'model_label', 'model\u200bname')],
    ['leading NBSP label', (v: Page) => Reflect.set(choice(v), 'model_label', '\u00a0model')],
    ['trailing NBSP label', (v: Page) => Reflect.set(choice(v), 'model_label', 'model\u00a0')],
    ['control NEL label', (v: Page) => Reflect.set(choice(v), 'model_label', 'model\u0085name')],
    ['control DEL label', (v: Page) => Reflect.set(choice(v), 'model_label', 'model\u007fname')],
    [
      'provider bidi label',
      (v: Page) => Reflect.set(choice(v), 'provider_label', 'provider\u202ename'),
    ],
    ['private page key', (v: Page) => Reflect.set(v, 'tenant_id', 'private-tenant')],
    ['version', (v: Page) => Reflect.set(v, 'version', 2)],
    ['input', (v: Page) => Reflect.set(v, 'input_kind', 'public')],
    ['null choices', (v: Page) => Reflect.set(v, 'choices', null)],
    [
      'five choices',
      (v: Page) => {
        while (v.choices.length < 5) v.choices.push(structuredClone(choice(v)));
      },
    ],
    ['page without cursor', (v: Page) => Reflect.set(v, 'coverage', 'page')],
    [
      'raw cursor',
      (v: Page) => {
        v.coverage = 'page';
        v.next_cursor = 'private-owner';
      },
    ],
    [
      'upper revision',
      (v: Page) => {
        v.registry_revision = 'A'.repeat(64);
      },
    ],
    [
      'oversize ID',
      (v: Page) => {
        choice(v).choice_id = 'a'.repeat(129);
      },
    ],
    [
      'private entry key',
      (v: Page) => Reflect.set(choice(v), 'endpoint', 'https://private.invalid'),
    ],
    ['state', (v: Page) => Reflect.set(choice(v), 'state', 'running')],
    ['placement', (v: Page) => Reflect.set(choice(v), 'placement', 'fallback')],
    [
      'eligible reason',
      (v: Page) => {
        choice(v).reason_code = 'unknown';
      },
    ],
    [
      'partial window',
      (v: Page) => {
        delete choice(v).observed_at;
      },
    ],
    [
      'expired eligible',
      (v: Page) => {
        choice(v).freshness = 'expired';
      },
    ],
    [
      'unknown capacity eligible',
      (v: Page) => {
        choice(v).capacity = { state: 'unknown' };
      },
    ],
    [
      'unavailable capacity eligible',
      (v: Page) => Reflect.set(choice(v).capacity, 'state', 'unavailable'),
    ],
    [
      'unknown reason',
      (v: Page) => {
        choice(v).state = 'unknown';
        Reflect.set(choice(v), 'reason_code', 'raw provider error');
      },
    ],
    [
      'missing denial reason',
      (v: Page) => {
        choice(v).state = 'unavailable';
      },
    ],
    [
      'stale without expired window',
      (v: Page) => {
        choice(v).state = 'unavailable';
        choice(v).reason_code = 'evidence_stale';
      },
    ],
    [
      'unknown capacity fabricated timestamp',
      (v: Page) => {
        choice(v).state = 'unknown';
        choice(v).reason_code = 'unknown';
        choice(v).capacity = { state: 'unknown' };
        Reflect.set(choice(v).capacity, 'observed_at', v.read_at);
      },
    ],
    [
      'private capacity quantity',
      (v: Page) => Reflect.set(choice(v).capacity, 'quota_remaining', 0),
    ],
    ['public class', (v: Page) => Reflect.set(choice(v).disclosure, 'data_class', 'public')],
    [
      'concealed host operator',
      (v: Page) => Reflect.set(choice(v).disclosure, 'host_operator_access', false),
    ],
    ['raw disclosure', (v: Page) => Reflect.set(choice(v).disclosure, 'text', 'private error')],
    [
      'duplicate input kinds',
      (v: Page) => {
        choice(v).disclosure.input_kinds = ['text', 'text'];
      },
    ],
    ['unknown input kind', (v: Page) => Reflect.set(choice(v).disclosure, 'input_kinds', ['url'])],
    [
      'private disclosure key',
      (v: Page) => Reflect.set(choice(v).disclosure, 'owner_id', 'private-owner'),
    ],
  ])('rejects %s without repair or coercion', (_name, mutate) => {
    const value = page();
    mutate(value);
    expect(validate(value), JSON.stringify(validate.errors)).toBe(false);
  });
});
