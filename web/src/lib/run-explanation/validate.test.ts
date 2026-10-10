import { describe, expect, it } from 'vitest';
import { explanation } from './test-fixtures';
import { validateRunExplanation } from './validate';

describe('closed RunExplanationV1 manifest', () => {
  it('keeps historical/managed unknown groups explicit without invented values', () => {
    const v = explanation();
    v.admission = { availability: 'unknown' };
    v.attempt = { availability: 'unknown' };
    v.attached = { availability: 'unknown' };
    v.coverage = {
      admission: 'unknown',
      terminal: 'not_applicable',
      attempt: 'unknown',
      operational: 'unknown',
    };
    expect(validateRunExplanation(v, 'run-1', 'ses-1')).toEqual(v);
  });

  it.each([
    'subscription_attention_required',
    'capacity_draining',
    'quota_reset_pending',
    'quota_exhausted_reset_unknown',
    'runtime_limit_exceeded',
    'turn_limit_exceeded',
    'input_limit_exceeded',
    'context_limit_exceeded',
    'artifact_limit_exceeded',
    'capacity_busy',
    'workspace_queue_limit',
    'workspace_active_run_limit',
  ] as const)('retains distinct last admission denial %s', (reason) => {
    const v = explanation();
    v.status = 'quota_blocked';
    v.admission = { ...v.admission, outcome: 'denied', reason_code: reason };
    expect(validateRunExplanation(v, 'run-1', 'ses-1').admission.reason_code).toBe(reason);
  });

  it.each([
    'execution_failed',
    'result_persistence_failed',
    'canonical_cancelled',
    'unclassified_failure',
  ] as const)(
    'accepts exact canonical terminal reason %s without expiring durable facts',
    (reason) => {
      const v = explanation();
      v.status = reason === 'canonical_cancelled' ? 'cancelled' : 'failed';
      v.finished_at = v.updated_at;
      v.terminal = {
        availability: 'recorded',
        reason_code: reason,
        observed_at: v.finished_at,
        event_id: 'evt-1',
        event_sequence: 1,
      };
      v.coverage.terminal = 'recorded';
      v.read_at = '2099-01-01T00:00:00Z';
      expect(validateRunExplanation(v, 'run-1', 'ses-1').terminal.reason_code).toBe(reason);
    },
  );

  it.each(['terminal_commit_recorded', 'fenced_outcome_unknown', 'receipt_retired'] as const)(
    'accepts historical durable attached fact %s without lease expiry',
    (fact) => {
      const v = explanation();
      v.attached = {
        availability: 'recorded',
        fact,
        observed_at: v.updated_at,
        freshness: 'durable',
      };
      v.read_at = '2099-01-01T00:00:00Z';
      expect(validateRunExplanation(v, 'run-1', 'ses-1').attached.freshness).toBe('durable');
    },
  );

  it('distinguishes lease expiry at nanosecond precision', () => {
    const v = explanation();
    v.read_at = '2026-10-10T12:00:10.000000001Z';
    v.attached.valid_until = '2026-10-10T12:00:10.000000002Z';
    v.attached.freshness = 'within_lease';
    expect(validateRunExplanation(v, 'run-1', 'ses-1').attached.freshness).toBe('within_lease');
    v.read_at = v.attached.valid_until;
    v.attached.freshness = 'expired';
    expect(validateRunExplanation(v, 'run-1', 'ses-1').attached.freshness).toBe('expired');
  });

  it.each([
    [
      'version',
      (v) => {
        v.version = 2;
      },
    ],
    [
      'missing group',
      (v) => {
        Reflect.deleteProperty(v, 'coverage');
      },
    ],
    [
      'oversized ID',
      (v) => {
        v.run_id = 'x'.repeat(161);
      },
    ],
    [
      'invalid ID',
      (v) => {
        v.run_id = 'run/1';
      },
    ],
    [
      'empty ID',
      (v) => {
        v.session_id = '';
      },
    ],
    [
      'Run enum',
      (v) => {
        v.status = 'unknown';
      },
    ],
    [
      'terminal Run missing finish',
      (v) => {
        v.status = 'failed';
      },
    ],
    [
      'nonterminal Run finish',
      (v) => {
        v.finished_at = v.updated_at;
      },
    ],
    [
      'updated before create',
      (v) => {
        v.updated_at = '2026-10-09T00:00:00Z';
      },
    ],
    [
      'updated after read',
      (v) => {
        v.updated_at = '2026-10-11T00:00:00Z';
      },
    ],
    [
      'zero instant',
      (v) => {
        v.created_at = '0001-01-01T00:00:00Z';
      },
    ],
    [
      'admission unknown fields',
      (v) => {
        v.admission.availability = 'unknown';
      },
    ],
    [
      'admission missing field',
      (v) => {
        delete v.admission.outcome;
      },
    ],
    [
      'admission enum',
      (v) => {
        v.admission.outcome = 'invalid';
      },
    ],
    [
      'admission count zero',
      (v) => {
        v.admission.decision_revision = 0;
      },
    ],
    [
      'admission numeric string',
      (v) => {
        v.admission.decision_revision = '1';
      },
    ],
    [
      'admission fractional count',
      (v) => {
        v.admission.decision_revision = 1.5;
      },
    ],
    [
      'admission coverage enum',
      (v) => {
        v.admission.coverage = 'complete';
      },
    ],
    [
      'admission outcome/reason mismatch',
      (v) => {
        v.admission.reason_code = 'capacity_busy';
      },
    ],
    [
      'admission phase mismatch',
      (v) => {
        v.status = 'created';
      },
    ],
    [
      'admission future time',
      (v) => {
        v.admission.observed_at = '2026-10-11T00:00:00Z';
      },
    ],
    [
      'admission before create',
      (v) => {
        v.admission.observed_at = '2026-10-09T00:00:00Z';
      },
    ],
    [
      'terminal enum',
      (v) => {
        v.terminal.availability = 'no_reason';
      },
    ],
    [
      'terminal applicability',
      (v) => {
        v.terminal.availability = 'unknown';
      },
    ],
    [
      'terminal unknown fields',
      (v) => {
        v.terminal.event_id = 'evt-1';
      },
    ],
    [
      'attempt unknown fields',
      (v) => {
        v.attempt.availability = 'unknown';
      },
    ],
    [
      'attempt number overflow',
      (v) => {
        v.attempt.number = 4294967296;
      },
    ],
    [
      'attempt number zero',
      (v) => {
        v.attempt.number = 0;
      },
    ],
    [
      'attempt enum',
      (v) => {
        v.attempt.status = 'queued';
      },
    ],
    [
      'attempt missing finish',
      (v) => {
        v.attempt.status = 'failed';
      },
    ],
    [
      'attempt unwanted finish',
      (v) => {
        v.attempt.finished_at = v.attempt.updated_at;
      },
    ],
    [
      'attempt future time',
      (v) => {
        v.attempt.updated_at = '2026-10-11T00:00:00Z';
      },
    ],
    [
      'attached unknown fields',
      (v) => {
        v.attached.availability = 'unknown';
      },
    ],
    [
      'attached enum',
      (v) => {
        v.attached.fact = 'cancel_ack';
      },
    ],
    [
      'attached future time',
      (v) => {
        v.attached.observed_at = '2026-10-11T00:00:00Z';
      },
    ],
    [
      'attached missing lease',
      (v) => {
        delete v.attached.valid_until;
      },
    ],
    [
      'attached invalid lease',
      (v) => {
        v.attached.valid_until = v.created_at;
      },
    ],
    [
      'attached live durable claim',
      (v) => {
        v.attached.freshness = 'durable';
      },
    ],
    [
      'attached historical lease claim',
      (v) => {
        v.attached.fact = 'receipt_retired';
      },
    ],
    [
      'coverage unexpected key',
      (v) => {
        v.coverage.worker_id = 'private';
      },
    ],
  ] satisfies [
    string,
    (
      v: Record<string, Record<string, unknown> | unknown> & {
        admission: Record<string, unknown>;
        terminal: Record<string, unknown>;
        attempt: Record<string, unknown>;
        attached: Record<string, unknown>;
        coverage: Record<string, unknown>;
      },
    ) => void,
  ][])('rejects inconsistent %s', (_name, mutate) => {
    const v = explanation() as unknown as Parameters<typeof mutate>[0];
    mutate(v);
    expect(() => validateRunExplanation(v, 'run-1', 'ses-1')).toThrow('Invalid run evidence.');
  });
});
