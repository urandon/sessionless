import type { RunExplanationV1 } from '../api/client';

export function explanation(): RunExplanationV1 {
  return {
    version: 1,
    run_id: 'run-1',
    session_id: 'ses-1',
    read_at: '2026-10-10T12:00:10Z',
    created_at: '2026-10-10T12:00:00Z',
    updated_at: '2026-10-10T12:00:01Z',
    status: 'running',
    admission: {
      availability: 'recorded',
      outcome: 'admitted',
      reason_code: 'admitted',
      observed_at: '2026-10-10T12:00:01Z',
      decision_revision: 1,
      coverage: 'last_recorded_decision',
    },
    terminal: { availability: 'not_applicable' },
    attempt: {
      availability: 'recorded',
      attempt_id: 'att-1',
      number: 1,
      status: 'running',
      updated_at: '2026-10-10T12:00:01Z',
    },
    attached: {
      availability: 'recorded',
      fact: 'claim_recorded',
      observed_at: '2026-10-10T12:00:02Z',
      valid_until: '2026-10-10T12:00:10Z',
      freshness: 'expired',
    },
    coverage: {
      admission: 'recorded',
      terminal: 'not_applicable',
      attempt: 'recorded',
      operational: 'recorded',
    },
  };
}

export function barrier<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
