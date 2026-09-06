import type {
  AttachedWorkerDiagnosticCohort,
  AttachedWorkerDiagnosticCode,
  AttachedWorkerDiagnostics,
} from '$lib/api/client';

import { reasonExplanations, validTimestamp } from './presentation';

export interface DiagnosticExplanation {
  title: string;
  description: string;
}

export const diagnosticCohortOrder = [
  'identity',
  'readiness',
  'connectivity',
  'eligibility',
  'execution',
  'governance',
] as const;

export const diagnosticFactExplanations = {
  desired_state: ['Desired state', 'Current durable lifecycle intent.'],
  observed_state: ['Observed state', 'Current durable control-plane observation.'],
  enrollment_state: ['Enrollment state', 'Current enrollment lifecycle truth.'],
  daemon_state: ['Daemon state', 'Bounded daemon observation, or unknown when unavailable.'],
  last_daemon_failure: [
    'Last daemon failure',
    'Last bounded daemon failure observation, separate from current daemon state.',
  ],
  credential_state: ['Credential state', 'Current bounded credential-readiness observation.'],
  isolation_configuration: [
    'Isolation configuration',
    'Configured isolation support, separate from advertisement and verification.',
  ],
  isolation_verification: [
    'Isolation verification',
    'External isolation verification evidence, distinct from configuration.',
  ],
  connection_state: ['Connection state', 'Current authenticated connection state.'],
  last_contact: [
    'Last contact',
    'Last authenticated contact observation and its independent freshness.',
  ],
  transport_failure: [
    'Last transport failure',
    'Last bounded transport failure, separate from current connection truth.',
  ],
  capability_state: [
    'Advertised capability',
    'Signed capability advertisement evidence; it is not authorization.',
  ],
  admission_preview: [
    'Admission preview',
    'Advisory evaluation only; the admission transaction remains authoritative.',
  ],
  entitlement_state: ['Entitlement state', 'Current bounded entitlement observation.'],
  quota_state: ['Quota state', 'Quota observation; unknown, zero, and exhausted remain distinct.'],
  attempt_state: ['Attempt state', 'Current durable attached-worker attempt state.'],
  cancel_request: [
    'Cancellation request',
    'Durable cancellation request, separate from acknowledgement and process stop.',
  ],
  cancel_ack: [
    'Cancellation acknowledgement',
    'Durable acknowledgement evidence, distinct from process termination.',
  ],
  process_observation: [
    'Process observation',
    'Bound process evidence, separate from CancelAck and terminal commit.',
  ],
  worker_terminal: [
    'Worker terminal evidence',
    'Worker-reported terminal evidence, not the canonical commit.',
  ],
  canonical_terminal: ['Canonical terminal', 'Canonical committed terminal truth.'],
  admission_control: [
    'Admission control',
    'Current admission-control authority without client-side inference.',
  ],
  remote_erase: [
    'Remote erase acknowledgement',
    'Remote erasure acknowledgement, distinct from revoke or fence application.',
  ],
} satisfies Record<AttachedWorkerDiagnosticCode, readonly [string, string]>;

type EvidenceShape = 'none' | 'time' | 'freshness' | 'time_freshness';
type EvidenceRule = readonly [shape: EvidenceShape, allowedFreshness?: readonly string[]];

interface DiagnosticFactRule {
  cohort: AttachedWorkerDiagnosticCohort;
  code: AttachedWorkerDiagnosticCode;
  states: Readonly<Record<string, EvidenceRule>>;
}

const none = ['none'] as const;
const time = ['time'] as const;
const unknownFreshness = ['freshness', ['unknown']] as const;
const observedFreshness = ['time_freshness', ['fresh', 'expired']] as const;

export const diagnosticFactCatalog = [
  {
    cohort: 'identity',
    code: 'desired_state',
    states: { active: none, drain: none, revoked: none },
  },
  {
    cohort: 'identity',
    code: 'observed_state',
    states: { pending: none, offline: none, online: none, draining: none, revoked: none },
  },
  { cohort: 'identity', code: 'enrollment_state', states: { consumed: none } },
  { cohort: 'readiness', code: 'daemon_state', states: { unknown: unknownFreshness } },
  { cohort: 'readiness', code: 'last_daemon_failure', states: { unknown: none } },
  { cohort: 'readiness', code: 'credential_state', states: { unknown: none } },
  {
    cohort: 'readiness',
    code: 'isolation_configuration',
    states: { unsupported: none },
  },
  {
    cohort: 'readiness',
    code: 'isolation_verification',
    states: { unsupported: none },
  },
  {
    cohort: 'connectivity',
    code: 'connection_state',
    states: {
      unknown: none,
      attaching: none,
      online: none,
      draining: none,
      offline: none,
      superseded: none,
      revoked: none,
    },
  },
  {
    cohort: 'connectivity',
    code: 'last_contact',
    states: { unknown: unknownFreshness, recorded: observedFreshness },
  },
  { cohort: 'connectivity', code: 'transport_failure', states: { unknown: none } },
  {
    cohort: 'eligibility',
    code: 'capability_state',
    states: { unknown: none, advertised: time },
  },
  { cohort: 'eligibility', code: 'admission_preview', states: { not_evaluated: none } },
  { cohort: 'eligibility', code: 'entitlement_state', states: { unknown: none } },
  { cohort: 'eligibility', code: 'quota_state', states: { unknown: none } },
  {
    cohort: 'execution',
    code: 'attempt_state',
    states: {
      none,
      offered: none,
      claimed: none,
      cancel_requested: none,
      cancel_acknowledged: none,
      terminal_pending: none,
      terminal_committed: none,
      cancelled_before_claim: none,
      fenced_unknown: none,
      retired: none,
    },
  },
  {
    cohort: 'execution',
    code: 'cancel_request',
    states: { none, requested: time },
  },
  {
    cohort: 'execution',
    code: 'cancel_ack',
    states: { none, pending: none, unknown: none, acknowledged: time },
  },
  {
    cohort: 'execution',
    code: 'process_observation',
    states: { unknown: unknownFreshness },
  },
  {
    cohort: 'execution',
    code: 'worker_terminal',
    states: { none, received: none },
  },
  {
    cohort: 'execution',
    code: 'canonical_terminal',
    states: { none, committed: time },
  },
  { cohort: 'governance', code: 'admission_control', states: { unavailable: none } },
  {
    cohort: 'governance',
    code: 'remote_erase',
    states: { not_requested: none, unknown: none },
  },
] as const satisfies ReadonlyArray<DiagnosticFactRule>;

export function explainDiagnostic(code: string): DiagnosticExplanation {
  const value = (diagnosticFactExplanations as Record<string, readonly [string, string]>)[code];
  return value
    ? { title: value[0], description: value[1] }
    : {
        title: `Unknown diagnostic (${safeCode(code)})`,
        description: 'This version does not recognize the reported diagnostic code.',
      };
}

export interface PublicDiagnosticBundleV1 {
  version: 1;
  evaluated_at: string;
  worker_id: string;
  facts: Array<{
    cohort: string;
    code: string;
    state: string;
    observed_at?: string;
    freshness?: string;
  }>;
  warnings: string[];
}

const safeToken = /^[a-z][a-z0-9_]{0,63}$/;
const workerID = /^[A-Za-z0-9._:-]{1,160}$/;
const requiredFacts = diagnosticFactCatalog.length;
const maxWarnings = 27;
const maxBundleBytes = 64 * 1024;
const freshnessValues = new Set(['unknown', 'fresh', 'expired']);
const reasonCodes = new Set(Object.keys(reasonExplanations));

export function buildDiagnosticBundleV1(
  diagnostics: AttachedWorkerDiagnostics,
): PublicDiagnosticBundleV1 {
  if (
    diagnostics.version !== 1 ||
    !validTimestamp(diagnostics.evaluated_at) ||
    typeof diagnostics.worker_id !== 'string' ||
    !workerID.test(diagnostics.worker_id) ||
    !Array.isArray(diagnostics.facts) ||
    diagnostics.facts.length !== requiredFacts ||
    !Array.isArray(diagnostics.warnings) ||
    diagnostics.warnings.length > maxWarnings
  ) {
    throw new Error('invalid_diagnostics');
  }

  const facts = diagnostics.facts.map((fact, index) => {
    const expected = diagnosticFactCatalog[index];
    if (
      expected === undefined ||
      fact.cohort !== expected.cohort ||
      fact.code !== expected.code ||
      !safeToken.test(fact.state) ||
      (fact.observed_at !== undefined && !validTimestamp(fact.observed_at)) ||
      (fact.freshness !== undefined && !freshnessValues.has(fact.freshness))
    ) {
      throw new Error('invalid_diagnostics');
    }
    const stateRule = (expected.states as Record<string, EvidenceRule>)[fact.state];
    if (stateRule === undefined || !validEvidenceShape(fact, stateRule)) {
      throw new Error('invalid_diagnostics');
    }
    return {
      cohort: fact.cohort,
      code: fact.code,
      state: fact.state,
      ...(fact.observed_at === undefined ? {} : { observed_at: fact.observed_at }),
      ...(fact.freshness === undefined ? {} : { freshness: fact.freshness }),
    };
  });
  const warnings = diagnostics.warnings.map((warning) => {
    if (!reasonCodes.has(warning)) throw new Error('invalid_diagnostics');
    return warning;
  });
  if (new Set(warnings).size !== warnings.length) throw new Error('invalid_diagnostics');

  return {
    version: 1,
    evaluated_at: diagnostics.evaluated_at,
    worker_id: diagnostics.worker_id,
    facts,
    warnings,
  };
}

export function serializeDiagnosticBundleV1(diagnostics: AttachedWorkerDiagnostics): string {
  const result = `${JSON.stringify(buildDiagnosticBundleV1(diagnostics), null, 2)}\n`;
  if (new TextEncoder().encode(result).byteLength > maxBundleBytes) {
    throw new Error('invalid_diagnostics');
  }
  return result;
}

function safeCode(value: string): string {
  const safe = value
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '')
    .slice(0, 48);
  return safe || 'unknown';
}

function validEvidenceShape(
  fact: { observed_at?: string; freshness?: string },
  rule: EvidenceRule,
): boolean {
  const [shape, allowedFreshness = []] = rule;
  const hasTime = fact.observed_at !== undefined;
  const hasFreshness = fact.freshness !== undefined;
  if (hasFreshness && !(allowedFreshness as readonly string[]).includes(fact.freshness ?? '')) {
    return false;
  }
  switch (shape) {
    case 'none':
      return !hasTime && !hasFreshness;
    case 'time':
      return hasTime && !hasFreshness;
    case 'freshness':
      return !hasTime && hasFreshness;
    case 'time_freshness':
      return hasTime && hasFreshness;
  }
}
