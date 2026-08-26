import type {
  AttachedWorkerDiagnostics,
  AttachedWorkerList,
  AttachedWorkerReadModel,
  AttachedWorkerSummary,
} from '$lib/api/client';

const evaluatedAt = '2026-08-26T08:00:00.123456Z';

export function attachedWorkerSummary(workerId = 'worker-one'): AttachedWorkerSummary {
  return {
    evaluated_at: evaluatedAt,
    worker: {
      worker_id: workerId,
      display_name: workerId === 'worker-one' ? 'Studio Mac' : 'Build server',
      revision: '12',
      enrollment_generation: '2',
      connection_generation: '4',
      desired_state: 'active',
      observed_state: 'online',
      created_at: '2026-08-01T09:00:00Z',
      updated_at: evaluatedAt,
    },
    connectivity: {
      connection_id: `connection-${workerId}`,
      state: 'online',
      connected_at: '2026-08-26T07:30:00Z',
      last_contact_at: '2026-08-26T07:59:58.765432Z',
      presence_expires_at: '2026-08-26T08:01:00Z',
      authentication_expires_at: '2026-08-27T08:00:00Z',
      freshness: 'fresh',
      last_failure: { state: 'unknown' },
    },
    execution_state: 'cancel_requested',
    observation_warnings: ['isolation_unsupported', 'quota_zero'],
  };
}

export function attachedWorkerList(
  items: AttachedWorkerSummary[] = [attachedWorkerSummary()],
): AttachedWorkerList {
  return {
    version: 1,
    evaluated_at: evaluatedAt,
    items,
    has_more: false,
  };
}

export function attachedWorkerDetail(): AttachedWorkerReadModel {
  const summary = attachedWorkerSummary();
  return {
    version: 1,
    evaluated_at: evaluatedAt,
    worker: summary.worker,
    identity: {
      algorithm: 'Ed25519',
      fingerprint: 'sha256:public-fingerprint',
      enrollment_state: 'consumed',
    },
    readiness: {
      daemon_observation: {
        state: 'unknown',
        source: 'unavailable',
        freshness: 'unknown',
      },
      last_daemon_failure: {
        state: 'unknown',
      },
      credential_state: 'unknown',
      isolation: {
        configuration_state: 'unsupported',
        advertised_evidence: ['network_boundary', 'process_boundary'],
        verification_state: 'unsupported',
      },
    },
    connectivity: summary.connectivity,
    capability: {
      state: 'advertised',
      manifest_revision: '8',
      digest_fingerprint: 'sha256:manifest',
      operating_system: 'darwin',
      architecture: 'arm64',
      build_id: 'worker-build-1',
      harness: { name: 'sessionless', version: '1.0', surface: 'session_turn_v1' },
      isolation_evidence: ['network_boundary'],
      features: ['exec', 'files'],
      max_concurrent_attempts: 1,
      observed_at: '2026-08-26T07:59:57Z',
    },
    admission_preview: { state: 'not_evaluated' },
    observation_warnings: ['isolation_unsupported', 'quota_zero', 'control_contract_unavailable'],
    resource: {
      state: 'observed',
      resource_ref: 'codex-subscription',
      credential_state: 'unknown',
      entitlement_state: 'active',
      quota: {
        state: 'zero',
        remaining: '0',
        observed_at: '2026-08-26T07:58:00Z',
        source: 'worker_report',
        freshness: 'fresh',
      },
    },
    execution: {
      state: 'cancel_requested',
      run_id: 'run-one',
      attempt_id: 'attempt-one',
      lease_id: 'lease-one',
      lease_generation: '5',
      fence_fingerprint: 'sha256:fence',
      lease_expires_at: '2026-08-26T08:02:00Z',
      cancel_request: {
        state: 'requested',
        revision: '3',
        requested_at: '2026-08-26T07:59:50Z',
        ack_deadline: '2026-08-26T08:00:10Z',
      },
      cancel_ack: { state: 'pending', revision: '3' },
      process_observation: {
        state: 'running',
        attempt_id: 'attempt-one',
        lease_generation: '5',
        fence_fingerprint: 'sha256:fence',
        source: 'worker_report',
        observed_at: '2026-08-26T07:59:51Z',
        freshness: 'fresh',
      },
      worker_terminal: {
        state: 'received',
        sequence: '11',
        status: 'cancelled',
        evidence_fingerprint: 'sha256:terminal',
      },
      canonical_terminal: { state: 'not_committed' },
    },
    governance: {
      admission_control: 'paused',
      remote_erase: 'not_acknowledged',
      available_actions: [
        {
          code: 'revoke',
          enabled: false,
          reason_code: 'control_contract_unavailable',
        },
      ],
    },
  };
}

export function attachedWorkerDiagnostics(): AttachedWorkerDiagnostics {
  return {
    version: 1,
    evaluated_at: evaluatedAt,
    worker_id: 'worker-one',
    facts: [
      { cohort: 'identity', code: 'desired_state', state: 'active' },
      { cohort: 'identity', code: 'observed_state', state: 'online' },
      { cohort: 'identity', code: 'enrollment_state', state: 'consumed' },
      { cohort: 'readiness', code: 'daemon_state', state: 'unknown', freshness: 'unknown' },
      { cohort: 'readiness', code: 'last_daemon_failure', state: 'unknown' },
      { cohort: 'readiness', code: 'credential_state', state: 'unknown' },
      { cohort: 'readiness', code: 'isolation_configuration', state: 'unsupported' },
      { cohort: 'readiness', code: 'isolation_verification', state: 'unsupported' },
      { cohort: 'connectivity', code: 'connection_state', state: 'online' },
      {
        cohort: 'connectivity',
        code: 'last_contact',
        state: 'recorded',
        observed_at: '2026-08-26T07:59:58.765432Z',
        freshness: 'fresh',
      },
      { cohort: 'connectivity', code: 'transport_failure', state: 'unknown' },
      { cohort: 'eligibility', code: 'capability_state', state: 'advertised' },
      { cohort: 'eligibility', code: 'admission_preview', state: 'not_evaluated' },
      { cohort: 'eligibility', code: 'entitlement_state', state: 'active' },
      {
        cohort: 'eligibility',
        code: 'quota_state',
        state: 'zero',
        observed_at: '2026-08-26T07:58:00Z',
        freshness: 'fresh',
      },
      { cohort: 'execution', code: 'attempt_state', state: 'cancel_requested' },
      {
        cohort: 'execution',
        code: 'cancel_request',
        state: 'requested',
        observed_at: '2026-08-26T07:59:50Z',
      },
      { cohort: 'execution', code: 'cancel_ack', state: 'pending' },
      {
        cohort: 'execution',
        code: 'process_observation',
        state: 'running',
        observed_at: '2026-08-26T07:59:51Z',
        freshness: 'fresh',
      },
      { cohort: 'execution', code: 'worker_terminal', state: 'received' },
      { cohort: 'execution', code: 'canonical_terminal', state: 'not_committed' },
      { cohort: 'governance', code: 'admission_control', state: 'paused' },
      { cohort: 'governance', code: 'remote_erase', state: 'not_acknowledged' },
    ],
    warnings: ['isolation_unsupported', 'quota_zero', 'control_contract_unavailable'],
  };
}
