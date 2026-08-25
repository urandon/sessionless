import type {
  AttachedWorkerActionCode,
  AttachedWorkerActionUnavailableCode,
  AttachedWorkerReasonCode,
} from '$lib/api/client';

export interface PublicExplanation {
  title: string;
  description: string;
}

export const reasonExplanations = {
  worker_not_active: [
    'Worker is not active',
    'The worker lifecycle does not currently allow work.',
  ],
  worker_revoked: [
    'Worker is revoked',
    'Control-plane authority for this worker has been revoked.',
  ],
  worker_draining: ['Worker is draining', 'New work is not admitted while existing work drains.'],
  worker_offline: ['Worker is offline', 'The authoritative worker state is offline.'],
  connection_attaching: [
    'Connection is attaching',
    'The current connection has not completed attachment.',
  ],
  connection_superseded: [
    'Connection was superseded',
    'A newer connection generation replaced this connection.',
  ],
  presence_expired: ['Presence expired', 'The last authenticated presence lease has expired.'],
  authentication_expired: [
    'Authentication expired',
    'The current connection authentication has expired.',
  ],
  protocol_incompatible: [
    'Protocol is incompatible',
    'The worker and platform have no accepted protocol version.',
  ],
  capability_missing: [
    'Capability is unavailable',
    'No current signed capability manifest is available.',
  ],
  capability_stale: [
    'Capability is stale',
    'The capability observation does not match current worker truth.',
  ],
  capability_mismatch: [
    'Capability does not match',
    'The advertised capability does not match the required scope.',
  ],
  policy_mismatch: ['Policy does not match', 'Current policy does not allow the requested scope.'],
  isolation_unsupported: [
    'Isolation is unsupported',
    'No reviewed production isolation launcher is available.',
  ],
  isolation_unverified: [
    'Isolation is unverified',
    'Configured isolation has no current external verification.',
  ],
  credential_unavailable: [
    'Credential is unavailable',
    'Credential readiness could not be established.',
  ],
  credential_reauth_required: [
    'Credential needs reauthentication',
    'The owner must reauthenticate the credential resource.',
  ],
  entitlement_unknown: [
    'Entitlement is unknown',
    'No trustworthy entitlement observation is available.',
  ],
  entitlement_inactive: ['Entitlement is inactive', 'The observed entitlement does not allow use.'],
  quota_unknown: ['Quota is unknown', 'No trustworthy quota observation is available.'],
  quota_zero: ['Quota is zero', 'The observed remaining quota is exactly zero.'],
  quota_exhausted: ['Quota is exhausted', 'The provider reported that quota is exhausted.'],
  capacity_busy: ['Capacity is busy', 'The worker has no advisory capacity for another attempt.'],
  attempt_active: ['Attempt is active', 'A durable attempt is still active for this worker.'],
  attempt_ambiguous: [
    'Attempt outcome is ambiguous',
    'Automatic retry and finalization remain blocked.',
  ],
  control_contract_unavailable: [
    'Control is unavailable',
    'The required reviewed control contract is not available.',
  ],
  backend_unavailable: [
    'Observation is unavailable',
    'The bounded backend observation could not be loaded.',
  ],
} satisfies Record<AttachedWorkerReasonCode, readonly [string, string]>;

export const actionExplanations = {
  create_enrollment: 'Create enrollment',
  consume_enrollment: 'Consume enrollment',
  rename: 'Rename worker',
  rotate_identity: 'Rotate identity',
  pause_admission: 'Pause admission',
  resume_admission: 'Resume admission',
  drain: 'Drain worker',
  revoke: 'Revoke worker',
  request_cancel: 'Request cancellation',
  reconnect_remediation: 'Reconnect',
  reauth_remediation: 'Reauthenticate credential',
  check_update: 'Check for updates',
  logout: 'Log out local worker',
  uninstall_plan: 'Plan uninstall',
} satisfies Record<AttachedWorkerActionCode, string>;

export const unavailableExplanations = {
  not_found: ['Not found', 'The resource is unavailable in the active workspace.'],
  stale_revision: ['State changed', 'The worker revision changed after the action was planned.'],
  stale_generation: [
    'Generation changed',
    'A newer identity or connection generation is authoritative.',
  ],
  invalid_state: [
    'State does not allow this action',
    'The current authoritative state rejects this action.',
  ],
  active_attempt: ['Attempt is active', 'An active attempt prevents this action.'],
  ambiguous_attempt: [
    'Attempt is ambiguous',
    'The attempt must remain fenced until an explicit resolution exists.',
  ],
  awaiting_acknowledgement: [
    'Acknowledgement is pending',
    'The requested transition has not been acknowledged.',
  ],
  already_applied: ['Already applied', 'The exact requested transition has already been applied.'],
  unsupported_platform: [
    'Platform is unsupported',
    'This action has no reviewed implementation for the platform.',
  ],
  feature_disabled: [
    'Feature is disabled',
    'The implemented feature is not enabled in this deployment.',
  ],
  control_contract_unavailable: [
    'Control contract is unavailable',
    'No reviewed authority and receipt contract exists yet.',
  ],
  confirmation_required: [
    'Confirmation is required',
    'A bounded action plan must be confirmed before apply.',
  ],
  operation_in_progress: ['Operation is in progress', 'An earlier operation is still in progress.'],
} satisfies Record<AttachedWorkerActionUnavailableCode, readonly [string, string]>;

export function explainReason(code: string): PublicExplanation {
  const explanation = (reasonExplanations as Record<string, readonly [string, string]>)[code];
  return explanation
    ? { title: explanation[0], description: explanation[1] }
    : {
        title: `Unknown reason (${safeCode(code)})`,
        description: 'This version does not recognize the reported reason.',
      };
}

export function explainAction(code: string): string {
  return (
    (actionExplanations as Record<string, string>)[code] ?? `Unknown action (${safeCode(code)})`
  );
}

export function explainUnavailable(code: string | undefined): PublicExplanation {
  if (!code)
    return { title: 'Unavailable', description: 'No bounded availability reason was reported.' };
  const explanation = (unavailableExplanations as Record<string, readonly [string, string]>)[code];
  return explanation
    ? { title: explanation[0], description: explanation[1] }
    : {
        title: `Unknown reason (${safeCode(code)})`,
        description: 'This version does not recognize the reported action reason.',
      };
}

export function labelState(value: string | undefined): string {
  if (!value) return 'Unknown';
  return value
    .split('_')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

export function validTimestamp(value: string | undefined): value is string {
  if (!value) return false;
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$/.test(value)) return false;
  return !Number.isNaN(Date.parse(value));
}

export function exactUTCTime(value: string): string {
  if (!validTimestamp(value)) return 'Unknown time';
  return value.replace('T', ' ').replace(/Z$/, ' UTC');
}

function safeCode(value: string): string {
  const safe = value.replace(/[^a-z0-9_-]/gi, '').slice(0, 64);
  return safe || 'unrecognized';
}
