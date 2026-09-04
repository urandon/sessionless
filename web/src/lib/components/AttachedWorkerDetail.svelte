<script module lang="ts">
  import type { AttachedWorkerReadModel, CanonicalApiClient } from '$lib/api/client';

  export interface AttachedWorkerDetailApi {
    getAttachedWorker: CanonicalApiClient['getAttachedWorker'];
  }

  type ViewState = 'loading' | 'ready' | 'not-found' | 'error';
</script>

<script lang="ts">
  import { onMount } from 'svelte';
  import { resolve } from '$app/paths';

  import { ApiError } from '$lib/api/client';
  import { labelState } from '$lib/attached-worker/presentation';
  import AttachedWorkerActionCatalog from './AttachedWorkerActionCatalog.svelte';
  import AttachedWorkerTime from './AttachedWorkerTime.svelte';
  import AttachedWorkerWarnings from './AttachedWorkerWarnings.svelte';

  let { client, workerId }: { client: AttachedWorkerDetailApi; workerId: string } = $props();

  let view = $state<ViewState>('loading');
  let model = $state<AttachedWorkerReadModel | undefined>();
  let errorMessage = $state('');

  onMount(() => void load());

  async function load(): Promise<void> {
    view = 'loading';
    errorMessage = '';
    try {
      model = await client.getAttachedWorker(workerId);
      view = 'ready';
    } catch (error) {
      if (error instanceof ApiError && error.code === 'not_found') {
        view = 'not-found';
        return;
      }
      errorMessage =
        error instanceof ApiError ? error.message : 'The worker view is temporarily unavailable.';
      view = 'error';
    }
  }

  function shown(value: string | number | undefined): string {
    return value === undefined || value === '' ? 'Unknown' : String(value);
  }
</script>

<svelte:head>
  <title>{model?.worker.display_name ?? 'Attached worker'} · Sessionless</title>
</svelte:head>

<a class="back-link" href={resolve('/workers')}>← Attached workers</a>

{#if view === 'loading'}
  <section class="panel loading-state" aria-live="polite" aria-busy="true">
    <p>Loading worker details…</p>
  </section>
{:else if view === 'not-found'}
  <section class="panel error-state" aria-labelledby="worker-not-found-title">
    <p class="eyebrow">Worker unavailable</p>
    <h1 id="worker-not-found-title">This worker cannot be opened</h1>
    <p>It may not exist, or it may not belong to the owner in the active workspace.</p>
    <a class="button primary" href={resolve('/workers')}>Return to workers</a>
  </section>
{:else if view === 'error'}
  <section class="panel error-state" aria-labelledby="worker-load-error-title">
    <p class="eyebrow">Unable to load</p>
    <h1 id="worker-load-error-title">Worker details are temporarily unavailable</h1>
    <p role="alert">{errorMessage}</p>
    <button class="button primary" type="button" onclick={() => void load()}>Try again</button>
  </section>
{:else if model}
  <article class="worker-detail" aria-labelledby="worker-title">
    <header class="detail-heading">
      <div>
        <p class="eyebrow">Attached worker</p>
        <h1 id="worker-title">{model.worker.display_name}</h1>
        <p class="lede">
          Evaluated <AttachedWorkerTime value={model.evaluated_at} />. Current truth, observations,
          failures, and acknowledgements remain separate.
        </p>
      </div>
      <code>{model.worker.worker_id}</code>
    </header>

    <section class="cohort" aria-labelledby="identity-title">
      <div class="cohort-heading">
        <p class="cohort-number" aria-hidden="true">01</p>
        <div>
          <h2 id="identity-title">Identity and ownership</h2>
          <p>Current durable identity truth.</p>
        </div>
      </div>
      <dl class="facts">
        <div>
          <dt>Desired state</dt>
          <dd>{labelState(model.worker.desired_state)}</dd>
        </div>
        <div>
          <dt>Observed state</dt>
          <dd>{labelState(model.worker.observed_state)}</dd>
        </div>
        <div>
          <dt>Worker revision</dt>
          <dd>{model.worker.revision}</dd>
        </div>
        <div>
          <dt>Enrollment generation</dt>
          <dd>{model.worker.enrollment_generation}</dd>
        </div>
        <div>
          <dt>Connection generation</dt>
          <dd>{model.worker.connection_generation}</dd>
        </div>
        <div>
          <dt>Enrollment state</dt>
          <dd>{labelState(model.identity.enrollment_state)}</dd>
        </div>
        <div>
          <dt>Identity algorithm</dt>
          <dd>{model.identity.algorithm}</dd>
        </div>
        <div>
          <dt>Identity fingerprint</dt>
          <dd><code>{model.identity.fingerprint}</code></dd>
        </div>
        <div>
          <dt>Created</dt>
          <dd><AttachedWorkerTime value={model.worker.created_at} /></dd>
        </div>
        <div>
          <dt>Current truth updated</dt>
          <dd><AttachedWorkerTime value={model.worker.updated_at} /></dd>
        </div>
        {#if model.worker.revoked_at}
          <div>
            <dt>Revoked</dt>
            <dd><AttachedWorkerTime value={model.worker.revoked_at} /></dd>
          </div>
        {/if}
      </dl>
    </section>

    <section class="cohort" aria-labelledby="readiness-title">
      <div class="cohort-heading">
        <p class="cohort-number" aria-hidden="true">02</p>
        <div>
          <h2 id="readiness-title">Readiness and isolation</h2>
          <p>Local evidence never upgrades server authority.</p>
        </div>
      </div>
      <div class="split">
        <div>
          <h3>Current observation</h3>
          <dl class="facts compact">
            <div>
              <dt>Daemon state</dt>
              <dd>{labelState(model.readiness.daemon_observation.state)}</dd>
            </div>
            <div>
              <dt>Daemon source</dt>
              <dd>{labelState(model.readiness.daemon_observation.source)}</dd>
            </div>
            <div>
              <dt>Daemon freshness</dt>
              <dd>{labelState(model.readiness.daemon_observation.freshness)}</dd>
            </div>
            {#if model.readiness.daemon_observation.observed_at}
              <div>
                <dt>Daemon observed</dt>
                <dd>
                  <AttachedWorkerTime value={model.readiness.daemon_observation.observed_at} />
                </dd>
              </div>
            {/if}
            <div>
              <dt>Credential readiness</dt>
              <dd>{labelState(model.readiness.credential_state)}</dd>
            </div>
          </dl>
        </div>
        <div class="history">
          <h3>Last daemon failure</h3>
          <dl class="facts compact">
            <div>
              <dt>Failure state</dt>
              <dd>{labelState(model.readiness.last_daemon_failure.state)}</dd>
            </div>
            {#if model.readiness.last_daemon_failure.code}
              <div>
                <dt>Failure code</dt>
                <dd><code>{model.readiness.last_daemon_failure.code}</code></dd>
              </div>
            {/if}
            {#if model.readiness.last_daemon_failure.occurred_at}
              <div>
                <dt>Occurred</dt>
                <dd>
                  <AttachedWorkerTime value={model.readiness.last_daemon_failure.occurred_at} />
                </dd>
              </div>
            {/if}
            {#if model.readiness.last_daemon_failure.operation}
              <div>
                <dt>Operation</dt>
                <dd>{labelState(model.readiness.last_daemon_failure.operation)}</dd>
              </div>
            {/if}
            {#if model.readiness.last_daemon_failure.retry_class}
              <div>
                <dt>Retry class</dt>
                <dd>{labelState(model.readiness.last_daemon_failure.retry_class)}</dd>
              </div>
            {/if}
          </dl>
        </div>
      </div>
      <h3>Isolation</h3>
      <dl class="facts">
        <div>
          <dt>Configuration</dt>
          <dd>{labelState(model.readiness.isolation.configuration_state)}</dd>
        </div>
        <div>
          <dt>External verification</dt>
          <dd>{labelState(model.readiness.isolation.verification_state)}</dd>
        </div>
        <div class="wide">
          <dt>Worker-advertised evidence</dt>
          <dd>
            {model.readiness.isolation.advertised_evidence.length
              ? model.readiness.isolation.advertised_evidence.map(labelState).join(', ')
              : 'None reported'}
          </dd>
        </div>
      </dl>
    </section>

    <section class="cohort" aria-labelledby="connectivity-title">
      <div class="cohort-heading">
        <p class="cohort-number" aria-hidden="true">03</p>
        <div>
          <h2 id="connectivity-title">Connectivity and presence</h2>
          <p>Current connection, contact freshness, and history are independent.</p>
        </div>
      </div>
      <div class="split">
        <div>
          <h3>Current connection</h3>
          <dl class="facts compact">
            <div>
              <dt>Connection state</dt>
              <dd>{labelState(model.connectivity.state)}</dd>
            </div>
            <div>
              <dt>Contact freshness</dt>
              <dd>{labelState(model.connectivity.freshness)}</dd>
            </div>
            {#if model.connectivity.connection_id}<div>
                <dt>Connection ID</dt>
                <dd><code>{model.connectivity.connection_id}</code></dd>
              </div>{/if}
            {#if model.connectivity.connected_at}<div>
                <dt>Connected</dt>
                <dd><AttachedWorkerTime value={model.connectivity.connected_at} /></dd>
              </div>{/if}
            {#if model.connectivity.last_contact_at}<div>
                <dt>Last authenticated contact</dt>
                <dd><AttachedWorkerTime value={model.connectivity.last_contact_at} /></dd>
              </div>{/if}
            {#if model.connectivity.presence_expires_at}<div>
                <dt>Presence expires</dt>
                <dd><AttachedWorkerTime value={model.connectivity.presence_expires_at} /></dd>
              </div>{/if}
            {#if model.connectivity.authentication_expires_at}<div>
                <dt>Authentication expires</dt>
                <dd><AttachedWorkerTime value={model.connectivity.authentication_expires_at} /></dd>
              </div>{/if}
          </dl>
        </div>
        <div class="history">
          <h3>Last transport failure</h3>
          <dl class="facts compact">
            <div>
              <dt>Failure state</dt>
              <dd>{labelState(model.connectivity.last_failure.state)}</dd>
            </div>
            {#if model.connectivity.last_failure.code}<div>
                <dt>Failure code</dt>
                <dd><code>{model.connectivity.last_failure.code}</code></dd>
              </div>{/if}
            {#if model.connectivity.last_failure.occurred_at}<div>
                <dt>Occurred</dt>
                <dd><AttachedWorkerTime value={model.connectivity.last_failure.occurred_at} /></dd>
              </div>{/if}
            {#if model.connectivity.last_failure.operation}<div>
                <dt>Operation</dt>
                <dd>{labelState(model.connectivity.last_failure.operation)}</dd>
              </div>{/if}
            {#if model.connectivity.last_failure.retry_class}<div>
                <dt>Retry class</dt>
                <dd>{labelState(model.connectivity.last_failure.retry_class)}</dd>
              </div>{/if}
          </dl>
        </div>
      </div>
    </section>

    <section class="cohort" aria-labelledby="eligibility-title">
      <div class="cohort-heading">
        <p class="cohort-number" aria-hidden="true">04</p>
        <div>
          <h2 id="eligibility-title">Eligibility, capability, and capacity</h2>
          <p>Advertisement and admission are not the same fact.</p>
        </div>
      </div>
      <div class="split">
        <div>
          <h3>Advertised capability</h3>
          <dl class="facts compact">
            <div>
              <dt>Manifest state</dt>
              <dd>{labelState(model.capability.state)}</dd>
            </div>
            <div>
              <dt>Manifest revision</dt>
              <dd>{shown(model.capability.manifest_revision)}</dd>
            </div>
            <div>
              <dt>Operating system</dt>
              <dd>{shown(model.capability.operating_system)}</dd>
            </div>
            <div>
              <dt>Architecture</dt>
              <dd>{shown(model.capability.architecture)}</dd>
            </div>
            <div>
              <dt>Harness</dt>
              <dd>{shown(model.capability.harness.name)}</dd>
            </div>
            <div>
              <dt>Harness version</dt>
              <dd>{shown(model.capability.harness.version)}</dd>
            </div>
            <div>
              <dt>Surface</dt>
              <dd>{labelState(model.capability.harness.surface)}</dd>
            </div>
            <div>
              <dt>Advertised concurrency</dt>
              <dd>{shown(model.capability.max_concurrent_attempts)}</dd>
            </div>
            {#if model.capability.observed_at}<div>
                <dt>Manifest observed</dt>
                <dd><AttachedWorkerTime value={model.capability.observed_at} /></dd>
              </div>{/if}
            <div class="wide">
              <dt>Features</dt>
              <dd>
                {model.capability.features.length
                  ? model.capability.features.map(labelState).join(', ')
                  : 'None reported'}
              </dd>
            </div>
          </dl>
        </div>
        <div>
          <h3>Admission preview</h3>
          <dl class="facts compact">
            <div>
              <dt>Evaluation state</dt>
              <dd>{labelState(model.admission_preview.state)}</dd>
            </div>
            {#if model.admission_preview.decision_code}<div>
                <dt>Canonical decision</dt>
                <dd><code>{model.admission_preview.decision_code}</code></dd>
              </div>{/if}
            {#if model.admission_preview.evaluated_at}<div>
                <dt>Evaluated</dt>
                <dd><AttachedWorkerTime value={model.admission_preview.evaluated_at} /></dd>
              </div>{/if}
          </dl>
          <p class="note">
            This observation is not a placement grant. A generic worker view is not evaluated for
            admission.
          </p>
        </div>
      </div>
      <h3>Credential resource and quota</h3>
      <dl class="facts">
        <div>
          <dt>Resource state</dt>
          <dd>{labelState(model.resource.state)}</dd>
        </div>
        <div>
          <dt>Credential state</dt>
          <dd>{labelState(model.resource.credential_state)}</dd>
        </div>
        <div>
          <dt>Entitlement</dt>
          <dd>{labelState(model.resource.entitlement_state)}</dd>
        </div>
        <div>
          <dt>Quota state</dt>
          <dd>{labelState(model.resource.quota.state)}</dd>
        </div>
        {#if model.resource.quota.remaining !== undefined}<div>
            <dt>Quota remaining</dt>
            <dd>{model.resource.quota.remaining}</dd>
          </div>{/if}
        {#if model.resource.quota.observed_at}<div>
            <dt>Quota observed</dt>
            <dd><AttachedWorkerTime value={model.resource.quota.observed_at} /></dd>
          </div>{/if}
        {#if model.resource.quota.reset_at}<div>
            <dt>Quota resets</dt>
            <dd><AttachedWorkerTime value={model.resource.quota.reset_at} /></dd>
          </div>{/if}
      </dl>
    </section>

    <section class="cohort" aria-labelledby="execution-title">
      <div class="cohort-heading">
        <p class="cohort-number" aria-hidden="true">05</p>
        <div>
          <h2 id="execution-title">Execution and recovery</h2>
          <p>Cancellation, process, worker terminal, and canonical terminal remain separate.</p>
        </div>
      </div>
      <dl class="facts">
        <div>
          <dt>Attempt state</dt>
          <dd>{labelState(model.execution.state)}</dd>
        </div>
        {#if model.execution.run_id}<div>
            <dt>Run ID</dt>
            <dd><code>{model.execution.run_id}</code></dd>
          </div>{/if}
        {#if model.execution.attempt_id}<div>
            <dt>Attempt ID</dt>
            <dd><code>{model.execution.attempt_id}</code></dd>
          </div>{/if}
        {#if model.execution.lease_id}<div>
            <dt>Lease ID</dt>
            <dd><code>{model.execution.lease_id}</code></dd>
          </div>{/if}
        {#if model.execution.lease_generation !== undefined}<div>
            <dt>Lease generation</dt>
            <dd>{model.execution.lease_generation}</dd>
          </div>{/if}
        {#if model.execution.lease_expires_at}<div>
            <dt>Lease expires</dt>
            <dd><AttachedWorkerTime value={model.execution.lease_expires_at} /></dd>
          </div>{/if}
      </dl>
      <div class="phase-grid">
        <section aria-labelledby="cancel-request-title">
          <h3 id="cancel-request-title">Cancellation request</h3>
          <p>{labelState(model.execution.cancel_request.state)}</p>
          {#if model.execution.cancel_request.revision !== undefined}<small
              >Revision {model.execution.cancel_request.revision}</small
            >{/if}
          <small
            >Requested: <AttachedWorkerTime
              value={model.execution.cancel_request.requested_at}
            /></small
          >
          <small
            >Acknowledgement deadline: <AttachedWorkerTime
              value={model.execution.cancel_request.ack_deadline}
            /></small
          >
        </section>
        <section aria-labelledby="cancel-ack-title">
          <h3 id="cancel-ack-title">Cancellation acknowledgement</h3>
          <p>{labelState(model.execution.cancel_ack.state)}</p>
          {#if model.execution.cancel_ack.revision !== undefined}<small
              >Revision {model.execution.cancel_ack.revision}</small
            >{/if}
          <small
            >Acknowledged: <AttachedWorkerTime
              value={model.execution.cancel_ack.acknowledged_at}
            /></small
          >
        </section>
        <section aria-labelledby="process-title">
          <h3 id="process-title">Process observation</h3>
          <p>{labelState(model.execution.process_observation.state)}</p>
          {#if model.execution.process_observation.attempt_id}<small
              >Attempt: <code>{model.execution.process_observation.attempt_id}</code></small
            >{/if}
          {#if model.execution.process_observation.lease_generation !== undefined}<small
              >Lease generation: {model.execution.process_observation.lease_generation}</small
            >{/if}
          {#if model.execution.process_observation.fence_fingerprint}<small
              >Fence: <code>{model.execution.process_observation.fence_fingerprint}</code></small
            >{/if}
          <small>Source: {labelState(model.execution.process_observation.source)}</small>
          <small
            >Observed: <AttachedWorkerTime
              value={model.execution.process_observation.observed_at}
            /></small
          >
          <small>Freshness: {labelState(model.execution.process_observation.freshness)}</small>
        </section>
        <section aria-labelledby="worker-terminal-title">
          <h3 id="worker-terminal-title">Worker terminal evidence</h3>
          <p>{labelState(model.execution.worker_terminal.state)}</p>
          {#if model.execution.worker_terminal.sequence !== undefined}<small
              >Sequence: {model.execution.worker_terminal.sequence}</small
            >{/if}
          {#if model.execution.worker_terminal.status}<small
              >Status: {labelState(model.execution.worker_terminal.status)}</small
            >{/if}
          {#if model.execution.worker_terminal.evidence_fingerprint}<small
              >Evidence: <code>{model.execution.worker_terminal.evidence_fingerprint}</code></small
            >{/if}
        </section>
        <section aria-labelledby="canonical-terminal-title">
          <h3 id="canonical-terminal-title">Canonical terminal commit</h3>
          <p>{labelState(model.execution.canonical_terminal.state)}</p>
          {#if model.execution.canonical_terminal.sequence !== undefined}<small
              >Sequence: {model.execution.canonical_terminal.sequence}</small
            >{/if}
          {#if model.execution.canonical_terminal.status}<small
              >Status: {labelState(model.execution.canonical_terminal.status)}</small
            >{/if}
          <small
            >Committed: <AttachedWorkerTime
              value={model.execution.canonical_terminal.committed_at}
            /></small
          >
        </section>
      </div>
    </section>

    <section class="cohort" aria-labelledby="governance-title">
      <div class="cohort-heading">
        <p class="cohort-number" aria-hidden="true">06</p>
        <div>
          <h2 id="governance-title">Policy, lifecycle, and governance</h2>
          <p>Revocation authority and remote erasure acknowledgement are distinct.</p>
        </div>
      </div>
      <dl class="facts">
        <div>
          <dt>Admission control</dt>
          <dd>{labelState(model.governance.admission_control)}</dd>
        </div>
        <div>
          <dt>Remote erase acknowledgement</dt>
          <dd>{labelState(model.governance.remote_erase)}</dd>
        </div>
      </dl>
      <h3>Controls</h3>
      <p class="note">
        This interface is read-only. Controls remain inert and show their exact bounded reason.
      </p>
      <AttachedWorkerActionCatalog actions={model.governance.available_actions} />
    </section>

    <aside class="warnings" aria-labelledby="warnings-title">
      <div class="cohort-heading">
        <p class="cohort-number" aria-hidden="true">!</p>
        <div>
          <h2 id="warnings-title">All observation warnings</h2>
          <p>Warnings are evidence, not an admission decision.</p>
        </div>
      </div>
      <AttachedWorkerWarnings codes={model.observation_warnings} />
    </aside>
  </article>
{/if}

<style>
  .worker-detail {
    display: grid;
    gap: 1.25rem;
  }

  .detail-heading {
    display: flex;
    align-items: end;
    justify-content: space-between;
    gap: 2rem;
    margin-bottom: 1rem;
  }

  .detail-heading > code {
    max-width: 24rem;
    overflow-wrap: anywhere;
  }

  .cohort,
  .warnings {
    display: grid;
    gap: 1.25rem;
    border: 1px solid var(--line);
    border-radius: 1rem;
    padding: clamp(1rem, 3vw, 1.75rem);
    background: var(--paper);
  }

  .cohort-heading {
    display: grid;
    grid-template-columns: 2rem minmax(0, 1fr);
    gap: 0.75rem;
  }

  .cohort-heading h2,
  .cohort-heading p,
  h3,
  dd,
  .phase-grid p {
    margin: 0;
  }

  .cohort-heading p:not(.cohort-number),
  .note,
  small {
    color: var(--muted);
  }

  .cohort-number {
    color: var(--accent-dark);
    font-weight: 850;
  }

  .facts {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(min(100%, 13rem), 1fr));
    gap: 0.85rem;
    margin: 0;
  }

  .facts > div {
    min-width: 0;
    border-top: 1px solid var(--line);
    padding-top: 0.65rem;
  }

  .facts.compact {
    grid-template-columns: 1fr;
  }

  .facts .wide {
    grid-column: 1 / -1;
  }

  dt {
    color: var(--muted);
    font-size: 0.78rem;
    font-weight: 750;
  }

  dd {
    margin-top: 0.25rem;
    overflow-wrap: anywhere;
  }

  .split {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 1.25rem;
  }

  .split > div,
  .phase-grid > section {
    display: grid;
    align-content: start;
    gap: 0.75rem;
  }

  .history {
    border-left: 0.25rem solid var(--line);
    padding-left: 1rem;
  }

  .phase-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(min(100%, 13rem), 1fr));
    gap: 0.75rem;
  }

  .phase-grid > section {
    border: 1px solid var(--line);
    border-radius: 0.75rem;
    padding: 0.9rem;
  }

  .phase-grid small {
    display: block;
  }

  .note {
    margin-bottom: 0;
  }

  .warnings {
    border-color: #c98248;
  }

  .error-state {
    display: grid;
    gap: 1rem;
    justify-items: start;
  }

  .error-state h1,
  .error-state p {
    margin-bottom: 0;
  }

  @media (max-width: 46rem) {
    .detail-heading,
    .split {
      grid-template-columns: 1fr;
    }

    .detail-heading {
      align-items: start;
      flex-direction: column;
    }

    .history {
      border-top: 0.25rem solid var(--line);
      border-left: 0;
      padding-top: 1rem;
      padding-left: 0;
    }
  }
</style>
