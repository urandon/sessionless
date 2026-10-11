<script lang="ts">
  import { onMount } from 'svelte';
  import type { RunExplanationV1 } from '$lib/api/client';
  import type { RunExplanationState } from '$lib/run-explanation/controller';

  let {
    state: explanationState,
    onClose,
    onRefresh,
    refreshDisabled,
  }: {
    state: RunExplanationState;
    onClose: () => void;
    onRefresh: () => void;
    refreshDisabled: boolean;
  } = $props();

  const id = $props.id();
  let panel: HTMLElement;
  let heading: HTMLHeadingElement;
  let announcement = $state('');
  let previousMaterial = '';

  const runStatus: Record<RunExplanationV1['status'], string> = {
    created: 'Created',
    admitted: 'Admitted',
    queued: 'Queued',
    running: 'Running',
    succeeded: 'Succeeded',
    failed: 'Failed',
    cancelled: 'Cancelled',
    quota_blocked: 'Quota blocked',
  };
  const admissionReason: Record<
    NonNullable<RunExplanationV1['admission']['reason_code']>,
    string
  > = {
    admitted: 'Admission was recorded.',
    subscription_attention_required: 'Subscription attention was required.',
    capacity_draining: 'Capacity was draining.',
    quota_reset_pending: 'Quota reset was pending.',
    quota_exhausted_reset_unknown: 'Quota was exhausted; reset time was unknown.',
    runtime_limit_exceeded: 'The runtime limit was exceeded.',
    turn_limit_exceeded: 'The turn limit was exceeded.',
    input_limit_exceeded: 'The input limit was exceeded.',
    context_limit_exceeded: 'The context limit was exceeded.',
    artifact_limit_exceeded: 'The artifact limit was exceeded.',
    capacity_busy: 'Capacity was busy.',
    workspace_queue_limit: 'The workspace queue limit was reached.',
    workspace_active_run_limit: 'The workspace active Run limit was reached.',
    compute_unavailable: 'The selected compute was unavailable.',
    choice_stale: 'The selected compute choice was stale.',
    consent_required: 'Consent for the selected compute was required.',
    compute_policy_denied: 'Policy denied the selected compute.',
  };
  const terminalReason: Record<NonNullable<RunExplanationV1['terminal']['reason_code']>, string> = {
    execution_failed: 'An execution phase failure was recorded.',
    result_persistence_failed: 'A result persistence failure was recorded.',
    canonical_cancelled: 'Canonical cancellation was recorded.',
    unclassified_failure: 'A failure notice was recorded; its detailed cause is unknown.',
  };
  const attachedFact: Record<NonNullable<RunExplanationV1['attached']['fact']>, string> = {
    offer_recorded: 'An offer was recorded.',
    claim_recorded: 'A claim was recorded.',
    terminal_candidate_recorded: 'A terminal candidate was recorded, not canonical completion.',
    terminal_commit_recorded: 'A terminal commit receipt was recorded.',
    fenced_outcome_unknown: 'A fenced receipt was recorded; the physical outcome is unknown.',
    receipt_retired: 'The receipt was retired.',
  };
  const freshness: Record<NonNullable<RunExplanationV1['attached']['freshness']>, string> = {
    within_lease: 'Within lease at the server read time',
    expired: 'Expired at the server read time',
    durable: 'Durable historical receipt',
  };
  const coverage = { recorded: 'Recorded', unknown: 'Unknown', not_applicable: 'Not applicable' };

  // Permission loss must hide evidence even if a caller accidentally retains it.
  let evidence = $derived(
    ['ready', 'loading', 'refresh-failed'].includes(explanationState.phase)
      ? explanationState.evidence
      : undefined,
  );
  let partial = $derived(
    evidence && Object.values(evidence.coverage).some((value) => value === 'unknown'),
  );
  let cannotRefresh = $derived(
    refreshDisabled || ['loading', 'permission-lost', 'disposed'].includes(explanationState.phase),
  );

  onMount(() => {
    heading.focus();
  });

  function dismissFromPanel(event: KeyboardEvent) {
    if (event.key === 'Escape' && event.target instanceof Node && panel.contains(event.target)) {
      event.preventDefault();
      onClose();
    }
  }

  // Read times, spinner transitions and unchanged polls are not material facts.
  $effect(() => {
    const value = evidence;
    const errorPhase = ['refresh-failed', 'permission-lost', 'disposed'].includes(
      explanationState.phase,
    )
      ? explanationState.phase
      : 'read';
    const material = JSON.stringify([
      errorPhase,
      value?.run_id,
      value?.status,
      value?.admission.availability,
      value?.admission.outcome,
      value?.admission.reason_code,
      value?.terminal.availability,
      value?.terminal.reason_code,
      value?.attempt.availability,
      value?.attempt.attempt_id,
      value?.attempt.status,
      value?.attached.availability,
      value?.attached.fact,
      value?.attached.freshness,
      value?.coverage.admission,
      value?.coverage.terminal,
      value?.coverage.attempt,
      value?.coverage.operational,
    ]);
    if (material === previousMaterial) return;
    previousMaterial = material;
    if (explanationState.phase === 'permission-lost') {
      announcement = 'Run evidence is no longer available. Previously read evidence was removed.';
    } else if (explanationState.phase === 'refresh-failed') {
      announcement = value
        ? 'Refresh failed. Previously read evidence remains historical, not a new observation.'
        : 'Run evidence could not be read. Execution details remain unknown.';
    } else if (value) {
      const admission = value.admission.reason_code
        ? admissionReason[value.admission.reason_code]
        : 'Admission reason unknown.';
      const terminal = value.terminal.reason_code
        ? terminalReason[value.terminal.reason_code]
        : `Terminal reason: ${coverage[value.terminal.availability]}.`;
      const attempt = value.attempt.status ? runStatus[value.attempt.status] : 'Unknown';
      const receipt = value.attached.fact
        ? attachedFact[value.attached.fact]
        : 'Attached receipt unknown.';
      const receiptFreshness = value.attached.freshness
        ? freshness[value.attached.freshness]
        : 'Unknown';
      announcement = `Recorded Run evidence updated. Canonical Run: ${runStatus[value.status]}. Last recorded admission: ${value.admission.outcome === 'denied' ? 'Denied. ' : ''}${admission} ${terminal} Selected Attempt: ${attempt}. ${receipt} Server freshness: ${receiptFreshness}. ${partial ? 'Coverage is partial; some evidence is unknown.' : 'Per-source coverage is shown.'}`;
    } else {
      announcement = '';
    }
  });
</script>

<svelte:document onkeydown={dismissFromPanel} />

<aside
  id="run-explanation-panel"
  bind:this={panel}
  class="explanation-panel"
  aria-labelledby={`${id}-title`}
>
  <header>
    <h2 bind:this={heading} id={`${id}-title`} tabindex="-1">Run explanation</h2>
    <div class="controls">
      <button type="button" onclick={onRefresh} disabled={cannotRefresh}>Refresh explanation</button
      >
      <button type="button" onclick={onClose}>Close explanation</button>
    </div>
  </header>
  <p class="context">
    Recorded evidence for this Run. This drawer is read-only and does not retry or change execution.
  </p>
  <p class="announcement" role="status" aria-live="polite" aria-atomic="true">{announcement}</p>

  {#if explanationState.phase === 'permission-lost'}
    <p class="read-warning">
      Run evidence is no longer available in this scope. Previously read evidence was removed. Close
      this drawer to return to the conversation.
    </p>
  {:else if explanationState.phase === 'disposed'}
    <p class="read-warning">This explanation view is closed. Execution details are unavailable.</p>
  {:else if explanationState.phase === 'refresh-failed'}
    <p class="read-warning">
      {explanationState.refreshError?.code === 'rate_limited'
        ? 'Refresh was rate limited.'
        : 'Evidence refresh failed.'}
      {#if evidence}Previously read evidence is retained below at its original read time; it is not
        a new observation.{:else}Execution details remain unknown; no successful read is available.{/if}
      Refresh when available, or close this drawer to return to the conversation.
    </p>
  {:else if explanationState.phase === 'loading'}
    <p class="read-note">
      Reading recorded evidence… {evidence
        ? 'The previous read remains below until refresh finishes.'
        : 'Execution details are not yet available.'}
    </p>
  {:else if !evidence}
    <p class="read-note">
      No Run evidence has been read. Execution details are unknown. Refresh when available.
    </p>
  {/if}
  {#if cannotRefresh && !['permission-lost', 'disposed'].includes(explanationState.phase)}
    <p class="read-note">
      Refresh is unavailable while a read is in progress or the refresh limit applies.
    </p>
  {/if}

  {#if evidence}
    <p class="read-note">
      {explanationState.phase === 'refresh-failed' || explanationState.phase === 'loading'
        ? 'Previously read at'
        : 'Server read at'} <time datetime={evidence.read_at}>{evidence.read_at}</time>. Lease
      freshness below is the server assessment at this read, not a live health check.
    </p>
    {#if partial}<p class="read-warning">
        Partial evidence: one or more sources are unknown. Unknown does not mean healthy, ready or
        succeeded.
      </p>{/if}

    <section aria-labelledby={`${id}-run`}>
      <h3 id={`${id}-run`}>Canonical Run</h3>
      <dl>
        <dt>Run</dt>
        <dd>{evidence.run_id}</dd>
        <dt>Status</dt>
        <dd>{runStatus[evidence.status]}</dd>
        <dt>Created at</dt>
        <dd><time datetime={evidence.created_at}>{evidence.created_at}</time></dd>
        <dt>Updated at</dt>
        <dd><time datetime={evidence.updated_at}>{evidence.updated_at}</time></dd>
        {#if evidence.finished_at}<dt>Finished at</dt>
          <dd><time datetime={evidence.finished_at}>{evidence.finished_at}</time></dd>{/if}
      </dl>
      <p>
        Canonical status is authoritative. Receipt age or refresh failure does not change canonical
        completion.
      </p>
    </section>

    <section aria-labelledby={`${id}-admission`}>
      <h3 id={`${id}-admission`}>Last recorded admission decision</h3>
      {#if evidence.admission.availability === 'recorded'}
        <p>
          {evidence.admission.outcome === 'denied' ? 'Denied' : 'Admitted'} — {evidence.admission
            .reason_code
            ? admissionReason[evidence.admission.reason_code]
            : 'Reason unknown.'}
        </p>
        <dl>
          <dt>Observed at</dt>
          <dd>
            <time datetime={evidence.admission.observed_at}>{evidence.admission.observed_at}</time>
          </dd>
          <dt>Coverage</dt>
          <dd>Last recorded decision only</dd>
        </dl>
        <p>
          This is a recorded decision, not a current entitlement guarantee or a new policy
          evaluation.
        </p>
      {:else}<p>Unknown: no supported admission decision is available for this Run.</p>{/if}
    </section>

    <section aria-labelledby={`${id}-terminal`}>
      <h3 id={`${id}-terminal`}>Canonical terminal reason</h3>
      {#if evidence.terminal.availability === 'recorded'}
        <p>
          Recorded — {evidence.terminal.reason_code
            ? terminalReason[evidence.terminal.reason_code]
            : 'Reason unknown.'}
        </p>
        <dl>
          <dt>Observed at</dt>
          <dd>
            <time datetime={evidence.terminal.observed_at}>{evidence.terminal.observed_at}</time>
          </dd>
        </dl>
        <p>A recorded phase notice is not an independently diagnosed root cause.</p>
      {:else if evidence.terminal.availability === 'not_applicable'}
        <p>Not applicable: this Run is not canonical terminal.</p>
      {:else}<p>
          Unknown: canonical terminal reason evidence is unavailable. Canonical status above remains
          authoritative.
        </p>{/if}
    </section>

    <section aria-labelledby={`${id}-attempt`}>
      <h3 id={`${id}-attempt`}>Selected Attempt</h3>
      {#if evidence.attempt.availability === 'recorded'}
        <dl>
          <dt>Attempt</dt>
          <dd>{evidence.attempt.attempt_id}</dd>
          <dt>Number</dt>
          <dd>{evidence.attempt.number}</dd>
          <dt>Canonical status</dt>
          <dd>{evidence.attempt.status ? runStatus[evidence.attempt.status] : 'Unknown'}</dd>
          <dt>Updated at</dt>
          <dd><time datetime={evidence.attempt.updated_at}>{evidence.attempt.updated_at}</time></dd>
          {#if evidence.attempt.finished_at}<dt>Finished at</dt>
            <dd>
              <time datetime={evidence.attempt.finished_at}>{evidence.attempt.finished_at}</time>
            </dd>{/if}
        </dl>
        <p>
          The exact selected canonical Attempt is shown, not all earlier attempts. Its state is not
          a physical-process heartbeat.
        </p>
      {:else}<p>
          Unknown: the selected Attempt is unavailable. No physical-process state can be inferred.
        </p>{/if}
    </section>

    <section aria-labelledby={`${id}-attached`}>
      <h3 id={`${id}-attached`}>Attached receipt observation</h3>
      {#if evidence.attached.availability === 'recorded'}
        <p>{evidence.attached.fact ? attachedFact[evidence.attached.fact] : 'Fact unknown.'}</p>
        <dl>
          <dt>Observed at</dt>
          <dd>
            <time datetime={evidence.attached.observed_at}>{evidence.attached.observed_at}</time>
          </dd>
          <dt>Server freshness</dt>
          <dd>
            {evidence.attached.freshness ? freshness[evidence.attached.freshness] : 'Unknown'}
          </dd>
          {#if evidence.attached.valid_until}<dt>Lease valid until</dt>
            <dd>
              <time datetime={evidence.attached.valid_until}>{evidence.attached.valid_until}</time>
            </dd>{/if}
        </dl>
        <p>
          This receipt is separate from canonical completion. Lease validity does not prove the host
          is alive or a process has stopped.
        </p>
      {:else}<p>
          Unknown: no matching supported attached receipt is available. Managed remote-process
          observation is unknown in this version; unknown is not “no work”.
        </p>{/if}
    </section>

    <section aria-labelledby={`${id}-coverage`}>
      <h3 id={`${id}-coverage`}>Per-source coverage</h3>
      <dl>
        <dt>Admission</dt>
        <dd>{coverage[evidence.coverage.admission]}</dd>
        <dt>Terminal reason</dt>
        <dd>{coverage[evidence.coverage.terminal]}</dd>
        <dt>Selected Attempt</dt>
        <dd>{coverage[evidence.coverage.attempt]}</dd>
        <dt>Operational receipt</dt>
        <dd>{coverage[evidence.coverage.operational]}</dd>
      </dl>
      <p>
        Coverage describes source presence and consistency, not complete history, telemetry or
        billing.
      </p>
    </section>
  {/if}
</aside>

<style>
  .explanation-panel {
    box-sizing: border-box;
    width: min(100%, 30rem);
    max-width: 100%;
    max-height: min(36rem, 70dvh);
    overflow-y: auto;
    overscroll-behavior: contain;
    min-width: 0;
    margin-block: 1rem;
    padding: 1rem;
    border: 1px solid var(--line);
    border-radius: 0.75rem;
    background: var(--paper);
    color: var(--ink);
    overflow-wrap: anywhere;
  }
  header,
  .controls {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
  }
  header {
    position: sticky;
    top: 0;
    background: var(--paper);
    padding-bottom: 0.5rem;
    z-index: 1;
    align-items: baseline;
    justify-content: space-between;
    gap: 1rem;
  }
  h2 {
    margin: 0;
    font-size: 1.4rem;
    line-height: 1.2;
  }
  h3 {
    margin: 0 0 0.5rem;
    font-size: 1rem;
  }
  button {
    min-height: 44px;
    max-width: 100%;
    padding: 0.5rem 0.75rem;
    border: 1px solid var(--line);
    border-radius: 0.5rem;
    background: var(--paper);
    color: var(--ink);
    font: inherit;
    cursor: pointer;
  }
  button:disabled {
    cursor: default;
    color: var(--muted);
  }
  button:focus-visible,
  h2:focus {
    outline: 3px solid var(--focus);
    outline-offset: 3px;
  }
  p {
    margin: 0.5rem 0 0;
    line-height: 1.5;
  }
  .context,
  .read-note,
  section p {
    color: var(--muted);
  }
  .read-warning {
    padding: 0.75rem;
    border-inline-start: 3px solid var(--accent);
    background: #f4f1e8;
  }
  section {
    min-width: 0;
    padding-block: 1rem;
    border-top: 1px solid var(--line);
    margin-top: 1rem;
  }
  dl {
    display: grid;
    grid-template-columns: minmax(0, 1fr) minmax(0, 1.5fr);
    gap: 0.5rem;
    margin: 0.5rem 0;
  }
  dt {
    color: var(--muted);
  }
  dd {
    margin: 0;
    min-width: 0;
  }
  .announcement {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip-path: inset(50%);
    white-space: nowrap;
    border: 0;
  }
  @media (max-width: 480px) {
    dl {
      grid-template-columns: minmax(0, 1fr);
      gap: 0.25rem;
    }
    dd {
      margin-bottom: 0.5rem;
    }
  }
</style>
