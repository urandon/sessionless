<script module lang="ts">
  import type { AttachedWorkerDiagnostics, CanonicalApiClient } from '$lib/api/client';

  export interface AttachedWorkerDiagnosticsApi {
    getAttachedWorkerDiagnostics: CanonicalApiClient['getAttachedWorkerDiagnostics'];
  }

  type ViewState = 'idle' | 'loading' | 'ready' | 'unavailable' | 'error';
</script>

<script lang="ts">
  import { tick } from 'svelte';

  import { ApiError } from '$lib/api/client';
  import {
    diagnosticCohortOrder,
    explainDiagnostic,
    serializeDiagnosticBundleV1,
  } from '$lib/attached-worker/diagnostics';
  import { labelState } from '$lib/attached-worker/presentation';
  import AttachedWorkerTime from './AttachedWorkerTime.svelte';
  import AttachedWorkerWarnings from './AttachedWorkerWarnings.svelte';

  let { client, workerId }: { client: AttachedWorkerDiagnosticsApi; workerId: string } = $props();

  let view = $state<ViewState>('idle');
  let diagnostics = $state<AttachedWorkerDiagnostics | undefined>();
  let bundle = $state('');
  let announcement = $state('');
  let announcementRole = $state<'status' | 'alert'>('status');
  let resultFocus = $state<HTMLElement | undefined>();

  async function load(): Promise<void> {
    view = 'loading';
    diagnostics = undefined;
    bundle = '';
    announce('', 'status');
    try {
      const result = await client.getAttachedWorkerDiagnostics(workerId);
      if (result.worker_id !== workerId) throw new Error('selector_mismatch');
      const serialized = serializeDiagnosticBundleV1(result);
      diagnostics = result;
      bundle = serialized;
      view = 'ready';
      announce('Redacted diagnostics loaded.', 'status');
      await focusResult();
    } catch (error) {
      if (
        error instanceof ApiError &&
        (error.code === 'not_found' || error.code === 'access_denied')
      ) {
        view = 'unavailable';
        announce('Diagnostics are unavailable.', 'alert');
        await focusResult();
        return;
      }
      view = 'error';
      announce('Diagnostics are temporarily unavailable.', 'alert');
      await focusResult();
    }
  }

  async function copy(): Promise<void> {
    if (view !== 'ready' || bundle === '') return;
    try {
      if (!navigator.clipboard?.writeText) throw new Error('clipboard_unavailable');
      await navigator.clipboard.writeText(bundle);
      announce('Redacted diagnostics copied.', 'status');
    } catch {
      announce('Copy failed. The redacted JSON remains available for manual selection.', 'alert');
    }
  }

  function download(): void {
    if (view !== 'ready' || bundle === '') return;
    try {
      const url = URL.createObjectURL(new Blob([bundle], { type: 'application/json' }));
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = 'sessionless-attached-worker-diagnostics-v1.json';
      anchor.click();
      URL.revokeObjectURL(url);
      announce('Redacted diagnostics downloaded.', 'status');
    } catch {
      announce(
        'Download failed. The redacted JSON remains available for manual selection.',
        'alert',
      );
    }
  }

  function announce(message: string, role: 'status' | 'alert'): void {
    announcement = message;
    announcementRole = role;
  }

  async function focusResult(): Promise<void> {
    await tick();
    resultFocus?.focus();
  }
</script>

<div class="diagnostics" aria-labelledby="diagnostics-title">
  <div>
    <h3 id="diagnostics-title">Support diagnostics</h3>
    <p>
      Load a bounded, owner-scoped evidence report. This performs no probe, control action, or
      automatic refresh.
    </p>
  </div>

  {#if view === 'idle'}
    <button class="button" type="button" onclick={() => void load()}>
      Load redacted diagnostics
    </button>
  {:else if view === 'loading'}
    <p aria-live="polite" aria-busy="true">Loading redacted diagnostics…</p>
  {:else if view === 'unavailable'}
    <div class="diagnostic-error" tabindex="-1" bind:this={resultFocus}>
      <p>Diagnostics are unavailable for this worker in the active workspace.</p>
      <button class="button" type="button" onclick={() => void load()}>Try again</button>
    </div>
  {:else if view === 'error'}
    <div class="diagnostic-error" tabindex="-1" bind:this={resultFocus}>
      <p>Diagnostics are temporarily unavailable.</p>
      <button class="button" type="button" onclick={() => void load()}>Try again</button>
    </div>
  {:else if diagnostics}
    <div
      class="diagnostic-ready"
      role="region"
      aria-label="Loaded redacted diagnostics"
      tabindex="-1"
      bind:this={resultFocus}
    >
      <p>
        Evidence evaluated <AttachedWorkerTime value={diagnostics.evaluated_at} />. Diagnostic facts
        do not create an overall health or admission decision.
      </p>

      <div class="diagnostic-groups">
        {#each diagnosticCohortOrder as cohort (cohort)}
          {@const facts = diagnostics.facts.filter((fact) => fact.cohort === cohort)}
          {#if facts.length > 0}
            <section class="diagnostic-group" aria-labelledby={`diagnostic-${cohort}`}>
              <h4 id={`diagnostic-${cohort}`}>{labelState(cohort)}</h4>
              <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
              <div
                class="table-region"
                role="region"
                aria-label={`${labelState(cohort)} diagnostic facts`}
                tabindex="0"
              >
                <table>
                  <caption class="sr-only">{labelState(cohort)} diagnostic facts</caption>
                  <thead>
                    <tr
                      ><th scope="col">Fact</th><th scope="col">State</th><th scope="col"
                        >Evidence</th
                      ></tr
                    >
                  </thead>
                  <tbody>
                    {#each facts as fact (fact.code)}
                      {@const explanation = explainDiagnostic(fact.code)}
                      <tr>
                        <th scope="row">
                          {explanation.title}
                          <code>{fact.code}</code>
                          <small>{explanation.description}</small>
                        </th>
                        <td>{labelState(fact.state)}</td>
                        <td>
                          <span>Observed: <AttachedWorkerTime value={fact.observed_at} /></span>
                          <span>Freshness: {labelState(fact.freshness)}</span>
                        </td>
                      </tr>
                    {/each}
                  </tbody>
                </table>
              </div>
            </section>
          {/if}
        {/each}
      </div>

      <div class="diagnostic-warnings">
        <h4>Diagnostic warnings</h4>
        <AttachedWorkerWarnings codes={diagnostics.warnings} />
      </div>

      <label for="redacted-diagnostic-bundle">Redacted diagnostic JSON</label>
      <textarea id="redacted-diagnostic-bundle" readonly rows="14" value={bundle}></textarea>
      <div class="diagnostic-actions">
        <button class="button" type="button" onclick={() => void copy()}>
          Copy redacted diagnostics
        </button>
        <button class="button" type="button" onclick={download}>Download redacted JSON</button>
      </div>
    </div>
  {/if}

  {#if announcement}
    <p class="announcement" role={announcementRole}>{announcement}</p>
  {/if}
</div>

<style>
  .diagnostics,
  .diagnostic-groups,
  .diagnostic-ready,
  .diagnostic-group,
  .diagnostic-warnings,
  .diagnostic-error {
    display: grid;
    gap: 0.75rem;
  }

  .diagnostics {
    border-top: 1px solid var(--line);
    padding-top: 1rem;
  }

  .diagnostics h3,
  .diagnostics h4,
  .diagnostics p {
    margin: 0;
  }

  .table-region {
    overflow-x: auto;
  }

  table {
    width: 100%;
    min-width: 38rem;
    border-collapse: collapse;
  }

  th,
  td {
    padding: 0.75rem;
    border-bottom: 1px solid var(--line);
    text-align: left;
    vertical-align: top;
  }

  th code,
  th small,
  td span {
    display: block;
    margin-top: 0.25rem;
  }

  textarea {
    width: 100%;
    resize: vertical;
    font:
      0.85rem/1.5 ui-monospace,
      SFMono-Regular,
      Consolas,
      monospace;
  }

  .diagnostic-actions {
    display: flex;
    flex-wrap: wrap;
    gap: 0.75rem;
  }

  .announcement {
    font-weight: 650;
  }
</style>
