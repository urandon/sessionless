<script module lang="ts">
  import type {
    Attachment,
    CanonicalApiClient,
    ComputeStatus,
    ConditionalResult,
    DownloadCapability,
    EventPage,
    Identity,
    Run,
    RunPage,
    SessionEvent,
    SessionSummary,
    UploadCommit,
    UploadIntent,
  } from '$lib/api/client';

  export interface SessionDetailApi {
    getIdentity: CanonicalApiClient['getIdentity'];
    getRunExplanation: CanonicalApiClient['getRunExplanation'];
    getSession: CanonicalApiClient['getSession'];
    listEvents: CanonicalApiClient['listEvents'];
    listRuns: CanonicalApiClient['listRuns'];
    getComputeStatus: CanonicalApiClient['getComputeStatus'];
    setSessionArchived: CanonicalApiClient['setSessionArchived'];
    createUpload: CanonicalApiClient['createUpload'];
    commitUpload: CanonicalApiClient['commitUpload'];
    createMessage: CanonicalApiClient['createMessage'];
    getAttachmentCapability: CanonicalApiClient['getAttachmentCapability'];
  }

  type ViewState =
    'loading' | 'ready' | 'unauthenticated' | 'not-found' | 'access-denied' | 'error';
  type TransferState = 'selected' | 'hashing' | 'uploading' | 'committing' | 'ready';
  interface SelectedFile {
    id: string;
    file: File;
    uploadKey: string;
    uploadId?: string;
    progress: number;
    state: TransferState;
  }
  interface Submission {
    idempotencyKey: string;
    text: string;
    files: SelectedFile[];
  }
</script>

<script lang="ts">
  import { onMount, tick } from 'svelte';
  import { resolve } from '$app/paths';

  import { ApiError } from '$lib/api/client';
  import RunExplanationDrawer from './RunExplanationDrawer.svelte';
  import {
    RunExplanationController,
    type RunExplanationState,
  } from '$lib/run-explanation/controller';
  import { boundedUTF8, downloadCapability, hashFile, putUpload } from '$lib/session/fileTransfer';

  let {
    client,
    sessionId,
    hashFileFn = hashFile,
    putUploadFn = putUpload,
    downloadCapabilityFn = downloadCapability,
    explanationNow = Date.now,
    explanationEnabled = import.meta.env.VITE_RUN_EXPLANATION_ENABLED === 'true',
  }: {
    client: SessionDetailApi;
    sessionId: string;
    hashFileFn?: typeof hashFile;
    putUploadFn?: typeof putUpload;
    downloadCapabilityFn?: typeof downloadCapability;
    explanationNow?: () => number;
    explanationEnabled?: boolean;
  } = $props();

  const maxEvents = 100;
  const maxRuns = 50;
  const maxPollFailures = 5;
  const defaultPollMs = 2500;

  let view = $state<ViewState>('loading');
  let session = $state<SessionSummary>();
  let events = $state<SessionEvent[]>([]);
  let runs = $state<Run[]>([]);
  let compute = $state<ComputeStatus>();
  let canWrite = $state(true);
  let computeUnavailable = $state(false);
  let errorMessage = $state('');
  let pollMessage = $state('');
  let message = $state('');
  let selectedFiles = $state<SelectedFile[]>([]);
  let submission = $state<Submission>();
  let submitting = $state(false);
  let archivePending = $state(false);
  let archiveKey = $state<string>();
  let archiveTarget = $state<boolean>();
  let downloading = $state<string>();
  let transferMessage = $state('');
  let abortController: AbortController | undefined;
  let pollTimer: ReturnType<typeof setTimeout> | undefined;
  let pollFailures = $state(0);
  let sessionETag: string | undefined;
  let eventETag: string | undefined;
  let eventETagSequence: number | undefined;
  let runETag: string | undefined;
  let lastSequence = 0;
  let composer = $state<HTMLTextAreaElement>();
  let conversationHeading = $state<HTMLHeadingElement>();
  let identity = $state<Identity>();
  let explanationState = $state<RunExplanationState>({ phase: 'idle', nextRefreshAt: 0 });
  let explanationRunId = $state<string>();
  let explanationController: RunExplanationController | undefined;
  let explanationTrigger: HTMLButtonElement | undefined;
  let identityAbort: AbortController | undefined;
  let identityGeneration = 0;
  let lastIdentityScope: { webIdentity: string; tenantId: string } | undefined;
  let accessGeneration = 0;
  let alive = false;
  const identityScope = $derived(scopeForIdentity(identity));

  const runByTrigger = $derived(new Map(runs.map((run) => [run.trigger_event_id, run])));
  const computeReady = $derived(computeAllowsSend(compute));
  const canSubmit = $derived(
    canWrite &&
      computeReady &&
      !submitting &&
      session?.status === 'active' &&
      (message.trim().length > 0 || selectedFiles.length > 0),
  );

  onMount(() => {
    alive = true;
    abortController = new AbortController();
    if (explanationEnabled)
      explanationController = new RunExplanationController({
        read: (runId, expectedSessionId, signal) =>
          client.getRunExplanation(runId, expectedSessionId, signal),
        now: explanationNow,
      });
    explanationController?.setVisible(false);
    const unsubscribe = explanationController?.subscribe((state) => {
      explanationState = state;
      if (state.phase === 'permission-lost') {
        // Never reconstitute permission from stale identity props on reopen.
        identity = undefined;
        identityGeneration += 1;
        identityAbort?.abort();
      }
    });
    const visibility = (): void => {
      clearPoll();
      if (document.hidden) {
        identityGeneration += 1;
        identityAbort?.abort();
        identity = undefined;
        closeExplanation(false);
      } else {
        // Ordinary auth lifecycle, not a drawer-open probe or explanation poll.
        if (explanationEnabled) void refreshIdentity();
        if (view === 'ready') schedulePoll(0);
      }
    };
    document.addEventListener('visibilitychange', visibility);
    if (explanationEnabled) void refreshIdentity();
    void loadInitial();
    return () => {
      alive = false;
      accessGeneration += 1;
      identityGeneration += 1;
      identityAbort?.abort();
      unsubscribe?.();
      explanationController?.dispose();
      document.removeEventListener('visibilitychange', visibility);
      clearPoll();
      abortController?.abort();
    };
  });

  async function loadInitial(): Promise<void> {
    const generation = accessGeneration;
    view = 'loading';
    errorMessage = '';
    canWrite = true;
    computeUnavailable = false;
    try {
      const signal = abortController?.signal;
      const [sessionResult, eventResult, runResult] = await Promise.all([
        client.getSession(sessionId, undefined, signal),
        client.listEvents(sessionId, { limit: maxEvents, signal }),
        client.listRuns(sessionId, { limit: maxRuns, signal }),
      ]);
      if (!alive || generation !== accessGeneration || signal?.aborted) return;
      applySession(sessionResult);
      applyEvents(eventResult, true);
      applyRuns(runResult);
      await loadComputeAccess(signal);
      if (!alive || generation !== accessGeneration || signal?.aborted) return;
      view = 'ready';
      schedulePoll(nextPollDelay(sessionResult, eventResult, runResult));
    } catch (error) {
      if (!alive || generation !== accessGeneration) return;
      if (isAbort(error)) return;
      if (isReadPermissionLoss(error)) discardReadAccess(error);
      else showLoadFailure(error);
    }
  }

  async function loadComputeAccess(signal?: AbortSignal): Promise<void> {
    try {
      compute = await client.getComputeStatus(sessionId, signal);
      canWrite = true;
      computeUnavailable = false;
    } catch (error) {
      if (
        error instanceof ApiError &&
        (error.code === 'access_denied' || error.code === 'not_found')
      ) {
        // The read routes already proved participant access. The compute probe
        // is write-authorized, so an opaque denial here means this participant
        // has a read-only session role.
        canWrite = false;
        compute = undefined;
        computeUnavailable = false;
        return;
      }
      if (isAbort(error)) throw error;
      compute = undefined;
      computeUnavailable = true;
    }
  }

  async function poll(): Promise<void> {
    if (document.hidden || abortController?.signal.aborted || view !== 'ready') return;
    const generation = accessGeneration;
    try {
      const signal = abortController?.signal;
      const afterSequence = lastSequence || undefined;
      const [sessionResult, eventResult, runResult] = await Promise.all([
        client.getSession(sessionId, sessionETag, signal),
        client.listEvents(sessionId, {
          afterSequence,
          limit: maxEvents,
          etag: eventETagSequence === afterSequence ? eventETag : undefined,
          signal,
        }),
        client.listRuns(sessionId, { limit: maxRuns, etag: runETag, signal }),
      ]);
      if (!alive || generation !== accessGeneration || signal?.aborted) return;
      applySession(sessionResult);
      applyEvents(eventResult, false, afterSequence);
      applyRuns(runResult);
      pollFailures = 0;
      pollMessage = '';
      schedulePoll(nextPollDelay(sessionResult, eventResult, runResult));
    } catch (error) {
      if (!alive || generation !== accessGeneration) return;
      if (isAbort(error)) return;
      if (isReadPermissionLoss(error)) {
        discardReadAccess(error);
        return;
      }
      pollFailures += 1;
      if (pollFailures >= maxPollFailures) {
        pollMessage = 'Live updates paused after repeated connection failures.';
        return;
      }
      pollMessage = 'Live update delayed. Retrying…';
      schedulePoll(Math.min(15000, 1000 * 2 ** (pollFailures - 1)));
    }
  }

  function scopeForIdentity(
    value?: Identity,
  ): { webIdentity: string; tenantId: string } | undefined {
    if (
      !value ||
      typeof value.user_id !== 'string' ||
      !value.user_id ||
      typeof value.provider !== 'string' ||
      !value.provider ||
      !Array.isArray(value.tenants)
    )
      return;
    const active = value.tenants.filter((tenant) => tenant.active === true);
    if (active.length !== 1 || typeof active[0]?.tenant_id !== 'string' || !active[0].tenant_id)
      return;
    return {
      webIdentity: JSON.stringify([value.user_id, value.provider]),
      tenantId: active[0].tenant_id,
    };
  }

  async function refreshIdentity(): Promise<void> {
    const generation = ++identityGeneration;
    identityAbort?.abort();
    const abort = new AbortController();
    identityAbort = abort;
    try {
      const next = await client.getIdentity(abort.signal);
      if (!alive || generation !== identityGeneration || abort.signal.aborted || document.hidden)
        return;
      const previous = lastIdentityScope,
        nextScope = scopeForIdentity(next);
      const changed =
        previous &&
        nextScope &&
        (previous.webIdentity !== nextScope.webIdentity ||
          previous.tenantId !== nextScope.tenantId);
      if (!nextScope || changed) {
        closeExplanation(false);
        explanationController?.setScope(undefined);
      }
      identity = nextScope ? next : undefined;
      if (nextScope) lastIdentityScope = nextScope;
      if (changed) {
        // An actual auth-scope change is not a recoverable transport error.
        // Discard private transcript/drafts and fence all earlier reads first.
        accessGeneration += 1;
        abortController?.abort();
        abortController = new AbortController();
        events = [];
        runs = [];
        session = undefined;
        compute = undefined;
        message = '';
        selectedFiles = [];
        submission = undefined;
        sessionETag = eventETag = runETag = undefined;
        eventETagSequence = undefined;
        lastSequence = 0;
        clearPoll();
        void loadInitial();
      }
    } catch (error) {
      if (!alive || generation !== identityGeneration || abort.signal.aborted) return;
      identity = undefined;
      closeExplanation(false);
      explanationController?.setScope(undefined);
      if (isReadPermissionLoss(error)) discardReadAccess(error);
    }
  }

  function isReadPermissionLoss(error: unknown): boolean {
    return error instanceof ApiError && [401, 403, 404].includes(error.status);
  }

  function discardReadAccess(error: unknown): void {
    accessGeneration += 1;
    identityGeneration += 1;
    identityAbort?.abort();
    identity = undefined;
    closeExplanation(false);
    explanationController?.setScope(undefined);
    clearPoll();
    abortController?.abort();
    events = [];
    runs = [];
    session = undefined;
    compute = undefined;
    message = '';
    selectedFiles = [];
    submission = undefined;
    showLoadFailure(error);
  }

  function explanationRun(event: SessionEvent): string | undefined {
    // Both selectors are already authorized canonical correlation. Never guess
    // from notice content, temporal adjacency or reconstructed opaque IDs.
    return event.run_id ?? runByTrigger.get(event.event_id)?.run_id;
  }

  function openExplanation(runId: string, trigger: HTMLButtonElement): void {
    if (!explanationEnabled || !identityScope || !explanationController || document.hidden) return;
    explanationTrigger = trigger;
    explanationRunId = runId;
    explanationController.setScope({ ...identityScope, sessionId, runId });
    explanationController.setVisible(true);
    void explanationController.refresh();
  }

  function closeExplanation(restoreFocus = true): void {
    explanationRunId = undefined;
    explanationController?.setVisible(false);
    if (restoreFocus) {
      if (explanationTrigger?.isConnected && !explanationTrigger.disabled)
        explanationTrigger.focus();
      else if (composer?.isConnected && !composer.disabled) composer.focus();
      else conversationHeading?.focus();
    }
    explanationTrigger = undefined;
  }

  function restartPolling(): void {
    pollFailures = 0;
    pollMessage = 'Refreshing…';
    schedulePoll(0);
  }

  function schedulePoll(delay: number): void {
    clearPoll();
    if (document.hidden || abortController?.signal.aborted) return;
    pollTimer = setTimeout(() => void poll(), clampPollDelay(delay));
  }

  function clearPoll(): void {
    if (pollTimer !== undefined) clearTimeout(pollTimer);
    pollTimer = undefined;
  }

  function applySession(result: ConditionalResult<SessionSummary>): void {
    if (result.state === 'fresh') {
      session = result.data;
      sessionETag = result.etag;
    }
  }

  function applyEvents(
    result: ConditionalResult<EventPage>,
    replace: boolean,
    querySequence?: number,
  ): void {
    if (result.state !== 'fresh') return;
    eventETag = result.etag;
    eventETagSequence = replace ? undefined : querySequence;
    const incoming = result.data.items;
    const merged = replace
      ? incoming
      : [...new Map([...events, ...incoming].map((event) => [event.event_id, event])).values()];
    events = merged.sort((left, right) => left.sequence - right.sequence).slice(-maxEvents);
    lastSequence = events.at(-1)?.sequence ?? lastSequence;
  }

  function applyRuns(result: ConditionalResult<RunPage>): void {
    if (result.state !== 'fresh') return;
    runETag = result.etag;
    runs = result.data.items.slice(0, maxRuns);
  }

  function nextPollDelay(...results: ConditionalResult<unknown>[]): number {
    const hints = results
      .map((result) => result.pollAfterMs)
      .filter((value): value is number => value !== undefined);
    return hints.length > 0 ? Math.min(...hints) : defaultPollMs;
  }

  async function toggleArchived(): Promise<void> {
    if (!canWrite || !session || archivePending) return;
    archivePending = true;
    errorMessage = '';
    const desired = session.status === 'active';
    if (archiveTarget !== desired) {
      archiveKey = newIdempotencyKey();
      archiveTarget = desired;
    }
    try {
      session = await client.setSessionArchived(sessionId, {
        archived: desired,
        idempotency_key: archiveKey ?? newIdempotencyKey(),
      });
      archiveKey = undefined;
      archiveTarget = undefined;
      await tick();
      composer?.focus();
    } catch (error) {
      errorMessage = publicMessage(error, 'Unable to update this session.');
    } finally {
      archivePending = false;
    }
  }

  function chooseFiles(event: Event): void {
    const input = event.currentTarget as HTMLInputElement;
    const chosen = Array.from(input.files ?? []);
    const incoming = chosen.filter((file) => file.size > 0);
    if (incoming.length !== chosen.length) {
      errorMessage = 'Empty files cannot be attached.';
      input.value = '';
      return;
    }
    if (selectedFiles.length + incoming.length > 8) {
      errorMessage = 'Attach no more than 8 files to one message.';
      input.value = '';
      return;
    }
    errorMessage = '';
    abandonSubmission();
    selectedFiles = [
      ...selectedFiles,
      ...incoming.map((file) => ({
        id: crypto.randomUUID(),
        file,
        uploadKey: newIdempotencyKey(),
        progress: 0,
        state: 'selected' as const,
      })),
    ];
    input.value = '';
  }

  function removeFile(id: string): void {
    if (submitting) return;
    abandonSubmission();
    selectedFiles = selectedFiles.filter((item) => item.id !== id);
  }

  function editMessage(event: Event): void {
    message = (event.currentTarget as HTMLTextAreaElement).value;
    if (!submitting) abandonSubmission();
  }

  function composerKeydown(event: KeyboardEvent): void {
    if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      if (canSubmit || submission) void send();
    }
  }

  async function send(): Promise<void> {
    if (!canWrite || submitting || (!canSubmit && !submission)) return;
    submission ??= {
      idempotencyKey: newIdempotencyKey(),
      text: message.trim(),
      files: selectedFiles,
    };
    const activeSubmission = submission;
    submitting = true;
    errorMessage = '';
    transferMessage = 'Checking compute and quota…';
    try {
      compute = await client.getComputeStatus(sessionId, abortController?.signal);
      if (!computeAllowsSend(compute)) {
        errorMessage = computeLabel(compute);
        transferMessage = '';
        return;
      }
      transferMessage = 'Preparing your message…';
      const uploadIds: string[] = [];
      for (const item of activeSubmission.files) uploadIds.push(await uploadOne(item));
      transferMessage = 'Sending message…';
      const created = await client.createMessage(sessionId, {
        idempotency_key: activeSubmission.idempotencyKey,
        text: activeSubmission.text || undefined,
        upload_ids: uploadIds.length > 0 ? uploadIds : undefined,
      });
      message = '';
      selectedFiles = [];
      submission = undefined;
      transferMessage = 'Message sent. Waiting for the agent…';
      const now = new Date().toISOString();
      const optimisticRun: Run = {
        run_id: created.run_id,
        session_id: created.session_id,
        trigger_event_id: created.event_id,
        subscription_connection_id: '',
        provider: created.compute.provider,
        status: 'created',
        created_at: now,
        updated_at: now,
      };
      runs = [optimisticRun, ...runs.filter((run) => run.run_id !== created.run_id)].slice(
        0,
        maxRuns,
      );
      clearPoll();
      schedulePoll(0);
      await tick();
      composer?.focus();
    } catch (error) {
      if (!isAbort(error)) {
        errorMessage = publicMessage(
          error,
          'The message was not sent. Retry keeps the same request.',
        );
        transferMessage = '';
      }
    } finally {
      submitting = false;
    }
  }

  async function uploadOne(item: SelectedFile): Promise<string> {
    if (item.uploadId) return item.uploadId;
    item.state = 'hashing';
    item.progress = 0;
    transferMessage = `Hashing ${item.file.name}…`;
    const digests = await hashFileFn(item.file, (fraction) => {
      item.progress = fraction * 0.2;
    });
    const intent: UploadIntent = await client.createUpload({
      session_id: sessionId,
      idempotency_key: item.uploadKey,
      name: boundedUTF8(item.file.name, 255, 'attachment'),
      media_type: boundedUTF8(item.file.type, 127, 'application/octet-stream'),
      size: item.file.size,
      sha256: digests.sha256,
      content_md5: digests.contentMD5,
    });
    item.state = 'uploading';
    transferMessage = `Uploading ${item.file.name}…`;
    const uploadID = intent.upload_id;
    const transfer = putUploadFn(
      intent,
      item.file,
      (fraction) => {
        item.progress = 0.2 + fraction * 0.75;
      },
      undefined,
      abortController?.signal,
    );
    intent.url = '';
    intent.headers = {};
    await transfer;
    item.state = 'committing';
    item.progress = 0.97;
    transferMessage = `Verifying ${item.file.name}…`;
    const committed: UploadCommit = await client.commitUpload(uploadID);
    item.uploadId = committed.upload_id;
    item.state = 'ready';
    item.progress = 1;
    return committed.upload_id;
  }

  function abandonSubmission(): void {
    if (submission) {
      selectedFiles = selectedFiles.map((item) => ({
        ...item,
        uploadKey: newIdempotencyKey(),
        uploadId: undefined,
        progress: 0,
        state: 'selected',
      }));
    }
    submission = undefined;
    transferMessage = '';
  }

  async function downloadAttachment(
    event: SessionEvent,
    attachment: Attachment,
    index: number,
  ): Promise<void> {
    const key = `${event.event_id}:${index}`;
    if (downloading) return;
    downloading = key;
    errorMessage = '';
    try {
      const capability: DownloadCapability = await client.getAttachmentCapability(
        sessionId,
        event.sequence,
        index,
      );
      const download = downloadCapabilityFn(
        capability,
        attachment.size,
        attachment.name,
        undefined,
        abortController?.signal,
      );
      capability.url = '';
      capability.headers = undefined;
      await download;
    } catch (error) {
      errorMessage = publicMessage(error, 'The attachment could not be downloaded.');
    } finally {
      downloading = undefined;
    }
  }

  function showLoadFailure(error: unknown): void {
    if (error instanceof ApiError && error.code === 'unauthenticated') view = 'unauthenticated';
    else if (error instanceof ApiError && error.code === 'not_found') view = 'not-found';
    else if (error instanceof ApiError && error.code === 'access_denied') view = 'access-denied';
    else {
      view = 'error';
      errorMessage = publicMessage(error, 'This session is temporarily unavailable.');
    }
  }

  function computeLabel(status: ComputeStatus | undefined): string {
    if (!canWrite) return 'Read-only access · sending and session changes are unavailable.';
    if (computeUnavailable) return 'Compute status is temporarily unavailable.';
    if (!status) return 'Checking compute and quota…';
    if (status.availability === 'not_configured') return 'No compute connection is configured.';
    if (status.availability === 'ambiguous') return 'Choose one compute connection before sending.';
    const connection = status.connection;
    if (!connection) return 'Compute status is unavailable.';
    if (connection.entitlement !== 'active') return 'Compute subscription needs attention.';
    if (connection.quota === 'exhausted') return 'Compute quota is exhausted.';
    return `${connection.provider} · quota ${connection.quota}`;
  }

  async function refreshCompute(): Promise<void> {
    if (!canWrite || submitting) return;
    errorMessage = '';
    await loadComputeAccess(abortController?.signal);
    if (computeUnavailable) errorMessage = 'Unable to refresh compute status.';
  }

  function computeAllowsSend(status: ComputeStatus | undefined): boolean {
    return (
      status?.availability === 'ready' &&
      status.connection?.entitlement === 'active' &&
      status.connection.quota !== 'exhausted'
    );
  }

  function publicMessage(error: unknown, fallback: string): string {
    return error instanceof ApiError ? error.message : fallback;
  }

  function kindLabel(kind: SessionEvent['kind']): string {
    return {
      user_message: 'You',
      assistant_message: 'Agent',
      tool_call: 'Tool call',
      tool_result: 'Tool result',
      system_notice: 'System',
    }[kind];
  }

  function runLabel(run: Run | undefined): string | undefined {
    if (!run) return undefined;
    return run.status.replace('_', ' ');
  }

  function formatBytes(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }

  function clampPollDelay(value: number): number {
    if (!Number.isFinite(value)) return defaultPollMs;
    return Math.max(500, Math.min(15000, value));
  }

  function newIdempotencyKey(): string {
    return `web-${crypto.randomUUID()}`;
  }

  function isAbort(error: unknown): boolean {
    return error instanceof DOMException && error.name === 'AbortError';
  }
</script>

{#if view === 'loading'}
  <section class="panel loading-state" aria-live="polite" aria-busy="true">
    <p>Loading the authorized session history…</p>
  </section>
{:else if view === 'not-found' || view === 'access-denied'}
  <section class="narrow panel" aria-labelledby="missing-title">
    <p class="eyebrow">Session unavailable</p>
    <h1 id="missing-title">This conversation cannot be opened</h1>
    <p>The session may not exist, or your active workspace is not a participant.</p>
    <a class="button primary" href={resolve('/')}>Return to sessions</a>
  </section>
{:else if view === 'unauthenticated'}
  <section class="narrow panel" aria-labelledby="sign-in-title">
    <p class="eyebrow">Sign in required</p>
    <h1 id="sign-in-title">Continue to this conversation</h1>
    <p>Your tenant membership will be checked after authentication.</p>
    <!-- eslint-disable svelte/no-navigation-without-resolve -- route is resolved before the encoded query is appended -->
    <a
      class="button primary"
      href={`${resolve('/login')}?return_to=${encodeURIComponent(`/sessions/${sessionId}`)}`}
      >Sign in</a
    >
    <!-- eslint-enable svelte/no-navigation-without-resolve -->
  </section>
{:else if view === 'error'}
  <section class="narrow panel" aria-labelledby="load-error-title">
    <p class="eyebrow">Unable to load</p>
    <h1 id="load-error-title">This session is temporarily unavailable</h1>
    <p role="alert">{errorMessage}</p>
    <button class="button primary" type="button" onclick={() => void loadInitial()}
      >Try again</button
    >
  </section>
{:else}
  <section class="conversation" aria-labelledby="conversation-title">
    <header class="conversation-heading">
      <div>
        <a class="back-link" href={resolve('/')}>← All sessions</a>
        <p class="eyebrow">Canonical session</p>
        <h1 id="conversation-title" bind:this={conversationHeading} tabindex="-1">
          {session?.title || 'Conversation'}
        </h1>
        <p class="resource-id" aria-label="Session identifier">{sessionId}</p>
      </div>
      {#if canWrite}
        <button
          class="button"
          type="button"
          disabled={archivePending}
          onclick={() => void toggleArchived()}
        >
          {archivePending ? 'Saving…' : session?.status === 'active' ? 'Archive' : 'Unarchive'}
        </button>
      {/if}
    </header>

    {#if errorMessage}
      <div class="error-banner" role="alert">{errorMessage}</div>
    {/if}

    <div class:compute-ready={computeReady} class="compute-status" role="status">
      <div>
        <strong>Compute</strong>
        <span>{computeLabel(compute)}</span>
      </div>
      {#if canWrite && !computeReady}
        <button
          class="button quiet"
          type="button"
          disabled={submitting}
          onclick={() => void refreshCompute()}>Refresh</button
        >
      {/if}
    </div>

    {#if pollMessage}
      <div class="poll-status" role="status">
        <span>{pollMessage}</span>
        {#if pollFailures >= maxPollFailures}
          <button class="button quiet" type="button" onclick={restartPolling}>Resume updates</button
          >
        {/if}
      </div>
    {/if}

    <section class="panel transcript" aria-label="Conversation history" aria-live="polite">
      {#if events.length === 0}
        <div class="empty-state">
          <h2>No messages yet</h2>
          <p>
            {canWrite ? 'Start the canonical conversation below.' : 'This session is read-only.'}
          </p>
        </div>
      {:else}
        <ol class="event-list">
          {#each events as event (event.event_id)}
            <li class:event-user={event.kind === 'user_message'} class="event-card">
              <div class="event-meta">
                <strong>{kindLabel(event.kind)}</strong>
                <time datetime={event.created_at}
                  >{new Date(event.created_at).toLocaleString()}</time
                >
              </div>
              {#if event.content.text}
                <p class="event-text">{event.content.text}</p>
              {:else if event.kind === 'tool_call' || event.kind === 'tool_result'}
                <p class="event-muted">
                  Structured {kindLabel(event.kind).toLowerCase()} recorded.
                </p>
              {/if}
              {#if event.content.attachments?.length}
                <ul class="attachment-list" aria-label="Attachments">
                  {#each event.content.attachments as attachment, index (`${event.event_id}:${index}`)}
                    <li>
                      <button
                        type="button"
                        class="attachment-button"
                        disabled={downloading !== undefined}
                        onclick={() => void downloadAttachment(event, attachment, index)}
                      >
                        <span>{attachment.name}</span>
                        <small>
                          {downloading === `${event.event_id}:${index}`
                            ? 'Downloading…'
                            : `${attachment.media_type} · ${formatBytes(attachment.size)}`}
                        </small>
                      </button>
                    </li>
                  {/each}
                </ul>
              {/if}
              {#if event.content.artifact_manifest}
                <p class="event-muted">Agent artifacts are available for this result.</p>
              {/if}
              {#if runLabel(runByTrigger.get(event.event_id))}
                <span class={`run-status run-${runByTrigger.get(event.event_id)?.status}`}>
                  Run {runLabel(runByTrigger.get(event.event_id))}
                </span>
              {/if}
              {#if explanationEnabled && explanationRun(event)}
                <button
                  class="button quiet"
                  type="button"
                  disabled={!identityScope}
                  aria-expanded={explanationRunId === explanationRun(event)}
                  aria-controls={explanationRunId ? 'run-explanation-panel' : undefined}
                  onclick={(click) => openExplanation(explanationRun(event)!, click.currentTarget)}
                  >Explain run</button
                >
              {/if}
            </li>
          {/each}
        </ol>
      {/if}
    </section>

    {#if explanationRunId}
      <RunExplanationDrawer
        state={explanationState}
        onClose={() => closeExplanation()}
        onRefresh={() => void explanationController?.refresh()}
        refreshDisabled={!identityScope ||
          explanationState.phase === 'loading' ||
          explanationState.phase === 'permission-lost'}
      />
    {/if}

    <form
      class="composer"
      aria-label="Message composer"
      onsubmit={(event) => {
        event.preventDefault();
        void send();
      }}
    >
      <label for="message">Message</label>
      <textarea
        bind:this={composer}
        id="message"
        name="message"
        rows="4"
        maxlength="32000"
        value={message}
        disabled={!canWrite || submitting || session?.status !== 'active'}
        placeholder={!canWrite
          ? 'This session is read-only'
          : session?.status === 'active'
            ? 'Write a message…'
            : 'Unarchive to write'}
        oninput={editMessage}
        onkeydown={composerKeydown}></textarea>

      {#if selectedFiles.length > 0}
        <ul class="selected-files" aria-label="Files to attach">
          {#each selectedFiles as item (item.id)}
            <li>
              <div>
                <strong>{item.file.name}</strong>
                <small>{formatBytes(item.file.size)} · {item.state}</small>
                {#if item.state !== 'selected'}
                  <progress
                    max="1"
                    value={item.progress}
                    aria-label={`Upload progress for ${item.file.name}`}
                  ></progress>
                {/if}
              </div>
              <button
                class="button quiet"
                type="button"
                disabled={submitting}
                aria-label={`Remove ${item.file.name}`}
                onclick={() => removeFile(item.id)}>Remove</button
              >
            </li>
          {/each}
        </ul>
      {/if}

      <p class="composer-hint">Ctrl/⌘ + Enter to send · up to 8 files</p>
      {#if transferMessage}<p class="transfer-status" role="status">{transferMessage}</p>{/if}
      <div class="composer-actions">
        <label
          class:disabled={!canWrite || submitting || selectedFiles.length >= 8}
          class="button file-picker"
        >
          Attach files
          <input
            type="file"
            multiple
            disabled={!canWrite ||
              submitting ||
              selectedFiles.length >= 8 ||
              session?.status !== 'active'}
            onchange={chooseFiles}
          />
        </label>
        <button
          class="button primary"
          type="submit"
          disabled={!canWrite || submitting || (!canSubmit && !submission)}
        >
          {submitting ? 'Sending…' : submission ? 'Retry send' : 'Send'}
        </button>
      </div>
    </form>
  </section>
{/if}
