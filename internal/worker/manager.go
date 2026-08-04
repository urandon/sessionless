// Package worker implements the harness-neutral, concurrency-one worker
// invocation lifecycle.
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
)

type Config struct {
	ScratchRoot          string
	WorkerID             string
	LeaseTTL             time.Duration
	RetryDelay           time.Duration
	MaxDeliveryCount     uint32
	MaxMaterializedBytes int64
}

type Outcome string

const (
	OutcomeCompleted    Outcome = "completed"
	OutcomeDuplicate    Outcome = "duplicate"
	OutcomeRetried      Outcome = "retried"
	OutcomeDeadLettered Outcome = "dead_lettered"
	OutcomeCancelled    Outcome = "cancelled"
	OutcomeFailed       Outcome = "failed"
)

type Manager struct {
	config  Config
	clock   ports.Clock
	queue   ports.Queue
	state   ports.WorkerStateStore
	blobs   ports.BlobStore
	harness ports.HarnessDriver
}

func New(
	config Config,
	clock ports.Clock,
	queue ports.Queue,
	state ports.WorkerStateStore,
	blobs ports.BlobStore,
	harness ports.HarnessDriver,
) (*Manager, error) {
	if config.LeaseTTL <= 0 {
		config.LeaseTTL = 2 * time.Minute
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = 5 * time.Second
	}
	if config.MaxDeliveryCount == 0 {
		config.MaxDeliveryCount = 5
	}
	if config.MaxMaterializedBytes <= 0 {
		config.MaxMaterializedBytes = 64 << 20
	}
	if err := domain.ValidateOpaqueID("worker.worker_id", config.WorkerID); err != nil {
		return nil, err
	}
	root, err := validateScratchRoot(config.ScratchRoot)
	if err != nil {
		return nil, err
	}
	config.ScratchRoot = root
	if clock == nil || queue == nil || state == nil || blobs == nil || harness == nil {
		return nil, fmt.Errorf("worker dependencies must not be nil")
	}
	return &Manager{
		config: config, clock: clock, queue: queue, state: state,
		blobs: blobs, harness: harness,
	}, nil
}

func (manager *Manager) RunOnce(ctx context.Context) (Outcome, error) {
	message, err := manager.queue.Receive(ctx)
	if err != nil {
		return "", err
	}
	if message.Envelope.Kind != queuecontract.KindDispatchRun {
		return manager.deadLetter(ctx, message, "unexpected_message_kind")
	}
	runID := domain.RunID(message.Envelope.SubjectID)
	if err := runID.Validate(); err != nil {
		return manager.deadLetter(ctx, message, "invalid_run_id")
	}
	loaded, found, err := manager.state.LoadWorkerJob(
		ctx, message.Envelope.TenantID, runID,
	)
	if err != nil {
		return manager.retry(ctx, message, err)
	}
	if !found {
		return manager.deadLetter(ctx, message, "worker_job_not_found")
	}
	if loaded.Run.Status.Terminal() {
		if err := manager.queue.Ack(ctx, message.ReceiptHandle); err != nil {
			return "", err
		}
		return OutcomeDuplicate, nil
	}
	now := manager.clock.Now().UTC()
	if loaded.Reservation.Status != domain.ReservationHeld ||
		!loaded.Reservation.ExpiresAt.After(now) {
		return manager.deadLetter(ctx, message, "reservation_not_held")
	}
	leaseID := domain.LeaseID(stableID("lea", string(loaded.Run.ID), string(loaded.Attempt.ID)))
	lease, err := manager.state.ClaimWorkerLease(ctx, ports.WorkerLeaseRequest{
		TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID,
		AttemptID: loaded.Attempt.ID, LeaseID: leaseID,
		WorkerID: manager.config.WorkerID, Now: now,
		ExpiresAt: now.Add(manager.config.LeaseTTL),
	})
	if err != nil {
		return manager.retry(ctx, message, err)
	}
	if err := manager.state.StartWorkerJob(ctx, loaded, lease, now); err != nil {
		return manager.retry(ctx, message, err)
	}
	cancelled, err := manager.state.CancellationRequested(
		ctx, loaded.Run.TenantID, loaded.Run.ID,
	)
	if err != nil {
		return manager.retry(ctx, message, err)
	}
	if cancelled {
		return manager.finishFailure(ctx, message, loaded, lease, true, "cancelled_before_start")
	}
	executionCtx, cancelExecution := context.WithTimeout(ctx, loaded.Job.Limits.MaxRuntime)
	defer cancelExecution()

	workDir, err := manager.createInvocationDir()
	if err != nil {
		return manager.retry(ctx, message, err)
	}
	defer manager.cleanupInvocationDir(workDir)

	if err := manager.materialize(executionCtx, loaded, workDir); err != nil {
		if errors.Is(executionCtx.Err(), context.DeadlineExceeded) {
			return manager.finishFailure(
				context.Background(), message, loaded, lease, false, "runtime_limit_exceeded",
			)
		}
		return manager.finishFailure(ctx, message, loaded, lease, false, "materialization_failed")
	}
	request := ports.ExecutionRequest{
		TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID,
		SessionID: loaded.Run.SessionID, TriggerEventID: loaded.Run.TriggerEventID,
		AttemptID: loaded.Attempt.ID, WorkDir: workDir,
		ContextSnapshot:   loaded.Job.ContextSnapshot,
		InputArtifacts:    loaded.InputManifest.Artifacts,
		ResumeCheckpoint:  loaded.Checkpoint,
		AllowedMCPServers: append([]string(nil), loaded.Job.AllowedMCPServers...),
	}
	if err := request.Validate(); err != nil {
		return manager.finishFailure(ctx, message, loaded, lease, false, "invalid_execution_request")
	}
	sink := &eventSink{
		manager: manager, loaded: loaded, lease: lease,
		lastSequence: checkpointSequence(loaded.Checkpoint),
	}
	result, err := manager.harness.Execute(executionCtx, request, sink)
	if err != nil {
		_ = manager.harness.Cancel(context.Background(), ports.ExecutionIdentity{
			TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID,
			AttemptID: loaded.Attempt.ID,
		})
		cancelled, cancelErr := manager.state.CancellationRequested(
			context.Background(), loaded.Run.TenantID, loaded.Run.ID,
		)
		if cancelErr == nil && cancelled {
			return manager.finishFailure(context.Background(), message, loaded, lease, true, "cancelled")
		}
		var classified *domain.ClassifiedError
		if errors.As(err, &classified) && classified.Retryable() {
			return manager.retry(ctx, message, err)
		}
		if errors.Is(executionCtx.Err(), context.DeadlineExceeded) {
			return manager.finishFailure(
				context.Background(), message, loaded, lease, false, "runtime_limit_exceeded",
			)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return manager.retry(context.Background(), message, err)
		}
		return manager.finishFailure(context.Background(), message, loaded, lease, false, "harness_failed")
	}
	lease = sink.lease
	lease, err = manager.ensureLease(executionCtx, loaded.Run.TenantID, lease)
	if err != nil {
		return manager.retry(ctx, message, err)
	}
	manifest, err := manager.uploadOutputs(executionCtx, loaded, workDir, result.Outputs)
	if err != nil {
		if errors.Is(executionCtx.Err(), context.DeadlineExceeded) {
			return manager.finishFailure(
				context.Background(), message, loaded, lease, false, "runtime_limit_exceeded",
			)
		}
		return manager.finishFailure(ctx, message, loaded, lease, false, "artifact_upload_failed")
	}
	finishedAt := manager.clock.Now().UTC()
	lease, err = manager.ensureLease(executionCtx, loaded.Run.TenantID, lease)
	if err != nil {
		return manager.retry(ctx, message, err)
	}
	delivery := domain.TelegramDeliveryOutbox{
		ID:       domain.TelegramDeliveryID(stableID("tdl", string(loaded.Run.ID))),
		TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID,
		Chat: loaded.Job.DeliveryChat, ReplyToMessageID: loaded.Job.ReplyToMessageID,
		Text: normalizedSummary(result.Summary), ArtifactManifestID: &manifest.ID,
		Status:         domain.DeliveryPending,
		IdempotencyKey: domain.IdempotencyKey(stableID("delivery", string(loaded.Run.ID))),
		CreatedAt:      finishedAt, UpdatedAt: finishedAt,
	}
	if err := manager.state.CompleteWorkerJob(ctx, ports.WorkerCompletion{
		TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID,
		AttemptID: loaded.Attempt.ID, ReservationID: loaded.Reservation.ID,
		LeaseID: lease.ID, Fence: lease.FenceToken, At: finishedAt,
		Manifest: manifest, Delivery: delivery,
	}); err != nil {
		return manager.retry(ctx, message, err)
	}
	if err := manager.queue.Ack(ctx, message.ReceiptHandle); err != nil {
		return "", err
	}
	return OutcomeCompleted, nil
}

type eventSink struct {
	manager      *Manager
	loaded       ports.WorkerJobState
	lease        domain.Lease
	lastSequence uint64
}

func (sink *eventSink) Emit(ctx context.Context, event ports.ExecutionEvent) error {
	if event.Sequence != sink.lastSequence+1 {
		return domain.ValidationError{
			Field: "execution_event.sequence", Reason: "must be contiguous and monotonic",
		}
	}
	if strings.TrimSpace(event.Boundary) == "" || len(event.CheckpointState) == 0 {
		return domain.ValidationError{
			Field: "execution_event", Reason: "requires a boundary and checkpoint state",
		}
	}
	if event.Sequence > uint64(sink.loaded.Job.Limits.MaxTurns) {
		return domain.ValidationError{
			Field: "execution_event.sequence", Reason: "exceeds the admitted turn limit",
		}
	}
	cancelled, err := sink.manager.state.CancellationRequested(
		ctx, sink.loaded.Run.TenantID, sink.loaded.Run.ID,
	)
	if err != nil {
		return err
	}
	if cancelled {
		return context.Canceled
	}
	at := sink.manager.clock.Now().UTC()
	sink.lease, err = sink.manager.ensureLease(ctx, sink.loaded.Run.TenantID, sink.lease)
	if err != nil {
		return err
	}
	ref, err := sink.manager.blobs.Put(
		ctx, sink.loaded.Run.TenantID,
		fmt.Sprintf("runs/%s/checkpoints/%020d-%s.json",
			sink.loaded.Run.ID, event.Sequence, digestHex(event.CheckpointState)),
		bytes.NewReader(event.CheckpointState),
	)
	if err != nil {
		return err
	}
	checkpoint := domain.Checkpoint{
		ID: domain.CheckpointID(stableID(
			"chk", string(sink.loaded.Attempt.ID), fmt.Sprint(event.Sequence),
		)),
		TenantID: sink.loaded.Run.TenantID, RunID: sink.loaded.Run.ID,
		AttemptID: sink.loaded.Attempt.ID, Sequence: event.Sequence,
		State: ref, CreatedAt: at,
	}
	var usage *domain.UsageObservation
	if event.InputTokens != nil || event.OutputTokens != nil {
		value := domain.UsageObservation{
			ID: domain.UsageObservationID(stableID(
				"use", string(sink.loaded.Attempt.ID), fmt.Sprint(event.Sequence),
			)),
			TenantID: sink.loaded.Run.TenantID, RunID: sink.loaded.Run.ID,
			AttemptID:                sink.loaded.Attempt.ID,
			SubscriptionConnectionID: sink.loaded.Run.SubscriptionConnectionID,
			Source:                   domain.UsageSourceHarness, InputTokens: event.InputTokens,
			OutputTokens: event.OutputTokens, ObservedAt: at,
		}
		usage = &value
	}
	if err := sink.manager.state.CommitWorkerEvent(ctx, ports.WorkerEventCommit{
		Checkpoint: checkpoint, Usage: usage, LeaseID: sink.lease.ID,
		Fence: sink.lease.FenceToken, At: at,
	}); err != nil {
		return err
	}
	sink.lastSequence = event.Sequence
	return nil
}

func (manager *Manager) materialize(
	ctx context.Context,
	loaded ports.WorkerJobState,
	workDir string,
) error {
	var inputBytes uint64
	for _, artifact := range loaded.InputManifest.Artifacts {
		if artifact.Blob.Size < 0 ||
			uint64(artifact.Blob.Size) > loaded.Job.Limits.MaxInputBytes ||
			inputBytes > loaded.Job.Limits.MaxInputBytes-uint64(artifact.Blob.Size) {
			return domain.ValidationError{
				Field: "input_manifest.artifacts", Reason: "exceeds the admitted input limit",
			}
		}
		inputBytes += uint64(artifact.Blob.Size)
	}
	if err := manager.writeBlob(
		ctx, loaded.Run.TenantID, loaded.Job.ContextSnapshot,
		filepath.Join(workDir, "context", "snapshot"),
	); err != nil {
		return err
	}
	for _, artifact := range loaded.InputManifest.Artifacts {
		if err := validateFilename(artifact.Name); err != nil {
			return err
		}
		if err := manager.writeBlob(
			ctx, loaded.Run.TenantID, artifact.Blob,
			filepath.Join(workDir, "inputs", artifact.Name),
		); err != nil {
			return err
		}
	}
	if loaded.Job.WorkspaceSnapshot != nil {
		if err := manager.writeBlob(
			ctx, loaded.Run.TenantID, *loaded.Job.WorkspaceSnapshot,
			filepath.Join(workDir, "workspace", "snapshot"),
		); err != nil {
			return err
		}
	}
	if loaded.Job.SkillBundle != nil {
		if err := manager.writeBlob(
			ctx, loaded.Run.TenantID, *loaded.Job.SkillBundle,
			filepath.Join(workDir, "skills", "bundle"),
		); err != nil {
			return err
		}
	}
	if loaded.Checkpoint != nil {
		if err := manager.writeBlob(
			ctx, loaded.Run.TenantID, loaded.Checkpoint.State,
			filepath.Join(workDir, "resume", "checkpoint"),
		); err != nil {
			return err
		}
	}
	return os.MkdirAll(filepath.Join(workDir, "outputs"), 0o700)
}

func (manager *Manager) writeBlob(
	ctx context.Context,
	tenantID domain.TenantID,
	ref domain.BlobRef,
	target string,
) error {
	reader, err := manager.blobs.Open(ctx, tenantID, ref)
	if err != nil {
		return err
	}
	defer reader.Close()
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(file, hash), io.LimitReader(reader, manager.config.MaxMaterializedBytes+1))
	closeErr := file.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if written > manager.config.MaxMaterializedBytes || written != ref.Size ||
		hex.EncodeToString(hash.Sum(nil)) != ref.SHA256 {
		return fmt.Errorf("materialized blob does not match its immutable reference")
	}
	return nil
}

func (manager *Manager) uploadOutputs(
	ctx context.Context,
	loaded ports.WorkerJobState,
	workDir string,
	outputs []ports.ExecutionOutput,
) (domain.ArtifactManifest, error) {
	manifest := domain.ArtifactManifest{
		ID:       domain.ArtifactManifestID(stableID("art", string(loaded.Run.ID))),
		TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID,
		CreatedAt: manager.clock.Now().UTC(),
	}
	if uint32(len(outputs)) > loaded.Job.Limits.MaxArtifacts {
		return manifest, domain.ValidationError{
			Field: "execution_result.outputs", Reason: "exceeds the admitted artifact limit",
		}
	}
	outputRoot := filepath.Join(workDir, "outputs")
	for _, output := range outputs {
		if err := validateFilename(output.Name); err != nil {
			return manifest, err
		}
		if strings.TrimSpace(output.MediaType) == "" {
			return manifest, domain.ValidationError{Field: "execution_output.media_type", Reason: "must not be empty"}
		}
		path, err := confinedPath(outputRoot, output.RelativePath)
		if err != nil {
			return manifest, err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return manifest, err
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
			info.Size() > manager.config.MaxMaterializedBytes {
			return manifest, fmt.Errorf("execution output must be a bounded regular file")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return manifest, err
		}
		digest := digestHex(data)
		ref, err := manager.blobs.Put(
			ctx, loaded.Run.TenantID,
			fmt.Sprintf("runs/%s/artifacts/sha256/%s", loaded.Run.ID, digest),
			bytes.NewReader(data),
		)
		if err != nil {
			return manifest, err
		}
		manifest.Artifacts = append(manifest.Artifacts, domain.Artifact{
			Name: output.Name, MediaType: output.MediaType, Blob: ref,
		})
	}
	return manifest, manifest.ValidateForRun(loaded.Run)
}

func (manager *Manager) finishFailure(
	ctx context.Context,
	message ports.ReceivedMessage,
	loaded ports.WorkerJobState,
	lease domain.Lease,
	cancelled bool,
	code string,
) (Outcome, error) {
	if lease.ID == "" {
		if err := manager.queue.Ack(ctx, message.ReceiptHandle); err != nil {
			return "", err
		}
		if cancelled {
			return OutcomeCancelled, nil
		}
		return OutcomeFailed, nil
	}
	var err error
	lease, err = manager.ensureLease(ctx, loaded.Run.TenantID, lease)
	if err != nil {
		return manager.retry(ctx, message, err)
	}
	failedAt := manager.clock.Now().UTC()
	if err := manager.state.FailWorkerJob(ctx, ports.WorkerFailure{
		TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID,
		AttemptID: loaded.Attempt.ID, ReservationID: loaded.Reservation.ID,
		LeaseID: lease.ID, Fence: lease.FenceToken,
		At: failedAt, Cancelled: cancelled, Code: code,
		Delivery: failureDelivery(loaded, cancelled, code, failedAt),
	}); err != nil {
		return manager.retry(ctx, message, err)
	}
	if err := manager.queue.Ack(ctx, message.ReceiptHandle); err != nil {
		return "", err
	}
	if cancelled {
		return OutcomeCancelled, nil
	}
	return OutcomeFailed, nil
}

func (manager *Manager) ensureLease(
	ctx context.Context,
	tenantID domain.TenantID,
	lease domain.Lease,
) (domain.Lease, error) {
	now := manager.clock.Now().UTC()
	if lease.ExpiresAt.After(now.Add(manager.config.LeaseTTL / 2)) {
		return lease, nil
	}
	return manager.state.RenewWorkerLease(
		ctx, tenantID, lease.ID, lease.FenceToken,
		now, now.Add(manager.config.LeaseTTL),
	)
}

func failureDelivery(
	loaded ports.WorkerJobState,
	cancelled bool,
	code string,
	at time.Time,
) domain.TelegramDeliveryOutbox {
	text := "Run failed."
	if cancelled {
		text = "Run cancelled."
	}
	if strings.TrimSpace(code) != "" {
		text += " Reference: " + code
	}
	return domain.TelegramDeliveryOutbox{
		ID:               domain.TelegramDeliveryID(stableID("tdl", string(loaded.Run.ID))),
		TenantID:         loaded.Run.TenantID,
		RunID:            loaded.Run.ID,
		Chat:             loaded.Job.DeliveryChat,
		ReplyToMessageID: loaded.Job.ReplyToMessageID,
		Text:             text,
		Status:           domain.DeliveryPending,
		IdempotencyKey:   domain.IdempotencyKey(stableID("delivery", string(loaded.Run.ID))),
		CreatedAt:        at,
		UpdatedAt:        at,
	}
}

func (manager *Manager) retry(
	ctx context.Context,
	message ports.ReceivedMessage,
	cause error,
) (Outcome, error) {
	if message.DeliveryCount >= manager.config.MaxDeliveryCount {
		if err := manager.queue.DeadLetter(ctx, message.ReceiptHandle, "retry_exhausted"); err != nil {
			return "", errors.Join(cause, err)
		}
		return OutcomeDeadLettered, nil
	}
	if err := manager.queue.Retry(ctx, message.ReceiptHandle, manager.config.RetryDelay); err != nil {
		return "", errors.Join(cause, err)
	}
	return OutcomeRetried, nil
}

func (manager *Manager) deadLetter(
	ctx context.Context,
	message ports.ReceivedMessage,
	code string,
) (Outcome, error) {
	if err := manager.queue.DeadLetter(ctx, message.ReceiptHandle, code); err != nil {
		return "", err
	}
	return OutcomeDeadLettered, nil
}

func validateScratchRoot(root string) (string, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return "", domain.ValidationError{
			Field: "worker.scratch_root", Reason: "must be a normalized absolute path",
		}
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	return resolved, nil
}

func (manager *Manager) createInvocationDir() (string, error) {
	dir, err := os.MkdirTemp(manager.config.ScratchRoot, "invocation-")
	if err != nil {
		return "", err
	}
	if filepath.Dir(dir) != manager.config.ScratchRoot {
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("invocation directory escaped scratch root")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		_ = os.RemoveAll(dir)
		return "", err
	}
	return dir, nil
}

func (manager *Manager) cleanupInvocationDir(dir string) {
	if filepath.Dir(dir) == manager.config.ScratchRoot &&
		strings.HasPrefix(filepath.Base(dir), "invocation-") {
		_ = os.RemoveAll(dir)
	}
}

func validateFilename(name string) error {
	if strings.TrimSpace(name) == "" || name == "." || name == ".." ||
		filepath.Base(name) != name || strings.ContainsAny(name, `/\`) {
		return domain.ValidationError{
			Field: "artifact.name", Reason: "must be a single safe path segment",
		}
	}
	return nil
}

func confinedPath(root, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) != relative {
		return "", domain.ValidationError{
			Field: "execution_output.relative_path", Reason: "must be normalized and relative",
		}
	}
	target := filepath.Join(root, relative)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", domain.ValidationError{
			Field: "execution_output.relative_path", Reason: "escapes the output root",
		}
	}
	return target, nil
}

func stableID(prefix string, values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		_, _ = io.WriteString(hash, value)
		_, _ = hash.Write([]byte{0})
	}
	return prefix + "_" + hex.EncodeToString(hash.Sum(nil)[:16])
}

func digestHex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func checkpointSequence(checkpoint *domain.Checkpoint) uint64 {
	if checkpoint == nil {
		return 0
	}
	return checkpoint.Sequence
}

func normalizedSummary(summary string) string {
	summary = strings.TrimSpace(summary)
	if summary == "" {
		return "Run completed."
	}
	return summary
}

var _ ports.ExecutionEventSink = (*eventSink)(nil)
