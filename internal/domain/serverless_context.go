package domain

import (
	"sort"
)

// ServerlessWorkerJobDigestsV1 binds the immutable execution inputs loaded
// from the canonical WorkerJob and ArtifactManifest. Queue delivery fields,
// frontend routing and creation timestamps are deliberately excluded.
func ServerlessWorkerJobDigestsV1(job WorkerJob, manifest ArtifactManifest) (contextDigest string, inputDigest string, err error) {
	if job.ExecutionPlacementV2.Kind != ExecutionPlacementManaged {
		return "", "", ValidationError{Field: "worker_job.execution_placement", Reason: "must select managed execution"}
	}
	if err := job.ExecutionPlacementV2.Validate(); err != nil {
		return "", "", err
	}
	if err := validateAttachedWorkerInputManifest(job, manifest); err != nil {
		return "", "", err
	}
	for _, validation := range []func() error{
		job.TenantID.Validate, job.RunID.Validate, job.SessionID.Validate,
		job.TriggerEventID.Validate, job.AttemptID.Validate,
		job.ReservationID.Validate, job.InputManifestID.Validate,
	} {
		if err := validation(); err != nil {
			return "", "", err
		}
	}
	if job.ContextWindow == nil {
		if err := validateWorkerBlob(job.TenantID, "worker_job.context_snapshot", job.ContextSnapshot); err != nil {
			return "", "", err
		}
	} else if err := job.ContextWindow.Validate(); err != nil {
		return "", "", err
	}
	for name, blob := range map[string]*BlobRef{
		"worker_job.workspace_snapshot": job.WorkspaceSnapshot,
		"worker_job.skill_bundle":       job.SkillBundle,
	} {
		if blob != nil {
			if err := validateWorkerBlob(job.TenantID, name, *blob); err != nil {
				return "", "", err
			}
		}
	}
	if err := job.Limits.ValidateForAdmission(); err != nil {
		return "", "", err
	}

	input := newServerlessDigest("sessionless.serverless-input-manifest.v1")
	for _, field := range []string{string(manifest.ID), string(manifest.TenantID), string(manifest.RunID)} {
		input.str(field)
	}
	artifacts := append([]Artifact(nil), manifest.Artifacts...)
	sort.Slice(artifacts, func(left, right int) bool { return artifacts[left].Name < artifacts[right].Name })
	input.uint(uint64(len(artifacts)))
	for _, artifact := range artifacts {
		input.str(artifact.Name)
		input.str(artifact.MediaType)
		appendServerlessBlobDigest(input, artifact.Blob)
	}
	inputDigest = input.sum()

	context := newServerlessDigest("sessionless.serverless-job-context.v1")
	for _, field := range []string{
		string(job.TenantID), string(job.RunID), string(job.SessionID), string(job.TriggerEventID),
		string(job.AttemptID), string(job.ReservationID), string(job.InputManifestID), inputDigest,
	} {
		context.str(field)
	}
	if job.ContextWindow == nil {
		context.str("snapshot_blob")
		appendServerlessBlobDigest(context, job.ContextSnapshot)
	} else {
		context.str("window")
		if job.ContextWindow.SnapshotVersion == nil {
			context.uint(0)
		} else {
			context.uint(1)
			context.uint(*job.ContextWindow.SnapshotVersion)
		}
		context.uint(job.ContextWindow.AfterSequence)
		context.uint(job.ContextWindow.ThroughSequence)
	}
	appendOptionalServerlessBlobDigest(context, job.WorkspaceSnapshot)
	appendOptionalServerlessBlobDigest(context, job.SkillBundle)
	servers := append([]string(nil), job.AllowedMCPServers...)
	sort.Strings(servers)
	context.uint(uint64(len(servers)))
	for _, server := range servers {
		context.str(server)
	}
	context.str(string(job.CredentialOwnerUserID))
	limits := job.Limits
	for _, value := range []uint64{
		uint64(limits.MaxTenantQueueDepth), uint64(limits.MaxActiveRuns), uint64(limits.MaxRuntime),
		uint64(limits.MaxTurns), limits.MaxInputBytes, limits.MaxContextBytes, limits.MaxContextEvents,
		uint64(limits.MaxArtifacts), uint64(limits.MaxToolEvents), limits.MaxToolEventBytes,
	} {
		context.uint(value)
	}
	return context.sum(), inputDigest, nil
}

func appendServerlessBlobDigest(digest *serverlessDigest, blob BlobRef) {
	digest.str(string(blob.TenantID))
	digest.str(blob.Key)
	digest.uint(uint64(blob.Size))
	digest.str(blob.SHA256)
}

func appendOptionalServerlessBlobDigest(digest *serverlessDigest, blob *BlobRef) {
	if blob == nil {
		digest.uint(0)
		return
	}
	digest.uint(1)
	appendServerlessBlobDigest(digest, *blob)
}
