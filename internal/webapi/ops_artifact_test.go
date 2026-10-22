package webapi_test

import (
	"context"
	"errors"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func TestOpsArtifactPresignFailureAndCapabilitySuccess(t *testing.T) {
	sink := &opsSink{err: errors.New("sink dropped")}
	h := newHarnessWithBinder(t, sessionlessharness.NewDeterministicFixtureBinderV1(), sink)
	runID := domain.RunID("run-private")
	manifestID := domain.ArtifactManifestID("manifest-private")
	h.backend.runs[runID] = ports.RunRecord{Run: domain.Run{ID: runID, TenantID: "tenant-a", SessionID: "session-a"}}
	key := domain.SessionRunObjectPrefix("tenant-a", "session-a", runID) + "artifacts/result"
	h.objects.put(key, "application/pdf", []byte("secret-payload"), "etag-private")
	h.backend.manifests[manifestID] = domain.ArtifactManifest{ID: manifestID, TenantID: "tenant-a", RunID: runID, CreatedAt: testNow, Artifacts: []domain.Artifact{{Name: "private.pdf", MediaType: "application/pdf", Blob: h.objects.metadata[key].Blob}}}
	failure := errors.New("private.pdf secret-payload https://objects.invalid")
	h.objects.downloadErr = failure
	ctx := context.Background()
	_, err := h.service.DownloadRunArtifact(ctx, "tenant-a", "user-a", "session-a", runID, manifestID, 0)
	if !errors.Is(err, failure) {
		t.Fatalf("presign error=%v want=%v", err, failure)
	}
	assertOpsFailure(t, sink, ctx, opssignal.WebArtifactAccessFailure, opssignal.ArtifactCapability, opssignal.ObjectStore)
	h.objects.downloadErr = nil
	sink.records = nil
	response, err := h.service.DownloadRunArtifact(ctx, "tenant-a", "user-a", "session-a", runID, manifestID, 0)
	if err != nil || response.Download.Method != "GET" || len(sink.records) != 0 {
		t.Fatalf("capability=%+v err=%v manufactured bucket-GET observations=%+v", response, err, sink.records)
	}
}
