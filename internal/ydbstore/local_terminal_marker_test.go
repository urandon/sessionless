package ydbstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// A finite in-memory accounting context for codec-only fixture construction.
// It owns no SQL transaction, Store, execution capability or approval hook.
func markerLegacyDraftsData(t *testing.T, run domain.Run, records []sessioncontext.EventPayload) ([]domain.SessionEventDraft, domain.ArtifactManifestID, error) {
	t.Helper()
	b := &ownedAuthorityBudget{}
	callback, err := b.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer b.finish(callback)
	return localTerminalLegacyDrafts(&ownedAuthorityQuery{budget: b, callback: callback}, run, records)
}

func TestLocalFinalizationMarkerV2ClosedEncoding(t *testing.T) {
	digest := strings.Repeat("a", 64)
	marker, err := encodeLocalFinalizationMarkerV2(digest)
	if err != nil || len(marker) != 73 || marker != "local_v2:"+digest {
		t.Fatalf("closed marker len=%d err=%v", len(marker), err)
	}
	decoded, err := decodeLocalFinalizationMarkerV2(marker)
	if err != nil || decoded != digest {
		t.Fatal("exact marker roundtrip", err)
	}
	for _, bad := range []string{"", digest, "local_v1:" + digest, "local_v3:" + digest, "local_v2:" + strings.Repeat("A", 64), marker + "a", marker[:72], " " + marker, marker + "\n"} {
		if _, err := decodeLocalFinalizationMarkerV2(bad); err == nil {
			t.Errorf("invalid marker len=%d accepted", len(bad))
		}
	}
	for _, bad := range []string{"", strings.Repeat("A", 64), strings.Repeat("a", 65)} {
		if _, err := encodeLocalFinalizationMarkerV2(bad); err == nil {
			t.Errorf("invalid digest len=%d accepted", len(bad))
		}
	}
	if !strings.Contains(localTerminalMarkerSQL, "content_digest AS String),0,74)") || !strings.Contains(localTerminalHistorySQL, "content_digest AS String),0,74)") || !strings.Contains(localTerminalHistoryRunsSQL(1), "content_digest AS String),0,74)") {
		t.Fatal("a local marker query lost the 74-byte overflow sentinel")
	}
}

// Uses the ORIGINAL completion encoder and original V1 finalization digest.
// Event/reference material is seeded DATA, not evidence of worker execution or
// a first canonical commit. Connected tests separately exercise the actual
// Source/Store consumer and physical local terminal producer.
func markerLegacyComposerData(t *testing.T, run domain.Run, manifestID domain.ArtifactManifestID) ([]sessioncontext.EventPayload, []domain.SessionEventDraft, domain.ArtifactManifest, string) {
	t.Helper()
	limits := currentWorkerAuthorityFixture(t).Job.Limits
	plan, err := canonicaloutput.EncodeCompletionV1(run.ID, limits, ports.ExecutionResult{Summary: " \noriginal exact answer\t ", ToolEvents: []ports.ExecutionToolEvent{
		{Kind: domain.SessionEventToolCall, CallID: "call-a", ToolName: "history_tool", Payload: json.RawMessage(`{"input":"original"}`)},
		{Kind: domain.SessionEventToolResult, CallID: "call-a", ToolName: "history_tool", Payload: json.RawMessage(`{"output":"original"}`)},
	}}, manifestID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(plan.Release)
	var records []sessioncontext.EventPayload
	var drafts []domain.SessionEventDraft
	for i, encoded := range plan.Events() {
		h := sha256.Sum256(encoded.Payload)
		ref := domain.BlobRef{TenantID: run.TenantID, Key: domain.SessionEventObjectPrefix(run.TenantID, run.SessionID, encoded.ID) + "payload.json", SHA256: hex.EncodeToString(h[:]), Size: int64(len(encoded.Payload))}
		runID := run.ID
		event := domain.SessionEvent{ID: encoded.ID, TenantID: run.TenantID, SessionID: run.SessionID, Sequence: uint64(i + 1), Kind: encoded.Kind, RunID: &runID, IdempotencyKey: domain.IdempotencyKey("original-" + string(encoded.ID)), Payload: ref, CreatedAt: run.UpdatedAt}
		if err := event.Validate(); err != nil {
			t.Fatal(err)
		}
		records = append(records, sessioncontext.EventPayload{Event: event, Payload: encoded.Payload})
		drafts = append(drafts, domain.SessionEventDraft{ID: event.ID, Kind: event.Kind, IdempotencyKey: event.IdempotencyKey, Payload: ref, DisplayText: encoded.DisplayText, CreatedAt: event.CreatedAt})
	}
	manifest := domain.ArtifactManifest{ID: manifestID, TenantID: run.TenantID, RunID: run.ID, CreatedAt: run.UpdatedAt}
	if err := manifest.ValidateForRun(run); err != nil {
		t.Fatal(err)
	}
	seal, err := runFinalizationDigest(domain.RunSucceeded, &manifest, drafts, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return records, drafts, manifest, seal
}

func TestLocalTerminalLegacyOriginalComposerSeal(t *testing.T) {
	for _, scenario := range []string{"managed_manifest_recipe", "attached_distinct_manifest_identity", "incomplete_events", "changed_manifest", "changed_canonical_payload", "unmatched_raw_digest"} {
		t.Run(scenario, func(t *testing.T) {
			store, sqlFixture, state := canonicalFixture(t)
			run := state.Run.Clone()
			if err := run.Transition(domain.RunSucceeded, run.CreatedAt.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			sqlFixture.at = run.UpdatedAt
			id := canonicaloutput.ManifestIDV1(run.ID)
			if scenario == "attached_distinct_manifest_identity" {
				id = "attached-output-manifest-not-the-managed-recipe"
			}
			records, originalDrafts, manifest, seal := markerLegacyComposerData(t, run, id)
			drafts, actualID, err := markerLegacyDraftsData(t, run, records)
			if err != nil || actualID != id || !reflect.DeepEqual(drafts, originalDrafts) {
				t.Fatal("exact original composer identities were not reconstructed", err)
			}
			manifests := map[domain.ArtifactManifestID]domain.ArtifactManifest{id: manifest}
			want := scenario == "managed_manifest_recipe" || scenario == "attached_distinct_manifest_identity"
			switch scenario {
			case "incomplete_events":
				records = records[1:]
			case "changed_manifest":
				manifest.CreatedAt = manifest.CreatedAt.Add(-time.Microsecond)
				manifests[id] = manifest
			case "changed_canonical_payload":
				records[len(records)-1].Payload = []byte(`{"schema":"sessionless.assistant-message.v1","summary":"changed","artifact_manifest_id":"wrong"}`)
			case "unmatched_raw_digest":
				// A valid raw digest is not enough. The connected A0 negative
				// separately strips a genuine V2 tag and omits genuine custody.
				seal = strings.Repeat("e", 64)
			}
			err = store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
				drafts, manifestID, err := localTerminalLegacyDrafts(q, run, records)
				if err != nil {
					return err
				}
				_, err = readLocalTerminalLegacySnapshot(ctx, q, run, seal, localTerminalLegacyMaterial{Drafts: drafts, ManifestID: manifestID}, manifests)
				return err
			})
			if (err == nil) != want {
				t.Fatalf("positive legacy V1 equality scenario=%s err=%v want=%t", scenario, err, want)
			}
		})
	}
}

func TestLocalTerminalLegacyPreparationRequiresOriginalBudget(t *testing.T) {
	run := currentWorkerAuthorityFixture(t).Run
	if err := run.Transition(domain.RunSucceeded, run.CreatedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	records, original, _, _ := markerLegacyComposerData(t, run, "legacy-budget-manifest")
	for _, scenario := range []string{"positive", "exhausted_before_first_copy", "missing_budget"} {
		t.Run(scenario, func(t *testing.T) {
			b := &ownedAuthorityBudget{}
			callback, err := b.begin()
			if err != nil {
				t.Fatal(err)
			}
			defer b.finish(callback)
			q := &ownedAuthorityQuery{budget: b, callback: callback}
			if scenario == "exhausted_before_first_copy" {
				b.bytes = ownedAuthorityMaxBytes
			}
			if scenario == "missing_budget" {
				q = nil
			}
			drafts, _, err := localTerminalLegacyDrafts(q, run, records)
			if scenario == "positive" {
				if err != nil || !reflect.DeepEqual(drafts, original) || b.failed || b.bytes == 0 || b.bytes > ownedAuthorityMaxBytes {
					t.Fatalf("bounded positive parser err=%v charged=%d failed=%t", err, b.bytes, b.failed)
				}
			} else if err == nil || len(drafts) != 0 || (scenario == "exhausted_before_first_copy" && (!b.failed || b.bytes != ownedAuthorityMaxBytes)) {
				t.Fatalf("unbudgeted parsing returned material: scenario=%s err=%v drafts=%d charged=%d failed=%t", scenario, err, len(drafts), b.bytes, b.failed)
			}
			if b.statements != 0 || b.writes != 0 {
				t.Fatal("codec budget control performed SQL/effects")
			}
		})
	}
}

func TestLocalCompactionSourceDigestSealsCompleteLegacyRun(t *testing.T) {
	f := newCompactionSourceFixture(t, true)
	source, err := f.entry.CaptureCompactionSource(t.Context())
	if err != nil {
		t.Fatal("actual original source with positively verified legacy DATA", err)
	}
	t.Cleanup(source.Clear)
	source.state.mu.Lock()
	defer source.state.mu.Unlock()
	if source.state.history == nil || len(source.state.history.legacy) != 1 {
		t.Fatal("complete validated legacy classification missing")
	}
	history := source.state.history.commitmentV1()
	original, err := f.entry.compactionSourceDigest(source.state.snapshot, source.state.view.Digest(), history)
	if err != nil || original != source.state.digest {
		t.Fatal("actual source did not use complete typed history commitment", err)
	}
	history.Legacy = append([]localTerminalLegacySnapshot(nil), history.Legacy...)
	old := history.Legacy[0].Run
	changed := old.Clone()
	changed.CreatedAt = changed.CreatedAt.Add(-time.Microsecond)
	if changed.Validate() != nil || changed.CreatedAt.Equal(old.CreatedAt) {
		t.Fatal("hash discriminator requires structurally valid different legacy Run")
	}
	// V1 seals output, not Run.CreatedAt. Keep all canonical/manifest/marker
	// bytes exactly unchanged: only the full classification postimage differs.
	history.Legacy[0].Run = changed
	digest, err := f.entry.compactionSourceDigest(source.state.snapshot, source.state.view.Digest(), history)
	if err != nil || digest == original {
		t.Fatalf("different valid legacy Run collapsed into source hash: err=%v equal=%t", err, digest == original)
	}
	if _, err := f.entry.compactionSourceDigest(source.state.snapshot, source.state.view.Digest(), localTerminalHistoryCommitmentV1{}); err == nil {
		t.Fatal("absent/unversioned history became an empty-history fallback")
	}
}
