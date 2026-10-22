package attachedworkeroutput

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func inlineCheckpointFixtureV2(t testing.TB, artifacts int) InlineReceiptCheckpointV2 {
	t.Helper()
	accepted := time.Date(2026, 10, 22, 12, 0, 0, 123456789, time.UTC)
	a := ports.AttachedWorkerSealedInputAuthorization{TenantID: "checkpoint-tenant", OwnerUserID: "checkpoint-owner", WorkerID: "checkpoint-worker", ConnectionID: "checkpoint-connection", EnrollmentGeneration: 1, ConnectionGeneration: 2, RunID: "checkpoint-run", AttemptID: "checkpoint-attempt", AttemptSequence: 1, LeaseID: "checkpoint-lease", LeaseGeneration: 3, LeaseExpiresAtUnixMicro: accepted.Add(time.Minute).UnixMicro(), ContextDigest: domain.AttachedWorkerContextDigest(strings.Repeat("1", 64)), CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("2", 64)), PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("3", 64))}
	var err error
	a.FenceToken, err = domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
	if err != nil {
		t.Fatalf("fixture fence: %v", err)
	}
	plan := InlineReceiptPlanV2{Authorization: a, Nonce: "checkpoint-nonce", Fingerprint: strings.Repeat("8", 64), Producer: InlineProducerBindingV2{SessionID: "checkpoint-session", HarnessBindingDigest: domain.HarnessBindingDigestV2(strings.Repeat("4", 64)), ReservationID: "checkpoint-reservation", AcceptedAt: accepted, SchemaDigest: strings.Repeat("5", 64), FinalDigest: strings.Repeat("6", 64), AnswerDigest: strings.Repeat("7", 64)}, ObservationDigest: strings.Repeat("9", 64), ExpiresAt: accepted.Add(10 * time.Second), WorkerAttemptSequence: 2, PlatformAttemptSequence: 2, TerminalSequence: 1}
	for i := range artifacts {
		plan.Artifacts = append(plan.Artifacts, InlineArtifactV2{Name: fmt.Sprintf("report-%d.txt", i), MediaType: "text/plain", Size: MaxInlineArtifactBytesV2, SHA256: strings.Repeat("a", 64)})
	}
	digest, err := plan.Digest()
	if err != nil {
		t.Fatalf("fixture plan digest: %v", err)
	}
	return InlineReceiptCheckpointV2{Version: 2, Plan: plan, PlanSHA256: digest, Progress: InlineReceiptProgressV2{Revision: 1, PrepareBurned: true}}
}

func inlineCheckpointCloneV2(c InlineReceiptCheckpointV2) InlineReceiptCheckpointV2 {
	c.Plan.Artifacts = append([]InlineArtifactV2(nil), c.Plan.Artifacts...)
	return c
}
func inlineCheckpointEncodeV2(t testing.TB, c InlineReceiptCheckpointV2) []byte {
	t.Helper()
	raw, err := EncodeInlineCheckpointV2(c)
	if err != nil {
		t.Fatalf("encode fixture checkpoint: %v", err)
	}
	return raw
}
func inlineCheckpointReadyV2(t testing.TB, artifacts int) InlineReceiptCheckpointV2 {
	t.Helper()
	c := inlineCheckpointFixtureV2(t, artifacts)
	if artifacts > 0 {
		c.Progress.BodyBurned = true
		c.Progress.Revision++
	}
	c.Progress.CanonicalDigest = domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("c", 64))
	c.Progress.Revision++
	return c
}

func TestInlineCheckpointV2MonotonicArtifactLifecycle(t *testing.T) {
	steps := []struct {
		name string
		add  func(*InlineReceiptProgressV2)
	}{
		{"body", func(p *InlineReceiptProgressV2) { p.BodyBurned = true }},
		{"reconcile", func(p *InlineReceiptProgressV2) { p.ReconcileBurned = true }},
		{"ready", func(p *InlineReceiptProgressV2) {
			p.CanonicalDigest = domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("c", 64))
		}},
		{"ack", func(p *InlineReceiptProgressV2) { p.TerminalAcked = true }},
	}
	for index, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			previous := inlineCheckpointFixtureV2(t, 2)
			for _, earlier := range steps[:index] {
				previous.Progress.Revision++
				earlier.add(&previous.Progress)
			}
			current := inlineCheckpointCloneV2(previous)
			current.Progress.Revision++
			step.add(&current.Progress)
			if err := current.AdvanceFrom(previous); err != nil {
				t.Fatalf("add %s at revision=%d: %v", step.name, current.Progress.Revision, err)
			}
			if err := current.AdvanceFrom(current); err != nil {
				t.Fatalf("idempotent %s at revision=%d: %v", step.name, current.Progress.Revision, err)
			}
			raw := inlineCheckpointEncodeV2(t, current)
			decoded, err := DecodeInlineCheckpointV2(raw)
			if err != nil || !reflect.DeepEqual(decoded, current) {
				t.Fatalf("round trip %s equal=%v err=%v", step.name, reflect.DeepEqual(decoded, current), err)
			}
			for _, field := range []string{`"answer":`, `"summary":`, `"body":`, `"capability":`, `"bearer":`} {
				if bytes.Contains(raw, []byte(field)) {
					t.Errorf("metadata checkpoint includes forbidden content field %s", field)
				}
			}
		})
	}
}

func TestInlineCheckpointV2AnswerOnlyAndPostReadyReconcile(t *testing.T) {
	initial := inlineCheckpointFixtureV2(t, 0)
	ready := inlineCheckpointReadyV2(t, 0)
	if err := ready.AdvanceFrom(initial); err != nil {
		t.Fatalf("answer-only READY: %v", err)
	}
	reconcile := ready
	reconcile.Progress.ReconcileBurned = true
	reconcile.Progress.Revision++
	if err := reconcile.AdvanceFrom(ready); err != nil {
		t.Fatalf("post-READY metadata reconcile: %v", err)
	}
	acked := reconcile
	acked.Progress.TerminalAcked = true
	acked.Progress.Revision++
	if err := acked.AdvanceFrom(reconcile); err != nil {
		t.Fatalf("answer-only ACK: %v", err)
	}
	if err := acked.AdvanceFrom(acked); err != nil {
		t.Fatalf("idempotent post-ACK observation: %v", err)
	}
	for _, raw := range [][]byte{inlineCheckpointEncodeV2(t, initial), inlineCheckpointEncodeV2(t, ready), inlineCheckpointEncodeV2(t, acked)} {
		if _, err := DecodeInlineCheckpointV2(raw); err != nil {
			t.Fatalf("answer-only metadata decode: %v", err)
		}
	}
}

func TestInlineCheckpointV2RejectsProgressForgeryAndRollback(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*InlineReceiptCheckpointV2)
	}{
		{"prepare-not-burned", func(c *InlineReceiptCheckpointV2) { c.Progress.PrepareBurned = false }},
		{"zero-revision", func(c *InlineReceiptCheckpointV2) { c.Progress.Revision = 0 }},
		{"revision-skipped", func(c *InlineReceiptCheckpointV2) { c.Progress.Revision = 3 }},
		{"revision-overflow", func(c *InlineReceiptCheckpointV2) { c.Progress.Revision = math.MaxUint64 }},
		{"revision-six", func(c *InlineReceiptCheckpointV2) { c.Progress.Revision = 6 }},
		{"ready-before-body", func(c *InlineReceiptCheckpointV2) {
			c.Progress.CanonicalDigest = domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("c", 64))
			c.Progress.Revision = 2
		}},
		{"ack-without-ready", func(c *InlineReceiptCheckpointV2) { c.Progress.TerminalAcked = true; c.Progress.Revision = 2 }},
		{"invalid-ready-digest", func(c *InlineReceiptCheckpointV2) {
			c.Progress.BodyBurned = true
			c.Progress.CanonicalDigest = "private-not-a-digest"
			c.Progress.Revision = 3
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			initial := inlineCheckpointFixtureV2(t, 1)
			current := inlineCheckpointCloneV2(initial)
			tc.mutate(&current)
			if current.Validate() == nil || current.AdvanceFrom(initial) == nil {
				t.Errorf("invalid progress %s was accepted", tc.name)
			}
			if raw, err := EncodeInlineCheckpointV2(current); err == nil || raw != nil {
				t.Errorf("invalid progress %s encoded bytes=%d err=%v", tc.name, len(raw), err)
			}
		})
	}
	for _, name := range []string{"body-rollback", "reconcile-rollback", "digest-rollback", "digest-replaced", "two-facts", "duplicate-new-revision", "after-ack", "ack-rollback", "divergent-plan", "answer-only-body"} {
		t.Run(name, func(t *testing.T) {
			previous := inlineCheckpointReadyV2(t, 1)
			previous.Progress.ReconcileBurned = true
			previous.Progress.Revision++
			current := inlineCheckpointCloneV2(previous)
			switch name {
			case "body-rollback":
				current.Progress.BodyBurned = false
				current.Progress.CanonicalDigest = ""
				current.Progress.Revision -= 2
			case "reconcile-rollback":
				current.Progress.ReconcileBurned = false
				current.Progress.Revision--
			case "digest-rollback":
				current.Progress.CanonicalDigest = ""
				current.Progress.Revision--
			case "digest-replaced":
				current.Progress.CanonicalDigest = domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("d", 64))
			case "two-facts":
				previous = inlineCheckpointFixtureV2(t, 1)
				current = inlineCheckpointCloneV2(previous)
				current.Progress.BodyBurned = true
				current.Progress.ReconcileBurned = true
				current.Progress.Revision += 2
			case "duplicate-new-revision":
				current.Progress.Revision++
			case "after-ack":
				previous = inlineCheckpointReadyV2(t, 1)
				previous.Progress.TerminalAcked = true
				previous.Progress.Revision++
				current = inlineCheckpointCloneV2(previous)
				current.Progress.ReconcileBurned = true
				current.Progress.Revision++
			case "ack-rollback":
				previous.Progress.TerminalAcked = true
				previous.Progress.Revision++
				current = inlineCheckpointCloneV2(previous)
				current.Progress.TerminalAcked = false
				current.Progress.Revision--
			case "divergent-plan":
				current.Plan.Nonce = "different-nonce"
				current.PlanSHA256, _ = current.Plan.Digest()
			case "answer-only-body":
				previous = inlineCheckpointReadyV2(t, 0)
				current = inlineCheckpointCloneV2(previous)
				current.Progress.BodyBurned = true
				current.Progress.Revision++
			}
			if err := current.AdvanceFrom(previous); err == nil {
				t.Errorf("transition %s accepted", name)
			}
		})
	}
}

func TestInlineCheckpointV2PlanScopeAndBounds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*InlineReceiptPlanV2)
	}{
		{"tenant-empty", func(p *InlineReceiptPlanV2) { p.Authorization.TenantID = "" }},
		{"owner-empty", func(p *InlineReceiptPlanV2) { p.Authorization.OwnerUserID = "" }},
		{"worker-empty", func(p *InlineReceiptPlanV2) { p.Authorization.WorkerID = "" }},
		{"connection-empty", func(p *InlineReceiptPlanV2) { p.Authorization.ConnectionID = "" }},
		{"run-empty", func(p *InlineReceiptPlanV2) { p.Authorization.RunID = "" }},
		{"attempt-empty", func(p *InlineReceiptPlanV2) { p.Authorization.AttemptID = "" }},
		{"lease-empty", func(p *InlineReceiptPlanV2) { p.Authorization.LeaseID = "" }},
		{"nonce-empty", func(p *InlineReceiptPlanV2) { p.Nonce = "" }},
		{"oversize-ID", func(p *InlineReceiptPlanV2) { p.Authorization.OwnerUserID = domain.UserID(strings.Repeat("x", 161)) }},
		{"invalid-ID", func(p *InlineReceiptPlanV2) { p.Authorization.RunID = "run/private" }},
		{"secret-present", func(p *InlineReceiptPlanV2) {
			p.Authorization.PresentedSecretDigest = domain.AttachedWorkerConnectionSecretDigest(strings.Repeat("0", 64))
		}},
		{"expected-revision", func(p *InlineReceiptPlanV2) { p.Authorization.ExpectedAttemptRevision = 1 }},
		{"enrollment-zero", func(p *InlineReceiptPlanV2) { p.Authorization.EnrollmentGeneration = 0 }},
		{"connection-zero", func(p *InlineReceiptPlanV2) { p.Authorization.ConnectionGeneration = 0 }},
		{"connection-overflow", func(p *InlineReceiptPlanV2) { p.Authorization.ConnectionGeneration = math.MaxUint64 }},
		{"auth-attempt-sequence", func(p *InlineReceiptPlanV2) { p.Authorization.AttemptSequence = 2 }},
		{"lease-generation-zero", func(p *InlineReceiptPlanV2) { p.Authorization.LeaseGeneration = 0 }},
		{"worker-sequence-zero", func(p *InlineReceiptPlanV2) { p.WorkerAttemptSequence = 0 }},
		{"worker-sequence-overflow", func(p *InlineReceiptPlanV2) { p.WorkerAttemptSequence = math.MaxUint64 }},
		{"platform-sequence-zero", func(p *InlineReceiptPlanV2) { p.PlatformAttemptSequence = 0 }},
		{"terminal-sequence-zero", func(p *InlineReceiptPlanV2) { p.TerminalSequence = 0 }},
		{"terminal-sequence-nonoriginal", func(p *InlineReceiptPlanV2) { p.TerminalSequence = 2 }},
		{"fence-scope", func(p *InlineReceiptPlanV2) {
			p.Authorization.FenceToken = domain.AttachedWorkerFenceToken(strings.Repeat("f", 64))
		}},
		{"tenant-scope", func(p *InlineReceiptPlanV2) { p.Authorization.TenantID = "different-tenant" }},
		{"context-digest", func(p *InlineReceiptPlanV2) { p.Authorization.ContextDigest = "invalid" }},
		{"capability-digest", func(p *InlineReceiptPlanV2) { p.Authorization.CapabilityDigest = "invalid" }},
		{"policy-digest", func(p *InlineReceiptPlanV2) { p.Authorization.PolicyDigest = "invalid" }},
		{"fingerprint", func(p *InlineReceiptPlanV2) { p.Fingerprint = strings.Repeat("A", 64) }},
		{"observation-digest", func(p *InlineReceiptPlanV2) { p.ObservationDigest = "" }},
		{"session-empty", func(p *InlineReceiptPlanV2) { p.Producer.SessionID = "" }},
		{"reservation-empty", func(p *InlineReceiptPlanV2) { p.Producer.ReservationID = "" }},
		{"harness-digest", func(p *InlineReceiptPlanV2) { p.Producer.HarnessBindingDigest = "" }},
		{"schema-digest", func(p *InlineReceiptPlanV2) { p.Producer.SchemaDigest = "" }},
		{"final-digest", func(p *InlineReceiptPlanV2) { p.Producer.FinalDigest = "" }},
		{"answer-digest", func(p *InlineReceiptPlanV2) { p.Producer.AnswerDigest = "" }},
		{"accepted-zero", func(p *InlineReceiptPlanV2) { p.Producer.AcceptedAt = time.Time{} }},
		{"expiry-zero", func(p *InlineReceiptPlanV2) { p.ExpiresAt = time.Time{} }},
		{"epoch-zero", func(p *InlineReceiptPlanV2) { p.Producer.AcceptedAt = time.Unix(0, 0).UTC() }},
		{"non-UTC", func(p *InlineReceiptPlanV2) { p.ExpiresAt = p.ExpiresAt.In(time.FixedZone("offset", 3600)) }},
		{"invalid-year", func(p *InlineReceiptPlanV2) { p.Producer.AcceptedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{"expiry-not-after-accepted", func(p *InlineReceiptPlanV2) { p.ExpiresAt = p.Producer.AcceptedAt }},
		{"expiry-after-lease", func(p *InlineReceiptPlanV2) {
			p.ExpiresAt = time.UnixMicro(p.Authorization.LeaseExpiresAtUnixMicro).UTC().Add(time.Nanosecond)
		}},
		{"lease-zero", func(p *InlineReceiptPlanV2) { p.Authorization.LeaseExpiresAtUnixMicro = 0 }},
		{"lease-outside-range", func(p *InlineReceiptPlanV2) { p.Authorization.LeaseExpiresAtUnixMicro = math.MaxInt64 }},
		{"artifact-count", func(p *InlineReceiptPlanV2) { p.Artifacts = append(p.Artifacts, p.Artifacts[0], p.Artifacts[0]) }},
		{"artifact-empty-name", func(p *InlineReceiptPlanV2) { p.Artifacts[0].Name = "" }},
		{"artifact-path", func(p *InlineReceiptPlanV2) { p.Artifacts[0].Name = "../private" }},
		{"artifact-control", func(p *InlineReceiptPlanV2) { p.Artifacts[0].Name = "private\nname" }},
		{"artifact-UTF8", func(p *InlineReceiptPlanV2) { p.Artifacts[0].Name = string([]byte{0xff}) }},
		{"artifact-name-bound", func(p *InlineReceiptPlanV2) { p.Artifacts[0].Name = strings.Repeat("x", 256) }},
		{"artifact-media", func(p *InlineReceiptPlanV2) { p.Artifacts[0].MediaType = "image/png" }},
		{"artifact-size-zero", func(p *InlineReceiptPlanV2) { p.Artifacts[0].Size = 0 }},
		{"artifact-size-overflow", func(p *InlineReceiptPlanV2) { p.Artifacts[0].Size = math.MaxUint64 }},
		{"artifact-hash", func(p *InlineReceiptPlanV2) { p.Artifacts[0].SHA256 = "invalid" }},
		{"artifact-collision", func(p *InlineReceiptPlanV2) {
			p.Artifacts = append(p.Artifacts, p.Artifacts[0])
			p.Artifacts[1].Name = strings.ToUpper(p.Artifacts[0].Name)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := inlineCheckpointFixtureV2(t, 1)
			tc.mutate(&c.Plan)
			if digest, err := c.Plan.Digest(); err == nil || digest != "" {
				t.Errorf("invalid %s plan digest=%q err=%v", tc.name, digest, err)
			}
			if c.Validate() == nil {
				t.Errorf("invalid scope %s checkpoint accepted", tc.name)
			}
		})
	}
}

func TestInlineCheckpointV2ReconcileBeforeBody(t *testing.T) {
	initial := inlineCheckpointFixtureV2(t, 1)
	reconciled := initial
	reconciled.Progress.ReconcileBurned = true
	reconciled.Progress.Revision++
	body := reconciled
	body.Progress.BodyBurned = true
	body.Progress.Revision++
	if reconciled.AdvanceFrom(initial) != nil || body.AdvanceFrom(reconciled) != nil {
		t.Error("metadata reconcile before the original body burn should remain a valid order")
	}
}

func TestInlineCheckpointV2DigestSealsExactNanosecondsAndImmutablePlan(t *testing.T) {
	c := inlineCheckpointFixtureV2(t, 1)
	raw, err := json.Marshal(c.Plan)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	_, _ = h.Write([]byte("sessionless:attached-worker-inline-checkpoint-plan:v2\x00"))
	_, _ = h.Write(raw)
	if want := hex.EncodeToString(h.Sum(nil)); c.PlanSHA256 != want {
		t.Fatalf("domain-separated plan digest=%s want=%s", c.PlanSHA256, want)
	}
	encoded := inlineCheckpointEncodeV2(t, c)
	if !bytes.Contains(encoded, []byte(".123456789Z")) {
		t.Error("checkpoint lost original nanosecond declaration precision")
	}
	changed := inlineCheckpointCloneV2(c)
	changed.Plan.ExpiresAt = changed.Plan.ExpiresAt.Add(time.Nanosecond)
	changed.PlanSHA256, err = changed.Plan.Digest()
	if err != nil || changed.PlanSHA256 == c.PlanSHA256 || changed.AdvanceFrom(c) == nil {
		t.Fatalf("changed exact expiry digest-equal=%v advance-accepted=%v err=%v", changed.PlanSHA256 == c.PlanSHA256, changed.AdvanceFrom(c) == nil, err)
	}
	changed = c
	changed.Plan.Producer.AcceptedAt = changed.Plan.Producer.AcceptedAt.In(time.FixedZone("zero-offset", 0))
	digest, err := changed.Plan.Digest()
	if err != nil || digest != c.PlanSHA256 {
		t.Fatalf("identical UTC semantic instant digest-equal=%v err=%v", digest == c.PlanSHA256, err)
	}
}

func TestInlineCheckpointV2ChecksumIsNotProvenanceOrCASAuthority(t *testing.T) {
	initial := inlineCheckpointFixtureV2(t, 1)
	full := inlineCheckpointCloneV2(initial)
	full.Progress = InlineReceiptProgressV2{Revision: 5, PrepareBurned: true, BodyBurned: true, ReconcileBurned: true, CanonicalDigest: domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("c", 64)), TerminalAcked: true}
	if full.Validate() != nil {
		t.Fatal("well-shaped progressed metadata should validate independently of an external producer proof")
	}
	if full.AdvanceFrom(initial) == nil {
		t.Error("valid full clone skipped durable individual transitions")
	}
	if full.AdvanceFrom(full) != nil {
		t.Error("identical progressed observation should be idempotent, not a new authority token")
	}
	forged := inlineCheckpointCloneV2(initial)
	forged.Plan.Nonce = "other-nonce"
	if forged.Validate() == nil {
		t.Error("modified plan retained old checksum")
	}
	forged.PlanSHA256, _ = forged.Plan.Digest()
	if forged.Validate() != nil || forged.AdvanceFrom(initial) == nil {
		t.Error("self-consistent metadata must not replace an existing immutable plan")
	}
}

func TestInlineCheckpointV2MaximumMetadataAndExpiredReady(t *testing.T) {
	c := inlineCheckpointFixtureV2(t, 2)
	maxID := strings.Repeat("x", 160)
	a := &c.Plan.Authorization
	a.TenantID = domain.TenantID(maxID)
	a.OwnerUserID = domain.UserID(maxID)
	a.WorkerID = domain.AttachedWorkerID(maxID)
	a.ConnectionID = domain.AttachedWorkerConnectionID(maxID)
	a.RunID = domain.RunID(maxID)
	a.AttemptID = domain.AttemptID(maxID)
	a.LeaseID = domain.LeaseID(maxID)
	a.EnrollmentGeneration = math.MaxUint64 - 1
	a.ConnectionGeneration = math.MaxUint64 - 1
	a.LeaseGeneration = math.MaxUint64 - 1
	a.FenceToken, _ = domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
	c.Plan.Nonce = domain.IdempotencyKey(maxID)
	c.Plan.Producer.SessionID = domain.SessionID(maxID)
	c.Plan.Producer.ReservationID = domain.QuotaReservationID(maxID)
	c.Plan.WorkerAttemptSequence = math.MaxUint64 - 1
	c.Plan.PlatformAttemptSequence = math.MaxUint64 - 1
	c.Plan.Artifacts[0].Name = "a" + strings.Repeat("<", 254)
	c.Plan.Artifacts[1].Name = "b" + strings.Repeat("<", 254)
	c.PlanSHA256, _ = c.Plan.Digest()
	raw := inlineCheckpointEncodeV2(t, c)
	if len(raw) > MaxInlineCheckpointBytesV2 || !bytes.Contains(raw, []byte(`\u003c`)) {
		t.Fatalf("maximum compact metadata bytes=%d escaped-names-preserved=%v", len(raw), bytes.Contains(raw, []byte(`\u003c`)))
	}
	if _, err := DecodeInlineCheckpointV2(raw); err != nil {
		t.Fatalf("maximum compact metadata decode: %v", err)
	}
	t.Logf("maximum bounded metadata checkpoint: %d bytes", len(raw))
	expired := inlineCheckpointReadyV2(t, 1)
	accepted := time.Date(2000, 1, 1, 0, 0, 0, 987654321, time.UTC)
	expired.Plan.Producer.AcceptedAt = accepted
	expired.Plan.ExpiresAt = accepted.Add(10 * time.Second)
	expired.Plan.Authorization.LeaseExpiresAtUnixMicro = accepted.Add(time.Minute).UnixMicro()
	expired.PlanSHA256, _ = expired.Plan.Digest()
	if err := expired.Validate(); err != nil {
		t.Fatalf("expired READY metadata must not consult current time: %v", err)
	}
}

func TestInlineCheckpointV2StrictDecodeAndRedaction(t *testing.T) {
	c := inlineCheckpointFixtureV2(t, 1)
	base := inlineCheckpointEncodeV2(t, c)
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"v1", bytes.Replace(base, []byte(`"version":2`), []byte(`"version":1`), 1)},
		{"unknown", bytes.Replace(base, []byte(`"version":2`), []byte(`"version":2,"private":"secret"`), 1)},
		{"content-field", bytes.Replace(base, []byte(`"artifacts":`), []byte(`"answer":"secret","artifacts":`), 1)},
		{"capability-field", bytes.Replace(base, []byte(`"progress":`), []byte(`"capability":"secret","progress":`), 1)},
		{"duplicate", bytes.Replace(base, []byte(`"version":2`), []byte(`"version":2,"version":2`), 1)},
		{"duplicate-alias", bytes.Replace(base, []byte(`"version":2`), []byte(`"version":2,"Version":2`), 1)},
		{"escaped-duplicate", bytes.Replace(base, []byte(`"version":2`), []byte(`"version":2,"\u0076ersion":2`), 1)},
		{"case-alias", bytes.Replace(base, []byte(`"version"`), []byte(`"VERSION"`), 1)},
		{"auth-case-alias", bytes.Replace(base, []byte(`"TenantID"`), []byte(`"tenantid"`), 1)},
		{"missing-false-field", bytes.Replace(base, []byte(`,"body_burned":false`), nil, 1)},
		{"secret-auth", bytes.Replace(base, []byte(`"PresentedSecretDigest":""`), []byte(`"PresentedSecretDigest":"private-bearer"`), 1)},
		{"invalid-UTF8", bytes.Replace(base, []byte("checkpoint-nonce"), []byte{0xff}, 1)},
		{"unpaired-surrogate", bytes.Replace(base, []byte("checkpoint-nonce"), []byte(`\ud800`), 1)},
		{"number-overflow", bytes.Replace(base, []byte(`"revision":1`), []byte(`"revision":18446744073709551616`), 1)},
		{"number-fraction", bytes.Replace(base, []byte(`"revision":1`), []byte(`"revision":1.0`), 1)},
		{"trailing", append(bytes.Clone(base), []byte(`{}`)...)},
		{"whitespace", append([]byte(" "), base...)},
		{"oversized", bytes.Repeat([]byte("x"), MaxInlineCheckpointBytesV2+1)},
		{"excess-depth", []byte(strings.Repeat("[", 13) + "0" + strings.Repeat("]", 13))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decoded, err := DecodeInlineCheckpointV2(tc.raw)
			if !errors.Is(err, ErrCandidateInvalid) || decoded.Version != 0 {
				t.Fatalf("strict decode %s version=%d err=%v", tc.name, decoded.Version, err)
			}
		})
	}
	for _, value := range []any{c, c.Plan, c.Progress, &c, &c.Plan, &c.Progress} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x"} {
			rendered := fmt.Sprintf(format, value)
			if !strings.Contains(rendered, "[redacted metadata]") || strings.Contains(rendered, "checkpoint-") || strings.Contains(rendered, c.PlanSHA256) {
				t.Errorf("format=%s exposed checkpoint metadata: %q", format, rendered)
			}
		}
	}
}

func FuzzInlineCheckpointV2(f *testing.F) {
	valid := inlineCheckpointEncodeV2(f, inlineCheckpointFixtureV2(f, 1))
	for _, seed := range [][]byte{valid, []byte(`{}`), []byte(`{"version":2,"Version":2}`), []byte(`"\ud800"`), []byte(`null`)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > MaxInlineCheckpointBytesV2 {
			return
		}
		value, err := DecodeInlineCheckpointV2(raw)
		if err != nil {
			if !errors.Is(err, ErrCandidateInvalid) || value.Version != 0 {
				t.Fatal("invalid checkpoint decode did not fail closed")
			}
			return
		}
		encoded, err := EncodeInlineCheckpointV2(value)
		if err != nil || !bytes.Equal(raw, encoded) || value.AdvanceFrom(value) != nil {
			t.Fatalf("accepted checkpoint did not preserve canonical idempotent metadata: %v", err)
		}
	})
}
