package ydbstore

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// The wrapper delegates authentication to the original issuer. Its only added
// behavior is a test barrier at the boundary before historical bookkeeping.
type compactionReadBarrierIssuer struct {
	ports.AttemptOperationCapabilityIssuerV1
	afterAuthentication func()
}

func (i compactionReadBarrierIssuer) AuthenticateAttemptOperationObservation(g ports.AttemptOperationObservationGrantV1, o domain.AttemptOperationObservationV1) (ports.AuthenticatedAttemptOperationObservationV1, error) {
	v, err := i.AttemptOperationCapabilityIssuerV1.AuthenticateAttemptOperationObservation(g, o)
	if err == nil {
		i.afterAuthentication()
	}
	return v, err
}

func recordedCompactionEffect(t *testing.T) (*compactionSourceFixture, *localCompactionGenerationV1, ports.AttemptOperationObservationGrantV1, ports.AuthenticatedAttemptOperationObservationV1) {
	t.Helper()
	f, g, _ := preparedCompactionEffectFixture(t, false)
	r, err := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: g.operation})
	if err != nil || r.Capability == nil {
		t.Fatalf("actual reserve: %v", err)
	}
	if err := f.store.AuthorizeAttemptOperationBeforeSend(t.Context(), r.Capability.Clone()); err != nil {
		t.Fatalf("actual before-send: %v", err)
	}
	refs := g.program.References()
	if len(refs) == 0 {
		t.Fatal("original source references missing")
	}
	summary, err := json.Marshal(map[string]any{"schema": "sessionless.compaction-summary.v1", "task": []any{map[string]any{"text": "Preserve the original task", "source_ids": []string{refs[0].SourceID}}}, "constraints": []any{}, "facts": []any{}, "unresolved": []any{}, "artifacts": []any{}, "omissions": []any{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.program.ValidateSummary(summary); err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(map[string]any{"model": f.actual.config.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": string(summary)}, "finish_reason": "stop"}}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := g.frozen.NormalizeResponseBodyV1(wire, g.operation.Kind, g.operation.ResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := f.entry.bodies.Write(t.Context(), body)
	clear(body)
	if err != nil {
		t.Fatal(err)
	}
	rd, _ := r.Reservation.Digest()
	o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: r.Reservation.OperationDigest, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: ref.Ref, ResponseDigest: ref.Digest, ResponseBytes: ref.Bytes, ObservedAt: f.parent.graph.at, Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
	observed, err := f.bootstrap.operate.AuthenticateAttemptOperationObservation(r.ObservationGrant, o)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordAttemptOperationObservation(t.Context(), ports.RecordAttemptOperationObservationRequestV1{ObservationGrant: r.ObservationGrant, Observation: observed}); err != nil {
		t.Fatalf("actual record: %v", err)
	}
	return f, g, r.ObservationGrant, observed
}

func TestLocalCompactionEffectReadCancellationBeforeBookkeeping(t *testing.T) {
	for _, cancelRead := range []bool{false, true} {
		name := "live_control"
		if cancelRead {
			name = "cancelled_after_authentication"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, g, grant, expected := recordedCompactionEffect(t)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				authenticated := make(chan struct{})
				f.store.attemptOperationIssuer = compactionReadBarrierIssuer{AttemptOperationCapabilityIssuerV1: f.bootstrap.operate, afterAuthentication: func() {
					g.source.state.mu.Lock()
					close(authenticated)
				}}
				type result struct {
					value ports.AuthenticatedAttemptOperationObservationV1
					err   error
				}
				done := make(chan result, 1)
				go func() {
					v, err := f.store.ReadAttemptOperationObservation(ctx, grant)
					done <- result{v, err}
				}()
				select {
				case <-authenticated:
				case r := <-done:
					t.Fatalf("read failed before original authentication barrier: %v", r.err)
				}
				// Cancellation is ordered after the last old context guard and
				// before releasing the bookkeeping mutex; no scheduler timing or
				// sleep decides whether the new stage must reject it.
				if cancelRead {
					cancel()
				}
				g.source.state.mu.Unlock()
				r := <-done
				if cancelRead {
					if r.err == nil || !reflect.DeepEqual(r.value, ports.AuthenticatedAttemptOperationObservationV1{}) || g.observation.Version != 0 {
						t.Fatalf("cancelled historical read returned/cached success: err=%v returned=%d cached=%d", r.err, r.value.Version, g.observation.Version)
					}
				} else if r.err != nil || !reflect.DeepEqual(r.value, expected) || !reflect.DeepEqual(g.observation, expected) {
					t.Fatalf("live original authenticated read lost historical DATA: %v", r.err)
				}
			})
		})
	}
}
