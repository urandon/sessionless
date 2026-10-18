package operationbody_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
)

// These cooperative fakes prove shared physical bounds and byte integrity,
// not current registration/authority, summary semantics, actual S3 or custody.
func retainedLimits() domain.AgentLoopLimitsV1 {
	limits := bodyLimits()
	limits.MaxAggregateBodyBytes = 1 << 20
	limits.MaxCompactionCalls = 2
	limits.MaxModelCalls = 3
	limits.MaxOperations = 4
	return limits
}

func retainedStore(t *testing.T, blobs *bodyBlobs, limits domain.AgentLoopLimitsV1) *operationbody.StoreV1 {
	t.Helper()
	if err := limits.Validate(); err != nil {
		t.Fatalf("retained limits fixture: %v", err)
	}
	s, err := operationbody.NewV1(bodyScope(), limits, blobs)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func retainedPlan(t *testing.T, sequence, generation uint64, summary, manifest []byte) domain.DerivedContextPlanV1 {
	t.Helper()
	h := strings.Repeat("b", 64)
	p := domain.DerivedContextPlanV1{Version: 1, Scope: bodyScope(), Generation: generation, OperationSequence: sequence,
		ReservationDigest: domain.AttemptOperationReservationDigestV1(h), ObservationDigest: domain.AttemptOperationObservationDigestV1(h), NormalizedResponseDigest: h, SourceViewDigest: h}
	for i, raw := range [][]byte{summary, manifest} {
		sum := sha256.Sum256(raw)
		ref := domain.BlobRef{TenantID: p.Scope.TenantID, Size: int64(len(raw)), SHA256: hex.EncodeToString(sum[:])}
		kind := "summary"
		if i == 1 {
			kind = "manifest"
		}
		var err error
		ref.Key, err = domain.DerivedContextObjectKeyV1(p.Scope, generation, kind, ref.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			p.Summary = ref
		} else {
			p.Manifest = ref
		}
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRetainedPairVerifiedObjectsOneUseAcrossCopies(t *testing.T) {
	b := newBodyBlobs()
	s := retainedStore(t, b, retainedLimits())
	copyStore := *s
	summary, manifest := []byte("summary DATA"), []byte("manifest DATA")
	for generation := uint64(1); generation <= 2; generation++ {
		pair, err := copyStore.ReserveRetainedPair(t.Context(), generation, generation)
		if err != nil {
			t.Fatal(err)
		}
		copyPair := *pair
		plan := retainedPlan(t, generation, generation, summary, manifest)
		if err := pair.Write(t.Context(), plan, summary, manifest); err != nil {
			t.Fatalf("generation %d write: %v", generation, err)
		}
		if err := copyPair.Write(t.Context(), plan, summary, manifest); err == nil {
			t.Fatal("pair wrapper copy restored write ownership")
		}
		b.mu.Lock()
		gotSummary, gotManifest := bytes.Clone(b.objects[plan.Summary.Key]), bytes.Clone(b.objects[plan.Manifest.Key])
		b.mu.Unlock()
		if !bytes.Equal(gotSummary, summary) || !bytes.Equal(gotManifest, manifest) {
			t.Fatal("retained object contents differ from exact plan bytes")
		}
	}
	if string(summary) != "summary DATA" || string(manifest) != "manifest DATA" {
		t.Fatal("adapter cleared caller buffers")
	}
	if _, err := s.ReserveRetainedPair(t.Context(), 3, 3); err == nil {
		t.Fatal("third physical pair admitted")
	}
	// Retained writes do not consume or expand the ordinary 2*MaxOperations cap.
	for range 2 * retainedLimits().MaxOperations {
		if _, err := copyStore.Write(t.Context(), []byte("ordinary")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Write(t.Context(), []byte("ordinary overflow")); err == nil {
		t.Fatal("retained allowance expanded ordinary write cap")
	}
	if p, o, c, d, _ := b.counts(); p != 12 || o != 12 || c != 12 || d != 0 {
		t.Fatalf("one-use write/readback counts put=%d open=%d close=%d delete=%d", p, o, c, d)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, key := range b.keys[:4] {
		if !strings.HasPrefix(key, domain.SessionRunObjectPrefix(bodyScope().TenantID, bodyScope().SessionID, bodyScope().RunID)+"derived-context-v1/") || strings.Contains(key, "attempt-operations-v1") {
			t.Fatal("retained object entered erasable ordinary namespace")
		}
	}
}

func TestRetainedPairReservationBoundsAndNonrefundableAggregate(t *testing.T) {
	for name, args := range map[string][2]uint64{
		"zero_sequence": {0, 1}, "sequence_overflow": {5, 1}, "zero_generation": {1, 0}, "first_generation2": {1, 2},
	} {
		t.Run(name, func(t *testing.T) {
			b := newBodyBlobs()
			s := retainedStore(t, b, retainedLimits())
			if _, err := s.ReserveRetainedPair(t.Context(), args[0], args[1]); err == nil {
				t.Fatal("invalid slot admitted")
			}
			if _, err := s.ReserveRetainedPair(t.Context(), 1, 1); err != nil {
				t.Fatalf("invalid reservation consumed valid slot: %v", err)
			}
			if p, o, _, _, _ := b.counts(); p != 0 || o != 0 {
				t.Fatal("capacity reservation performed blob I/O")
			}
		})
	}
	for _, available := range []uint64{(160 << 10) - 1, 160 << 10} {
		t.Run("aggregate_"+strconv.FormatUint(available, 10), func(t *testing.T) {
			b := newBodyBlobs()
			limits := retainedLimits()
			limits.MaxAggregateBodyBytes = available
			s := retainedStore(t, b, limits)
			pair, err := s.ReserveRetainedPair(t.Context(), 1, 1)
			if available < 160<<10 {
				if err == nil || pair != nil {
					t.Fatal("insufficient aggregate admitted pair upper bounds")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			copyStore := *s
			if _, err := copyStore.Write(t.Context(), []byte("x")); err == nil {
				t.Fatal("unused reserved pair refunded aggregate bytes")
			}
			plan := retainedPlan(t, 1, 1, []byte("s"), []byte("m"))
			if err := pair.Write(t.Context(), plan, []byte("s"), []byte("m")); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(t.Context(), []byte("x")); err == nil {
				t.Fatal("small actual objects refunded reserved upper bounds")
			}
		})
	}
	b := newBodyBlobs()
	limits := retainedLimits()
	limits.MaxAggregateBodyBytes = 160 << 10
	s := retainedStore(t, b, limits)
	if _, err := s.Write(t.Context(), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveRetainedPair(t.Context(), 1, 1); err == nil {
		t.Fatal("reservation ignored already charged ordinary byte")
	}
	limits = retainedLimits()
	limits.MaxCompactionCalls = 1
	s = retainedStore(t, newBodyBlobs(), limits)
	if _, err := s.ReserveRetainedPair(t.Context(), 2, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveRetainedPair(t.Context(), 3, 1); err == nil {
		t.Fatal("duplicate generation restored a reserved slot")
	}
	if _, err := s.ReserveRetainedPair(t.Context(), 3, 2); err == nil {
		t.Fatal("reservation exceeded policy compaction cap")
	}
	s = retainedStore(t, newBodyBlobs(), retainedLimits())
	if _, err := s.ReserveRetainedPair(t.Context(), 2, 1); err != nil {
		t.Fatal(err)
	}
	for _, sequence := range []uint64{1, 2} {
		if _, err := s.ReserveRetainedPair(t.Context(), sequence, 2); err == nil {
			t.Fatal("nonincreasing compaction sequence admitted")
		}
	}
	if _, err := s.ReserveRetainedPair(t.Context(), 3, 2); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedPairExactRoleByteCeilings(t *testing.T) {
	b := newBodyBlobs()
	s := retainedStore(t, b, retainedLimits())
	pair, err := s.ReserveRetainedPair(t.Context(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	summary, manifest := bytes.Repeat([]byte("s"), 32<<10), bytes.Repeat([]byte("m"), 128<<10)
	plan := retainedPlan(t, 1, 1, summary, manifest)
	if err := pair.Write(t.Context(), plan, summary, manifest); err != nil {
		t.Fatal(err)
	}
	if p, o, c, _, read := b.counts(); p != 2 || o != 2 || c != 2 || read != len(summary)+len(manifest) {
		t.Fatalf("exact role limits put=%d open=%d close=%d read=%d", p, o, c, read)
	}
}

func TestRetainedPairBadInputsHaveZeroBlobEffects(t *testing.T) {
	for name, mutate := range map[string]func(*domain.DerivedContextPlanV1){
		"scope":              func(p *domain.DerivedContextPlanV1) { p.Scope.AttemptID = "other-attempt" },
		"owner":              func(p *domain.DerivedContextPlanV1) { p.Scope.OwnerUserID = "other-owner" },
		"sequence":           func(p *domain.DerivedContextPlanV1) { p.OperationSequence = 2 },
		"generation":         func(p *domain.DerivedContextPlanV1) { p.Generation = 2 },
		"summary_role":       func(p *domain.DerivedContextPlanV1) { p.Summary.Key = p.Manifest.Key },
		"ordinary_namespace": func(p *domain.DerivedContextPlanV1) { p.Summary.Key = "attempt-operations-v1/wrong" },
		"summary_digest":     func(p *domain.DerivedContextPlanV1) { p.Summary.SHA256 = strings.Repeat("f", 64) },
		"manifest_size":      func(p *domain.DerivedContextPlanV1) { p.Manifest.Size++ },
		"oversized_summary":  func(p *domain.DerivedContextPlanV1) { p.Summary.Size = (32 << 10) + 1 },
		"oversized_manifest": func(p *domain.DerivedContextPlanV1) { p.Manifest.Size = (128 << 10) + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			b := newBodyBlobs()
			s := retainedStore(t, b, retainedLimits())
			pair, err := s.ReserveRetainedPair(t.Context(), 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			valid := retainedPlan(t, 1, 1, []byte("s"), []byte("m"))
			bad := valid
			mutate(&bad)
			if err := pair.Write(t.Context(), bad, []byte("s"), []byte("m")); err == nil {
				t.Fatal("bad plan reached retained write")
			}
			if err := pair.Write(t.Context(), valid, []byte("x"), []byte("m")); err == nil {
				t.Fatal("wrong actual bytes accepted")
			}
			if err := pair.Write(t.Context(), valid, []byte("s"), []byte("x")); err == nil {
				t.Fatal("wrong actual manifest accepted")
			}
			if p, o, _, d, _ := b.counts(); p != 0 || o != 0 || d != 0 {
				t.Fatal("bad data reached blob I/O")
			}
			if err := pair.Write(t.Context(), valid, []byte("s"), []byte("m")); err != nil {
				t.Fatalf("pre-I/O invalid input consumed pair: %v", err)
			}
		})
	}
	var nilPair *operationbody.RetainedPairV1
	var zero operationbody.RetainedPairV1
	for _, pair := range []*operationbody.RetainedPairV1{nilPair, &zero} {
		if pair.Write(t.Context(), domain.DerivedContextPlanV1{}, nil, nil) == nil {
			t.Fatal("unowned pair wrote")
		}
	}
}

type retainedFaultBlobs struct {
	*bodyBlobs
	role, fault string
}

func (b *retainedFaultBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, raw io.Reader) (domain.BlobRef, error) {
	b.mu.Lock()
	b.putFault, b.forge = "", ""
	if strings.Contains(key, "/"+b.role+"/") {
		switch b.fault {
		case "put":
			b.putFault = "ambiguous"
		case "tenant", "key", "size", "digest", "uppercase":
			b.forge = b.fault
		}
	}
	b.mu.Unlock()
	return b.bodyBlobs.Put(ctx, tenant, key, raw)
}

func (b *retainedFaultBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	b.mu.Lock()
	b.openFault, b.closeFault = "", false
	if strings.Contains(ref.Key, "/"+b.role+"/") {
		switch b.fault {
		case "close":
			b.closeFault = true
		case "open", "corrupt", "oversize", "read":
			b.openFault = b.fault
		}
	}
	b.mu.Unlock()
	return b.bodyBlobs.Open(ctx, tenant, ref)
}

func TestRetainedPairUncertaintyPoisonsAllOriginalWritesButNotReads(t *testing.T) {
	for _, role := range []string{"summary", "manifest"} {
		for _, fault := range []string{"put", "tenant", "key", "size", "digest", "uppercase", "open", "corrupt", "oversize", "read", "close"} {
			t.Run(role+"_"+fault, func(t *testing.T) {
				b := &retainedFaultBlobs{bodyBlobs: newBodyBlobs(), role: role, fault: fault}
				s, err := operationbody.NewV1(bodyScope(), retainedLimits(), b)
				if err != nil {
					t.Fatal(err)
				}
				ordinary, err := s.Write(t.Context(), []byte("reconcile"))
				if err != nil {
					t.Fatal(err)
				}
				pair, err := s.ReserveRetainedPair(t.Context(), 1, 1)
				if err != nil {
					t.Fatal(err)
				}
				other, err := s.ReserveRetainedPair(t.Context(), 2, 2)
				if err != nil {
					t.Fatal(err)
				}
				plan := retainedPlan(t, 1, 1, []byte("s"), []byte("m"))
				if pair.Write(t.Context(), plan, []byte("s"), []byte("m")) == nil {
					t.Fatal("uncertain retained write succeeded")
				}
				before, _, _, _, read := b.counts()
				want := 2
				if role == "manifest" {
					want = 3
				}
				if before != want {
					t.Fatalf("physical attempts=%d want=%d", before, want)
				}
				if read > len("reconcile")+1+2 {
					t.Fatalf("readback escaped exact size+1 bound: %d", read)
				}
				if pair.Write(t.Context(), plan, []byte("s"), []byte("m")) == nil {
					t.Fatal("ambiguous pair retried")
				}
				if other.Write(t.Context(), retainedPlan(t, 2, 2, []byte("s"), []byte("m")), []byte("s"), []byte("m")) == nil {
					t.Fatal("poisoned Store wrote another pair")
				}
				if _, err := s.Write(t.Context(), []byte("x")); err == nil {
					t.Fatal("retained uncertainty did not poison ordinary writes")
				}
				if p, _, _, d, _ := b.counts(); p != before || d != 0 {
					t.Fatal("uncertainty caused retry/delete")
				}
				got, err := s.ReadAttemptOperationBody(t.Context(), bodyScope(), ordinary.Ref, 1024)
				if err != nil || string(got) != "reconcile" {
					t.Fatalf("ordinary reconciliation unavailable: %v", err)
				}
			})
		}
	}
	// The same poison propagates in the opposite direction.
	b := newBodyBlobs()
	s := retainedStore(t, b, retainedLimits())
	pair, err := s.ReserveRetainedPair(t.Context(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	b.putFault = "ambiguous"
	if _, err := s.Write(t.Context(), []byte("x")); err == nil {
		t.Fatal("ordinary ambiguous write succeeded")
	}
	if pair.Write(t.Context(), retainedPlan(t, 1, 1, []byte("s"), []byte("m")), []byte("s"), []byte("m")) == nil {
		t.Fatal("ordinary uncertainty did not poison retained writes")
	}
}

func TestRetainedPairSharedGateClonesAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBodyBlobs()
		s := retainedStore(t, b, retainedLimits())
		pair, err := s.ReserveRetainedPair(t.Context(), 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		copiedPair, copiedStore := *pair, *s
		release, entered := make(chan struct{}), make(chan struct{}, 1)
		var once sync.Once
		t.Cleanup(func() { once.Do(func() { close(release) }) })
		b.blockPut, b.entered = release, entered
		summary, manifest := []byte("s"), []byte("m")
		plan := retainedPlan(t, 1, 1, summary, manifest)
		done := make(chan error, 1)
		go func() { done <- pair.Write(t.Context(), plan, summary, manifest) }()
		<-entered
		summary[0], manifest[0] = 'x', 'y'
		if err := copiedPair.Write(t.Context(), plan, []byte("s"), []byte("m")); !errors.Is(err, operationbody.ErrBusy) {
			t.Fatalf("pair bypassed shared gate: %v", err)
		}
		if _, err := copiedStore.Write(t.Context(), []byte("x")); !errors.Is(err, operationbody.ErrBusy) {
			t.Fatalf("ordinary writer bypassed shared gate: %v", err)
		}
		if _, err := copiedStore.ReserveRetainedPair(t.Context(), 2, 2); !errors.Is(err, operationbody.ErrBusy) {
			t.Fatalf("reserve bypassed shared gate: %v", err)
		}
		if _, err := copiedStore.ReadAttemptOperationBody(t.Context(), bodyScope(), "opbody-v1.1."+strings.Repeat("a", 64), 1); !errors.Is(err, operationbody.ErrBusy) {
			t.Fatalf("read bypassed shared gate: %v", err)
		}
		once.Do(func() { close(release) })
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		b.mu.Lock()
		gotSummary, gotManifest := string(b.objects[plan.Summary.Key]), string(b.objects[plan.Manifest.Key])
		b.blockPut, b.entered = nil, nil
		b.mu.Unlock()
		if gotSummary != "s" || gotManifest != "m" || string(summary) != "x" || string(manifest) != "y" {
			t.Fatal("private clone/caller ownership lost")
		}
		if _, err := copiedStore.Write(t.Context(), []byte("next")); err != nil {
			t.Fatalf("busy denials poisoned original Store: %v", err)
		}
	})
	t.Run("cancelled_before_start", func(t *testing.T) {
		b := newBodyBlobs()
		s := retainedStore(t, b, retainedLimits())
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := s.ReserveRetainedPair(ctx, 1, 1); err == nil {
			t.Fatal("pre-cancelled reservation succeeded")
		}
		pair, err := s.ReserveRetainedPair(t.Context(), 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		plan := retainedPlan(t, 1, 1, []byte("s"), []byte("m"))
		if pair.Write(ctx, plan, []byte("s"), []byte("m")) == nil {
			t.Fatal("pre-cancelled pair wrote")
		}
		if p, o, _, _, _ := b.counts(); p != 0 || o != 0 {
			t.Fatal("pre-cancelled call reached blob I/O")
		}
		if err := pair.Write(t.Context(), plan, []byte("s"), []byte("m")); err != nil {
			t.Fatalf("pre-I/O cancellation consumed ownership: %v", err)
		}
	})
	t.Run("during_put", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			b := newBodyBlobs()
			b.blockPut = make(chan struct{})
			s := retainedStore(t, b, retainedLimits())
			pair, err := s.ReserveRetainedPair(t.Context(), 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			plan := retainedPlan(t, 1, 1, []byte("s"), []byte("m"))
			if pair.Write(t.Context(), plan, []byte("s"), []byte("m")) == nil {
				t.Fatal("cooperative blocked PUT ignored deadline")
			}
			if _, err := s.Write(t.Context(), []byte("x")); err == nil {
				t.Fatal("cancelled physical write failed to poison Store")
			}
			if p, o, _, _, _ := b.counts(); p != 1 || o != 0 {
				t.Fatalf("cancelled write retried/readback: put=%d open=%d", p, o)
			}
		})
	})
	t.Run("during_readback", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			b := newBodyBlobs()
			b.blockOpen = make(chan struct{})
			s := retainedStore(t, b, retainedLimits())
			pair, err := s.ReserveRetainedPair(t.Context(), 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			plan := retainedPlan(t, 1, 1, []byte("s"), []byte("m"))
			if pair.Write(t.Context(), plan, []byte("s"), []byte("m")) == nil {
				t.Fatal("cooperative blocked readback ignored deadline")
			}
			if _, err := s.Write(t.Context(), []byte("x")); err == nil {
				t.Fatal("cancelled readback failed to poison Store")
			}
			if p, o, _, _, _ := b.counts(); p != 1 || o != 1 {
				t.Fatalf("cancelled readback retried: put=%d open=%d", p, o)
			}
		})
	})
}
