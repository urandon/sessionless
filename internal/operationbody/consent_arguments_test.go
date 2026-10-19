package operationbody_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
)

// These are physical custody tests with the existing cooperative fake. They
// prove no model provenance, durable consent decision, finalizer or S3 guarantee.
func consentBodyRef(t *testing.T, body []byte) domain.BlobRef {
	t.Helper()
	sum := sha256.Sum256(body)
	ref, err := domain.AttemptOperationConsentArgumentsRefV1(bodyScope(), uint64(len(body)), hex.EncodeToString(sum[:]))
	if err != nil {
		t.Fatal("valid closed consent DATA ref", err)
	}
	return ref
}

func TestConsentArgumentsOwnedRoundTripAndOrdinaryPreservation(t *testing.T) {
	b := newBodyBlobs()
	s := newBodyStore(t, b)
	body := []byte(`{"query":"public compiler документация","max_results":3}`)
	want := bytes.Clone(body)
	expected := consentBodyRef(t, body)
	ref, err := s.WriteConsentArguments(t.Context(), body)
	if err != nil || ref != expected {
		t.Fatalf("actual consent writer got=%+v err=%v want=%+v", ref, err, expected)
	}
	clear(body)
	for i := 0; i < 2; i++ {
		got, err := s.ReadAttemptOperationBody(t.Context(), bodyScope(), ref.Key, uint64(ref.Size))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("original reader roundtrip=%d bytes=%d err=%v", i, len(got), err)
		}
		clear(got)
	}
	ordinary, err := s.Write(t.Context(), []byte("ordinary"))
	if err != nil {
		t.Fatal("ordinary path after consent", err)
	}
	sum := sha256.Sum256([]byte("ordinary"))
	if ordinary.Ref != "opbody-v1.8."+hex.EncodeToString(sum[:]) || ordinary.Bytes != 8 {
		t.Fatalf("ordinary format changed: %+v", ordinary)
	}
	got, err := s.ReadAttemptOperationBody(t.Context(), bodyScope(), ordinary.Ref, 8)
	if err != nil || string(got) != "ordinary" {
		t.Fatalf("ordinary read changed: %q err=%v", got, err)
	}
	puts, opens, closes, deletes, _ := b.counts()
	if puts != 2 || opens != 5 || closes != opens || deletes != 0 {
		t.Fatalf("unexpected custody I/O puts=%d opens=%d closes=%d deletes=%d", puts, opens, closes, deletes)
	}
}

// This positive reaches the existing reader independently of the new writer,
// so a declaration-only writer does not conceal its ordinary-only dispatch.
func TestConsentArgumentsExistingReaderClosedArm(t *testing.T) {
	b := newBodyBlobs()
	s := newBodyStore(t, b)
	body := []byte(`{"query":"bounded"}`)
	ref := consentBodyRef(t, body)
	b.objects[ref.Key] = bytes.Clone(body)
	got, err := s.ReadAttemptOperationBody(t.Context(), bodyScope(), ref.Key, uint64(len(body)))
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("valid closed arm reader got=%q err=%v", got, err)
	}
	beforePut, beforeOpen, _, _, _ := b.counts()
	for _, value := range []string{
		strings.Replace(ref.Key, "/consent-arguments-v1/", "/consent-arguments-v2/", 1),
		strings.Replace(ref.Key, "consent-args-v1.", "opbody-v1.", 1),
		ref.Key + "/extra",
		strings.TrimSuffix(ref.Key, ref.SHA256) + strings.ToUpper(ref.SHA256),
	} {
		if got, err := s.ReadAttemptOperationBody(t.Context(), bodyScope(), value, 4096); err == nil || got != nil {
			t.Fatalf("noncanonical/foreign arm accepted key=%q bytes=%d err=%v", value, len(got), err)
		}
	}
	other := bodyScope()
	other.PhysicalInvocationClaimID = "foreign-claim"
	if got, err := s.ReadAttemptOperationBody(t.Context(), other, ref.Key, 4096); err == nil || got != nil {
		t.Fatalf("foreign scope materialized bytes=%d err=%v", len(got), err)
	}
	if got, err := s.ReadAttemptOperationBody(t.Context(), bodyScope(), ref.Key, uint64(len(body)-1)); err == nil || got != nil {
		t.Fatalf("underfunded read materialized bytes=%d err=%v", len(got), err)
	}
	if puts, opens, _, _, _ := b.counts(); puts != beforePut || opens != beforeOpen {
		t.Fatalf("rejected ref reached storage puts=%d/%d opens=%d/%d", puts, beforePut, opens, beforeOpen)
	}
}

func TestConsentArgumentsShareOriginalPhysicalCapacity(t *testing.T) {
	for _, name := range []string{"slots", "bytes"} {
		t.Run(name, func(t *testing.T) {
			b := newBodyBlobs()
			s := newBodyStore(t, b)
			if name == "slots" {
				for i := 0; i < 4; i++ {
					if _, err := s.Write(t.Context(), []byte{byte('a' + i)}); err != nil {
						t.Fatal("fill original four physical slots", err)
					}
				}
			} else if _, err := s.Write(t.Context(), bytes.Repeat([]byte("a"), 1020)); err != nil {
				t.Fatal("fill original byte pool", err)
			}
			beforePut, beforeOpen, _, _, _ := b.counts()
			if got, err := s.WriteConsentArguments(t.Context(), []byte(`{"query":"x"}`)); err == nil || got != (domain.BlobRef{}) {
				t.Fatalf("exhausted %s admitted consent ref=%+v err=%v", name, got, err)
			}
			if puts, opens, _, _, _ := b.counts(); puts != beforePut || opens != beforeOpen {
				t.Fatalf("exhausted %s performed I/O puts=%d/%d opens=%d/%d", name, puts, beforePut, opens, beforeOpen)
			}
		})
	}
	b := newBodyBlobs()
	s := retainedStore(t, b, retainedLimits())
	for _, size := range []int{0, 4097} {
		if got, err := s.WriteConsentArguments(t.Context(), bytes.Repeat([]byte("x"), size)); err == nil || got != (domain.BlobRef{}) {
			t.Fatalf("out-of-arm size=%d accepted ref=%+v err=%v", size, got, err)
		}
	}
	if puts, opens, _, _, _ := b.counts(); puts != 0 || opens != 0 {
		t.Fatal("out-of-arm input performed I/O", puts, opens)
	}
	if _, err := s.WriteConsentArguments(t.Context(), bytes.Repeat([]byte("x"), 4096)); err != nil {
		t.Fatal("unchanged 4-KiB argument bound denied", err)
	}
}

func TestConsentArgumentsCopiesUseOriginalIOGate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBodyBlobs()
		release := make(chan struct{})
		b.blockPut, b.entered = release, make(chan struct{}, 1)
		s := newBodyStore(t, b)
		alias := *s
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		t.Cleanup(cancel)
		done := make(chan error, 1)
		go func() { _, err := s.WriteConsentArguments(ctx, []byte(`{"query":"original"}`)); done <- err }()
		select {
		case <-b.entered:
		case err := <-done:
			t.Fatalf("writer never reached original gate/PUT: %v", err)
		case <-ctx.Done():
			<-done
			t.Fatal("writer did not enter bounded original PUT")
		}
		// Release and join before any fatal return; no test-owned work survives.
		joined := false
		releaseAndJoin := func() {
			if !joined {
				joined = true
				close(release)
				if err := <-done; err != nil {
					t.Errorf("original consent writer failed: %v", err)
				}
			}
		}
		t.Cleanup(releaseAndJoin)
		if _, err := alias.WriteConsentArguments(ctx, []byte(`{"query":"loser"}`)); !errors.Is(err, operationbody.ErrBusy) {
			t.Errorf("copied consent holder bypassed gate: %v", err)
		}
		if _, err := alias.Write(ctx, []byte("ordinary loser")); !errors.Is(err, operationbody.ErrBusy) {
			t.Errorf("ordinary holder bypassed consent I/O: %v", err)
		}
		if puts, _, _, _, _ := b.counts(); puts != 1 {
			t.Errorf("concurrent loser PUT count=%d want=1", puts)
		}
		releaseAndJoin()
	})
}

func TestConsentArgumentsShareOriginalTwentyFourSlots(t *testing.T) {
	b := &cleanupBlobs{bodyBlobs: newBodyBlobs()}
	limits := retainedLimits()
	limits.MaxModelCalls = 8
	limits.MaxToolDispatches = 4
	limits.MaxOperations = 12
	s, owner := cleanupOwner(t, b, limits)
	for i := 0; i < 24; i++ {
		if i%2 == 0 {
			if _, err := s.WriteConsentArguments(t.Context(), []byte{byte('a' + i)}); err != nil {
				t.Fatalf("shared consent slot=%d err=%v", i, err)
			}
		} else if _, err := s.Write(t.Context(), []byte{byte('a' + i)}); err != nil {
			t.Fatalf("shared ordinary slot=%d err=%v", i, err)
		}
	}
	if _, err := s.WriteConsentArguments(t.Context(), []byte("overflow")); err == nil {
		t.Error("consent invented a twenty-fifth physical slot")
	}
	if _, err := s.Write(t.Context(), []byte("ordinary overflow")); err == nil {
		t.Error("ordinary invented a twenty-fifth physical slot")
	}
	if puts, _, _, _, _ := b.counts(); puts != 24 {
		t.Fatalf("shared fixed-slot PUT count=%d want=24", puts)
	}
	ctx, _ := cleanupContext(t)
	receipt, err := owner.CloseOperationNamespace(ctx)
	if err != nil || receipt == nil {
		t.Fatalf("full mixed inventory cleanup err=%v", err)
	}
	count, err := receipt.DeletedBodies()
	if err != nil || count != 24 || b.absences != 24 {
		t.Fatalf("full fixed inventory count=%d absence=%d err=%v", count, b.absences, err)
	}
}

func TestConsentArgumentsAmbiguousPutTaintsSameOwner(t *testing.T) {
	b := &cleanupBlobs{bodyBlobs: newBodyBlobs(), fault: "put_absent"}
	s, owner := cleanupOwner(t, b, retainedLimits())
	if ref, err := s.WriteConsentArguments(t.Context(), []byte(`{"query":"unknown"}`)); err == nil || ref != (domain.BlobRef{}) {
		t.Fatalf("unknown PUT exposed usable content ref=%+v err=%v", ref, err)
	}
	if puts, _, _, _, _ := b.counts(); puts != 1 {
		t.Fatalf("causal uncertain PUT count=%d want=1", puts)
	}
	b.fault = ""
	alias := *s
	if _, err := alias.Write(t.Context(), []byte("ordinary retry")); err == nil {
		t.Error("unknown consent PUT allowed ordinary replay")
	}
	if _, err := alias.WriteConsentArguments(t.Context(), []byte(`{"query":"retry"}`)); err == nil {
		t.Error("unknown consent PUT allowed consent replay")
	}
	ctx, _ := cleanupContext(t)
	if r, err := owner.CloseOperationNamespace(ctx); err == nil || r != nil {
		t.Fatalf("unknown PUT turned absence into erasure receipt=%v err=%v", r, err)
	}
	if puts, _, _, deletes, _ := b.counts(); puts != 1 || deletes != 0 || b.absences != 0 {
		t.Fatalf("unknown PUT replay/cleanup effects puts=%d deletes=%d absence=%d", puts, deletes, b.absences)
	}
}

func TestConsentArgumentsMixedOriginalCleanupInventory(t *testing.T) {
	b := &cleanupBlobs{bodyBlobs: newBodyBlobs()}
	s, owner := cleanupOwner(t, b, retainedLimits())
	ordinary, err := s.Write(t.Context(), []byte("ordinary request"))
	if err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	ordinaryKey := b.keys[0]
	b.mu.Unlock()
	body := []byte(`{"query":"public"}`)
	consent, err := s.WriteConsentArguments(t.Context(), body)
	if err != nil {
		t.Fatal("actual mixed writer prerequisite", err)
	}
	want := []domain.BlobRef{{TenantID: bodyScope().TenantID, Key: ordinaryKey, Size: int64(ordinary.Bytes), SHA256: ordinary.Digest}, consent}
	ctx, _ := cleanupContext(t)
	receipt, err := owner.CloseOperationNamespace(ctx)
	if err != nil || receipt == nil {
		t.Fatalf("actual mixed-key original cleanup receipt=%v err=%v", receipt, err)
	}
	if err := owner.VerifyNamespaceErasure(receipt); err != nil {
		t.Fatal("original mixed receipt rejected", err)
	}
	got, err := receipt.Bodies()
	if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(b.deleted, want) || b.absences != 2 || b.deadlineChanged {
		t.Fatalf("mixed exact inventory got=%+v deleted=%+v absence=%d changedDeadline=%t err=%v", got, b.deleted, b.absences, b.deadlineChanged, err)
	}
	got[0].Key = "caller mutation"
	again, err := receipt.Bodies()
	if err != nil || !reflect.DeepEqual(again, want) {
		t.Fatal("mixed inventory aliases caller", err)
	}
	count, _ := receipt.DeletedBodies()
	total, _ := receipt.DeletedBytes()
	if count != 2 || total != ordinary.Bytes+uint64(len(body)) {
		t.Fatalf("mixed physical totals count=%d bytes=%d", count, total)
	}
	if _, err := s.WriteConsentArguments(t.Context(), body); err == nil {
		t.Fatal("mixed cleanup failed to fence original holder")
	}
}

// Faults apply only to the consent arm, after a lawful ordinary object. This
// makes a cleanup test reach the new arm rather than fail on the first object.
type consentFaultBlobs struct {
	*cleanupBlobs
	faultKey, consentFault string
	cancel                 context.CancelFunc
}

func (b *consentFaultBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	r, err := b.cleanupBlobs.Open(ctx, tenant, ref)
	if err != nil || r == nil || b.consentFault == "" ||
		(b.faultKey != "" && b.faultKey != ref.Key) ||
		!strings.Contains(ref.Key, "/consent-arguments-v1/") {
		return r, err
	}
	return &consentFaultReader{ReadCloser: r, ctx: ctx, fault: b.consentFault, cancel: b.cancel}, nil
}

type consentFaultReader struct {
	io.ReadCloser
	ctx       context.Context
	fault     string
	cancel    context.CancelFunc
	corrupted bool
}

func (r *consentFaultReader) Read(p []byte) (int, error) {
	if r.fault == "read" {
		return 0, errors.New("consent-only controlled read failure")
	}
	n, err := r.ReadCloser.Read(p)
	if r.fault == "corrupt" && n > 0 && !r.corrupted {
		p[0] ^= 1
		r.corrupted = true
	}
	if r.fault == "cancel" && r.cancel != nil {
		r.cancel()
	}
	return n, err
}

func (r *consentFaultReader) Close() error {
	if r.fault == "late-close" {
		// Cooperative synchronous Close joins the original canceled call. Its
		// late nil ACK is deliberately not evidence that the write succeeded.
		<-r.ctx.Done()
		_ = r.ReadCloser.Close()
		return nil
	}
	err := r.ReadCloser.Close()
	if r.fault == "close" {
		return errors.New("consent-only controlled Close failure")
	}
	return err
}

func TestConsentArgumentsFailedReadbackNeverReopensOwner(t *testing.T) {
	for _, fault := range []string{"corrupt", "read", "close", "late-close"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := &consentFaultBlobs{cleanupBlobs: &cleanupBlobs{bodyBlobs: newBodyBlobs()}}
				s, owner := cleanupOwner(t, b, retainedLimits())
				if _, err := s.Write(t.Context(), []byte("lawful ordinary")); err != nil {
					t.Fatal(err)
				}
				b.consentFault = fault
				if ref, err := s.WriteConsentArguments(t.Context(), []byte(`{"query":"unknown readback"}`)); err == nil || ref != (domain.BlobRef{}) {
					t.Fatalf("uncertain %s readback exposed ref=%+v err=%v", fault, ref, err)
				}
				puts, opens, _, _, _ := b.counts()
				if puts != 2 || opens != 2 {
					t.Fatalf("causal readback never reached consent puts=%d opens=%d", puts, opens)
				}
				b.consentFault = ""
				if _, err := s.Write(t.Context(), []byte("ordinary retry")); err == nil {
					t.Error("failed consent readback reopened ordinary writes")
				}
				if _, err := s.WriteConsentArguments(t.Context(), []byte(`{"query":"retry"}`)); err == nil {
					t.Error("failed consent readback reopened consent writes")
				}
				ctx, _ := cleanupContext(t)
				if receipt, err := owner.CloseOperationNamespace(ctx); err == nil || receipt != nil {
					t.Errorf("unknown readback became erasure receipt=%v err=%v", receipt, err)
				}
				if p, o, _, d, _ := b.counts(); p != puts || o != opens || d != 0 || b.absences != 0 {
					t.Fatalf("unknown readback retry/effects puts=%d opens=%d deletes=%d absences=%d", p, o, d, b.absences)
				}
			})
		})
	}
}

func TestConsentArgumentsMixedCleanupVerifiesEveryArmBeforeDelete(t *testing.T) {
	for _, fault := range []string{"corrupt", "read", "close", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			b := &consentFaultBlobs{cleanupBlobs: &cleanupBlobs{bodyBlobs: newBodyBlobs()}}
			s, owner := cleanupOwner(t, b, retainedLimits())
			if _, err := s.Write(t.Context(), []byte("first ordinary")); err != nil {
				t.Fatal(err)
			}
			ref, err := s.WriteConsentArguments(t.Context(), []byte(`{"query":"second consent"}`))
			if err != nil {
				t.Fatal("actual consent write prerequisite", err)
			}
			ctx, cancel := cleanupContext(t)
			b.faultKey, b.consentFault, b.cancel = ref.Key, fault, cancel
			_, before, _, _, _ := b.counts()
			if receipt, err := owner.CloseOperationNamespace(ctx); err == nil || receipt != nil {
				t.Fatalf("mixed %s cleanup issued receipt=%v err=%v", fault, receipt, err)
			}
			puts, opens, closes, deletes, _ := b.counts()
			if opens != before+2 || closes != opens || deletes != 0 || b.absences != 0 {
				t.Fatalf("second-arm fault not proved before destruction opens=%d/%d closes=%d deletes=%d absences=%d", opens, before, closes, deletes, b.absences)
			}
			b.consentFault, b.cancel = "", nil
			if receipt, err := owner.CloseOperationNamespace(ctx); err == nil || receipt != nil {
				t.Error("failed mixed cleanup retried")
			}
			if _, err := s.WriteConsentArguments(t.Context(), []byte(`{"query":"late"}`)); err == nil {
				t.Error("failed mixed cleanup did not fence copies")
			}
			if p, o, _, d, _ := b.counts(); p != puts || o != opens || d != deletes {
				t.Fatalf("failed mixed cleanup repeated effects puts=%d opens=%d deletes=%d", p, o, d)
			}
		})
	}
}

func TestConsentArgumentsOrdinaryNamespaceAndInventoryGoldens(t *testing.T) {
	// Frozen predecessor encoding, independently calculated from the fixed
	// scope JSON and original hash domains; not recomputed with candidate code.
	const prefix = "tenants/body-tenant/sessions/body-session/runs/body-run/attempt-operations-v1/05b4e3839fa29f23a2a1ba324ae4321e46674089d5a98c76a9596f42a94677b2/"
	const bodyDigest = "160480cb913c164e3bfeea89feaebe9bde5327639c432f00ba7f7143e843c6f0"
	const namespaceDigest = "56edfde5771aad9ccc9f13687b13e736761c54c309eddd248538c264fe098508"
	const inventoryDigest = "c39800f23362fddec0d3052abbb81e27d08e8d69563b23188166a484d4f21d9a"
	b := &cleanupBlobs{bodyBlobs: newBodyBlobs()}
	s, owner := cleanupOwner(t, b, retainedLimits())
	ref, err := s.Write(t.Context(), []byte("ordinary golden"))
	if err != nil || ref.Ref != "opbody-v1.15."+bodyDigest || ref.Digest != bodyDigest || ref.Bytes != 15 {
		t.Fatalf("legacy ordinary ref=%+v err=%v", ref, err)
	}
	b.mu.Lock()
	key := b.keys[0]
	b.mu.Unlock()
	if key != prefix+ref.Ref {
		t.Fatalf("legacy physical key changed got=%q want=%q", key, prefix+ref.Ref)
	}
	ctx, _ := cleanupContext(t)
	receipt, err := owner.CloseOperationNamespace(ctx)
	if err != nil {
		t.Fatal("legacy cleanup golden", err)
	}
	ns, ne := receipt.NamespaceDigest()
	inv, ie := receipt.InventoryDigest()
	if ne != nil || ie != nil || ns != namespaceDigest || inv != inventoryDigest {
		t.Fatalf("legacy digests changed namespace=%q inventory=%q errors=%v/%v", ns, inv, ne, ie)
	}
}
