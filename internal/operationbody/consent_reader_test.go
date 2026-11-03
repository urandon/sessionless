package operationbody_test

import (
	"bytes"
	"context"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// Deliberately lacks Put, Delete and inventory methods. A read-only adapter
// must not need the original writer or accept its custody/erasure ownership.
type consentOpenOnly struct{ blobs *bodyBlobs }

func (o consentOpenOnly) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	return o.blobs.Open(ctx, tenant, ref)
}

func TestConsentReaderIndependentOriginalWriteAndFreshBytes(t *testing.T) {
	b := newBodyBlobs()
	writer := newBodyStore(t, b)
	body := []byte(`{"query":"public original query","max_results":3}`)
	ref, err := writer.WriteConsentArguments(t.Context(), body)
	if err != nil {
		t.Fatal("original writer prerequisite", err)
	}
	reader, err := operationbody.NewConsentArgumentReaderV1(consentOpenOnly{b})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		got, err := reader.ReadAttemptOperationBody(t.Context(), bodyScope(), ref.Key, uint64(ref.Size))
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("independent read %d: bytes=%d want=%d err=%v", i, len(got), len(body), err)
		}
		clear(got) // caller changes cannot mutate the original stored body
	}
	puts, opens, closes, deletes, _ := b.counts()
	if puts != 1 || opens != 3 || closes != opens || deletes != 0 {
		t.Fatalf("reader created custody effects: puts=%d opens=%d closes=%d deletes=%d", puts, opens, closes, deletes)
	}
}

func TestConsentReaderClosedScopeKeyAndCapBeforeOpen(t *testing.T) {
	for _, name := range []string{"ordinary", "grant", "arbitrary", "alias", "traversal", "foreign-tenant", "foreign-attempt", "foreign-claim", "uppercase", "zero-cap", "oversized-cap", "undersized-cap"} {
		t.Run(name, func(t *testing.T) {
			b := newBodyBlobs()
			writer := newBodyStore(t, b)
			body := []byte(`{"query":"public"}`)
			ref, err := writer.WriteConsentArguments(t.Context(), body)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := operationbody.NewConsentArgumentReaderV1(consentOpenOnly{b})
			if err != nil {
				t.Fatal(err)
			}
			scope, key, cap := bodyScope(), ref.Key, uint64(ref.Size)
			switch name {
			case "ordinary":
				key = "opbody-v1.1." + strings.Repeat("a", 64)
			case "grant":
				key = "owned-fixture-grant-" + strings.Repeat("a", 64)
			case "arbitrary":
				key = "arbitrary/private.json"
			case "alias":
				key += "?copy=true"
			case "traversal":
				key = strings.Replace(key, "/attempts/", "/../attempts/", 1)
			case "foreign-tenant":
				scope.TenantID = "foreign-tenant"
			case "foreign-attempt":
				scope.AttemptID = "foreign-attempt"
			case "foreign-claim":
				scope.PhysicalInvocationClaimID = "foreign-claim"
			case "uppercase":
				key = strings.ToUpper(key)
			case "zero-cap":
				cap = 0
			case "oversized-cap":
				cap = 4097
			case "undersized-cap":
				cap--
			}
			_, before, _, _, _ := b.counts()
			got, err := reader.ReadAttemptOperationBody(t.Context(), scope, key, cap)
			puts, opens, _, deletes, _ := b.counts()
			if err == nil || got != nil || opens != before || puts != 1 || deletes != 0 {
				t.Fatalf("invalid selector reached storage: bytes=%d err=%v opens=%d/%d puts=%d deletes=%d", len(got), err, opens, before, puts, deletes)
			}
		})
	}
}

func TestConsentReaderIntegrityAndStorageFailuresDeny(t *testing.T) {
	for _, fault := range []string{"missing", "open", "read", "corrupt", "oversize", "close"} {
		t.Run(fault, func(t *testing.T) {
			b := newBodyBlobs()
			writer := newBodyStore(t, b)
			ref, err := writer.WriteConsentArguments(t.Context(), []byte(`{"query":"public"}`))
			if err != nil {
				t.Fatal(err)
			}
			reader, err := operationbody.NewConsentArgumentReaderV1(consentOpenOnly{b})
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "missing":
				delete(b.objects, ref.Key)
			case "close":
				b.closeFault = true
			default:
				b.openFault = fault
			}
			got, err := reader.ReadAttemptOperationBody(t.Context(), bodyScope(), ref.Key, uint64(ref.Size))
			if err == nil || got != nil {
				t.Fatalf("unverified body escaped: bytes=%d err=%v", len(got), err)
			}
			puts, _, _, deletes, read := b.counts()
			if puts != 1 || deletes != 0 || read > int(ref.Size)*2+1 {
				t.Fatalf("failure repaired or overread: puts=%d deletes=%d bytes=%d", puts, deletes, read)
			}
		})
	}
}

func TestConsentReaderOriginalCleanupRemainsOriginalOwners(t *testing.T) {
	b := &cleanupBlobs{bodyBlobs: newBodyBlobs()}
	writer, owner := cleanupOwner(t, b, retainedLimits())
	ref, err := writer.WriteConsentArguments(t.Context(), []byte(`{"query":"public"}`))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := operationbody.NewConsentArgumentReaderV1(b)
	if err != nil {
		t.Fatal(err)
	}
	ctx, _ := cleanupContext(t)
	erasure, err := owner.CloseOperationNamespace(ctx)
	if err != nil || erasure == nil || owner.VerifyNamespaceErasure(erasure) != nil {
		t.Fatal("original cleanup must still own its genuine erasure", err)
	}
	if got, err := reader.ReadAttemptOperationBody(t.Context(), bodyScope(), ref.Key, uint64(ref.Size)); err == nil || got != nil {
		t.Fatal("independent reader recreated an erased body", err)
	}
	count, _ := erasure.DeletedBodies()
	if count != 1 {
		t.Fatalf("reader changed original inventory: got=%d want=1", count)
	}
}

func TestConsentReaderRejectsAbsentDependenciesAndCancelledContext(t *testing.T) {
	var typedNil *bodyBlobs
	if got, err := operationbody.NewConsentArgumentReaderV1(typedNil); err == nil || got != nil {
		t.Fatal("typed nil dependency accepted")
	}
	if got, err := operationbody.NewConsentArgumentReaderV1(nil); err == nil || got != nil {
		t.Fatal("nil dependency accepted")
	}
	b := newBodyBlobs()
	reader, err := operationbody.NewConsentArgumentReaderV1(consentOpenOnly{b})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ref := consentBodyRef(t, []byte(`{"query":"public"}`))
	for _, call := range []context.Context{nil, ctx} {
		if got, err := reader.ReadAttemptOperationBody(call, bodyScope(), ref.Key, uint64(ref.Size)); err == nil || got != nil {
			t.Fatal("absent/cancelled context read", err)
		}
	}
	_, opens, _, _, _ := b.counts()
	if opens != 0 {
		t.Fatal("cancelled reader touched storage")
	}
}

type consentGateOpener struct {
	opener interface {
		Open(context.Context, domain.TenantID, domain.BlobRef) (io.ReadCloser, error)
	}
	entered chan struct{}
	release <-chan struct{}
}

func (o consentGateOpener) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	close(o.entered)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-o.release:
		return o.opener.Open(ctx, tenant, ref)
	}
}

func TestConsentReaderCancellationDuringOpenJoinsWithoutEffects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBodyBlobs()
		writer := newBodyStore(t, b)
		ref, err := writer.WriteConsentArguments(t.Context(), []byte(`{"query":"public"}`))
		if err != nil {
			t.Fatal(err)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		reader, err := operationbody.NewConsentArgumentReaderV1(consentGateOpener{b, entered, release})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			body, err := reader.ReadAttemptOperationBody(ctx, bodyScope(), ref.Key, uint64(ref.Size))
			if body != nil {
				t.Error("cancelled reader released bytes")
			}
			done <- err
		}()
		<-entered
		cancel()
		synctest.Wait()
		if err := <-done; err == nil {
			t.Fatal("cancelled cooperative Open succeeded")
		}
		puts, opens, closes, deletes, _ := b.counts()
		if puts != 1 || opens != 1 || closes != 1 || deletes != 0 {
			t.Fatalf("cancelled DATA read changed original custody: %d/%d/%d/%d", puts, opens, closes, deletes)
		}
	})
}

func TestConsentReaderCannotDelayOriginalCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := &cleanupBlobs{bodyBlobs: newBodyBlobs()}
		writer, owner := cleanupOwner(t, b, retainedLimits())
		ref, err := writer.WriteConsentArguments(t.Context(), []byte(`{"query":"public"}`))
		if err != nil {
			t.Fatal(err)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		reader, err := operationbody.NewConsentArgumentReaderV1(consentGateOpener{b, entered, release})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			body, err := reader.ReadAttemptOperationBody(t.Context(), bodyScope(), ref.Key, uint64(ref.Size))
			if body != nil {
				t.Error("read recreated bytes removed by original cleanup")
			}
			done <- err
		}()
		<-entered
		ctx, _ := cleanupContext(t)
		erasure, err := owner.CloseOperationNamespace(ctx)
		if err != nil || erasure == nil || owner.VerifyNamespaceErasure(erasure) != nil {
			t.Fatal("independent BFF read delayed original cleanup", err)
		}
		close(release)
		synctest.Wait()
		if err := <-done; err == nil {
			t.Fatal("independent read accepted an erased object")
		}
	})
}

func TestConsentReaderExactFourKiBCapAndShorterDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBodyBlobs()
		limits := bodyLimits()
		limits.MaxAggregateBodyBytes, limits.MaxToolArgumentBytes = 1<<20, 4<<10
		writer, err := operationbody.NewV1(bodyScope(), domain.AgentLoopOwnedPublicV1, limits, b)
		if err != nil {
			t.Fatal(err)
		}
		body := []byte(`{"query":"public"}`)
		body = append(body, bytes.Repeat([]byte(" "), 4096-len(body))...)
		ref, err := writer.WriteConsentArguments(t.Context(), body)
		if err != nil {
			t.Fatal(err)
		}
		reader, err := operationbody.NewConsentArgumentReaderV1(consentOpenOnly{b})
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(3 * time.Second)
		ctx, cancel := context.WithDeadline(t.Context(), deadline)
		defer cancel()
		got, err := reader.ReadAttemptOperationBody(ctx, bodyScope(), ref.Key, 4096)
		if err != nil || !bytes.Equal(got, body) || !b.deadlines[len(b.deadlines)-1].Equal(deadline) {
			t.Fatalf("exact maximum/shorter context changed: bytes=%d err=%v", len(got), err)
		}
	})
}
