package operationbody_test

import (
	"reflect"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
)

// Cooperative immutable storage proves original physical ownership and DATA
// copy isolation, not Store retirement or remote-storage consistency.
func TestNamespaceErasureBodiesOriginalImmutableInventory(t *testing.T) {
	b := &cleanupBlobs{bodyBlobs: newBodyBlobs()}
	s, owner := cleanupOwner(t, b, retainedLimits())
	var want []domain.BlobRef
	for _, body := range []string{"original request", "original normalized result"} {
		ref, err := s.Write(t.Context(), []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		b.mu.Lock()
		key := b.keys[len(b.keys)-1]
		b.mu.Unlock()
		want = append(want, domain.BlobRef{TenantID: bodyScope().TenantID, Key: key, SHA256: ref.Digest, Size: int64(ref.Bytes)})
	}
	ctx, _ := cleanupContext(t)
	r, err := owner.CloseOperationNamespace(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.Bodies()
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("exact attempted original inventory", err)
	}
	got[0].Key = "caller mutation"
	got = append(got, domain.BlobRef{})
	again, err := r.Bodies()
	if err != nil || !reflect.DeepEqual(again, want) {
		t.Fatal("getter aliases original inventory", err)
	}
	copyReceipt := *r
	if _, err := copyReceipt.Bodies(); err == nil {
		t.Fatal("copied receipt acquired inventory ownership")
	}
	var absent *operationbody.NamespaceErasureV1
	if _, err := absent.Bodies(); err == nil {
		t.Fatal("nil receipt accepted")
	}
	if _, err := new(operationbody.NamespaceErasureV1).Bodies(); err == nil {
		t.Fatal("zero receipt accepted")
	}
	if err := owner.VerifyNamespaceErasure(r); err != nil {
		t.Fatal("DATA access changed original receipt", err)
	}
}
