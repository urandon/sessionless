package ydbstore_test

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

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localloopback"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Fake STORAGE only: no body-adapter, issuer, publication, Store/current gate or
// connector approval is substituted. Deliberate stored-byte corruption remains
// readable here so the real production adapter must catch its raw SHA mismatch.
type connectedOperationBlobs struct {
	mu                           sync.Mutex
	tenant                       domain.TenantID
	prefix                       string
	objects                      map[string][]byte
	puts, opens, closes, deletes int
	key                          string
}

func newConnectedOperationBlobs(scope domain.AttemptExecutionSourceScopeV1) *connectedOperationBlobs {
	return &connectedOperationBlobs{tenant: scope.TenantID, prefix: domain.SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) + "attempt-operations-v1/", objects: make(map[string][]byte)}
}
func (b *connectedOperationBlobs) validKey(key string, size int64, digest string) bool {
	if !strings.HasPrefix(key, b.prefix) {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(key, b.prefix), "/")
	if len(parts) != 2 || len(parts[0]) != 64 || parts[0] != strings.ToLower(parts[0]) {
		return false
	}
	if _, err := hex.DecodeString(parts[0]); err != nil {
		return false
	}
	return parts[1] == "opbody-v1."+strconv.FormatInt(size, 10)+"."+digest
}
func (b *connectedOperationBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	if ctx == nil || ctx.Err() != nil {
		return domain.BlobRef{}, errors.New("owned blob caller cancelled")
	}
	if _, ok := ctx.Deadline(); !ok {
		return domain.BlobRef{}, errors.New("owned body write missing bounded deadline")
	}
	raw, err := io.ReadAll(io.LimitReader(body, (1<<20)+1))
	if err != nil || len(raw) == 0 || len(raw) > 1<<20 {
		return domain.BlobRef{}, errors.New("owned body write overflow/read failure")
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	if tenant != b.tenant || !b.validKey(key, int64(len(raw)), digest) {
		return domain.BlobRef{}, errors.New("owned body write crossed exact namespace/ref")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.puts++
	if old, found := b.objects[key]; found && !bytes.Equal(old, raw) {
		return domain.BlobRef{}, errors.New("owned immutable fake storage replacement")
	}
	b.objects[key] = bytes.Clone(raw)
	b.key = key
	return domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(raw)), SHA256: digest}, nil
}
func (b *connectedOperationBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	if ctx == nil || ctx.Err() != nil || tenant != b.tenant || ref.TenantID != tenant || ref.Validate() != nil || ref.Size > 1<<20 || !b.validKey(ref.Key, ref.Size, ref.SHA256) {
		return nil, errors.New("owned body read crossed bounded original namespace/ref")
	}
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("owned body read missing deadline")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.opens++
	raw, found := b.objects[ref.Key]
	if !found {
		return nil, errors.New("owned body missing")
	}
	return &connectedOperationReader{ctx: ctx, owner: b, Reader: bytes.NewReader(bytes.Clone(raw))}, nil
}
func (b *connectedOperationBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deletes++
	return errors.New("unexpected automatic operation body deletion")
}
func (b *connectedOperationBlobs) counts() (int, int, int, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.puts, b.opens, b.closes, b.deletes
}
func (b *connectedOperationBlobs) corruptOriginal(t *testing.T) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	raw, found := b.objects[b.key]
	if !found || len(raw) == 0 {
		t.Fatal("actual stored original request missing")
	}
	raw = bytes.Clone(raw)
	raw[0] ^= 1
	b.objects[b.key] = raw
}

type connectedOperationReader struct {
	*bytes.Reader
	ctx    context.Context
	owner  *connectedOperationBlobs
	closed bool
}

func (r *connectedOperationReader) Read(p []byte) (int, error) {
	if r.ctx.Err() != nil {
		return 0, r.ctx.Err()
	}
	return r.Reader.Read(p)
}
func (r *connectedOperationReader) Close() error {
	if !r.closed {
		r.closed = true
		r.owner.mu.Lock()
		r.owner.closes++
		r.owner.mu.Unlock()
	}
	return nil
}

func prepareConnectedOperationBody(t *testing.T, p *localLoopbackPeer) (*ydbstore.LocalLoopbackConnectedFixture, *localloopback.KeeperV1, *connectedOperationBlobs) {
	t.Helper()
	f := ydbstore.NewLocalLoopbackConnectedFixture(t, p.endpoint())
	p.setResponse(f.Response(t))
	keeper, err := localloopback.NewKeeperV1(f.Source, f.OperationIssuer, f.Bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := keeper.Close(); err != nil {
			t.Error(err)
		}
	})
	blobs := newConnectedOperationBlobs(f.ExecutionSource.Scope)
	f.CompleteWithBlobStore(t, keeper, blobs)
	if puts, opens, closes, deletes := blobs.counts(); puts != 1 || opens != 1 || closes != 1 || deletes != 0 {
		t.Fatalf("real adapter must perform one request PUT and verified readback: put=%d open=%d close=%d delete=%d", puts, opens, closes, deletes)
	}
	if !strings.HasPrefix(f.Operation.RequestRef, "opbody-v1.") {
		t.Fatal("original immutable operation retained fixture body reference")
	}
	return f, keeper, blobs
}

// Real body adapter joins actual Source/Store/issuers/keeper/HTTP peer. SQL,
// effect-source verifier and external funding remain explicitly SCRIPTED; no
// model, native YDB, observation persistence or complete provider loop is proved.
func TestLocalOperationBodyConnectedRealStoreOneSend(t *testing.T) {
	p := newLocalLoopbackPeer(t, true)
	f, keeper, blobs := prepareConnectedOperationBody(t, p)
	conn, err := keeper.Preflight(f.Context, f.Allocation, f.ExecutionSource, f.Operation, f.Payload())
	if err != nil {
		t.Fatalf("genuine original preflight with stored request: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	p.waitAccepted(t)
	proof, err := conn.Publication()
	if err != nil {
		t.Fatal(err)
	}
	registered, capability := f.RegisterAndReserve(t, proof)
	puts, beforeSendOpens, closes, deletes := blobs.counts()
	if puts != 1 || beforeSendOpens < 2 || closes != beforeSendOpens || deletes != 0 {
		t.Fatal("actual Store reserve did not re-read production body custody")
	}
	result, err := conn.Invoke(f.Context, registered, capability, f.Payload())
	if err != nil || !result.SendAttempted {
		t.Fatalf("real Store before-send with production body adapter: err=%v attempted=%t", err, result.SendAttempted)
	}
	f.AssertOriginalCapabilityConsumed(t, capability)
	decoded, err := f.Frozen.DecodeResponse(result.Body)
	if err != nil || decoded.Stop != configuredinference.StopFinalV1 || decoded.Text != "owned peer result" || decoded.ModelProvenance != configuredinference.ModelReportedV1 {
		t.Fatalf("stored request codec→wire→codec: err=%v stop=%s", err, decoded.Stop)
	}
	usage, err := decoded.Usage.OperationUsage()
	if err != nil || usage.State != domain.AttemptOperationUsageProviderReportedV1 || usage.AmountMicrounits != nil {
		t.Fatal("actual decoded report became a measured bill")
	}
	peer := p.waitResult(t)
	if peer.err != nil || peer.request == nil || !bytes.Equal(peer.body, f.Payload()) || len(peer.extra) != 0 {
		t.Fatalf("actual stored request was not the sole HTTP body: bytes=%d extra=%d err=%v", len(peer.body), len(peer.extra), peer.err)
	}
	_, authorization := peer.request.Header["Authorization"]
	if authorization || peer.request.Method != "POST" || peer.request.RequestURI != "/v1/chat/completions" || peer.request.Host != p.listener.Addr().String() {
		t.Fatal("real Store/adapter path changed fixed no-auth route")
	}
	puts, afterSendOpens, closes, deletes := blobs.counts()
	if puts != 1 || afterSendOpens <= beforeSendOpens || closes != afterSendOpens || deletes != 0 {
		t.Fatal("before-send skipped actual current body re-read or rewrote/deleted it")
	}
	if again, err := conn.Invoke(f.Context, registered, capability, f.Payload()); err == nil || again.SendAttempted {
		t.Fatal("original capability/socket repeated physical effect")
	}
	f.AssertReplayNoNewCapability(t)
	if puts, _, _, deletes := blobs.counts(); puts != 1 || deletes != 0 {
		t.Fatal("canonical reservation recovery caused another body PUT/Delete")
	}
}

func TestLocalOperationBodyConnectedCorruptRequestAfterReserveZeroSend(t *testing.T) {
	p := newLocalLoopbackPeer(t, false)
	f, keeper, blobs := prepareConnectedOperationBody(t, p)
	conn, err := keeper.Preflight(f.Context, f.Allocation, f.ExecutionSource, f.Operation, f.Payload())
	if err != nil {
		t.Fatalf("corruption control needs actual original preflight: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	p.waitAccepted(t)
	proof, err := conn.Publication()
	if err != nil {
		t.Fatal(err)
	}
	registered, capability := f.RegisterAndReserve(t, proof)
	if err := capability.Validate(); err != nil {
		t.Fatal("corruption control original capability malformed", err)
	}
	_, beforeSendOpens, _, _ := blobs.counts()
	blobs.corruptOriginal(t)
	result, err := conn.Invoke(f.Context, registered, capability, f.Payload())
	if !errors.Is(err, domain.ErrMembershipDenied) || result.SendAttempted || result.RequestBytes != 0 || len(result.Body) != 0 {
		t.Fatalf("corrupt actual stored request bypassed real Store gate: err=%v attempted=%t bytes=%d", err, result.SendAttempted, result.RequestBytes)
	}
	peer := p.waitResult(t)
	if peer.err != nil || len(peer.body) != 0 {
		t.Fatalf("post-reserve corruption emitted HTTP before EOF: bytes=%d err=%v", len(peer.body), peer.err)
	}
	puts, opens, closes, deletes := blobs.counts()
	if puts != 1 || opens <= beforeSendOpens || closes != opens || deletes != 0 {
		t.Fatalf("corrupt read skipped verification/close or retried write: put=%d open=%d close=%d delete=%d", puts, opens, closes, deletes)
	}
	if again, err := conn.Invoke(f.Context, registered, capability, f.Payload()); err == nil || again.SendAttempted {
		t.Fatal("corruption denial reopened original socket")
	}
	f.AssertSingleOriginalWrites(t)
	f.AssertOriginalCapabilityUntouchedThenBurn(t, capability)
}
