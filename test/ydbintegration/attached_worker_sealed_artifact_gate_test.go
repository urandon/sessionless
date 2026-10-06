//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersealedinput"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

// Both owners have a claimed attempt in one tenant and share a worker locator.
// A's artifact read is held across revocation; B's HTTPS read must complete,
// while A's post-read YDB authority check must discard the bytes it already saw.
func TestAW07TwoOwnerYDBSealedArtifactRevocationKeepsPeerReadable(t *testing.T) {
	aStore, aDB, aWorker, aConnection, aDigest, _, _, aNow := readyAttachedWorkerForDrain(t, "aw07-artifact-a")
	bStore, bDB, bWorker, bConnection, bDigest, _, _, bNow := readyAttachedWorkerForDrainWithIdentity(t,
		"aw07-artifact-b", aWorker.TenantID, aWorker.ID)
	aContext, aArtifact := []byte("owner A canonical context"), []byte("owner A private artifact")
	bContext, bArtifact := []byte("owner B canonical context"), []byte("owner B private artifact")
	a := aw07ClaimedInputWithPayload(t, aStore, aDB, aWorker, aConnection, aDigest, aNow,
		attachedWorkerDrainTestSuffix(t, "aw07-artifact-claim-a"), aContext, aArtifact)
	b := aw07ClaimedInputWithPayload(t, bStore, bDB, bWorker, bConnection, bDigest, bNow,
		attachedWorkerDrainTestSuffix(t, "aw07-artifact-claim-b"), bContext, bArtifact)
	if a.request.TenantID != b.request.TenantID || a.request.OwnerUserID == b.request.OwnerUserID ||
		a.request.WorkerID != b.request.WorkerID {
		t.Fatal("artifact gate requires distinct owners in one tenant with a colliding worker locator")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	aJob, found, err := aStore.LoadWorkerJob(ctx, a.request.TenantID, a.request.RunID)
	if err != nil || !found || len(aJob.InputManifest.Artifacts) != 1 {
		t.Fatalf("load A's YDB job and artifact: found=%t err=%v", found, err)
	}
	bJob, found, err := bStore.LoadWorkerJob(ctx, b.request.TenantID, b.request.RunID)
	if err != nil || !found || len(bJob.InputManifest.Artifacts) != 1 {
		t.Fatalf("load B's YDB job and artifact: found=%t err=%v", found, err)
	}
	blobs := &aw07ArtifactBlobs{
		objects: map[string][]byte{
			aJob.Job.ContextSnapshot.Key:             aContext,
			aJob.InputManifest.Artifacts[0].Blob.Key: aArtifact,
			bJob.Job.ContextSnapshot.Key:             bContext,
			bJob.InputManifest.Artifacts[0].Blob.Key: bArtifact,
		},
		opens: make(map[string]int), blockedKey: aJob.InputManifest.Artifacts[0].Blob.Key,
		blocked: make(chan struct{}), release: make(chan struct{}),
	}
	release := func() { blobs.releaseOnce.Do(func() { close(blobs.release) }) }
	transport, err := attachedworkertransport.NewService(attachedworkertransport.ServiceConfig{
		IDs: testkit.NewSequenceIDGenerator("aw07-artifact-"), Audience: "sessionless:attached-worker:v1",
		PlatformOffer: attachedworkerprotocol.VersionOfferV1{
			Window:    attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
			Supported: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		},
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		ChallengeLifetime:   5 * time.Minute, ChallengeRetention: time.Hour,
		PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour,
		CheckpointInterval: attachedworkertransport.MinimumHeartbeatInterval,
	}, aStore, aStore)
	if err != nil {
		t.Fatalf("construct YDB-backed bearer authorizer: %v", err)
	}
	sealed, err := attachedworkersealedinput.NewService(transport, aStore, blobs)
	if err != nil {
		t.Fatalf("construct sealed-input service: %v", err)
	}
	server := httptest.NewTLSServer(attachedworkersealedinput.Handler(sealed))
	t.Cleanup(func() {
		release()
		server.Close()
	})
	newSource := func(bearer []byte) *attachedworkersealedinput.ClientSource {
		t.Helper()
		source, sourceErr := attachedworkersealedinput.NewClientSource(
			server.URL+attachedworkersealedinput.PathV1, server.Client(), bearer)
		if sourceErr != nil {
			t.Fatalf("construct HTTPS sealed-input source: %v", sourceErr)
		}
		t.Cleanup(func() { _ = source.Close() })
		return source
	}
	aSource := newSource(aw07JoinedBearer(t, aWorker, a.request.ConnectionID, aDigest))
	bSource := newSource(aw07JoinedBearer(t, bWorker, b.request.ConnectionID, bDigest))
	aRequest := aw07ArtifactRequest(a)
	bRequest := aw07ArtifactRequest(b)
	for _, foreign := range []struct {
		name    string
		source  *attachedworkersealedinput.ClientSource
		request attachedworkerdaemontransport.MaterializationRequestV1
	}{
		{name: "a_borrows_b", source: aSource, request: bRequest},
		{name: "b_borrows_a", source: bSource, request: aRequest},
	} {
		input, loadErr := foreign.source.Load(ctx, foreign.request)
		if !errors.Is(loadErr, attachedworkersealedinput.ErrUnavailable) || len(input.Context) != 0 || len(input.Artifacts) != 0 {
			t.Fatalf("%s: cross-owner HTTPS read returned input=%+v err=%v", foreign.name, input, loadErr)
		}
	}
	if got := blobs.totalOpens(); got != 0 {
		t.Fatalf("cross-owner bearer reached object storage: opens=%d", got)
	}

	aResult := make(chan struct {
		input attachedworkerdaemontransport.SealedInputV1
		err   error
	}, 1)
	go func() {
		input, loadErr := aSource.Load(ctx, aRequest)
		aResult <- struct {
			input attachedworkerdaemontransport.SealedInputV1
			err   error
		}{input, loadErr}
	}()
	select {
	case <-blobs.blocked:
	case <-ctx.Done():
		t.Fatalf("A's artifact read did not reach held EOF: %v", ctx.Err())
	}
	bInput, err := bSource.Load(ctx, bRequest)
	if err != nil || !bytes.Equal(bInput.Context, bContext) || len(bInput.Artifacts) != 1 ||
		!bytes.Equal(bInput.Artifacts[0].Body, bArtifact) {
		t.Fatalf("B's own artifact stalled or changed while A was held: err=%v input=%+v", err, bInput)
	}
	select {
	case result := <-aResult:
		t.Fatalf("A completed before revocation and held artifact release: err=%v", result.err)
	default:
	}
	aw07Revoke(t, aStore, ctx, aWorker)
	release()
	select {
	case result := <-aResult:
		if !errors.Is(result.err, attachedworkersealedinput.ErrUnavailable) ||
			len(result.input.Context) != 0 || len(result.input.Artifacts) != 0 {
			t.Fatalf("revoked A received held context or artifact: err=%v input=%+v", result.err, result.input)
		}
	case <-ctx.Done():
		t.Fatalf("revoked A did not fail closed after artifact release: %v", ctx.Err())
	}
	if got := blobs.totalOpens(); got != 4 {
		t.Fatalf("object reads=%d, want only A/B context and own artifact once", got)
	}
	if result, err := bSource.Load(ctx, bRequest); err != nil || !bytes.Equal(result.Context, bContext) ||
		len(result.Artifacts) != 1 || !bytes.Equal(result.Artifacts[0].Body, bArtifact) {
		t.Fatalf("A revocation changed B's durable artifact authority: err=%v input=%+v", err, result)
	}
}

func aw07ArtifactRequest(owner aw07ClaimedAuthorization) attachedworkerdaemontransport.MaterializationRequestV1 {
	return attachedworkerdaemontransport.MaterializationRequestV1{
		TenantID: owner.request.TenantID, OwnerUserID: owner.request.OwnerUserID,
		WorkerID: owner.request.WorkerID, ConnectionID: owner.request.ConnectionID,
		EnrollmentGeneration: owner.request.EnrollmentGeneration,
		ConnectionGeneration: owner.request.ConnectionGeneration,
		AttemptSequence:      1, Attempt: owner.binding,
	}
}

type aw07ArtifactBlobs struct {
	mu          sync.Mutex
	objects     map[string][]byte
	opens       map[string]int
	blockedKey  string
	blocked     chan struct{}
	release     chan struct{}
	blockedOnce sync.Once
	releaseOnce sync.Once
}

func (blobs *aw07ArtifactBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	if tenant != ref.TenantID {
		return nil, errors.New("foreign tenant blob request")
	}
	blobs.mu.Lock()
	body, found := blobs.objects[ref.Key]
	if found {
		blobs.opens[ref.Key]++
	}
	blobs.mu.Unlock()
	if !found {
		return nil, errors.New("unknown blob request")
	}
	if ref.Key == blobs.blockedKey {
		return io.NopCloser(&aw07HeldEOFReader{body: body, ctx: ctx, blobs: blobs}), nil
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

func (blobs *aw07ArtifactBlobs) totalOpens() int {
	blobs.mu.Lock()
	defer blobs.mu.Unlock()
	total := 0
	for _, count := range blobs.opens {
		total += count
	}
	return total
}

type aw07HeldEOFReader struct {
	body   []byte
	ctx    context.Context
	blobs  *aw07ArtifactBlobs
	offset int
}

func (reader *aw07HeldEOFReader) Read(p []byte) (int, error) {
	if reader.offset < len(reader.body) {
		n := copy(p, reader.body[reader.offset:])
		reader.offset += n
		return n, nil
	}
	reader.blobs.blockedOnce.Do(func() { close(reader.blobs.blocked) })
	select {
	case <-reader.blobs.release:
		return 0, io.EOF
	case <-reader.ctx.Done():
		return 0, reader.ctx.Err()
	}
}
