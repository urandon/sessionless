package ydbstore_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image/png"
	"io"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersealedinput"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Actual Service -> bearer transport -> Store -> current decision/principal
// source, with test-owned in-memory bodies. No fake authorizer, native YDB,
// qualified catalog, credential, SDK or provider operation is used here.
func piInputJoined(t *testing.T, inputLimit uint64) (ydbstore.PiInputSourceFixtureForTest, *attachedworkersealedinput.Service, *piInputBodies,
	[]byte, attachedworkerdaemontransport.MaterializationRequestV1,
) {
	t.Helper()
	f := ydbstore.NewPiInputSourceFixtureForTest(t, inputLimit)
	secret, err := attachedworkertransport.ParseConnectionSecret(f.Secret)
	if err != nil {
		t.Fatal(err)
	}
	a := f.Authorization
	bearer, err := attachedworkertransport.NewConnectionBearer(a.TenantID, a.OwnerUserID, a.WorkerID, a.ConnectionID, secret)
	if err != nil {
		t.Fatal(err)
	}
	offer := attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []attachedworkerprotocol.ProtocolVersion{1}}
	transport, err := attachedworkertransport.NewService(attachedworkertransport.ServiceConfig{IDs: piInputIDs{},
		Audience: "sessionless:attached-worker:v1", PlatformOffer: offer, ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1},
		ChallengeLifetime: time.Minute, ChallengeRetention: time.Hour, PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour,
		CheckpointInterval: attachedworkertransport.MinimumHeartbeatInterval}, f.Store, f.Store)
	if err != nil {
		t.Fatal(err)
	}
	blobs := &piInputBodies{bodies: f.Bodies}
	service, err := attachedworkersealedinput.NewService(transport, f.Store, blobs)
	if err != nil {
		t.Fatal(err)
	}
	digest := func(s string) []byte {
		t.Helper()
		v, err := hex.DecodeString(s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	r := attachedworkerdaemontransport.MaterializationRequestV1{TenantID: a.TenantID, OwnerUserID: a.OwnerUserID, WorkerID: a.WorkerID,
		ConnectionID: a.ConnectionID, EnrollmentGeneration: a.EnrollmentGeneration, ConnectionGeneration: a.ConnectionGeneration, AttemptSequence: 1,
		Attempt: attachedworkerprotocol.AttemptBindingV1{RunID: string(a.RunID), AttemptID: string(a.AttemptID), LeaseID: string(a.LeaseID),
			LeaseGeneration: a.LeaseGeneration, FenceToken: string(a.FenceToken), ExpiresAtUnixMicro: a.LeaseExpiresAtUnixMicro,
			ContextDigest: digest(string(a.ContextDigest)), CapabilityDigest: digest(string(a.CapabilityDigest)), PolicyDigest: digest(string(a.PolicyDigest))}}
	return f, service, blobs, bearer.Bytes(), r
}

func TestPiAPIInputJoinedCurrentSourcePreservesCanonicalHistoryWithoutCredential(t *testing.T) {
	f, service, blobs, bearer, request := piInputJoined(t, 1<<20)
	before, _ := f.Counts()
	got, err := service.Load(f.Context, bearer, request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		clear(got.Context)
		if got.CanonicalContext != nil {
			got.CanonicalContext.Clear()
		}
	})
	if got.Credential != nil || got.AcceptedAttempt != f.Head || got.CanonicalContext == nil ||
		len(got.CanonicalContext.Attachments) != 2 || blobs.opens != 4 || got.Job.ContextWindow == nil || got.Job.ContextWindow.ThroughSequence != 2 {
		t.Fatal("closed API input lost original head, history/images, or issued a credential")
	}
	records, err := sessioncontext.DecodeJSONL(got.Context, request.TenantID, got.Job.SessionID)
	if err != nil || len(records) != 2 || records[1].Event.ID != "event-2" {
		t.Fatal("full original canonical history was not preserved")
	}
	for i, dimensions := range [][2]int{{3000, 1000}, {64, 32}} {
		event := records[i].Event
		var original struct {
			Attachments []sessioncontext.AttachmentRef `json:"attachments"`
		}
		if err := json.Unmarshal(f.Bodies[event.Payload.Key], &original); err != nil || len(original.Attachments) != 1 {
			t.Fatal("missing original event attachment source")
		}
		want, actual := original.Attachments[0], got.CanonicalContext.Attachments[i]
		if event.Sequence != uint64(i+1) || event.ID != domain.SessionEventID(fmt.Sprintf("event-%d", i+1)) ||
			actual.Name != fmt.Sprintf("%020d-01-%s", event.Sequence, want.Name) || actual.MediaType != want.MediaType ||
			actual.Blob != want.Blob || !bytes.Equal(actual.Body, f.Bodies[want.Blob.Key]) {
			t.Fatal("historical original reference, sequence, name or exact image bytes changed")
		}
		config, err := png.DecodeConfig(bytes.NewReader(actual.Body))
		if err != nil || config.Width != dimensions[0] || config.Height != dimensions[1] {
			t.Fatal("original PNG dimensions were not preserved")
		}
	}
	encoded, err := json.Marshal(got)
	defer clear(encoded)
	if err != nil || len(encoded) > 1<<20 || !attachedworkerdaemontransport.BoundedSerializedCanonicalInputV1(got, 1<<20) {
		t.Fatal("real two-image input does not fit the unchanged original serialized seal")
	}
	after, writes := f.Counts()
	if after != before+3 || writes != 0 {
		t.Fatal("expected pre/post actual source authorization and actual job load, with no writes")
	}
}

func TestPiAPIInputJoinedDeniesBeforeBodiesAndRetiresAfterMidReadRevocation(t *testing.T) {
	for _, mode := range []string{"nil_catalog", "revoked", "api_generation", "membership_version", "session_deletion", "unknown_commit", "revoked_during_read"} {
		t.Run(mode, func(t *testing.T) {
			f, service, blobs, bearer, request := piInputJoined(t, 1<<20)
			if mode == "revoked_during_read" {
				blobs.afterFirstClose = func() { f.Change("revoked") }
			} else {
				f.Change(mode)
			}
			got, err := service.Load(f.Context, bearer, request)
			if err == nil || len(got.Context) != 0 || got.CanonicalContext != nil || got.Credential != nil ||
				len(got.Artifacts) != 0 || got.AcceptedAttempt != (domain.AttachedWorkerAttemptV1{}) || got.Job.RunID != "" {
				t.Fatalf("source denial mode=%s err=%v contextBytes=%d proof=%t artifacts=%d credential=%t headRevision=%d run=%q", mode, err, len(got.Context), got.CanonicalContext != nil, len(got.Artifacts), got.Credential != nil, got.AcceptedAttempt.Revision, got.Job.RunID)
			}
			if mode != "revoked_during_read" && blobs.opens != 0 {
				t.Fatal("stale source read a blob before initial authorization")
			}
			if mode == "revoked_during_read" {
				if !errors.Is(err, attachedworkersealedinput.ErrUnauthorized) || blobs.opens != 4 {
					t.Fatal("mid-read mutation did not reach the actual post-read source gate")
				}
				piInputRequireOwnedViewsCleared(t, blobs)
			}
			_, writes := f.Counts()
			if writes != 0 {
				t.Fatal("input authorization mutated source state")
			}
		})
	}
}

func TestPiAPIInputJoinedOriginalSerializedSealRejectsAndClearsAllContent(t *testing.T) {
	f, service, _, bearer, request := piInputJoined(t, 1<<20)
	control, err := service.Load(f.Context, bearer, request)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(control)
	if err != nil || len(encoded) < 128 {
		t.Fatal("invalid serialized positive control")
	}
	// Leave a gap larger than the decimal limit field's own size change.
	// This bound still fits the original raw bodies, but not their complete
	// proof/history/base64 representation. Recompute the original context/head
	// through the same fixture constructor, never mutate an accepted wire DTO.
	limit := uint64(len(encoded) - 64)
	clear(encoded)
	clear(control.Context)
	control.CanonicalContext.Clear()
	raw := uint64(0)
	for _, body := range f.Bodies {
		raw += uint64(len(body))
	}
	if raw >= limit {
		t.Fatal("negative must isolate serialized overhead, not raw input overflow")
	}
	f, service, blobs, bearer, request := piInputJoined(t, limit)
	got, err := service.Load(f.Context, bearer, request)
	if !errors.Is(err, attachedworkersealedinput.ErrInvalid) || blobs.opens != 4 || len(got.Context) != 0 ||
		got.CanonicalContext != nil || len(got.Artifacts) != 0 || got.Credential != nil || got.Job.RunID != "" ||
		got.AcceptedAttempt != (domain.AttachedWorkerAttemptV1{}) {
		t.Fatal("narrower original serialized seal released content or skipped body acquisition")
	}
	piInputRequireOwnedViewsCleared(t, blobs)
}

func piInputRequireOwnedViewsCleared(t *testing.T, blobs *piInputBodies) {
	t.Helper()
	if len(blobs.readViews) == 0 {
		t.Fatal("retirement test observed no final owned body allocations")
	}
	for _, view := range blobs.readViews {
		for _, b := range view {
			if b != 0 {
				t.Fatal("retired final owned canonical body allocation retained readable bytes")
			}
		}
	}
}

type piInputIDs struct{}

func (piInputIDs) NewID(context.Context, ports.IDKind) (string, error) {
	return "synthetic-unused-input-id", nil
}

type piInputBodies struct {
	bodies          map[string][]byte
	opens           int
	afterFirstClose func()
	readViews       [][]byte
}

func (b *piInputBodies) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	if ctx.Err() != nil || tenant != ref.TenantID {
		return nil, errors.New("foreign synthetic blob scope")
	}
	body, found := b.bodies[ref.Key]
	if !found {
		return nil, errors.New("unknown synthetic blob")
	}
	b.opens++
	return &piInputBody{owner: b, body: body, first: b.opens == 1, observeOwned: len(body) < 512}, nil
}

type piInputBody struct {
	owner *piInputBodies
	body  []byte
	first bool
	// Only small single-read bodies share the final ReadAll allocation. Larger
	// reads may grow/copy Go scratch buffers; this test makes no universal
	// zeroization assertion about those intermediate allocations.
	observeOwned bool
}

func (r *piInputBody) Read(p []byte) (int, error) {
	if len(r.body) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.body)
	r.body = r.body[n:]
	if r.observeOwned {
		r.owner.readViews = append(r.owner.readViews, p[:n])
	}
	return n, nil
}
func (r *piInputBody) Close() error {
	if r.first && r.owner.afterFirstClose != nil {
		f := r.owner.afterFirstClose
		r.owner.afterFirstClose = nil
		f()
	}
	return nil
}
