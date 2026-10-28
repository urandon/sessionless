package controlapi

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegeneration"
	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/buildinfo"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/harnessconformance"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func TestAttachedWorkerOptionsRealRoutesWithoutTelegram(t *testing.T) {
	t.Parallel()
	// Construction and rejection before authority do not touch a database or
	// object storage. These are real protocol handlers, not healthy substitutes.
	options, err := AttachedWorkerOptions("sessionless:attached-worker:v1", &ydbstore.Store{}, unusedControlBlobs{})
	if err != nil {
		t.Fatalf("compose attached-only routes: %v", err)
	}
	if options.TelegramWebhook != nil {
		t.Fatal("attached-only composition initialized a Telegram handler")
	}
	if options.AttachedResourceGeneration != nil {
		t.Fatal("nil generation registry enabled metadata routes")
	}
	handler := NewHandlerWithOptions(discardLogger(), buildinfo.Current("test"), options)
	for _, tc := range []struct {
		path string
		want int
	}{
		{path: "/attached-worker/v1/challenges", want: http.StatusUnsupportedMediaType},
		{path: "/attached-worker/v1/attach", want: http.StatusUnsupportedMediaType},
		{path: "/attached-worker/v1/exchange", want: http.StatusUnsupportedMediaType},
		{path: "/attached-worker/v1/sealed-input", want: http.StatusBadRequest},
		{path: "/attached-worker/v1/output-receipt", want: http.StatusBadRequest},
		{path: "/telegram/webhook", want: http.StatusNotFound},
		{path: attachedworkerhttp.ResourceGenerationPublishPathV1, want: http.StatusNotFound},
		{path: attachedworkerhttp.ResourceGenerationReadPathV1, want: http.StatusNotFound},
	} {
		t.Run(tc.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, tc.path, nil))
			if recorder.Code != tc.want {
				t.Errorf("POST %s: status=%d, want %d; body=%s", tc.path, recorder.Code, tc.want, recorder.Body.String())
			}
			recorder = httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if recorder.Code != http.StatusNotFound {
				t.Errorf("GET %s: status=%d, want 404", tc.path, recorder.Code)
			}
		})
	}
}

func TestAttachedWorkerOptionsRejectsMissingDependencies(t *testing.T) {
	t.Parallel()
	if _, err := AttachedWorkerOptions("sessionless:attached-worker:v1", nil, unusedControlBlobs{}); err == nil {
		t.Error("nil state accepted")
	}
	if _, err := AttachedWorkerOptions("sessionless:attached-worker:v1", &ydbstore.Store{}, nil); err == nil {
		t.Error("nil blobs accepted")
	}
	if _, err := AttachedWorkerOptions("", &ydbstore.Store{}, unusedControlBlobs{}); err == nil {
		t.Error("empty audience accepted")
	}
}

func TestAttachedResourceGenerationOptionalRouteRegistration(t *testing.T) {
	t.Parallel()
	calls := 0
	options := Options{AttachedResourceGeneration: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls++; w.WriteHeader(http.StatusUnauthorized) })}
	handler := NewHandlerWithOptions(discardLogger(), buildinfo.Current("test"), options)
	for _, path := range []string{attachedworkerhttp.ResourceGenerationPublishPathV1, attachedworkerhttp.ResourceGenerationReadPathV1} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, nil))
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("configured generation route not registered: path=%s status=%d", path, recorder.Code)
		}
		recorder = httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("generation metadata unexpectedly enabled GET: path=%s status=%d", path, recorder.Code)
		}
	}
	if calls != 2 {
		t.Fatalf("optional route calls=%d want2", calls)
	}
}

func controlGenerationRegistry(t *testing.T) *attachedresourcegeneration.Registry {
	t.Helper()
	// Committed credential-free conformance DATA, never operator files or keys.
	raw, err := os.ReadFile("../../test/fixtures/provider-conformance/v1/openrouter-ox-alpha-public.json")
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := harnessconformance.DecodeFixtureV1(raw)
	if err != nil || fixture.EvidenceBundle == nil {
		t.Fatalf("committed evidence fixture: %v", err)
	}
	binding, bundle := fixture.Binding.Clone(), fixture.EvidenceBundle.Clone()
	binding.RunID, binding.AttemptID, binding.ExecutionPlacementDigest = "", "", ""
	binding.Backend.BackendKind, binding.Backend.ArtifactKind = domain.HarnessBackendCodexExecV1, domain.HarnessArtifactExecutableV1
	binding.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
	binding.Resource.Kind = domain.ProviderResourceSubscriptionV1
	for _, scope := range []*domain.ProviderEvidenceScopeV1{&bundle.Catalog.Scope, &bundle.Route.Scope, &bundle.Capability.Scope, &bundle.Privacy.Scope, &bundle.Price.Scope, &bundle.Policy.Scope} {
		scope.Backend, scope.Resource = binding.Backend, binding.Resource
	}
	bundle.Route.State, bundle.Route.PolicyID, bundle.Route.Revision, bundle.Route.Routes = domain.ProviderEvidenceUnknownV1, "", 0, nil
	for _, item := range []struct {
		digest func() (domain.ProviderEvidenceDigestV1, error)
		fields []*string
	}{
		{bundle.Catalog.Digest, []*string{&binding.ProviderCatalogDigest}},
		{bundle.Route.Digest, []*string{&binding.ProviderRouteDigest, &bundle.Policy.RoutePolicyDigest}},
		{bundle.Capability.Digest, []*string{&binding.CapabilityEvidenceDigest, &bundle.Policy.CapabilityEvidenceDigest}},
		{bundle.Privacy.Digest, []*string{&binding.PrivacyPolicyDigest, &bundle.Policy.PrivacyEvidenceDigest}},
		{bundle.Price.Digest, []*string{&bundle.Policy.PriceObservationDigest}},
		{func() (domain.ProviderEvidenceDigestV1, error) { return bundle.Policy.Digest() }, []*string{&binding.EffectivePolicyDigest}},
	} {
		digest, err := item.digest()
		if err != nil {
			t.Fatal(err)
		}
		for _, field := range item.fields {
			*field = string(digest)
		}
	}
	registry, err := attachedresourcegeneration.NewRegistry([]domain.AttachedResourceGenerationBaselineV1{{Version: 1,
		TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, SubscriptionConnectionID: domain.SubscriptionConnectionID(binding.Resource.ResourceID), WorkerID: "worker-control-config",
		EnrollmentGeneration: 1, BaselineRevision: binding.Resource.Revision, CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("a", 64)), GenerationOnlyDelegation: true, Binding: binding, Evidence: bundle}})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

type unusedControlGenerationConnector struct{ calls atomic.Uint32 }

func (c *unusedControlGenerationConnector) Connect(context.Context) (driver.Conn, error) {
	c.calls.Add(1)
	return nil, errors.New("generation route rejection must not connect to a database")
}
func (*unusedControlGenerationConnector) Driver() driver.Driver {
	return unusedControlGenerationDriver{}
}

type unusedControlGenerationDriver struct{}

func (unusedControlGenerationDriver) Open(string) (driver.Conn, error) {
	panic("generation route rejection must not open a database")
}

func TestAttachedWorkerOptionsRealGenerationRoutesFromInstalledRegistry(t *testing.T) {
	t.Parallel()
	connector := new(unusedControlGenerationConnector)
	db := sql.OpenDB(connector)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	state, err := ydbstore.New(db, ydbstore.Options{AttachedResourceGenerations: controlGenerationRegistry(t)})
	if err != nil {
		t.Fatal(err)
	}
	// This calls the real enabled composition branch. A manually substituted
	// Options handler would not catch accidentally removing that branch.
	options, err := AttachedWorkerOptions("sessionless:attached-worker:v1", state, unusedControlBlobs{})
	if err != nil || options.AttachedResourceGeneration == nil {
		t.Fatalf("real enabled generation composition: err=%v handler=%T", err, options.AttachedResourceGeneration)
	}
	handler := NewHandlerWithOptions(discardLogger(), buildinfo.Current("test"), options)
	for _, path := range []string{attachedworkerhttp.ResourceGenerationPublishPathV1, attachedworkerhttp.ResourceGenerationReadPathV1} {
		t.Run(path, func(t *testing.T) {
			for _, tc := range []struct {
				name, method, bearer string
				want                 int
			}{
				{"unauthenticated", http.MethodPost, "", http.StatusUnauthorized},
				{"invalid closed request", http.MethodPost, "Bearer test-owned-synthetic-token", http.StatusBadRequest},
				{"no GET", http.MethodGet, "", http.StatusNotFound},
			} {
				t.Run(tc.name, func(t *testing.T) {
					request := httptest.NewRequest(tc.method, path, strings.NewReader("{}"))
					request.Header.Set("Content-Type", "application/json")
					request.Header.Set("Accept", "application/json")
					if tc.bearer != "" {
						request.Header.Set("Authorization", tc.bearer)
					}
					recorder := httptest.NewRecorder()
					handler.ServeHTTP(recorder, request)
					if recorder.Code != tc.want {
						t.Fatalf("%s %s: status=%d want=%d body=%q", tc.method, path, recorder.Code, tc.want, recorder.Body.String())
					}
					if tc.method == http.MethodPost && recorder.Header().Get("Cache-Control") != "no-store" {
						t.Fatalf("generation rejection cache header=%q", recorder.Header().Get("Cache-Control"))
					}
				})
			}
		})
	}
	if got := connector.calls.Load(); got != 0 {
		t.Fatalf("generation composition/rejection connected to DB %d times", got)
	}
}

func TestAttachedWorkerOptionsRejectsUnauthenticatedContentAndExchange(t *testing.T) {
	t.Parallel()
	options, err := AttachedWorkerOptions("sessionless:attached-worker:v1", &ydbstore.Store{}, unusedControlBlobs{})
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandlerWithOptions(discardLogger(), buildinfo.Current("test"), options)
	for _, path := range []string{"/attached-worker/v1/exchange", "/attached-worker/v1/sealed-input", "/attached-worker/v1/output-receipt"} {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "application/json")
			request.Header.Set("Authorization", "Basic denied")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("unauthenticated POST %s: status=%d, want 401; body=%s", path, recorder.Code, recorder.Body.String())
			}
			if recorder.Header().Get("Cache-Control") != "no-store" {
				t.Errorf("%s: Cache-Control=%q, want no-store", path, recorder.Header().Get("Cache-Control"))
			}
		})
	}
}

func TestAttachedWorkerOptionsTelegramRemainsIndependent(t *testing.T) {
	t.Parallel()
	options, err := AttachedWorkerOptions("sessionless:attached-worker:v1", &ydbstore.Store{}, unusedControlBlobs{})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	options.TelegramWebhook = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent)
	})
	handler := NewHandlerWithOptions(discardLogger(), buildinfo.Current("test"), options)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/telegram/webhook", nil))
	if recorder.Code != http.StatusNoContent || calls != 1 {
		t.Fatalf("optional webhook: status=%d calls=%d, want 204 and one call", recorder.Code, calls)
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/attached-worker/v1/attach", nil))
	if recorder.Code != http.StatusUnsupportedMediaType || calls != 1 {
		t.Fatalf("attached route interfered with webhook: status=%d calls=%d", recorder.Code, calls)
	}
}

type unusedControlBlobs struct{}

func (unusedControlBlobs) Put(context.Context, domain.TenantID, string, io.Reader) (domain.BlobRef, error) {
	panic("unauthenticated route must not write an object")
}
func (unusedControlBlobs) Open(context.Context, domain.TenantID, domain.BlobRef) (io.ReadCloser, error) {
	panic("unauthenticated route must not read an object")
}
func (unusedControlBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	panic("unauthenticated route must not delete an object")
}
