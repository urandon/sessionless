package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
)

type custodyClock struct{ at time.Time }

func (c custodyClock) Now() time.Time { return c.at }

type custodyIDs struct{ n uint64 }

func (c *custodyIDs) NewID(_ context.Context, k ports.IDKind) (string, error) {
	c.n++
	return fmt.Sprintf("custody-%s-%d", k, c.n), nil
}

type custodyAuthorizer struct{}

func (custodyAuthorizer) AuthorizeProviderCredentialMutation(_ context.Context, p ports.ProviderCredentialPrincipalV1, _ ports.ProviderCredentialMutationOperationV1) (bool, error) {
	return p.Validate() == nil, nil
}

type custodyRevoker struct{}

func (custodyRevoker) FenceProviderCredentialInvocations(context.Context, ports.ProviderCredentialLocatorV1, uint64) error {
	return nil
}

type custodyPlanner struct{}

func (custodyPlanner) PlanProviderCredentialDelivery(context.Context, domain.HarnessBindingV2) (ports.ProviderCredentialDeliveryTemplateV1, error) {
	return ports.ProviderCredentialDeliveryTemplateV1{Kind: domain.ProviderCredentialDeliveryDirectV1}, nil
}

type custodyBindings struct {
	mu          sync.Mutex
	values      map[ports.ProviderCredentialLocatorV1]domain.ProviderCredentialBindingV1
	cleanups    []ports.ProviderCredentialCleanupV1
	fences      map[string]bool
	loadErr     bool
	loadForeign bool
	casUnknown  bool
	acks        int
	casApplied  chan uint64
}

func newCustodyBindings() *custodyBindings {
	return &custodyBindings{values: map[ports.ProviderCredentialLocatorV1]domain.ProviderCredentialBindingV1{}, fences: map[string]bool{}}
}
func (b *custodyBindings) LoadProviderCredential(_ context.Context, l ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.loadErr {
		return domain.ProviderCredentialBindingV1{}, false, errors.New("SYNTHETIC-PRIVATE-ERROR")
	}
	v, ok := b.values[l]
	if b.loadForeign {
		v.OwnerUserID = "foreign-owner"
	}
	return v, ok, nil
}
func custodyAudit(v domain.ProviderCredentialBindingV1) (domain.ProviderCredentialAuditReceiptIDV1, error) {
	action := domain.ProviderCredentialAuditRotatedV1
	if v.ResourceRevision == 1 {
		action = domain.ProviderCredentialAuditIngestedV1
	}
	a, err := domain.NewProviderCredentialAuditEventV1(v, action)
	return a.ReceiptID, err
}
func (b *custodyBindings) CompareAndSwapProviderCredential(_ context.Context, revision uint64, next domain.ProviderCredentialBindingV1) (ports.ProviderCredentialSwapV1, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	l := providerBindingLocator(next)
	old, found := b.values[l]
	if found && old == next {
		a, err := custodyAudit(next)
		return ports.ProviderCredentialSwapV1{Applied: true, Found: true, Binding: next, AuditReceiptID: a}, err
	}
	if b.fences[next.CandidateMutationID] || found && old.ResourceRevision != revision || !found && revision != 0 {
		return ports.ProviderCredentialSwapV1{}, nil
	}
	if next.Validate() != nil {
		return ports.ProviderCredentialSwapV1{}, errors.New("invalid synthetic CAS")
	}
	if found && old.State == domain.ProviderCredentialActiveV1 && old.SecretRef != next.SecretRef {
		b.cleanups = append(b.cleanups, ports.ProviderCredentialCleanupV1{Locator: l, CredentialGeneration: old.CredentialGeneration, Reference: old.SecretRef})
	}
	b.values[l] = next
	if b.casApplied != nil {
		b.casApplied <- next.CredentialGeneration
	}
	a, err := custodyAudit(next)
	if b.casUnknown {
		b.casUnknown = false
		return ports.ProviderCredentialSwapV1{}, errors.New("SYNTHETIC-PRIVATE-CAS")
	}
	return ports.ProviderCredentialSwapV1{Applied: true, Found: true, Binding: next, AuditReceiptID: a}, err
}
func (b *custodyBindings) FenceProviderCredentialCandidate(_ context.Context, c ports.ProviderCredentialSecretCandidateV1, _ time.Time) (ports.ProviderCredentialCandidateFenceV1, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.values[c.Scope.Locator]
	if ok && providerCandidateMatches(v, c) {
		return ports.ProviderCredentialCandidateFenceV1{Authoritative: true, Binding: v}, nil
	}
	b.fences[c.Scope.MutationID] = true
	return ports.ProviderCredentialCandidateFenceV1{}, nil
}
func (b *custodyBindings) RevokeProviderCredential(_ context.Context, l ports.ProviderCredentialLocatorV1, at time.Time) (ports.ProviderCredentialSwapV1, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	old, ok := b.values[l]
	if !ok {
		return ports.ProviderCredentialSwapV1{}, nil
	}
	b.cleanups = append(b.cleanups, ports.ProviderCredentialCleanupV1{Locator: l, CredentialGeneration: old.CredentialGeneration, Reference: old.SecretRef})
	old.ResourceRevision++
	old.CredentialGeneration++
	old.State = domain.ProviderCredentialRevokedV1
	old.SecretRef = domain.CredentialSecretRef{}
	old.SecretFingerprint = ""
	old.UpdatedAt = at
	b.values[l] = old
	a, err := domain.NewProviderCredentialAuditEventV1(old, domain.ProviderCredentialAuditRevokedV1)
	return ports.ProviderCredentialSwapV1{Applied: true, Found: true, Binding: old, AuditReceiptID: a.ReceiptID}, err
}
func (b *custodyBindings) ListProviderCredentialCleanups(_ context.Context, l ports.ProviderCredentialLocatorV1, limit uint64) ([]ports.ProviderCredentialCleanupV1, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []ports.ProviderCredentialCleanupV1
	for _, v := range b.cleanups {
		if v.Locator == l && uint64(len(out)) < limit {
			out = append(out, v)
		}
	}
	return out, nil
}
func (b *custodyBindings) ListDueProviderCredentialCleanups(_ context.Context, _ uint32, before time.Time, _ ports.ProviderCredentialCleanupCursorV1, limit uint64) (ports.ProviderCredentialCleanupPageV1, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var p ports.ProviderCredentialCleanupPageV1
	for _, v := range b.cleanups {
		if uint64(len(p.Items)) == limit {
			p.HasMore = true
			break
		}
		p.Items = append(p.Items, ports.ProviderCredentialCleanupItemV1{Cleanup: v, CreatedAt: before.Add(-time.Second)})
	}
	return p, nil
}
func (b *custodyBindings) AcknowledgeProviderCredentialCleanup(_ context.Context, c ports.ProviderCredentialCleanupV1) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, v := range b.cleanups {
		if v == c {
			b.cleanups = append(b.cleanups[:i], b.cleanups[i+1:]...)
			b.acks++
			return nil
		}
	}
	// The actual YDB ACK treats an already removed exact row as success.
	return nil
}

type custodyObjectRequest struct {
	method, key         string
	query               url.Values
	conditional, replay bool
}
type custodyTransport struct {
	mu                                            sync.Mutex
	objects                                       map[string][]byte
	requests                                      []custodyObjectRequest
	unknownPut, failDelete, redirect, badReadback bool
	blockGet, entered                             chan struct{}
	overrideMethod                                string
	overrideStatus                                int
	overrideBody                                  string
	overrideCloseError                            bool
}

func (f *custodyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	key := strings.TrimPrefix(r.URL.Path, "/custody-bucket/")
	if r.URL.Path == "/custody-bucket" || r.URL.Path == "/custody-bucket/" {
		key = ""
	}
	f.requests = append(f.requests, custodyObjectRequest{r.Method, key, r.URL.Query(), r.Header.Get("If-None-Match") == "*", r.GetBody != nil})
	if f.overrideMethod == r.Method {
		status, body := f.overrideStatus, f.overrideBody
		var reader io.ReadCloser = io.NopCloser(strings.NewReader(body))
		if f.overrideCloseError {
			reader = custodyCloseError{strings.NewReader(body)}
		}
		f.mu.Unlock()
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/xml"}}, Body: reader, Request: r}, nil
	}
	if _, ok := r.Context().Deadline(); !ok {
		f.mu.Unlock()
		return nil, errors.New("missing finite deadline")
	}
	if f.redirect {
		f.mu.Unlock()
		return &http.Response{StatusCode: 307, Header: http.Header{"Location": {"https://redirect.invalid/leak"}}, Body: io.NopCloser(strings.NewReader("SYNTHETIC-PRIVATE-REDIRECT")), Request: r}, nil
	}
	if r.Method == http.MethodGet && key != "" && f.blockGet != nil {
		release, entered := f.blockGet, f.entered
		f.blockGet = nil
		close(entered)
		f.mu.Unlock()
		select {
		case <-release:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		f.mu.Lock()
	}
	status := http.StatusOK
	var body []byte
	switch r.Method {
	case http.MethodPut:
		if r.Header.Get("If-None-Match") != "*" {
			f.mu.Unlock()
			return nil, errors.New("conditional create omitted")
		}
		if _, ok := f.objects[key]; ok {
			status = 412
			break
		}
		body, _ = io.ReadAll(r.Body)
		f.objects[key] = append([]byte(nil), body...)
		body = nil
		if f.unknownPut {
			f.unknownPut = false
			f.mu.Unlock()
			return nil, errors.New("SYNTHETIC-SECRET-UNKNOWN-PUT")
		}
	case http.MethodGet:
		if key == "" {
			q := r.URL.Query()
			limit, _ := strconv.Atoi(q.Get("max-keys"))
			if limit < 1 || limit > 65 || q.Get("list-type") != "2" {
				f.mu.Unlock()
				return nil, errors.New("list bounds omitted")
			}
			var keys []string
			for k := range f.objects {
				if strings.HasPrefix(k, q.Get("prefix")) && k > q.Get("start-after") {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			more := len(keys) > limit
			if more {
				keys = keys[:limit]
			}
			var x strings.Builder
			x.WriteString("<ListBucketResult><IsTruncated>" + strconv.FormatBool(more) + "</IsTruncated>")
			for _, k := range keys {
				x.WriteString("<Contents><Key>" + k + "</Key></Contents>")
			}
			x.WriteString("</ListBucketResult>")
			body = []byte(x.String())
		} else if v, ok := f.objects[key]; ok {
			body = append([]byte(nil), v...)
			if f.badReadback {
				body[0] ^= 1
			}
		} else {
			status = 404
			body = []byte("<Error><Code>NoSuchKey</Code><Message>SYNTHETIC-PRIVATE-MISSING</Message></Error>")
		}
	case http.MethodDelete:
		if f.failDelete {
			f.mu.Unlock()
			return nil, errors.New("SYNTHETIC-PRIVATE-DELETE")
		}
		delete(f.objects, key)
		status = 204
	default:
		f.mu.Unlock()
		return nil, errors.New("unexpected storage method")
	}
	f.mu.Unlock()
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/xml"}}, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)), Request: r}, nil
}

func custodyFixture(t *testing.T, iam bool) (*ProviderCredentialObjectStore, *custodyTransport, *custodyBindings, context.Context, ports.ProviderCredentialCandidateScopeV1) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	t.Cleanup(cancel)
	peer := &custodyTransport{objects: map[string][]byte{}}
	httpClient := &http.Client{Transport: peer}
	objects := &Store{bucket: "custody-bucket"}
	if iam {
		endpoint, _ := url.Parse("https://objects.invalid")
		objects.iamClient = &iamObjectClient{endpoint: endpoint, http: httpClient, tokens: fixedToken("SYNTHETIC-IAM-TOKEN")}
	} else {
		client := s3.New(s3.Options{Region: "us-east-1", Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("SYNTHETIC-ID", "SYNTHETIC-KEY", "")), BaseEndpoint: aws.String("https://objects.invalid"), UsePathStyle: true, HTTPClient: httpClient, RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired, ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired})
		objects.s3Client = client
	}
	bindings := newCustodyBindings()
	store, err := NewProviderCredentialSecretStore(objects, ProviderCredentialObjectConfig{KeyEpoch: "epoch-1", Clock: custodyClock{time.Unix(100, 0).UTC()}}, bindings, bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(ctx); err != nil {
			t.Errorf("close custody: %v", err)
		}
	})
	scope := ports.ProviderCredentialCandidateScopeV1{Locator: ports.ProviderCredentialLocatorV1{TenantID: "tenant-custody", OwnerUserID: "owner-custody", ResourceKind: domain.ProviderResourceRouterAccountV1, ResourceID: "resource-custody"}, ResourceRevision: 1, CredentialGeneration: 1, MutationID: "mutation-custody"}
	return store, peer, bindings, ctx, scope
}
func custodyBinding(c ports.ProviderCredentialSecretCandidateV1) domain.ProviderCredentialBindingV1 {
	l := c.Scope.Locator
	return domain.ProviderCredentialBindingV1{Version: 1, TenantID: l.TenantID, OwnerUserID: l.OwnerUserID, ResourceKind: l.ResourceKind, ResourceID: l.ResourceID, ResourceRevision: c.Scope.ResourceRevision, CredentialGeneration: c.Scope.CredentialGeneration, CandidateMutationID: c.Scope.MutationID, State: domain.ProviderCredentialActiveV1, SecretRef: c.Reference, SecretFingerprint: c.Fingerprint, UpdatedAt: c.CreatedAt}
}

func TestProviderCustodyConnectedServiceInvocationAndRotation(t *testing.T) {
	for _, iam := range []bool{false, true} {
		name := "sdk"
		if iam {
			name = "iam"
		}
		t.Run(name, func(t *testing.T) {
			secrets, peer, bindings, ctx, scope := custodyFixture(t, iam)
			clock := custodyClock{time.Unix(100, 0).UTC()}
			ids := &custodyIDs{}
			invocations, err := providercredential.NewInvocationService(providercredential.InvocationConfig{Clock: clock, IDs: ids}, custodyPlanner{}, bindings, secrets)
			if err != nil {
				t.Fatal(err)
			}
			service, err := providercredential.New(providercredential.Config{Clock: clock, IDs: ids}, custodyAuthorizer{}, invocations, bindings, secrets)
			if err != nil {
				t.Fatal(err)
			}
			request := ports.ProviderCredentialIngestRequestV1{Principal: ports.ProviderCredentialPrincipalV1{TenantID: scope.Locator.TenantID, OwnerUserID: scope.Locator.OwnerUserID, Channel: ports.ProviderCredentialChannelLocalOperatorV1}, ResourceKind: scope.Locator.ResourceKind, ResourceID: scope.Locator.ResourceID}
			secret := []byte("SYNTHETIC-PROVIDER-SECRET-FIRST")
			receipt, err := service.Ingest(ctx, request, secret)
			if err != nil {
				t.Fatal(err)
			}
			expiry := clock.at.Add(time.Hour)
			binding := domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: scope.Locator.TenantID, OwnerUserID: scope.Locator.OwnerUserID, RunID: "run-custody", AttemptID: "attempt-custody", Resource: receipt.Resource, Backend: domain.HarnessBackendDescriptorV1{HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendDirectOpenRouterV1, ArtifactKind: domain.HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("1", 64), NativeProtocolVersion: "openrouter-http.v1", BackendProfileDigest: strings.Repeat("2", 64), ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: domain.ProviderCredentialDeliveryDirectV1}, ModelVendorID: "openrouter", ModelID: "synthetic-model", InputDataClass: domain.ProviderDataPublicV1, ProviderCatalogDigest: strings.Repeat("3", 64), ProviderRouteDigest: strings.Repeat("4", 64), PrivacyPolicyDigest: strings.Repeat("5", 64), CapabilityEvidenceDigest: strings.Repeat("6", 64), EffectivePolicyDigest: strings.Repeat("7", 64), ExecutionPlacementDigest: strings.Repeat("8", 64), EvidenceExpiresAt: &expiry}
			handle, err := invocations.IssueProviderCredential(ctx, ports.ProviderCredentialIssueRequestV1{HarnessBinding: binding, WorkerID: "worker-custody", LeaseID: "lease-custody", LeaseFence: 1, ExpiresAt: expiry})
			if err != nil {
				t.Fatal(err)
			}
			var borrowed []byte
			calls := 0
			if err := invocations.MaterializeProviderCredential(ctx, handle, func(_ ports.ProviderCredentialMaterializationV1, v []byte) error {
				calls++
				borrowed = v
				if !bytes.Equal(v, secret) {
					return errors.New("synthetic value differs")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if calls != 1 || !bytes.Equal(borrowed, make([]byte, len(borrowed))) {
				t.Fatal("borrowed secret not zeroed after synchronous callback")
			}
			if err := invocations.MaterializeProviderCredential(ctx, handle, func(ports.ProviderCredentialMaterializationV1, []byte) error { return nil }); err == nil {
				t.Fatal("duplicate materialization admitted")
			}
			if err := invocations.ReleaseProviderCredential(ctx, handle); err != nil {
				t.Fatal(err)
			}
			if _, err := service.Ingest(ctx, request, []byte("SYNTHETIC-PROVIDER-SECRET-SECOND")); err != nil {
				t.Fatal(err)
			}
			peer.mu.Lock()
			defer peer.mu.Unlock()
			if len(peer.objects) != 1 || bindings.acks != 1 {
				t.Errorf("rotation objects=%d ACKs=%d", len(peer.objects), bindings.acks)
			}
			for _, raw := range peer.objects {
				if bytes.Contains(raw, secret) {
					t.Error("plaintext persisted")
				}
			}
			for _, r := range peer.requests {
				if r.replay || r.method == http.MethodPut && !r.conditional {
					t.Errorf("unsafe transport: method=%s replay=%v conditional=%v", r.method, r.replay, r.conditional)
				}
			}
		})
	}
}

// This joins the actual encrypted SDK/IAM adapter, credential mutation service
// and distinct SEARCH invocation arm. Declaration/correlation values are DATA,
// not proof of an original MODEL, Source/Store consent or permission to search.
// The existing RoundTripper owns every request; no socket or service is used.
func TestProviderCustodyConnectedSearchCredentialUnion(t *testing.T) {
	for _, iam := range []bool{false, true} {
		name := "sdk"
		if iam {
			name = "iam"
		}
		t.Run(name, func(t *testing.T) {
			secrets, peer, bindings, ctx, scope := custodyFixture(t, iam)
			clock := custodyClock{time.Unix(100, 0).UTC()}
			ids := &custodyIDs{}
			invocations, err := providercredential.NewInvocationService(providercredential.InvocationConfig{Clock: clock, IDs: ids}, custodyPlanner{}, bindings, secrets)
			if err != nil {
				t.Fatal("construct actual invocation lifecycle", err)
			}
			observed := custodyObservedRevoker{delegate: invocations, entered: make(chan uint64, 8)}
			service, err := providercredential.New(providercredential.Config{Clock: clock, IDs: ids}, custodyAuthorizer{}, observed, bindings, secrets)
			if err != nil {
				t.Fatal("construct actual mutation service", err)
			}
			wait, cancel := context.WithTimeout(ctx, 5*time.Second)
			t.Cleanup(cancel)
			expectGeneration := func(want uint64, operation string) {
				t.Helper()
				var got uint64
				select {
				case got = <-observed.entered:
				case <-wait.Done():
					select {
					case got = <-observed.entered:
					default:
						t.Fatalf("%s did not reach generation join: %v", operation, wait.Err())
					}
				}
				if got != want {
					t.Fatalf("%s joined generation=%d want=%d", operation, got, want)
				}
			}
			modelRequest := ports.ProviderCredentialIngestRequestV1{Principal: ports.ProviderCredentialPrincipalV1{TenantID: scope.Locator.TenantID, OwnerUserID: scope.Locator.OwnerUserID, Channel: ports.ProviderCredentialChannelLocalOperatorV1}, ResourceKind: domain.ProviderResourceRouterAccountV1, ResourceID: scope.Locator.ResourceID}
			modelKey := []byte("SYNTHETIC-UNION-MODEL-KEY")
			modelReceipt, err := service.Ingest(ctx, modelRequest, modelKey)
			if err != nil {
				t.Fatal("ingest distinct MODEL resource", err)
			}
			expectGeneration(1, "MODEL ingest")
			searchRequest := modelRequest
			searchRequest.ResourceKind, searchRequest.ResourceID = domain.ProviderResourceWebSearchAccountV1, "search-custody"
			first, second := []byte("SYNTHETIC-UNION-SEARCH-FIRST"), []byte("SYNTHETIC-UNION-SEARCH-SECOND")
			searchReceipt, err := service.Ingest(ctx, searchRequest, first)
			if err != nil || searchReceipt.Resource.Kind != domain.ProviderResourceWebSearchAccountV1 {
				t.Fatalf("normal SEARCH ingest resource=%+v err=%v", searchReceipt.Resource, err)
			}
			expectGeneration(1, "SEARCH ingest")
			expiry := clock.at.Add(time.Hour)
			binding := domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: scope.Locator.TenantID, OwnerUserID: scope.Locator.OwnerUserID, RunID: "run-search-union", AttemptID: "attempt-search-union", Resource: modelReceipt.Resource,
				Backend:       domain.HarnessBackendDescriptorV1{HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendDirectOpenRouterV1, ArtifactKind: domain.HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("1", 64), NativeProtocolVersion: "openrouter-http.v1", BackendProfileDigest: strings.Repeat("2", 64), ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: domain.ProviderCredentialDeliveryDirectV1},
				ModelVendorID: "openrouter", ModelID: "synthetic-model", InputDataClass: domain.ProviderDataPublicV1, ProviderCatalogDigest: strings.Repeat("3", 64), ProviderRouteDigest: strings.Repeat("4", 64), PrivacyPolicyDigest: strings.Repeat("5", 64), CapabilityEvidenceDigest: strings.Repeat("6", 64), EffectivePolicyDigest: strings.Repeat("7", 64), ExecutionPlacementDigest: strings.Repeat("8", 64), EvidenceExpiresAt: &expiry}
			modelHandle, err := invocations.IssueProviderCredential(ctx, ports.ProviderCredentialIssueRequestV1{HarnessBinding: binding, WorkerID: "worker-search-union", LeaseID: "lease-search-union", LeaseFence: 1, ExpiresAt: expiry})
			if err != nil {
				t.Fatal("issue MODEL handle", err)
			}
			t.Cleanup(func() {
				if err := invocations.ReleaseProviderCredential(ctx, modelHandle); err != nil {
					t.Error("release owned MODEL handle", err)
				}
			})
			hash := strings.Repeat("a", 64)
			searchIssue := ports.SearchCredentialIssueRequestV1{
				Declaration: ports.SearchCredentialDeclarationV1{Version: 1, TenantID: scope.Locator.TenantID, Resource: searchReceipt.Resource, Audience: domain.WebSearchCredentialAudienceV1, DeliveryKind: domain.ProviderCredentialDeliveryDirectV1},
				Correlation: ports.SearchCredentialCorrelationV1{Scope: domain.AttemptOperationScopeV1{TenantID: scope.Locator.TenantID, SessionID: "session-search-union", OwnerUserID: scope.Locator.OwnerUserID, RunID: binding.RunID, AttemptID: binding.AttemptID, LeaseID: modelHandle.LeaseID, FenceToken: modelHandle.LeaseFence, PhysicalInvocationClaimID: "claim-search-union", EffectReservationDigest: domain.AttemptEffectReservationDigestV1(hash), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(hash), HarnessBindingDigest: domain.HarnessBindingDigestV2(hash), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(hash)}, OperationDigest: domain.AttemptOperationDigestV1(hash), ConsentRequestDigest: domain.WebSearchConsentRequestDigestV1(hash), ConsentDecisionDigest: domain.WebSearchConsentDecisionDigestV1(hash)},
				WorkerID:    modelHandle.WorkerID, ExpiresAt: expiry}
			searchHandle, err := invocations.IssueSearchCredential(ctx, searchIssue)
			if err != nil || searchHandle.Validate() != nil || searchHandle.Credential.HandleID == modelHandle.HandleID {
				t.Fatalf("issue distinct SEARCH handle=%+v err=%v", searchHandle, err)
			}
			t.Cleanup(func() {
				if err := invocations.ReleaseSearchCredential(ctx, searchHandle); err != nil {
					t.Error("release owned SEARCH handle", err)
				}
			})
			beforeDenied := len(peer.requests)
			if err := invocations.MaterializeProviderCredential(ctx, searchHandle.Credential, func(ports.ProviderCredentialMaterializationV1, []byte) error {
				t.Error("SEARCH key reached MODEL callback")
				return nil
			}); err == nil {
				t.Error("MODEL materialized SEARCH handle")
			}
			if err := invocations.ReleaseProviderCredential(ctx, searchHandle.Credential); err == nil {
				t.Error("MODEL released SEARCH handle")
			}
			foreign := searchHandle
			foreign.Credential.HandleID = modelHandle.HandleID
			if err := invocations.MaterializeSearchCredential(ctx, foreign, func(ports.ProviderCredentialMaterializationV1, []byte) error {
				t.Error("MODEL key reached SEARCH callback")
				return nil
			}); err == nil || len(peer.requests) != beforeDenied {
				t.Errorf("cross-purpose denial err=%v added-storage-requests=%d", err, len(peer.requests)-beforeDenied)
			}
			var modelBorrow []byte
			if err := invocations.MaterializeProviderCredential(ctx, modelHandle, func(plan ports.ProviderCredentialMaterializationV1, value []byte) error {
				modelBorrow = value
				if plan.Validate() != nil || plan.Kind != domain.ProviderCredentialDeliveryDirectV1 || !bytes.Equal(value, modelKey) {
					return errors.New("MODEL delivery differed from its own ingested key")
				}
				return nil
			}); err != nil || len(modelBorrow) == 0 || !bytes.Equal(modelBorrow, make([]byte, len(modelBorrow))) {
				t.Fatal("MODEL materialization/owned buffer clearing failed", err)
			}
			old, found, err := bindings.LoadProviderCredential(ctx, searchRequest.Locator())
			if err != nil || !found {
				t.Fatalf("load actual SEARCH binding found=%v err=%v", found, err)
			}
			oldKey, err := providerObjectKey(searchRequest.Locator(), old.SecretRef)
			if err != nil {
				t.Fatal("derive actual old ciphertext identity", err)
			}
			entered, release, materialized, rotated := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			var borrowed []byte
			var materializeErr, rotateErr error
			var rotatedReceipt ports.ProviderCredentialReceiptV1
			rotationStarted := false
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(release) })
				cleanup, done := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				defer done()
				waitCustodySearchEvent(t, cleanup, materialized, "SEARCH callback cleanup")
				if rotationStarted {
					waitCustodySearchEvent(t, cleanup, rotated, "SEARCH rotation cleanup")
				}
			})
			go func() {
				defer close(materialized)
				materializeErr = invocations.MaterializeSearchCredential(ctx, searchHandle, func(plan ports.ProviderCredentialMaterializationV1, value []byte) error {
					if plan.Validate() != nil || plan.Kind != domain.ProviderCredentialDeliveryDirectV1 || !bytes.Equal(value, first) {
						return errors.New("SEARCH delivery differed from its own ingested key")
					}
					borrowed = value
					close(entered)
					<-release
					return nil
				})
			}()
			waitCustodySearchEvent(t, wait, entered, "SEARCH callback entry")
			bindings.casApplied = make(chan uint64, 1)
			rotationStarted = true
			go func() {
				defer close(rotated)
				rotatedReceipt, rotateErr = service.Ingest(ctx, searchRequest, second)
			}()
			expectGeneration(2, "SEARCH rotation")
			// Both acknowledgements/completion may be ready; an entry notification
			// is not evidence of noncompletion. Check actual retained custody too.
			select {
			case <-rotated:
				t.Errorf("rotation completed while SEARCH callback still borrowed key: %v", rotateErr)
			default:
			}
			peer.mu.Lock()
			_, retained := peer.objects[oldKey]
			peer.mu.Unlock()
			if !retained {
				t.Error("old encrypted SEARCH object deleted before borrower join")
			}
			releaseOnce.Do(func() { close(release) })
			waitCustodySearchEvent(t, wait, materialized, "SEARCH callback completion")
			waitCustodySearchEvent(t, wait, rotated, "SEARCH rotation completion")
			if materializeErr != nil || rotateErr != nil || len(borrowed) == 0 || !bytes.Equal(borrowed, make([]byte, len(borrowed))) {
				t.Fatalf("joined SEARCH materialization=%v rotation=%v owned-buffer-bytes=%d", materializeErr, rotateErr, len(borrowed))
			}
			if rotatedReceipt.Resource.CredentialGeneration != 2 || rotatedReceipt.Resource.Revision != 2 {
				t.Fatalf("rotation resource=%+v want revision/generation2", rotatedReceipt.Resource)
			}
			if err := invocations.MaterializeSearchCredential(ctx, searchHandle, func(ports.ProviderCredentialMaterializationV1, []byte) error {
				t.Error("old SEARCH handle replayed")
				return nil
			}); err == nil {
				t.Error("rotated SEARCH handle materialized again")
			}
			if _, err := invocations.IssueSearchCredential(ctx, searchIssue); err == nil {
				t.Error("old declaration silently selected the rotated credential")
			}
			if err := invocations.ReleaseSearchCredential(ctx, searchHandle); err != nil {
				t.Error("release joined SEARCH handle", err)
			}
			peer.mu.Lock()
			defer peer.mu.Unlock()
			if _, exists := peer.objects[oldKey]; exists || len(peer.objects) != 2 {
				t.Errorf("rotation retained old=%v ciphertext objects=%d want two distinct current resources", exists, len(peer.objects))
			}
			puts := 0
			for _, raw := range peer.objects {
				if !bytes.HasPrefix(raw, []byte("SLPCS001")) || bytes.Contains(raw, first) || bytes.Contains(raw, second) || bytes.Contains(raw, modelKey) {
					t.Error("stored credential is not a private ciphertext envelope")
				}
			}
			for _, r := range peer.requests {
				if r.method == http.MethodPut {
					puts++
				}
				if r.replay || r.method == http.MethodPut && !r.conditional {
					t.Errorf("unsafe credential transport method=%s replay=%v conditional=%v", r.method, r.replay, r.conditional)
				}
			}
			if puts != 3 {
				t.Errorf("physical credential PUTs=%d want three (MODEL, SEARCH, SEARCH rotation)", puts)
			}
			bindings.mu.Lock()
			defer bindings.mu.Unlock()
			if bindings.acks != 1 {
				t.Errorf("SEARCH old-generation cleanup ACKs=%d want1", bindings.acks)
			}
		})
	}
}

func waitCustodySearchEvent(t *testing.T, ctx context.Context, event <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-event:
	case <-ctx.Done():
		select {
		case <-event:
		default:
			t.Fatalf("%s did not acknowledge within the original test/cleanup bound: %v", operation, ctx.Err())
		}
	}
}

type custodyObservedRevoker struct {
	delegate ports.ProviderCredentialInvocationRevokerV1
	entered  chan uint64
}

func (r custodyObservedRevoker) FenceProviderCredentialInvocations(ctx context.Context, l ports.ProviderCredentialLocatorV1, generation uint64) error {
	r.entered <- generation
	return r.delegate.FenceProviderCredentialInvocations(ctx, l, generation)
}

func TestProviderCustodyJoinedBorrowerRotationAndGC(t *testing.T) {
	for _, iam := range []bool{false, true} {
		backend := "sdk"
		if iam {
			backend = "iam"
		}
		t.Run(backend, func(t *testing.T) {
			for _, global := range []bool{false, true} {
				entry := "public-ingest-reconcile"
				if global {
					entry = "global-abandoned"
				}
				t.Run(entry, func(t *testing.T) {
					secrets, peer, bindings, ctx, scope := custodyFixture(t, iam)
					clock := custodyClock{time.Unix(100, 0).UTC()}
					ids := &custodyIDs{}
					invocations, err := providercredential.NewInvocationService(providercredential.InvocationConfig{Clock: clock, IDs: ids}, custodyPlanner{}, bindings, secrets)
					if err != nil {
						t.Fatal(err)
					}
					observed := custodyObservedRevoker{delegate: invocations, entered: make(chan uint64, 8)}
					service, err := providercredential.New(providercredential.Config{Clock: clock, IDs: ids}, custodyAuthorizer{}, observed, bindings, secrets)
					if err != nil {
						t.Fatal(err)
					}
					request := ports.ProviderCredentialIngestRequestV1{Principal: ports.ProviderCredentialPrincipalV1{TenantID: scope.Locator.TenantID, OwnerUserID: scope.Locator.OwnerUserID, Channel: ports.ProviderCredentialChannelLocalOperatorV1}, ResourceKind: scope.Locator.ResourceKind, ResourceID: scope.Locator.ResourceID}
					first := []byte("SYNTHETIC-JOINED-FIRST")
					second := []byte("SYNTHETIC-JOINED-SECOND")
					receipt, err := service.Ingest(ctx, request, first)
					if err != nil {
						t.Fatal(err)
					}
					if generation := <-observed.entered; generation != 1 {
						t.Fatal("initial generation mismatch")
					}
					old, _, err := bindings.LoadProviderCredential(ctx, scope.Locator)
					if err != nil {
						t.Fatal(err)
					}
					oldKey, err := providerObjectKey(scope.Locator, old.SecretRef)
					if err != nil {
						t.Fatal(err)
					}
					expiry := clock.at.Add(time.Hour)
					binding := domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: scope.Locator.TenantID, OwnerUserID: scope.Locator.OwnerUserID, RunID: "run-joined", AttemptID: "attempt-joined", Resource: receipt.Resource, Backend: domain.HarnessBackendDescriptorV1{HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendDirectOpenRouterV1, ArtifactKind: domain.HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("1", 64), NativeProtocolVersion: "openrouter-http.v1", BackendProfileDigest: strings.Repeat("2", 64), ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: domain.ProviderCredentialDeliveryDirectV1}, ModelVendorID: "openrouter", ModelID: "synthetic-model", InputDataClass: domain.ProviderDataPublicV1, ProviderCatalogDigest: strings.Repeat("3", 64), ProviderRouteDigest: strings.Repeat("4", 64), PrivacyPolicyDigest: strings.Repeat("5", 64), CapabilityEvidenceDigest: strings.Repeat("6", 64), EffectivePolicyDigest: strings.Repeat("7", 64), ExecutionPlacementDigest: strings.Repeat("8", 64), EvidenceExpiresAt: &expiry}
					handle, err := invocations.IssueProviderCredential(ctx, ports.ProviderCredentialIssueRequestV1{HarnessBinding: binding, WorkerID: "worker-joined", LeaseID: "lease-joined", LeaseFence: 1, ExpiresAt: expiry})
					if err != nil {
						t.Fatal(err)
					}
					type result struct {
						name string
						err  error
					}
					results := make(chan result, 3)
					entered, release := make(chan struct{}), make(chan struct{})
					var releaseOnce sync.Once
					started, received := 0, 0
					wait, cancel := context.WithTimeout(ctx, 5*time.Second)
					t.Cleanup(cancel)
					collect := func() {
						for received < started {
							select {
							case r := <-results:
								received++
								if r.err != nil {
									t.Errorf("%s: %v", r.name, r.err)
								}
							case <-wait.Done():
								t.Errorf("bounded join incomplete: received%d started%d", received, started)
								return
							}
						}
					}
					t.Cleanup(func() {
						releaseOnce.Do(func() { close(release) })
						collect()
						if err := invocations.ReleaseProviderCredential(ctx, handle); err != nil {
							t.Errorf("release joined handle: %v", err)
						}
					})
					started++
					go func() {
						results <- result{"materializer", invocations.MaterializeProviderCredential(ctx, handle, func(_ ports.ProviderCredentialMaterializationV1, v []byte) error {
							if !bytes.Equal(v, first) {
								return errors.New("borrowed synthetic value differs")
							}
							close(entered)
							<-release
							return nil
						})}
					}()
					select {
					case <-entered:
					case <-wait.Done():
						t.Fatal("materializer did not enter")
					}
					bindings.casApplied = make(chan uint64, 1)
					started++
					go func() { _, err := service.Ingest(ctx, request, second); results <- result{"rotation", err} }()
					select {
					case generation := <-bindings.casApplied:
						if generation != 2 {
							t.Fatal("rotation CAS generation differs")
						}
					case <-wait.Done():
						t.Fatal("rotation did not publish CAS")
					}
					select {
					case generation := <-observed.entered:
						if generation != 2 {
							t.Fatal("rotation join generation differs")
						}
					case r := <-results:
						received++
						t.Fatalf("%s completed before rotation join: %v", r.name, r.err)
					case <-wait.Done():
						t.Fatal("rotation did not reach join")
					}
					started++
					go func() {
						if global {
							_, err := service.DrainAbandonedCandidates(ctx, time.Unix(101, 0).UTC(), ports.ProviderCredentialCandidateCursorV1{})
							results <- result{"global-GC", err}
							return
						}
						_, err := service.Ingest(ctx, request, second)
						results <- result{"public-ingest-GC", err}
					}()
					select {
					case generation := <-observed.entered:
						if generation != 2 {
							t.Fatal("candidate GC joined wrong generation")
						}
					case r := <-results:
						received++
						t.Fatalf("%s completed before candidate join: %v", r.name, r.err)
					case <-wait.Done():
						t.Fatal("candidate GC did not reach join")
					}
					// Do not rely on select priority if completion and join are both ready.
					select {
					case r := <-results:
						received++
						t.Errorf("%s completed while callback still borrowed secret: %v", r.name, r.err)
					default:
					}
					peer.mu.Lock()
					_, retained := peer.objects[oldKey]
					peer.mu.Unlock()
					if !retained {
						t.Error("actual encrypted old object deleted before both local joins")
					}
					releaseOnce.Do(func() { close(release) })
					collect()
					peer.mu.Lock()
					remaining := len(peer.objects)
					_, oldRemains := peer.objects[oldKey]
					peer.mu.Unlock()
					if oldRemains || remaining != 1 {
						t.Errorf("joined cleanup old-remains=%v objects=%d", oldRemains, remaining)
					}
					current, found, err := bindings.LoadProviderCredential(ctx, scope.Locator)
					if err != nil || !found || current.CredentialGeneration != 2 {
						t.Errorf("GC retry renewed generation: found=%v gen=%d err=%v", found, current.CredentialGeneration, err)
					}
				})
			}
		})
	}
}
