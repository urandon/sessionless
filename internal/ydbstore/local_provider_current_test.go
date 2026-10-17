package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Synthetic rows exercise the existing SAME-Tx prerequisite only. They do not
// install a production profile, authorize funding or attest an actual connector.
type localProviderCurrentSQLFixture struct {
	*ownedCurrentSQLFixture
	credential      []driver.Value
	credentialArgs  []driver.Value
	credentialReads int
}

func (f *localProviderCurrentSQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *localProviderCurrentSQLFixture) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query != providerCredentialReadSQL {
		return f.ownedCurrentSQLFixture.QueryContext(ctx, query, args)
	}
	if err := f.statement(ctx, query, args); err != nil {
		return nil, err
	}
	f.credentialReads++
	values := make([]driver.Value, len(args))
	for i := range args {
		values[i] = args[i].Value
	}
	if !reflect.DeepEqual(values, f.credentialArgs) {
		return nil, errors.New("foreign credential lookup")
	}
	return &explanationFixtureRows{values: append([]driver.Value(nil), f.credential...)}, nil
}

func localCurrentProfileFixture(t *testing.T, bearer bool) (*ownedReaderFixture, *localProviderCurrentSQLFixture, domain.HarnessBindingV2, domain.ProviderCredentialBindingV1) {
	t.Helper()
	f := newOwnedReaderFixture(t)
	p := &f.profile
	p.Backend.BackendKind, p.Backend.ProviderContractKind, p.Backend.NativeProtocolVersion = domain.HarnessBackendDirectOpenAICompatibleV1, domain.ProviderContractConfiguredEndpointV1, domain.ConfiguredInferenceProtocolChatCompletionsV1
	p.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryNoneV1
	p.Resource.Kind, p.Resource.ResourceID, p.Resource.CredentialMode, p.Resource.CredentialGeneration = domain.ProviderResourceLocalEndpointV1, "synthetic-local-endpoint", domain.ProviderCredentialNoneV1, 0
	if bearer {
		p.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryDirectV1
		p.Resource.CredentialMode, p.Resource.CredentialGeneration = domain.ProviderCredentialInvocationV1, 1
	}
	p.Locator.ResourceKind, p.Locator.ResourceID = p.Resource.Kind, p.Resource.ResourceID
	s := &p.Substrate
	s.Kind, s.Region, s.ImageDigest, s.WorkloadMode = domain.SubstrateLocalDevelopmentV1, "", "", domain.SubstrateWorkloadInProcessDirectV1
	s.LocalInstallationID, s.LocalInstallationRevision = p.InstallationID, p.InstallationRevision
	s.LocalHostIdentityDigest, s.LocalNetworkNamespaceIdentityDigest = strings.Repeat("a", 64), strings.Repeat("b", 64)
	if err := p.ValidateAt(f.driver.at); err != nil {
		t.Fatalf("synthetic local profile shape: %v", err)
	}
	digest, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	f.ref = domain.OwnedAgentProfileReferenceV1{Locator: p.Locator, RevocationGeneration: p.RevocationGeneration, Digest: digest}
	f.sync(t)
	fixture := &localProviderCurrentSQLFixture{ownedCurrentSQLFixture: f.driver}
	db := sql.OpenDB(fixture)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	f.store = &Store{db: db}
	expires := p.Window.ExpiresAt
	b := domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: p.Locator.TenantID, OwnerUserID: p.Resource.OwnerUserID, RunID: f.scope.run, AttemptID: f.scope.attempt, Backend: p.Backend, Resource: p.Resource, ModelVendorID: p.Locator.ModelVendorID, ModelID: p.Locator.ModelID, InputDataClass: p.Locator.InputDataClass, ProviderCatalogDigest: p.ProviderCatalogDigest, ProviderRouteDigest: p.ProviderRouteDigest, PrivacyPolicyDigest: p.PrivacyPolicyDigest, CapabilityEvidenceDigest: p.CapabilityEvidenceDigest, EffectivePolicyDigest: p.EffectivePolicyDigest, ExecutionPlacementDigest: strings.Repeat("c", 64), EvidenceExpiresAt: &expires}
	sd, err := p.Substrate.Digest()
	if err != nil {
		t.Fatal(err)
	}
	placement, err := domain.ManagedExecutionPlacementV2(string(sd))
	if err != nil {
		t.Fatal(err)
	}
	pd, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	b.ExecutionPlacementDigest = string(pd)
	var credential domain.ProviderCredentialBindingV1
	if bearer {
		ref, err := domain.NewCredentialSecretRef("synthetic-local-reference")
		if err != nil {
			t.Fatal(err)
		}
		credential = domain.ProviderCredentialBindingV1{Version: 1, TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, ResourceKind: b.Resource.Kind, ResourceID: b.Resource.ResourceID, ResourceRevision: b.Resource.Revision, CredentialGeneration: b.Resource.CredentialGeneration, CandidateMutationID: "synthetic-local-mutation", State: domain.ProviderCredentialActiveV1, SecretRef: ref, SecretFingerprint: domain.FingerprintCredential([]byte("synthetic-marker-not-a-key")), UpdatedAt: f.driver.at}
		fixture.credentialArgs = []driver.Value{string(b.TenantID), string(b.OwnerUserID), string(b.Resource.Kind), b.Resource.ResourceID}
		syncLocalCredentialRow(t, fixture, credential)
	}
	if err := b.ValidateAt(f.driver.at); err != nil {
		t.Fatal(err)
	}
	return f, fixture, b, credential
}

func syncLocalCredentialRow(t *testing.T, f *localProviderCurrentSQLFixture, c domain.ProviderCredentialBindingV1) {
	t.Helper()
	f.credential = []driver.Value{int64(c.ResourceRevision), int64(c.CredentialGeneration), c.CandidateMutationID, string(c.State), c.SecretRef.StorageValue(), string(c.SecretFingerprint), c.UpdatedAt, choiceFixtureJSON(t, c)}
}

func TestLocalProviderCurrentPrerequisiteNeverUsesFixtureBypass(t *testing.T) {
	for _, bearer := range []bool{false, true} {
		t.Run(map[bool]string{false: "none", true: "bearer"}[bearer], func(t *testing.T) {
			for _, name := range []string{"valid", "no_original", "wrong_original_digest", "wrong_original_generation", "missing_head", "revoked", "expired", "resource_revision", "backend_profile", "model", "installation", "policy", "placement"} {
				t.Run(name, func(t *testing.T) {
					f, fixture, b, _ := localCurrentProfileFixture(t, bearer)
					original := &f.ref
					switch name {
					case "no_original":
						original = nil
					case "wrong_original_digest":
						f.ref.Digest = strings.Repeat("f", 64)
					case "wrong_original_generation":
						f.ref.RevocationGeneration++
					case "revoked":
						f.profile.State = domain.OwnedAgentCurrentRevokedV1
						f.profile.RevocationGeneration++
						f.sync(t)
					case "expired":
						fixture.at = f.profile.Window.ExpiresAt
					case "resource_revision":
						b.Resource.Revision++
					case "backend_profile":
						b.Backend.BackendProfileDigest = strings.Repeat("e", 64)
					case "model":
						b.ModelID = "changed-model"
					case "policy":
						b.EffectivePolicyDigest = strings.Repeat("e", 64)
					case "placement":
						b.ExecutionPlacementDigest = strings.Repeat("e", 64)
					case "installation":
						f.profile.InstallationRevision++
						f.profile.Substrate.LocalInstallationRevision++
						f.sync(t)
					case "missing_head":
						fixture.profileRow = nil
					}
					err := f.store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
						return currentServerlessProviderResourceTx(ctx, q, b, original, q.at)
					})
					if (err == nil) != (name == "valid") {
						t.Fatalf("case=%s bearer=%t err=%v", name, bearer, err)
					}
					if !bearer && fixture.credentialReads != 0 {
						t.Fatalf("no-auth read %d credential rows", fixture.credentialReads)
					}
					if name == "valid" && bearer && fixture.credentialReads != 1 {
						t.Fatalf("Bearer reads=%d want1", fixture.credentialReads)
					}
					if name != "valid" && fixture.commits != 0 {
						t.Fatal("denied current prerequisite committed")
					}
				})
			}
		})
	}
}

func TestLocalBearerCurrentPrerequisiteJoinsCredentialGeneration(t *testing.T) {
	for _, name := range []string{"missing", "generation", "revision", "owner", "revoked"} {
		t.Run(name, func(t *testing.T) {
			f, fixture, b, c := localCurrentProfileFixture(t, true)
			switch name {
			case "generation":
				c.CredentialGeneration++
			case "revision":
				c.ResourceRevision++
			case "owner":
				c.OwnerUserID = "foreign-owner"
			case "revoked":
				c.State = domain.ProviderCredentialRevokedV1
				c.SecretRef = domain.CredentialSecretRef{}
				c.SecretFingerprint = ""
			}
			syncLocalCredentialRow(t, fixture, c)
			if name == "missing" {
				fixture.credential = nil
			}
			err := f.store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
				return currentServerlessProviderResourceTx(ctx, q, b, &f.ref, q.at)
			})
			if !errors.Is(err, domain.ErrMembershipDenied) || fixture.credentialReads != 1 || fixture.commits != 0 {
				t.Fatalf("case=%s err=%v reads=%d commits=%d", name, err, fixture.credentialReads, fixture.commits)
			}
		})
	}
}

func TestLocalCurrentPrerequisiteRequiresOriginalBoundedTransaction(t *testing.T) {
	f, _, b, _ := localCurrentProfileFixture(t, false)
	err := f.store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
		return currentServerlessProviderResourceTx(ctx, q, b, &f.ref, q.at.Add(time.Microsecond))
	})
	if !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("independent clock accepted: %v", err)
	}
}

func TestLocalProfileCannotUseFixtureInstallationGrant(t *testing.T) {
	fixture := newOwnedReaderFixture(t)
	g := ports.OwnedProfilePublicationGrantV1{Profile: fixture.profile, IssuedAt: fixture.driver.at, ExpiresAt: fixture.driver.at.Add(time.Second), Authenticator: make([]byte, 32)}
	if err := g.Validate(); err != nil {
		t.Fatalf("fixture-only control grant: %v", err)
	}
	local, _, _, _ := localCurrentProfileFixture(t, false)
	g.Profile = local.profile
	if err := g.Profile.Validate(); err != nil {
		t.Fatalf("local data shape control: %v", err)
	}
	if err := g.Validate(); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("real local resource used fixture installation: %v", err)
	}
}
