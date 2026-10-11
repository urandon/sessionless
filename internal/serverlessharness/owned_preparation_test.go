package serverlessharness

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Scripted durable storage, deliberately shared across different process-local
// issuers. Exact original authority/parent value digests constrain the fixture;
// it is not a production source keeper or proof of native current-authority SQL.
type ownedPreparationBurnFixture struct {
	mu            sync.Mutex
	authority     domain.ServerlessInvocationAuthorityDigestV1
	parent        domain.AttemptEffectReservationDigestV1
	expires       time.Time
	burned        bool
	calls         int
	deny          bool
	unknownCommit bool
	failure       bool
	after         func()
}

func (burn *ownedPreparationBurnFixture) BeginOwnedAttemptPreparation(ctx context.Context, grant ports.AttemptEffectOwnershipGrantV1) (bool, error) {
	burn.mu.Lock()
	defer burn.mu.Unlock()
	burn.calls++
	a, aerr := grant.Authority.Digest()
	p, perr := grant.Reservation.DigestForAuthority(grant.Authority)
	if ctx.Err() != nil || aerr != nil || perr != nil || a != burn.authority || p != burn.parent || !grant.GrantExpiresAt.Equal(burn.expires) {
		return false, errors.New("scripted exact original parent denied")
	}
	if burn.burned || burn.deny {
		return false, nil
	}
	if burn.failure {
		return false, errors.New("scripted unknown storage outcome")
	}
	burn.burned = true
	if burn.after != nil {
		burn.after()
	}
	if burn.unknownCommit {
		return false, errors.New("scripted committed acknowledgement lost")
	}
	return true, nil
}

func (burn *ownedPreparationBurnFixture) snapshot() (bool, int) {
	burn.mu.Lock()
	defer burn.mu.Unlock()
	return burn.burned, burn.calls
}

type ownedPreparationDriverFixture struct {
	burn       *ownedPreparationBurnFixture
	authority  domain.ServerlessInvocationAuthorityDigestV1
	allocation domain.PreparedAllocationV1
	calls      atomic.Uint64
	failure    error
	before     func()
	invalid    bool
}

func (driver *ownedPreparationDriverFixture) Preflight(ctx context.Context, authority domain.ServerlessInvocationAuthorityV1) (domain.PreparedAllocationV1, error) {
	driver.calls.Add(1)
	burned, _ := driver.burn.snapshot()
	digest, err := authority.Digest()
	deadline, bounded := ctx.Deadline()
	if !burned || err != nil || digest != driver.authority || !bounded || time.Until(deadline) > 31*time.Second || ctx.Err() != nil {
		return domain.PreparedAllocationV1{}, errors.New("preflight preceded exact durable burn or lacked original bound")
	}
	if driver.before != nil {
		driver.before()
	}
	if driver.failure != nil {
		return domain.PreparedAllocationV1{}, driver.failure
	}
	allocation := driver.allocation.Clone()
	if driver.invalid {
		allocation.ObservedProxyIdentityDigest = strings.Repeat("0", 64)
	}
	return allocation, nil
}

func ownedPreparationAuthorityFixture(t testing.TB) (domain.ServerlessInvocationAuthorityV1, domain.AttemptEffectReservationV1, domain.PreparedAllocationV1, time.Time) {
	t.Helper()
	a, _, allocation, at := capabilityFixture(t)
	b := &a.HarnessBinding
	b.Profile, b.InputDataClass = domain.HarnessExecutionOwnedAgentV2, domain.ProviderDataPublicV1
	expires := at.Add(time.Minute)
	cost := a.AdmissionCostCeiling.Clone()
	for _, component := range []struct {
		kind  domain.ExternalServiceKindV1
		grant string
		slot  **domain.ExternalServiceCostCeilingV1
	}{
		{domain.ExternalServiceMCPToolV1, strings.Repeat("3", 64), &cost.MCPToolCost},
		{domain.ExternalServiceWebSearchV1, strings.Repeat("4", 64), &cost.WebSearchCost},
	} {
		zero := uint64(0)
		*component.slot = &domain.ExternalServiceCostCeilingV1{
			Version: domain.ExternalServiceCostCeilingVersionV1, Kind: component.kind,
			TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, BillingOwnerUserID: b.OwnerUserID,
			ResourceID: string(component.kind) + "-fixture", ResourceRevision: 1, RevocationGeneration: 1,
			ProfileID: "bounded-scripted-fixture", ProfileRevision: 1, ScopedGrantDigest: component.grant,
			Currency: cost.Currency, PriceRevision: "scripted-known-free.v1", PriceObservedAt: cost.PriceObservedAt, PriceExpiresAt: cost.PriceExpiresAt,
			PriceEvidenceDigest: strings.Repeat("5", 64), TermsEvidenceDigest: strings.Repeat("6", 64),
			PriceState: domain.ExternalServicePriceKnownFreeV1, MaxAmountMicrounits: &zero,
		}
	}
	b.AgentLoopPolicy = &domain.AgentLoopPolicyV1{
		Version: domain.AgentLoopPolicyVersionV1, Profile: domain.AgentLoopOwnedPublicV1, ProfileRevision: 1,
		TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, RunID: b.RunID, AttemptID: b.AttemptID,
		Backend: b.Backend, Resource: b.Resource, ModelVendorID: b.ModelVendorID, ModelID: b.ModelID,
		ProviderCatalogDigest: b.ProviderCatalogDigest, ProviderRouteDigest: b.ProviderRouteDigest,
		CapabilityEvidenceDigest: b.CapabilityEvidenceDigest, PrivacyPolicyDigest: b.PrivacyPolicyDigest, EffectivePolicyDigest: b.EffectivePolicyDigest,
		InputDataClass: b.InputDataClass, InputModalities: []domain.AgentInputModalityV1{domain.AgentInputTextV1},
		InstructionVersion: "scripted-owned-instructions.v1", CompactionPolicyDigest: strings.Repeat("7", 64),
		MCPToolGrantDigest: strings.Repeat("3", 64), WebSearchGrantDigest: strings.Repeat("4", 64),
		AdmissionCostCeiling: cost.Clone(), IssuedAt: at, ExpiresAt: expires,
		Limits: domain.AgentLoopLimitsV1{
			MaxConcurrentOperations: 1, MaxModelCalls: 1, MaxCompactionCalls: 1, MaxToolDispatches: 1, MaxWebSearches: 1,
			MaxInputTokensPerCall: 1, MaxAggregateInputTokens: 1, MaxOutputTokensPerCall: 1, MaxAggregateOutputTokens: 1,
			MaxActiveDuration: time.Second, MaxNetworkOperationDuration: time.Second, MaxCleanupDuration: time.Second,
			MaxContextViewBytes: 1, MaxSummaryBytes: 1, MaxToolArgumentBytes: 1, MaxToolResultBytes: 1,
			MaxMCPOutboundMessages: 1, MaxMCPCatalogPages: 1, MaxMCPInboundFrames: 1, MaxMCPInboundBytes: 1, MaxOperations: 1, MaxAggregateBodyBytes: 1,
		},
	}
	a.AdmissionCostCeiling = cost
	resealOwnedPreparationAuthority(t, &a, &allocation)
	reservation, err := domain.BuildAttemptEffectReservationV1(a, "owned-preparation-original-claim", nil, at)
	if err != nil || a.ValidateAt(at) != nil || allocation.ValidateForBinding(a.SubstrateBinding, b.Backend) != nil {
		t.Fatalf("valid owned preparation fixture: %v / %v", err, a.ValidateAt(at))
	}
	return a, reservation, allocation, at
}

func resealOwnedPreparationAuthority(t testing.TB, a *domain.ServerlessInvocationAuthorityV1, allocation *domain.PreparedAllocationV1) {
	t.Helper()
	cost, err := a.AdmissionCostCeiling.Digest()
	if err != nil {
		t.Fatal(err)
	}
	a.SubstrateBinding.AdmissionCostCeilingDigest = cost
	substrate, err := a.SubstrateBinding.Digest()
	if err != nil {
		t.Fatal(err)
	}
	a.ExecutionPlacementV2.SubstrateBindingDigest = string(substrate)
	placement, err := domain.ExecutionPlacementDigest(a.ExecutionPlacementV2)
	if err != nil {
		t.Fatal(err)
	}
	a.HarnessBinding.ExecutionPlacementDigest = string(placement)
	allocation.SubstrateBindingDigest = substrate
}

type ownedPreparationFixture struct {
	bridge *BoundedOwnedAttemptPreparationV1
	issuer *CapabilityIssuer
	grant  ports.AttemptEffectOwnershipGrantV1
	burn   *ownedPreparationBurnFixture
	driver *ownedPreparationDriverFixture
	now    time.Time
}

func newOwnedPreparationFixture(t testing.TB) *ownedPreparationFixture {
	t.Helper()
	a, parent, allocation, at := ownedPreparationAuthorityFixture(t)
	f := &ownedPreparationFixture{now: at}
	issuer, err := NewCapabilityIssuer(func() time.Time { return f.now }, bytes.NewReader(bytes.Repeat([]byte{23}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	f.issuer = issuer
	f.grant, err = issuer.MintAttemptEffectOwnershipGrant(a, parent, at.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	ad, _ := a.Digest()
	pd, _ := parent.DigestForAuthority(a)
	f.burn = &ownedPreparationBurnFixture{authority: ad, parent: pd, expires: f.grant.GrantExpiresAt}
	f.driver = &ownedPreparationDriverFixture{burn: f.burn, authority: ad, allocation: allocation}
	f.bridge, err = NewBoundedOwnedAttemptPreparationV1(issuer, a.HarnessBinding.Backend, a.SubstrateBinding, f.driver, f.burn)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func requireZeroOwnedPrepared(t testing.TB, prepared PreparedInvocation, err error) {
	t.Helper()
	if err == nil || !reflect.DeepEqual(prepared, PreparedInvocation{}) {
		t.Fatalf("failure leaked prepared capability: error=%v zero=%t", err, reflect.DeepEqual(prepared, PreparedInvocation{}))
	}
}

func TestBoundedOwnedPreparationActualPreflightAfterDurableBurn(t *testing.T) {
	f := newOwnedPreparationFixture(t)
	prepared, err := f.bridge.Prepare(t.Context(), f.grant)
	if err != nil || f.issuer.Validate(prepared) != nil || f.driver.calls.Load() != 1 {
		t.Fatalf("actual bounded preflight: %v / %v calls=%d", err, f.issuer.Validate(prepared), f.driver.calls.Load())
	}
	if !reflect.DeepEqual(prepared.Allocation(), f.driver.allocation) || !reflect.DeepEqual(prepared.Reservation(), f.grant.Reservation) || !prepared.ExecuteDeadline().Equal(f.grant.GrantExpiresAt) {
		t.Fatal("prepared capability did not preserve actual observation and original parent")
	}
	copy := prepared.Allocation()
	copy.InProcess.LinkedBackendProfileDigest = strings.Repeat("f", 64)
	if f.issuer.Validate(prepared) != nil {
		t.Fatal("returned observation aliased prepared capability")
	}
	// Both public serving registries continue denying owned execution. This
	// constructor is not installed there and grants no dispatch API.
	registry, err := NewSubstrateRegistryV1(func() time.Time { return f.now }, f.issuer, SubstrateRegistrationV1{Binding: f.grant.Authority.SubstrateBinding, Enabled: true, Driver: &substrateRegistryRecordingDriver{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.resolve(f.grant.Authority, true); err == nil {
		t.Fatal("public registry accepted owned authority")
	}
}

func TestBoundedOwnedPreparationNoRetryAcrossRemintAndIssuerRestart(t *testing.T) {
	for _, outcome := range []string{"success", "partial_unknown", "invalid_observation", "lost_commit_ack"} {
		t.Run(outcome, func(t *testing.T) {
			f := newOwnedPreparationFixture(t)
			switch outcome {
			case "partial_unknown":
				f.driver.failure = errors.New("private-sensitive-scripted-details")
			case "invalid_observation":
				f.driver.invalid = true
			case "lost_commit_ack":
				f.burn.unknownCommit = true
			}
			prepared, err := f.bridge.Prepare(t.Context(), f.grant)
			if outcome == "success" {
				if err != nil || f.issuer.Validate(prepared) != nil {
					t.Fatalf("initial success: %v", err)
				}
			} else {
				requireZeroOwnedPrepared(t, prepared, err)
				if strings.Contains(err.Error(), "private-sensitive") {
					t.Fatal("public error exposed driver details")
				}
			}
			wantCalls := uint64(1)
			if outcome == "lost_commit_ack" {
				wantCalls = 0
			}
			reminted, err := f.issuer.MintAttemptEffectOwnershipGrant(f.grant.Authority, f.grant.Reservation, f.grant.GrantExpiresAt)
			if err != nil {
				t.Fatal(err)
			}
			p, err := f.bridge.Prepare(t.Context(), reminted)
			requireZeroOwnedPrepared(t, p, err)
			// Restart loses the process-local map and key, but NOT the committed
			// burn. Recover an authentic original grant with the new issuer.
			restarted, err := NewCapabilityIssuer(func() time.Time { return f.now }, bytes.NewReader(bytes.Repeat([]byte{24}, 32)))
			if err != nil {
				t.Fatal(err)
			}
			grant, err := restarted.MintAttemptEffectOwnershipGrant(f.grant.Authority, f.grant.Reservation, f.grant.GrantExpiresAt)
			if err != nil {
				t.Fatal(err)
			}
			bridge, err := NewBoundedOwnedAttemptPreparationV1(restarted, f.grant.Authority.HarnessBinding.Backend, f.grant.Authority.SubstrateBinding, f.driver, f.burn)
			if err != nil {
				t.Fatal(err)
			}
			p, err = bridge.Prepare(t.Context(), grant)
			requireZeroOwnedPrepared(t, p, err)
			burned, burns := f.burn.snapshot()
			if !burned || burns != 2 || f.driver.calls.Load() != wantCalls {
				t.Fatalf("restart prepared replacement: burned=%t burns=%d preflights=%d want=%d", burned, burns, f.driver.calls.Load(), wantCalls)
			}
		})
	}
}

func TestBoundedOwnedPreparationDeniedAndUncertainBurnNeverPreflight(t *testing.T) {
	for _, outcome := range []string{"denied", "uncertain", "already_burned", "expired_after_burn", "cancelled_after_burn"} {
		t.Run(outcome, func(t *testing.T) {
			f := newOwnedPreparationFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			switch outcome {
			case "denied":
				f.burn.deny = true
			case "uncertain":
				f.burn.failure = true
			case "already_burned":
				f.burn.burned = true
			case "expired_after_burn":
				f.burn.after = func() { f.now = f.grant.GrantExpiresAt }
			case "cancelled_after_burn":
				f.burn.after = cancel
			}
			p, err := f.bridge.Prepare(ctx, f.grant)
			requireZeroOwnedPrepared(t, p, err)
			p, err = f.bridge.Prepare(ctx, f.grant)
			requireZeroOwnedPrepared(t, p, err)
			_, burns := f.burn.snapshot()
			if burns != 1 || f.driver.calls.Load() != 0 {
				t.Fatalf("denied/unknown burn retried: burns=%d preflights=%d", burns, f.driver.calls.Load())
			}
		})
	}
}

func TestBoundedOwnedPreparationRejectsUntrustedAndChangedFixedBindings(t *testing.T) {
	for _, denied := range []string{"authenticator", "nil_context", "cancelled", "expired", "single_turn", "other_backend", "other_substrate"} {
		t.Run(denied, func(t *testing.T) {
			f := newOwnedPreparationFixture(t)
			ctx := t.Context()
			grant := f.grant.Clone()
			switch denied {
			case "authenticator":
				grant.Authenticator[0] ^= 1
			case "nil_context":
				ctx = nil
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "expired":
				f.now = grant.GrantExpiresAt
			case "single_turn", "other_backend", "other_substrate":
				a := grant.Authority.Clone()
				allocation := f.driver.allocation.Clone()
				if denied == "single_turn" {
					a, _, allocation, _ = capabilityFixture(t)
				} else if denied == "other_backend" {
					a.HarnessBinding.Backend.ArtifactDigest = strings.Repeat("f", 64)
					a.HarnessBinding.AgentLoopPolicy.Backend = a.HarnessBinding.Backend
				} else {
					a.SubstrateBinding.ProfileRevision++
				}
				resealOwnedPreparationAuthority(t, &a, &allocation)
				parent, err := domain.BuildAttemptEffectReservationV1(a, grant.Reservation.PhysicalInvocationClaimID, nil, f.now)
				if err != nil {
					t.Fatal(err)
				}
				grant, err = f.issuer.MintAttemptEffectOwnershipGrant(a, parent, grant.GrantExpiresAt)
				if err != nil {
					t.Fatal(err)
				}
			}
			p, err := f.bridge.Prepare(ctx, grant)
			requireZeroOwnedPrepared(t, p, err)
			_, burns := f.burn.snapshot()
			if burns != 0 || f.driver.calls.Load() != 0 {
				t.Fatalf("unauthenticated or foreign binding caused IO: burns=%d preflight=%d", burns, f.driver.calls.Load())
			}
		})
	}
}

func TestBoundedOwnedPreparationConcurrentRestartedIssuersOnePhysicalPreflight(t *testing.T) {
	f := newOwnedPreparationFixture(t)
	other, err := NewCapabilityIssuer(func() time.Time { return f.now }, bytes.NewReader(bytes.Repeat([]byte{25}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := other.MintAttemptEffectOwnershipGrant(f.grant.Authority, f.grant.Reservation, f.grant.GrantExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
	bridge, err := NewBoundedOwnedAttemptPreparationV1(other, grant.Authority.HarnessBinding.Backend, grant.Authority.SubstrateBinding, f.driver, f.burn)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		p      PreparedInvocation
		err    error
		issuer *CapabilityIssuer
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	t.Cleanup(wg.Wait)
	for _, candidate := range []struct {
		b *BoundedOwnedAttemptPreparationV1
		g ports.AttemptEffectOwnershipGrantV1
	}{{f.bridge, f.grant}, {bridge, grant}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p, err := candidate.b.Prepare(t.Context(), candidate.g)
			results <- result{p, err, candidate.b.issuer}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	success := 0
	for result := range results {
		if result.err == nil {
			success++
			if result.issuer.Validate(result.p) != nil {
				t.Fatal("winner did not receive authentic actual observation")
			}
		} else {
			requireZeroOwnedPrepared(t, result.p, result.err)
		}
	}
	_, burns := f.burn.snapshot()
	if success != 1 || burns != 2 || f.driver.calls.Load() != 1 {
		t.Fatalf("durable concurrent fence: winners=%d burns=%d preflights=%d", success, burns, f.driver.calls.Load())
	}
}

func TestBoundedOwnedPreparationPostPreflightExpiryAndCancellationReturnZero(t *testing.T) {
	for _, failure := range []string{"expiry", "cancellation"} {
		t.Run(failure, func(t *testing.T) {
			f := newOwnedPreparationFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			if failure == "expiry" {
				f.driver.before = func() { f.now = f.grant.GrantExpiresAt }
			} else {
				f.driver.before = cancel
			}
			p, err := f.bridge.Prepare(ctx, f.grant)
			requireZeroOwnedPrepared(t, p, err)
			if f.driver.calls.Load() != 1 {
				t.Fatal("control did not exercise actual preflight")
			}
		})
	}
}

func TestBoundedOwnedPreparationConstructorClosedFixtureOnly(t *testing.T) {
	for _, denied := range []string{"issuer", "driver", "burn", "backend", "credential_backend", "live_substrate"} {
		t.Run(denied, func(t *testing.T) {
			f := newOwnedPreparationFixture(t)
			issuer, backend, binding := f.issuer, f.grant.Authority.HarnessBinding.Backend, f.grant.Authority.SubstrateBinding
			var driver OwnedAttemptPreflightV1 = f.driver
			var burn OwnedAttemptPreparationBurnV1 = f.burn
			switch denied {
			case "issuer":
				issuer = nil
			case "driver":
				driver = nil
			case "burn":
				burn = nil
			case "backend":
				backend.BackendProfileDigest = strings.Repeat("f", 64)
			case "credential_backend":
				backend.BackendKind, backend.ProviderContractKind, backend.CredentialDeliveryKind = domain.HarnessBackendDirectOpenRouterV1, domain.ProviderContractInvocationV1, domain.ProviderCredentialDeliveryDirectV1
			case "live_substrate":
				binding.Kind = domain.SubstrateCloudRunJobV1
			}
			bridge, err := NewBoundedOwnedAttemptPreparationV1(issuer, backend, binding, driver, burn)
			if bridge != nil || err == nil {
				t.Fatal("constructor allowed missing trust boundary or non-exact fixture implementation")
			}
		})
	}
}
