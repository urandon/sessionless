package domain_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Only pure original-source DATA is composed here. These fixtures do not
// authenticate an installation, publish funding, admit work or grant I/O.
func localCapacityProjectionFixture(t *testing.T, local, bearer bool) (domain.Run, domain.Attempt, domain.DispatchOutbox, domain.WorkerJob) {
	t.Helper()
	profile, at := ownedCurrentProfileFixture(t)
	if local {
		profile = localSpendingProfileFixture(t, bearer)
		at = profile.Window.PublishedAt
	}
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: profile.Locator.TenantID, OwnerUserID: profile.Locator.OwnerUserID, SessionID: "session-189", RunID: "run-189", AttemptID: "attempt-189"}
	if local {
		scope.SessionID, scope.RunID, scope.AttemptID = "local-session", "local-run", "local-attempt"
	}
	kinds := [4]domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1, domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1}
	var spending [4]domain.AttemptSpendingAuthorizationCurrentV1
	for i, kind := range kinds {
		if local {
			spending[i] = localSpendingFixture(t, profile, kind)
		} else {
			spending[i] = ownedSpendingFixture(t, profile, kind)
		}
	}
	pkg := mustAdmissionPackage(t, scope, profile, spending, at)
	raw, err := domain.EncodeAttemptExecutionSourceV1(pkg.Source)
	if err != nil {
		t.Fatal("encode original strict source", err)
	}
	run := domain.Run{ID: scope.RunID, TenantID: scope.TenantID, SessionID: scope.SessionID, TriggerEventID: "event", SubscriptionConnectionID: "subscription", Status: domain.RunQueued, IdempotencyKey: "local-capacity-run", CreatedAt: at, UpdatedAt: at}
	if local {
		run.SubscriptionConnectionID = ""
		run.LocalCapacityAssociation = &domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID, ResourceID: profile.Resource.ResourceID}
	}
	attempt := domain.Attempt{ID: scope.AttemptID, TenantID: scope.TenantID, RunID: scope.RunID, Number: 1, Status: domain.AttemptCreated, CreatedAt: at, UpdatedAt: at}
	origin := &domain.FrontendEventOrigin{BindingID: "binding", BindingRevision: 1, Frontend: domain.FrontendWeb, ExternalConversationID: "conversation", ExternalEventID: "event"}
	blob := domain.BlobRef{TenantID: scope.TenantID, Key: "tenants/" + string(scope.TenantID) + "/context", Size: 1, SHA256: strings.Repeat("c", 64)}
	job := domain.WorkerJob{TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID, SessionID: scope.SessionID, TriggerEventID: run.TriggerEventID,
		ReservationID: "reservation", InputManifestID: "manifest", ContextSnapshot: blob, CredentialOwnerUserID: scope.OwnerUserID,
		ExecutionPlacementV2: pkg.Placement, HarnessBinding: pkg.Binding, SubstrateBinding: &pkg.Substrate, AdmissionCostCeiling: &pkg.Cost,
		LocalCapacityAssociation: run.LocalCapacityAssociation, OwnedExecutionSourceRecord: string(raw), Origin: origin, CreatedAt: at,
		Limits: domain.ProductLimits{MaxTenantQueueDepth: 1, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 1, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxArtifacts: 1}}
	outbox := domain.DispatchOutbox{ID: "dispatch", TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID, InputManifestID: job.InputManifestID, ContextSnapshot: blob,
		CredentialOwnerUserID: scope.OwnerUserID, ExecutionPlacementV2: pkg.Placement, HarnessBinding: pkg.Binding, SubstrateBinding: &pkg.Substrate, AdmissionCostCeiling: &pkg.Cost,
		LocalCapacityAssociation: run.LocalCapacityAssociation, OwnedExecutionSourceRecord: string(raw), Origin: origin, Status: domain.DispatchPending, IdempotencyKey: "dispatch-intent", CreatedAt: at, UpdatedAt: at}
	if err := job.ValidateForRun(run); err != nil {
		t.Fatal("original coherent job fixture", err)
	}
	if err := outbox.ValidateForAttempt(run, attempt); err != nil {
		t.Fatal("original coherent outbox fixture", err)
	}
	return run, attempt, outbox, job
}

func TestLocalCapacityProjectionRejectsCrossArmAndForeignAssociation(t *testing.T) {
	for _, fault := range []string{"missing projection", "foreign resource", "foreign owner", "foreign tenant", "missing run arm", "legacy run", "legacy projection", "mixed run", "nonlocal source", "source missing", "source foreign"} {
		t.Run(fault, func(t *testing.T) {
			run, attempt, outbox, job := localCapacityProjectionFixture(t, true, false)
			a := *run.LocalCapacityAssociation
			job.LocalCapacityAssociation, outbox.LocalCapacityAssociation = &a, &a
			switch fault {
			case "missing projection":
				job.LocalCapacityAssociation, outbox.LocalCapacityAssociation = nil, nil
			case "foreign resource":
				a.ResourceID = "foreign"
			case "foreign owner":
				a.OwnerUserID = "foreign"
			case "foreign tenant":
				a.TenantID = "foreign"
			case "missing run arm":
				run.LocalCapacityAssociation = nil
			case "legacy run":
				run.LocalCapacityAssociation, run.SubscriptionConnectionID = nil, "subscription"
			case "legacy projection":
				job.LocalCapacityAssociation, outbox.LocalCapacityAssociation = nil, nil
				run.LocalCapacityAssociation, run.SubscriptionConnectionID = nil, "subscription"
			case "mixed run":
				run.SubscriptionConnectionID = "subscription"
			case "nonlocal source":
				legacyRun, legacyAttempt, legacyOutbox, legacyJob := localCapacityProjectionFixture(t, false, false)
				a.TenantID, a.OwnerUserID, a.ResourceID = legacyRun.TenantID, legacyJob.CredentialOwnerUserID, legacyJob.HarnessBinding.Resource.ResourceID
				legacyRun.SubscriptionConnectionID, legacyRun.LocalCapacityAssociation = "", &a
				legacyJob.LocalCapacityAssociation, legacyOutbox.LocalCapacityAssociation = &a, &a
				run, attempt, job, outbox = legacyRun, legacyAttempt, legacyJob, legacyOutbox
			case "source missing":
				job.OwnedExecutionSourceRecord, outbox.OwnedExecutionSourceRecord = "", ""
			case "source foreign":
				source, err := domain.DecodeAttemptExecutionSourceV1([]byte(job.OwnedExecutionSourceRecord))
				if err != nil {
					t.Fatal(err)
				}
				source.Scope.SessionID = "foreign"
				for i := range source.Spending {
					source.Spending[i].SessionID = source.Scope.SessionID
					bindOwnedSpendingSource(t, &source.Spending[i])
				}
				body, err := domain.EncodeAttemptExecutionSourceV1(source)
				if err != nil {
					t.Fatal("coherent foreign-Session source must remain strictly valid DATA", err)
				}
				job.OwnedExecutionSourceRecord, outbox.OwnedExecutionSourceRecord = string(body), string(body)
			}
			if err := job.ValidateForRun(run); err == nil {
				t.Errorf("job accepted fault=%s", fault)
			}
			if err := outbox.ValidateForAttempt(run, attempt); err == nil {
				t.Errorf("outbox accepted fault=%s", fault)
			}
		})
	}
}

func TestLocalCapacityProjectionLegacyJSONUnchanged(t *testing.T) {
	_, _, outbox, job := localCapacityProjectionFixture(t, false, false)
	for _, pair := range []struct{ original, cloned any }{{job, job.Clone()}, {outbox, outbox.Clone()}} {
		raw, err := json.Marshal(pair.original)
		if err != nil || bytes.Contains(raw, []byte("local_capacity_association")) {
			t.Fatalf("legacy projection gained local fields: marshalErr=%t", err != nil)
		}
		cloned, err := json.Marshal(pair.cloned)
		if err != nil || !bytes.Equal(raw, cloned) {
			t.Fatalf("legacy projection Clone changed bytes: marshalErr=%t", err != nil)
		}
	}
}

func TestLocalCapacityProjectionCloneOwnsAllMutableFields(t *testing.T) {
	_, _, outbox, job := localCapacityProjectionFixture(t, true, false)
	version := uint64(1)
	job.ContextWindow = &domain.SessionContextWindow{SnapshotVersion: &version, AfterSequence: 1, ThroughSequence: 2}
	workspace, skill := job.ContextSnapshot, job.ContextSnapshot
	job.WorkspaceSnapshot, job.SkillBundle = &workspace, &skill
	job.AllowedMCPServers = []string{"original"}
	outbox.ContextWindow, outbox.WorkspaceSnapshot, outbox.SkillBundle = job.ContextWindow, job.WorkspaceSnapshot, job.SkillBundle
	outbox.AllowedMCPServers = job.AllowedMCPServers
	originalJob, _ := json.Marshal(job)
	originalOutbox, _ := json.Marshal(outbox)
	j, o := job.Clone(), outbox.Clone()
	if !reflect.DeepEqual(j, job) || !reflect.DeepEqual(o, outbox) {
		t.Fatal("clone changed original projection values")
	}
	j.LocalCapacityAssociation.ResourceID, o.LocalCapacityAssociation.ResourceID = "foreign-job", "foreign-outbox"
	*j.ContextWindow.SnapshotVersion, *o.ContextWindow.SnapshotVersion = 7, 8
	j.ContextWindow.ThroughSequence, o.ContextWindow.ThroughSequence = 7, 8
	j.WorkspaceSnapshot.Key, o.WorkspaceSnapshot.Key = "foreign-job", "foreign-outbox"
	j.SkillBundle.Key, o.SkillBundle.Key = "foreign-job", "foreign-outbox"
	j.AllowedMCPServers[0], o.AllowedMCPServers[0] = "foreign-job", "foreign-outbox"
	j.Origin.BindingRevision, o.Origin.BindingRevision = 7, 8
	j.SubstrateBinding.ProfileID, o.SubstrateBinding.ProfileID = "foreign-job", "foreign-outbox"
	*j.AdmissionCostCeiling.MaxTotalAmountMicrounits, *o.AdmissionCostCeiling.MaxTotalAmountMicrounits = 7, 8
	*j.AdmissionCostCeiling.MCPToolCost.MaxAmountMicrounits, *o.AdmissionCostCeiling.MCPToolCost.MaxAmountMicrounits = 7, 8
	j.HarnessBinding.AgentLoopPolicy.InputModalities[0], o.HarnessBinding.AgentLoopPolicy.InputModalities[0] = domain.AgentInputFileV1, domain.AgentInputImageV1
	*j.HarnessBinding.AgentLoopPolicy.AdmissionCostCeiling.MaxTotalAmountMicrounits, *o.HarnessBinding.AgentLoopPolicy.AdmissionCostCeiling.MaxTotalAmountMicrounits = 7, 8
	*j.HarnessBinding.EvidenceExpiresAt, *o.HarnessBinding.EvidenceExpiresAt = j.CreatedAt, o.CreatedAt
	gotJob, _ := json.Marshal(job)
	gotOutbox, _ := json.Marshal(outbox)
	if !bytes.Equal(gotJob, originalJob) || !bytes.Equal(gotOutbox, originalOutbox) {
		t.Fatal("caller mutated original projection through clone pointers/slices")
	}
	for _, empty := range [][]string{nil, {}} {
		job.AllowedMCPServers, outbox.AllowedMCPServers = empty, empty
		if !reflect.DeepEqual(job.Clone().AllowedMCPServers, empty) || !reflect.DeepEqual(outbox.Clone().AllowedMCPServers, empty) {
			t.Fatal("clone changed nil/empty MCP collection distinction")
		}
	}
}

func TestLocalCapacityProjectionCanonicalRoundTrip(t *testing.T) {
	for _, bearer := range []bool{false, true} {
		name := "noauth"
		if bearer {
			name = "bearer"
		}
		t.Run(name, func(t *testing.T) {
			run, attempt, outbox, job := localCapacityProjectionFixture(t, true, bearer)
			jobRaw, err := json.Marshal(job)
			if err != nil {
				t.Fatal("marshal original local job", err)
			}
			outboxRaw, err := json.Marshal(outbox)
			if err != nil {
				t.Fatal("marshal original local outbox", err)
			}
			var decodedJob domain.WorkerJob
			var decodedOutbox domain.DispatchOutbox
			if err := json.Unmarshal(jobRaw, &decodedJob); err != nil {
				t.Fatal("decode original local job", err)
			}
			if err := json.Unmarshal(outboxRaw, &decodedOutbox); err != nil {
				t.Fatal("decode original local outbox", err)
			}
			if err := decodedJob.ValidateForRun(run); err != nil {
				t.Fatal("decoded local job lost original source/association", err)
			}
			if err := decodedOutbox.ValidateForAttempt(run, attempt); err != nil {
				t.Fatal("decoded local outbox lost original source/association", err)
			}
			if !reflect.DeepEqual(decodedJob.LocalCapacityAssociation, run.LocalCapacityAssociation) ||
				!reflect.DeepEqual(decodedOutbox.LocalCapacityAssociation, run.LocalCapacityAssociation) ||
				decodedJob.OwnedExecutionSourceRecord != job.OwnedExecutionSourceRecord || decodedOutbox.OwnedExecutionSourceRecord != outbox.OwnedExecutionSourceRecord {
				t.Fatal("round trip changed exact original association/source bytes")
			}
		})
	}
}
