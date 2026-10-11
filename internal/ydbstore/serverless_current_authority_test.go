package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const currentDeletionSQL = `SELECT SUBSTRING(CAST(session_id AS String),0,161) FROM session_deletions WHERE tenant_id=$1 AND session_id=$2`

func currentSessionAccessFixture(t *testing.T) (*computeChoiceSourceFixture, ports.WorkerJobState) {
	t.Helper()
	f := newComputeChoiceSourceFixture(t)
	s := currentWorkerAuthorityFixture(t)
	f.at = s.Run.CreatedAt.Add(time.Second)
	f.cookie.ActiveTenantID, f.cookie.UserID = s.Run.TenantID, s.Job.CredentialOwnerUserID
	f.session = domain.Session{ID: s.Run.SessionID, TenantID: s.Run.TenantID, CreatedBy: "different-creator", Status: domain.SessionActive, LastEventSequence: 1, CreatedAt: s.Run.CreatedAt, UpdatedAt: s.Run.CreatedAt}
	f.member = domain.TenantMembership{TenantID: s.Run.TenantID, UserID: s.Job.CredentialOwnerUserID, Role: domain.TenantMembershipMember, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: s.Run.CreatedAt, UpdatedAt: s.Run.CreatedAt}
	f.participant = domain.SessionParticipant{TenantID: s.Run.TenantID, SessionID: s.Run.SessionID, UserID: s.Job.CredentialOwnerUserID, Role: domain.SessionParticipantMember, Status: domain.SessionParticipantActive, CreatedAt: s.Run.CreatedAt, UpdatedAt: s.Run.CreatedAt}
	syncCurrentAccessRows(t, f, s)
	return f, s
}

func syncCurrentAccessRows(t *testing.T, f *computeChoiceSourceFixture, s ports.WorkerJobState) {
	t.Helper()
	f.syncRows(t)
	f.driver.expected[currentDeletionSQL] = []driver.Value{string(s.Run.TenantID), string(s.Run.SessionID)}
	delete(f.driver.rows, currentDeletionSQL)
}

func readCurrentAccessFixture(t *testing.T, f *computeChoiceSourceFixture, s ports.WorkerJobState) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return f.store.Transact(ctx, s.Run.TenantID, func(state ports.StateTx) error {
		return currentServerlessSessionPrincipalTx(ctx, state.(*stateTx).sqlTx, s, f.at)
	})
}

func TestCurrentServerlessSessionAuthorityUsesCanonicalWritePrincipal(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*computeChoiceSourceFixture)
	}{
		{"valid_member_not_creator", func(*computeChoiceSourceFixture) {}},
		{"valid_owner", func(f *computeChoiceSourceFixture) {
			f.member.Role = domain.TenantMembershipOwner
			f.participant.Role = domain.SessionParticipantOwner
		}},
		{"session_tenant", func(f *computeChoiceSourceFixture) { f.session.TenantID = "foreign-tenant" }},
		{"session_id", func(f *computeChoiceSourceFixture) { f.session.ID = "foreign-session" }},
		{"session_archived", func(f *computeChoiceSourceFixture) {
			f.session.Status = domain.SessionArchived
			at := f.at
			f.session.ArchivedAt = &at
		}},
		{"future_session", func(f *computeChoiceSourceFixture) { f.session.UpdatedAt = f.at.Add(time.Microsecond) }},
		{"membership_tenant", func(f *computeChoiceSourceFixture) { f.member.TenantID = "foreign-tenant" }},
		{"membership_user", func(f *computeChoiceSourceFixture) { f.member.UserID = "foreign-user" }},
		{"membership_viewer", func(f *computeChoiceSourceFixture) { f.member.Role = domain.TenantMembershipViewer }},
		{"membership_suspended", func(f *computeChoiceSourceFixture) { f.member.Status = domain.TenantMembershipSuspended }},
		{"membership_revoked", func(f *computeChoiceSourceFixture) { f.member.Status = domain.TenantMembershipRevoked }},
		{"membership_zero_version", func(f *computeChoiceSourceFixture) { f.member.SecurityVersion = 0 }},
		{"future_membership", func(f *computeChoiceSourceFixture) { f.member.UpdatedAt = f.at.Add(time.Microsecond) }},
		{"participant_tenant", func(f *computeChoiceSourceFixture) { f.participant.TenantID = "foreign-tenant" }},
		{"participant_session", func(f *computeChoiceSourceFixture) { f.participant.SessionID = "foreign-session" }},
		{"participant_user", func(f *computeChoiceSourceFixture) { f.participant.UserID = "foreign-user" }},
		{"participant_viewer", func(f *computeChoiceSourceFixture) { f.participant.Role = domain.SessionParticipantViewer }},
		{"participant_removed", func(f *computeChoiceSourceFixture) { f.participant.Status = domain.SessionParticipantRemoved }},
		{"future_participant", func(f *computeChoiceSourceFixture) { f.participant.UpdatedAt = f.at.Add(time.Microsecond) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f, s := currentSessionAccessFixture(t)
			c.mutate(f)
			syncCurrentAccessRows(t, f, s)
			err := readCurrentAccessFixture(t, f, s)
			want := strings.HasPrefix(c.name, "valid_")
			if (err == nil) != want {
				t.Fatalf("case=%s current-principal error=%v wantAuthorized=%t", c.name, err, want)
			}
			if want && (f.driver.commits != 1 || f.driver.deadlineMissing || len(f.driver.statements) != 4 || f.driver.isolations[0] != driver.IsolationLevel(sql.LevelSerializable)) {
				t.Fatalf("point/SAME-tx evidence: %+v", f.driver.ratedSQLFixture)
			}
		})
	}
}

func TestCurrentServerlessSessionAuthorityDeniesMissingAndCorruptProjections(t *testing.T) {
	for _, name := range []string{"missing_session", "missing_membership", "missing_participant", "deletion_row", "session_scalar", "membership_scalar", "participant_scalar", "oversize_record", "duplicate_field", "unknown_field"} {
		t.Run(name, func(t *testing.T) {
			f, s := currentSessionAccessFixture(t)
			switch name {
			case "missing_session":
				delete(f.driver.rows, computeChoiceSessionSQL)
			case "missing_membership":
				delete(f.driver.rows, computeChoiceMembershipSQL)
			case "missing_participant":
				delete(f.driver.rows, computeChoiceParticipantSQL)
			case "deletion_row":
				f.driver.rows[currentDeletionSQL] = []driver.Value{string(s.Run.SessionID)}
			case "session_scalar":
				f.driver.rows[computeChoiceSessionSQL][2] = string(domain.SessionArchived)
			case "membership_scalar":
				f.driver.rows[computeChoiceMembershipSQL][2] = string(domain.TenantMembershipSuspended)
			case "participant_scalar":
				f.driver.rows[computeChoiceParticipantSQL][2] = string(domain.SessionParticipantRemoved)
			case "oversize_record":
				f.driver.rows[computeChoiceSessionSQL][0] = strings.Repeat(" ", 8193)
			case "duplicate_field":
				f.driver.rows[computeChoiceSessionSQL][0] = strings.Replace(f.driver.rows[computeChoiceSessionSQL][0].(string), `"status":"active"`, `"status":"active","status":"active"`, 1)
			case "unknown_field":
				f.driver.rows[computeChoiceSessionSQL][0] = strings.Replace(f.driver.rows[computeChoiceSessionSQL][0].(string), `"status":"active"`, `"status":"active","approved":true`, 1)
			}
			if err := readCurrentAccessFixture(t, f, s); !errors.Is(err, domain.ErrMembershipDenied) {
				t.Fatalf("case=%s error=%v want current access denied", name, err)
			}
			if f.driver.commits != 0 {
				t.Fatalf("case=%s unauthorized transaction committed", name)
			}
		})
	}
}

func TestCurrentServerlessPrincipalRereadsAfterTransactionConflict(t *testing.T) {
	f, s := currentSessionAccessFixture(t)
	f.driver.commitModes = []string{"rollback"}
	f.driver.afterCommit = func() {
		f.participant.Status = domain.SessionParticipantRemoved
		syncCurrentAccessRows(t, f, s)
	}
	if err := readCurrentAccessFixture(t, f, s); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("revoked on retry error=%v", err)
	}
	if len(f.driver.isolations) != 2 || f.driver.commits != 1 || len(f.driver.statements) != 8 {
		t.Fatalf("authority not reread on same-tx retry: %+v", f.driver.ratedSQLFixture)
	}
}

func currentProviderAccessFixture(t *testing.T, kind domain.ProviderResourceKindV1) (*computeChoiceSourceFixture, domain.HarnessBindingV2, domain.ProviderCredentialBindingV1) {
	t.Helper()
	f, s := currentSessionAccessFixture(t)
	binding := s.Job.HarnessBinding.Clone()
	binding.Backend.BackendKind = domain.HarnessBackendDirectOpenRouterV1
	binding.Backend.ProviderContractKind = domain.ProviderContractInvocationV1
	binding.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryDirectV1
	expires := f.at.Add(time.Hour)
	binding.EvidenceExpiresAt = &expires
	ref, err := domain.NewCredentialSecretRef("isolated-native-reference")
	if err != nil {
		t.Fatal(err)
	}
	current := domain.ProviderCredentialBindingV1{Version: 1, TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, ResourceKind: kind, ResourceID: "isolated-provider", ResourceRevision: 2, CredentialGeneration: 3, CandidateMutationID: "fixture-mutation", State: domain.ProviderCredentialActiveV1, SecretRef: ref, SecretFingerprint: domain.FingerprintCredential([]byte("fixture-only-not-a-credential")), UpdatedAt: s.Run.CreatedAt}
	binding.Resource, err = current.ResourceBinding()
	if err != nil {
		t.Fatal(err)
	}
	if err := binding.ValidateAt(f.at); err != nil {
		t.Fatal(err)
	}
	f.driver.expected[providerCredentialReadSQL] = []driver.Value{string(binding.TenantID), string(binding.OwnerUserID), string(kind), binding.Resource.ResourceID}
	syncCurrentProviderRow(t, f, current)
	return f, binding, current
}

func syncCurrentProviderRow(t *testing.T, f *computeChoiceSourceFixture, current domain.ProviderCredentialBindingV1) {
	t.Helper()
	f.driver.rows[providerCredentialReadSQL] = []driver.Value{int64(current.ResourceRevision), int64(current.CredentialGeneration), current.CandidateMutationID, string(current.State), current.SecretRef.StorageValue(), string(current.SecretFingerprint), current.UpdatedAt, choiceFixtureJSON(t, current)}
}

func readCurrentProviderFixture(t *testing.T, f *computeChoiceSourceFixture, binding domain.HarnessBindingV2) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return f.store.Transact(ctx, binding.TenantID, func(state ports.StateTx) error {
		return currentServerlessProviderResourceTx(ctx, state.(*stateTx).sqlTx, binding, f.at)
	})
}

func TestCurrentServerlessProviderPrerequisiteUsesExistingCurrentResource(t *testing.T) {
	for _, kind := range []domain.ProviderResourceKindV1{domain.ProviderResourceAPIAccountV1, domain.ProviderResourceRouterAccountV1} {
		t.Run(string(kind), func(t *testing.T) {
			for _, name := range []string{"valid", "missing", "revoked", "changed_revision", "changed_generation", "foreign_tenant", "foreign_owner", "foreign_kind", "foreign_id", "future_time", "revision_scalar", "generation_scalar", "state_scalar", "oversize_record", "duplicate_field", "unknown_field"} {
				t.Run(name, func(t *testing.T) {
					f, b, current := currentProviderAccessFixture(t, kind)
					switch name {
					case "revoked":
						current.State = domain.ProviderCredentialRevokedV1
						current.SecretRef = domain.CredentialSecretRef{}
						current.SecretFingerprint = ""
					case "changed_revision":
						current.ResourceRevision++
					case "changed_generation":
						current.CredentialGeneration++
					case "foreign_tenant":
						current.TenantID = "foreign-tenant"
					case "foreign_owner":
						current.OwnerUserID = "foreign-user"
					case "foreign_kind":
						if kind == domain.ProviderResourceAPIAccountV1 {
							current.ResourceKind = domain.ProviderResourceRouterAccountV1
						} else {
							current.ResourceKind = domain.ProviderResourceAPIAccountV1
						}
					case "foreign_id":
						current.ResourceID = "foreign-resource"
					case "future_time":
						current.UpdatedAt = f.at.Add(time.Microsecond)
					}
					syncCurrentProviderRow(t, f, current)
					switch name {
					case "missing":
						delete(f.driver.rows, providerCredentialReadSQL)
					case "revision_scalar":
						f.driver.rows[providerCredentialReadSQL][0] = int64(99)
					case "generation_scalar":
						f.driver.rows[providerCredentialReadSQL][1] = int64(99)
					case "state_scalar":
						f.driver.rows[providerCredentialReadSQL][3] = string(domain.ProviderCredentialRevokedV1)
					case "oversize_record":
						f.driver.rows[providerCredentialReadSQL][7] = strings.Repeat(" ", 8193)
					case "duplicate_field":
						f.driver.rows[providerCredentialReadSQL][7] = strings.Replace(f.driver.rows[providerCredentialReadSQL][7].(string), `"state":"active"`, `"state":"active","state":"active"`, 1)
					case "unknown_field":
						f.driver.rows[providerCredentialReadSQL][7] = strings.Replace(f.driver.rows[providerCredentialReadSQL][7].(string), `"state":"active"`, `"state":"active","approved":true`, 1)
					}
					err := readCurrentProviderFixture(t, f, b)
					want := name == "valid"
					if (err == nil) != want {
						t.Fatalf("kind=%s case=%s resource error=%v wantAuthorized=%t", kind, name, err, want)
					}
					if len(f.driver.statements) != 1 || (want && f.driver.commits != 1) {
						t.Fatalf("current resource not read as exact same-tx point: %+v", f.driver.ratedSQLFixture)
					}
				})
			}
		})
	}
}

func TestCurrentServerlessCredentiallessExceptionRequiresExactFixture(t *testing.T) {
	f, s := currentSessionAccessFixture(t)
	if err := readCurrentProviderFixture(t, f, s.Job.HarnessBinding); err != nil || len(f.driver.statements) != 0 {
		t.Fatalf("exact fixture resource unexpectedly queries credentials: error=%v statements=%d", err, len(f.driver.statements))
	}
	b := s.Job.HarnessBinding.Clone()
	b.Resource.CredentialGeneration = 1
	if err := readCurrentProviderFixture(t, f, b); err == nil {
		t.Fatal("credentialed fixture accepted as credentialless")
	}
}
