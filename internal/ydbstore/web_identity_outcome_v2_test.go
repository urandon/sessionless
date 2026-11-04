package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Controlled transactional SQL DATA only, not native isolation/provider proof.
// The operation and phase transitions below use the actual private Store paths.
func identityOutcomeFixtureV2(t *testing.T, kind domain.IdentityOperationKindV2, provider domain.IdentityProvider, phase domain.IdentityOperationPhaseV2) (*identityProofSQLV2, *Store, context.Context, identityPhaseRequestV2, identityConfigurationV2, domain.IdentityOperationV2) {
	t.Helper()
	f, store, ctx, start, config := newIdentityProofStorageV2(t, kind)
	if provider == domain.IdentityProviderTelegram {
		// Canonical fixture inputs are selected BEFORE the original start; no
		// live operation/proof/scope is rebound to manufacture authorization.
		state := &f.base.durable
		cookie, err := domain.DecodeWebSessionV2([]byte(state.sessions[string(start.Phase.SessionDigest)]))
		if err != nil {
			t.Fatal(err)
		}
		if kind == domain.IdentityOperationLinkV2 {
			old := cookie.Session.AuthenticatedSubject
			identity, err := domain.DecodeExternalIdentityV2([]byte(state.forward[identityRowsV2SubjectKey(old)]))
			if err != nil {
				t.Fatal(err)
			}
			delete(state.forward, identityRowsV2SubjectKey(old))
			delete(state.reverse, identityRowsV2ReverseKey(identity.UserID, old))
			identity.Subject.Provider = provider
			state.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, identity)
			state.reverse[identityRowsV2ReverseKey(identity.UserID, identity.Subject)] = identityRowsV2Projection{user: identity.UserID, subject: identity.Subject, created: identity.CreatedAt}
			start.TargetProvider = domain.IdentityProviderYandex
		} else {
			start.TargetProvider = domain.IdentityProviderTelegram
		}
		cookie.Session.AuthenticatedSubject.Provider = provider
		state.sessions[string(start.Phase.SessionDigest)] = identityRowsV2JSON(t, cookie)
	}
	operation, err := store.startIdentityOperationV2(ctx, start, config)
	if err != nil {
		t.Fatal("actual original operation start", err)
	}
	for operation.Phase != phase {
		if operation.Phase == domain.IdentityPhaseConfirmV2 {
			t.Fatal("unsupported requested fixture phase", phase)
		}
		operation = completeIdentityProofStepV2(t, f, store, ctx, start.Phase, config, operation)
	}
	return f, store, ctx, start.Phase, config, operation
}

func requireIdentityOutcomeUnchangedV2(t *testing.T, f *identityProofSQLV2, state identityRowsV2State, children map[domain.SecretDigest][]any) {
	t.Helper()
	if !reflect.DeepEqual(f.base.durable, state) || !reflect.DeepEqual(f.children, children) {
		t.Fatal("outcome access changed canonical identity/cookie/member or child custody")
	}
}

func requireIdentityStopAuditV2(t *testing.T, f *identityProofSQLV2, store *Store, original domain.IdentityOperationV2, outcome domain.IdentityOperationOutcomeV2) {
	t.Helper()
	if len(f.audit) != 1 || len(f.audit[0]) != 12 {
		t.Fatal("stop lacks exactly one atomic security audit")
	}
	values := f.audit[0]
	var event domain.WebSecurityAuditEvent
	if json.Unmarshal([]byte(values[10].(string)), &event) != nil || event.Validate() != nil || event.RequestID != "identity-stop-"+string(original.OperationDigest) || event.Action != domain.WebSecurityIdentityOperationStopped || event.ReasonCode != string(outcome) || event.Provider != original.Pending.TargetProvider || event.UserID != original.Pending.Scope.UserID || event.TenantID != original.Pending.Scope.TenantID || event.SubjectFingerprint != "" || !event.OccurredAt.Equal(f.at) || values[11] != event.OccurredAt.Add(store.operationalRetention) {
		t.Fatal("security stop audit lost exact redacted correlation/retention")
	}
}

func TestIdentityOutcomeV2ActualPhaseCancelAndExpiry(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		for _, kind := range []domain.IdentityOperationKindV2{domain.IdentityOperationLinkV2, domain.IdentityOperationUnlinkV2} {
			phases := []domain.IdentityOperationPhaseV2{domain.IdentityPhaseSourceProofV2, domain.IdentityPhaseConfirmV2}
			if kind == domain.IdentityOperationLinkV2 {
				phases = append(phases, domain.IdentityPhaseTargetProofV2)
			}
			for _, phase := range phases {
				for _, expire := range []bool{false, true} {
					name := string(provider) + "/" + string(kind) + "/" + string(phase) + "/cancel"
					if expire {
						name = strings.TrimSuffix(name, "cancel") + "expire"
					}
					t.Run(name, func(t *testing.T) {
						f, store, ctx, request, config, original := identityOutcomeFixtureV2(t, kind, provider, phase)
						state, children := f.base.durable.clone(), cloneIdentityProofRowsV2(f.children)
						read := identityOutcomeReadV2{OperationDigest: request.OperationDigest, SessionDigest: request.SessionDigest}
						before, writes := f.operation, f.base.writes
						config.SensitiveOperationsEnabled = false
						view, err := store.readIdentityOperationOutcomeV2(ctx, read, config)
						if err != nil || view.Phase != phase || view.Kind != kind || view.Outcome != "" || view.BeforeRevision != request.ExpectedRevision || view.CompletedAt != nil || view.AfterRevision != nil || f.operation != before || f.base.writes != writes {
							t.Fatal("fresh current READ mutated pending or required sensitive enablement", err)
						}
						if expire {
							denied, err := store.expireIdentityOperationV2(ctx, request, config)
							if err == nil || !reflect.DeepEqual(denied, identityOperationOutcomeViewV2{}) || f.operation != before || f.base.writes != writes {
								t.Fatal("pre-deadline expiry invented a stop", err)
							}
							f.at = original.ExpiresAt
							view, err = store.readIdentityOperationOutcomeV2(ctx, read, config)
							if err != nil || view.Phase != phase || view.Outcome != domain.IdentityOutcomeExpiredV2 || view.CompletedAt != nil || f.operation != before || f.base.writes != writes {
								t.Fatal("expired pending READ fabricated receipt/terminal phase or mutated retention", err)
							}
							view, err = store.expireIdentityOperationV2(ctx, request, config)
						} else {
							view, err = store.cancelIdentityOperationV2(ctx, request, config)
						}
						want := domain.IdentityOutcomeCancelledV2
						if expire {
							want = domain.IdentityOutcomeExpiredV2
						}
						if err != nil || view.Phase != domain.IdentityPhaseTerminalV2 || view.Outcome != want || view.CompletedAt == nil || !view.CompletedAt.Equal(f.at) || view.AfterRevision != nil {
							t.Fatal("actual protected stop did not confirm its truthful negative result", err)
						}
						terminal, err := domain.DecodeIdentityOperationV2([]byte(f.operation))
						if err != nil || terminal.Pending != nil || terminal.Receipt == nil || terminal.Receipt.AfterRevision != nil || !terminal.CreatedAt.Equal(original.CreatedAt) || !terminal.ExpiresAt.Equal(original.ExpiresAt) || !terminal.DeleteAfter.Equal(original.CreatedAt.Add(24*time.Hour)) || !terminal.Receipt.CompletedAt.Equal(f.at) {
							t.Fatal("pruned JSON/physical writer lost original lifetime or retained private scope", err)
						}
						requireIdentityStopAuditV2(t, f, store, original, want)
						requireIdentityOutcomeUnchangedV2(t, f, state, children)
						stored, writes := f.operation, f.base.writes
						again, err := store.cancelIdentityOperationV2(ctx, request, config)
						if err != nil || !reflect.DeepEqual(again, view) || f.operation != stored || f.base.writes != writes || len(f.audit) != 1 {
							t.Fatal("exact retry rewrote/retagged an immutable terminal receipt", err)
						}
						got, err := store.readIdentityOperationOutcomeV2(ctx, read, config)
						if err != nil || !reflect.DeepEqual(got, view) || f.base.writes != writes {
							t.Fatal("terminal READ changed receipt or minted replacement cookie", err)
						}
					})
				}
			}
		}
	}
}

func TestIdentityOutcomeV2StopsCannotResumeProofOrSuccess(t *testing.T) {
	f, store, ctx, phase, config, original := identityOutcomeFixtureV2(t, domain.IdentityOperationLinkV2, domain.IdentityProviderYandex, domain.IdentityPhaseConfirmV2)
	if _, err := store.cancelIdentityOperationV2(ctx, phase, config); err != nil {
		t.Fatal(err)
	}
	before, writes := f.operation, f.base.writes
	confirmation := identityConfirmationV2{OperationDigest: phase.OperationDigest, SessionDigest: phase.SessionDigest, CSRFDigest: phase.CSRFDigest, BrowserBindingDigest: phase.BrowserBindingDigest, ExpectedRevision: phase.ExpectedRevision, NextSessionDigest: domain.DigestSecret("outcome-unused-next"), NextCSRFDigest: domain.DigestSecret("outcome-unused-csrf")}
	result, err := store.confirmIdentityOperationV2(ctx, confirmation, config)
	if err != nil || result.Receipt.Outcome != domain.IdentityOutcomeCancelledV2 || !reflect.DeepEqual(result.Session, domain.WebSessionV2{}) {
		t.Fatal("confirmed negative receipt became identity success/cookie", err)
	}
	if _, err := store.createIdentityProofV2(ctx, identityProofChildV2{Phase: phase, ChallengeDigest: original.Pending.SourceChallengeDigest, PKCEVerifier: strings.Repeat("p", 43)}, config); err == nil {
		t.Fatal("terminal operation resumed proof child")
	}
	if _, err := store.consumeIdentityProofV2(ctx, identityProofConsumeV2{Phase: phase, ChallengeDigest: original.Pending.SourceChallengeDigest, BrowserSecret: "proof-browser"}, config); err == nil {
		t.Fatal("terminal operation repeated exchange preparation")
	}
	if f.operation != before || f.base.writes != writes || len(f.audit) != 1 {
		t.Fatal("terminal continuation rewrote the outcome")
	}
}

func TestIdentityOutcomeV2UnknownAndAuthorityErrorsNeverStop(t *testing.T) {
	for _, method := range []string{"read", "cancel", "expire"} {
		faults := []string{"missing-cookie", "revoked-cookie", "stale-identity", "unlinked-identity", "suspended-member", "member-version", "foreign-operation-owner", "future-operation", "malformed-operation", "oversize-operation", "storage", "malformed-config", "disabled-auth-provider"}
		if method != "read" {
			faults = append(faults, "csrf", "browser", "revision", "manifest", "tenant")
		}
		for _, fault := range faults {
			t.Run(method+"/"+fault, func(t *testing.T) {
				f, store, ctx, phase, config, operation := identityOutcomeFixtureV2(t, domain.IdentityOperationLinkV2, domain.IdentityProviderYandex, domain.IdentityPhaseSourceProofV2)
				read := identityOutcomeReadV2{OperationDigest: phase.OperationDigest, SessionDigest: phase.SessionDigest}
				if view, err := store.readIdentityOperationOutcomeV2(ctx, read, config); err != nil || view.Phase != operation.Phase {
					t.Fatal("valid current baseline before denial", err)
				}
				if method == "expire" {
					f.at = operation.ExpiresAt
				}
				cookie, err := domain.DecodeWebSessionV2([]byte(f.base.durable.sessions[string(phase.SessionDigest)]))
				if err != nil {
					t.Fatal(err)
				}
				identityKey := identityRowsV2SubjectKey(cookie.Session.AuthenticatedSubject)
				identity, err := domain.DecodeExternalIdentityV2([]byte(f.base.durable.forward[identityKey]))
				if err != nil {
					t.Fatal(err)
				}
				memberKey := identityRowsV2MemberKey(cookie.Session.UserID, cookie.Session.ActiveTenantID)
				var member domain.TenantMembership
				if json.Unmarshal([]byte(f.base.durable.members[memberKey]), &member) != nil {
					t.Fatal("member prerequisite")
				}
				switch fault {
				case "missing-cookie":
					delete(f.base.durable.sessions, string(phase.SessionDigest))
				case "revoked-cookie":
					at := f.at
					cookie.Session.RevokedAt = &at
					f.base.durable.sessions[string(phase.SessionDigest)] = identityRowsV2JSON(t, cookie)
				case "stale-identity":
					identity.Revision++
					f.base.durable.forward[identityKey] = identityRowsV2JSON(t, identity)
				case "unlinked-identity":
					identity.State = domain.ExternalIdentityUnlinkedV2
					f.base.durable.forward[identityKey] = identityRowsV2JSON(t, identity)
				case "suspended-member":
					member.Status = domain.TenantMembershipSuspended
					f.base.durable.members[memberKey] = identityRowsV2JSON(t, member)
				case "member-version":
					member.SecurityVersion++
					f.base.durable.members[memberKey] = identityRowsV2JSON(t, member)
				case "foreign-operation-owner":
					operation.Pending.Scope.UserID = "another-valid-owner"
					if operation.Validate() != nil {
						t.Fatal("foreign operation remains structurally valid")
					}
					f.operation = identityRowsV2JSON(t, operation)
				case "future-operation":
					f.at = operation.CreatedAt.Add(-time.Microsecond)
				case "malformed-operation":
					f.operation = "{"
				case "oversize-operation":
					f.operation = strings.Repeat("x", domain.IdentityAuthMaxRecordBytesV2+1)
				case "storage":
					f.base.failStatement, f.base.failure = identityRowsV2Statement(identityOperationRowSQLV2), errors.New("controlled operation storage failure")
				case "malformed-config":
					config.ManifestDigest = ""
				case "disabled-auth-provider":
					config.Providers = config.Providers[1:]
				case "csrf":
					phase.CSRFDigest = domain.DigestSecret("other-csrf")
				case "browser":
					phase.BrowserBindingDigest = domain.DigestSecret("other-browser")
				case "revision":
					phase.ExpectedRevision++
				case "manifest":
					config.ManifestDigest = domain.DigestSecret("other-manifest")
				case "tenant":
					operation.Pending.Scope.TenantID = "another-tenant"
					if operation.Validate() != nil {
						t.Fatal("foreign original tenant remains structural DATA")
					}
					f.operation = identityRowsV2JSON(t, operation)
				}
				before, writes, audits := f.operation, f.base.writes, len(f.audit)
				var value identityOperationOutcomeViewV2
				switch method {
				case "read":
					value, err = store.readIdentityOperationOutcomeV2(ctx, read, config)
				case "cancel":
					value, err = store.cancelIdentityOperationV2(ctx, phase, config)
				case "expire":
					value, err = store.expireIdentityOperationV2(ctx, phase, config)
				}
				if err == nil || !reflect.DeepEqual(value, identityOperationOutcomeViewV2{}) || f.operation != before || f.base.writes != writes || len(f.audit) != audits {
					t.Fatalf("unknown/unauthorized %s fabricated result or stopped pending: err=%v", fault, err)
				}
			})
		}
	}
}

func TestIdentityOutcomeV2ReadUnavailableAndOriginalRetention(t *testing.T) {
	for _, state := range []string{"absent", "pending-expired", "pending-beyond-retention", "terminal-beyond-retention"} {
		t.Run(state, func(t *testing.T) {
			f, store, ctx, phase, config, original := identityOutcomeFixtureV2(t, domain.IdentityOperationLinkV2, domain.IdentityProviderYandex, domain.IdentityPhaseSourceProofV2)
			if state == "terminal-beyond-retention" {
				if _, err := store.cancelIdentityOperationV2(ctx, phase, config); err != nil {
					t.Fatal(err)
				}
			}
			switch state {
			case "absent":
				f.operation = ""
			case "pending-expired":
				f.at = original.ExpiresAt
			default:
				f.at = original.CreatedAt.Add(domain.IdentityReceiptRetentionV2)
			}
			// Current cookie/member/identity remain independently authenticable;
			// retention classification must not merely be an expired-cookie denial.
			if !f.at.Before(identityRowsV2At().Add(time.Hour)) {
				var cookie domain.WebSessionV2
				if json.Unmarshal([]byte(f.base.durable.sessions[string(phase.SessionDigest)]), &cookie) != nil {
					t.Fatal("current cookie prerequisite")
				}
				cookie.Session.IssuedAt, cookie.Session.LastSeenAt = f.at, f.at
				cookie.Session.IdleExpiresAt, cookie.Session.AbsoluteExpiresAt = f.at.Add(time.Hour), f.at.Add(2*time.Hour)
				// Explicit controlled current-session DATA, not a claim of provider
				// login. The separate test below executes the actual creation port.
				f.base.durable.sessions[string(phase.SessionDigest)] = identityRowsV2JSON(t, cookie)
			}
			before, writes := f.operation, f.base.writes
			config.SensitiveOperationsEnabled = false
			view, err := store.readIdentityOperationOutcomeV2(ctx, identityOutcomeReadV2{OperationDigest: phase.OperationDigest, SessionDigest: phase.SessionDigest}, config)
			want := domain.IdentityOutcomeUnavailableV2
			if state == "pending-expired" {
				want = domain.IdentityOutcomeExpiredV2
			}
			if err != nil || view.Outcome != want || view.CompletedAt != nil || view.AfterRevision != nil || f.operation != before || f.base.writes != writes {
				t.Fatalf("read retention state=%s invented commit/erasure/renewal: err=%v", state, err)
			}
		})
	}
}

func TestIdentityOutcomeV2LostSuccessCookieNeedsActualRelogin(t *testing.T) {
	f, store, ctx, request, config, _ := identityOperationConfirmationFixtureV2(t, domain.IdentityOperationLinkV2)
	result, err := store.confirmIdentityOperationV2(ctx, request, config)
	if err != nil || result.Receipt.AfterRevision == nil {
		t.Fatal("original atomic success prerequisite", err)
	}
	config.SensitiveOperationsEnabled = false
	old := identityOutcomeReadV2{OperationDigest: request.OperationDigest, SessionDigest: request.SessionDigest}
	if view, err := store.readIdentityOperationOutcomeV2(ctx, old, config); err == nil || !reflect.DeepEqual(view, identityOperationOutcomeViewV2{}) {
		t.Fatal("lost replacement cookie revived old cookie", err)
	}
	// Separate Store/connector shares the same canonical committed storage;
	// CreateWebSession snapshots current identity/membership through its real
	// private V2 serving path. No copied session row is installed as relogin.
	db := sql.OpenDB(&sessionServingSQLV2{identityOperationSQLV2: f})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	loginStore, err := New(db, Options{})
	if err != nil {
		t.Fatal(err)
	}
	loginStore.webIdentityV2 = &config
	login := result.Session.Session
	login.SessionDigest, login.CSRFTokenDigest = domain.DigestSecret("outcome-real-relogin-cookie"), domain.DigestSecret("outcome-real-relogin-csrf")
	if err := loginStore.CreateWebSession(ctx, login); err != nil {
		t.Fatal("actual current relogin session creation", err)
	}
	before, writes, audits := f.operation, f.base.writes, len(f.audit)
	read := identityOutcomeReadV2{OperationDigest: request.OperationDigest, SessionDigest: login.SessionDigest}
	view, err := store.readIdentityOperationOutcomeV2(ctx, read, config)
	if err != nil || view.Outcome != domain.IdentityOutcomeLinkedV2 || view.AfterRevision == nil || *view.AfterRevision != *result.Receipt.AfterRevision || view.BeforeRevision != result.Receipt.BeforeRevision || view.CompletedAt == nil || !view.CompletedAt.Equal(result.Receipt.CompletedAt) {
		t.Fatal("current same-user READ did not recover exact retained success", err)
	}
	*view.AfterRevision = *view.AfterRevision + 1
	again, err := store.readIdentityOperationOutcomeV2(ctx, read, config)
	if err != nil || again.AfterRevision == nil || *again.AfterRevision != *result.Receipt.AfterRevision || f.operation != before || f.base.writes != writes || len(f.audit) != audits {
		t.Fatal("GET view alias or replay mutated durable outcome", err)
	}
}

func TestIdentityOutcomeV2ForeignCurrentReaderOwnAbsenceBeforeOwnerDenial(t *testing.T) {
	f, store, ctx, start, config := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
	_, originalIdentity, originalCookie, originalMember := identityRowsV2Data(t)
	identity := originalIdentity
	identity.Subject.Subject, identity.UserID = "foreign-current-subject", "foreign-current-user"
	member := originalMember
	member.UserID, member.TenantID = identity.UserID, "foreign-current-tenant"
	cookie := originalCookie
	cookie.Session.SessionDigest, cookie.Session.CSRFTokenDigest = domain.DigestSecret("foreign-current-cookie"), domain.DigestSecret("foreign-current-csrf")
	cookie.Session.UserID, cookie.Session.ActiveTenantID, cookie.Session.AuthenticatedSubject = identity.UserID, member.TenantID, identity.Subject
	if identity.Validate() != nil || member.Validate() != nil || cookie.AuthorizeCurrent(identity, member, domain.TenantPermissionRead, f.at) != nil {
		t.Fatal("foreign current principal must be independently coherent")
	}
	// Controlled canonical inputs, all installed before the actual operation
	// starts; no original scope is rewritten to disguise a foreign read.
	f.base.durable.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, identity)
	f.base.durable.reverse[identityRowsV2ReverseKey(identity.UserID, identity.Subject)] = identityRowsV2Projection{user: identity.UserID, subject: identity.Subject, created: identity.CreatedAt}
	f.base.durable.members[identityRowsV2MemberKey(member.UserID, member.TenantID)] = identityRowsV2JSON(t, member)
	f.base.durable.sessions[string(cookie.Session.SessionDigest)] = identityRowsV2JSON(t, cookie)
	read := identityOutcomeReadV2{OperationDigest: start.Phase.OperationDigest, SessionDigest: cookie.Session.SessionDigest}
	view, err := store.readIdentityOperationOutcomeV2(ctx, read, config)
	if err != nil || view.Outcome != domain.IdentityOutcomeUnavailableV2 || view.Phase != "" || f.base.writes != 0 {
		t.Fatal("valid foreign current login lacks genuine absent-outcome baseline", err)
	}
	operation, err := store.startIdentityOperationV2(ctx, start, config)
	if err != nil {
		t.Fatal("actual original-owner operation producer", err)
	}
	if own, err := store.readIdentityOperationOutcomeV2(ctx, identityOutcomeReadV2{OperationDigest: start.Phase.OperationDigest, SessionDigest: start.Phase.SessionDigest}, config); err != nil || own.Phase != operation.Phase {
		t.Fatal("original owner lacks positive stored-outcome baseline", err)
	}
	before, writes, state := f.operation, f.base.writes, f.base.durable.clone()
	view, err = store.readIdentityOperationOutcomeV2(ctx, read, config)
	if err == nil || !reflect.DeepEqual(view, identityOperationOutcomeViewV2{}) || f.operation != before || f.base.writes != writes || !reflect.DeepEqual(state, f.base.durable) {
		t.Fatal("coherent foreign current login accessed another user's original operation", err)
	}
}

func TestIdentityOutcomeV2CancelCutoffAndLaterImmutableResult(t *testing.T) {
	for _, delta := range []time.Duration{-time.Microsecond, 0, time.Microsecond} {
		t.Run(delta.String(), func(t *testing.T) {
			f, store, ctx, phase, config, original := identityOutcomeFixtureV2(t, domain.IdentityOperationLinkV2, domain.IdentityProviderYandex, domain.IdentityPhaseSourceProofV2)
			f.at = original.ExpiresAt.Add(delta)
			view, err := store.cancelIdentityOperationV2(ctx, phase, config)
			want := domain.IdentityOutcomeExpiredV2
			if delta < 0 {
				want = domain.IdentityOutcomeCancelledV2
			}
			if err != nil || view.Outcome != want || view.CompletedAt == nil || !view.CompletedAt.Equal(f.at) || view.AfterRevision != nil {
				t.Fatal("cancel cutoff did not use original SQL deadline", err)
			}
			before, writes, completed := f.operation, f.base.writes, *view.CompletedAt
			f.at = original.ExpiresAt.Add(time.Minute)
			again, err := store.expireIdentityOperationV2(ctx, phase, config)
			if err != nil || again.Outcome != want || again.CompletedAt == nil || !again.CompletedAt.Equal(completed) || f.operation != before || f.base.writes != writes || len(f.audit) != 1 {
				t.Fatal("later expiry retagged or renewed retained terminal result", err)
			}
		})
	}
}

func TestIdentityOutcomeV2AuditRollbackAndLostCommit(t *testing.T) {
	for _, failure := range []string{"audit", "operation-write", "lost-commit"} {
		t.Run(failure, func(t *testing.T) {
			f, store, ctx, phase, config, _ := identityOutcomeFixtureV2(t, domain.IdentityOperationLinkV2, domain.IdentityProviderYandex, domain.IdentityPhaseConfirmV2)
			before, state, children := f.operation, f.base.durable.clone(), cloneIdentityProofRowsV2(f.children)
			switch failure {
			case "audit":
				f.base.failStatement, f.base.failure = identityOperationSecurityAuditSQLTestV2, errors.New("controlled atomic security audit unavailable")
			case "operation-write":
				f.base.failStatement, f.base.failure = identityOperationWriteSQLTestV2, errors.New("controlled pending record storage error")
			case "lost-commit":
				f.commitReplyError = errors.New("controlled unknown reply AFTER actual durable stop")
			}
			view, err := store.cancelIdentityOperationV2(ctx, phase, config)
			if err == nil || !reflect.DeepEqual(view, identityOperationOutcomeViewV2{}) {
				t.Fatal("unknown/failed commit published a speculative outcome", err)
			}
			requireIdentityOutcomeUnchangedV2(t, f, state, children)
			if failure != "lost-commit" {
				if f.operation != before || len(f.audit) != 0 {
					t.Fatal("storage/audit failure did not roll back prune and audit atomically")
				}
				return
			}
			terminal, err := domain.DecodeIdentityOperationV2([]byte(f.operation))
			if err != nil || terminal.Receipt == nil || terminal.Receipt.Outcome != domain.IdentityOutcomeCancelledV2 || len(f.audit) != 1 {
				t.Fatal("lost reply fixture did not actually commit exact stop", err)
			}
			writes, raw := f.base.writes, f.operation
			read, err := store.readIdentityOperationOutcomeV2(ctx, identityOutcomeReadV2{OperationDigest: phase.OperationDigest, SessionDigest: phase.SessionDigest}, config)
			if err != nil || read.Outcome != domain.IdentityOutcomeCancelledV2 || read.CompletedAt == nil || !read.CompletedAt.Equal(terminal.Receipt.CompletedAt) || f.base.writes != writes || f.operation != raw {
				t.Fatal("lost stop recovered without fresh exact READ or repeated mutation", err)
			}
		})
	}
}

func identityOutcomePhaseFromConfirmationV2(request identityConfirmationV2) identityPhaseRequestV2 {
	return identityPhaseRequestV2{OperationDigest: request.OperationDigest, SessionDigest: request.SessionDigest, CSRFDigest: request.CSRFDigest, BrowserBindingDigest: request.BrowserBindingDigest, ExpectedRevision: request.ExpectedRevision}
}

func TestIdentityOutcomeV2RetryRejoinsClockAndCurrentness(t *testing.T) {
	for _, fault := range []string{"cross-expiry", "stale-identity", "changed-browser", "retry-budget"} {
		t.Run(fault, func(t *testing.T) {
			// This older complete-confirm fixture supplies structural pending DATA;
			// the earlier phase matrix separately proves actual start/proof owners.
			f, store, ctx, confirmation, config, original := identityOperationConfirmationFixtureV2(t, domain.IdentityOperationLinkV2)
			phase := identityOutcomePhaseFromConfirmationV2(confirmation)
			before := f.operation
			f.outcomeRollbackOnce = true
			switch fault {
			case "cross-expiry":
				f.at, f.outcomeRetryAdvance = original.ExpiresAt.Add(-time.Microsecond), time.Microsecond
			case "stale-identity":
				f.outcomeAfterRollback = func(f *identityOperationSQLV2) {
					for key, raw := range f.base.durable.forward {
						identity, err := domain.DecodeExternalIdentityV2([]byte(raw))
						if err != nil {
							t.Fatal(err)
						}
						identity.Revision++
						f.base.durable.forward[key] = identityRowsV2JSON(t, identity)
					}
				}
			case "changed-browser":
				f.outcomeAfterRollback = func(f *identityOperationSQLV2) {
					var op domain.IdentityOperationV2
					if json.Unmarshal([]byte(f.operation), &op) != nil {
						t.Fatal("retry original operation")
					}
					op.Pending.Scope.BrowserBindingDigest = domain.DigestSecret("changed-original-browser")
					f.operation = identityRowsV2JSON(t, op)
				}
			case "retry-budget":
				f.outcomeAfterRollback = func(f *identityOperationSQLV2) { f.outcomeRollbackOnce = true }
			}
			view, err := store.cancelIdentityOperationV2(ctx, phase, config)
			if fault == "cross-expiry" {
				if err != nil || view.Outcome != domain.IdentityOutcomeExpiredV2 || view.CompletedAt == nil || !view.CompletedAt.Equal(original.ExpiresAt) || f.base.rollbacks != 1 || len(f.audit) != 1 {
					t.Fatal("retry used first timestamp/cancel result after original expiry", err)
				}
				return
			}
			if err == nil || !reflect.DeepEqual(view, identityOperationOutcomeViewV2{}) || len(f.audit) != 0 {
				t.Fatal("retry reused old authority or reset whole-call budget", err)
			}
			if fault != "changed-browser" && f.operation != before {
				t.Fatal("failed retry published an earlier speculative terminal record")
			}
			if fault == "retry-budget" && f.base.rollbacks > ownedAuthorityMaxCallbacks {
				t.Fatal("retry escaped original callback ceiling")
			}
		})
	}
}

func TestIdentityOutcomeV2SharedBudgetsRemainSticky(t *testing.T) {
	for _, method := range []string{"read", "cancel", "expire"} {
		caps := []string{"callbacks", "bytes", "statements"}
		if method != "read" {
			caps = append(caps, "copies")
		}
		for _, cap := range caps {
			t.Run(method+"/"+cap, func(t *testing.T) {
				f, store, parent, phase, config, original := identityOutcomeFixtureV2(t, domain.IdentityOperationLinkV2, domain.IdentityProviderYandex, domain.IdentityPhaseSourceProofV2)
				f.at = original.ExpiresAt
				ctx, cancel := ownedAuthorityCallContext(parent)
				defer cancel()
				budget := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
				switch cap {
				case "callbacks":
					budget.callbacks = ownedAuthorityMaxCallbacks
				case "bytes":
					budget.bytes = ownedAuthorityMaxBytes
				case "statements":
					budget.statements = ownedAuthorityMaxTotalStatements
				case "copies":
					budget.bytes = ownedAuthorityMaxBytes - identityStopMaterialBytesV2
				}
				before, writes := f.operation, f.base.writes
				var view identityOperationOutcomeViewV2
				var err error
				for i := 0; i < 2; i++ {
					switch method {
					case "read":
						view, err = store.readIdentityOperationOutcomeV2(ctx, identityOutcomeReadV2{OperationDigest: phase.OperationDigest, SessionDigest: phase.SessionDigest}, config)
					case "cancel":
						view, err = store.cancelIdentityOperationV2(ctx, phase, config)
					case "expire":
						view, err = store.expireIdentityOperationV2(ctx, phase, config)
					}
					if err == nil || !reflect.DeepEqual(view, identityOperationOutcomeViewV2{}) || !budget.failed || f.operation != before || f.base.writes != writes || len(f.audit) != 0 {
						t.Fatal("exhausted original budget renewed or fabricated a stop", method, cap, i, err)
					}
				}
			})
		}
	}
}

func TestIdentityOutcomeV2UnlinkDisabledTargetCanStillStop(t *testing.T) {
	for _, expiry := range []bool{false, true} {
		name := "cancel"
		if expiry {
			name = "expire"
		}
		t.Run(name, func(t *testing.T) {
			f, store, ctx, start, config := newIdentityProofStorageV2(t, domain.IdentityOperationUnlinkV2)
			cookie, err := domain.DecodeWebSessionV2([]byte(f.base.durable.sessions[string(start.Phase.SessionDigest)]))
			if err != nil {
				t.Fatal(err)
			}
			// Before start, select the enabled remaining method as current login.
			// Removing an unavailable OTHER method is a legitimate unlink input.
			cookie.Session.AuthenticatedSubject.Provider = domain.IdentityProviderTelegram
			f.base.durable.sessions[string(start.Phase.SessionDigest)] = identityRowsV2JSON(t, cookie)
			config.Providers = config.Providers[1:]
			original, err := store.startIdentityOperationV2(ctx, start, config)
			if err != nil || original.Pending.SourceSubject.Provider != domain.IdentityProviderTelegram || original.Pending.TargetProvider != domain.IdentityProviderYandex {
				t.Fatal("valid original unlink with disabled target", err)
			}
			config.SensitiveOperationsEnabled = false
			var view identityOperationOutcomeViewV2
			want := domain.IdentityOutcomeCancelledV2
			if expiry {
				f.at = original.ExpiresAt
				want = domain.IdentityOutcomeExpiredV2
				view, err = store.expireIdentityOperationV2(ctx, start.Phase, config)
			} else {
				view, err = store.cancelIdentityOperationV2(ctx, start.Phase, config)
			}
			if err != nil || view.Outcome != want || view.AfterRevision != nil {
				t.Fatal("disabled removed provider incorrectly blocked protected cleanup", err)
			}
		})
	}
}

func TestIdentityOutcomeV2CallerCancellationCannotInventStop(t *testing.T) {
	f, store, parent, phase, config, original := identityOutcomeFixtureV2(t, domain.IdentityOperationLinkV2, domain.IdentityProviderYandex, domain.IdentityPhaseSourceProofV2)
	f.at = original.ExpiresAt
	ctx, cancel := context.WithCancel(parent)
	cancel()
	before, writes := f.operation, f.base.writes
	if view, err := store.expireIdentityOperationV2(ctx, phase, config); err == nil || !reflect.DeepEqual(view, identityOperationOutcomeViewV2{}) || f.operation != before || f.base.writes != writes || len(f.audit) != 0 {
		t.Fatal("cancelled caller fabricated an expiry receipt", err)
	}
}

func TestIdentityOutcomeV2SecurityWriterPreservesLegacyPreparation(t *testing.T) {
	for _, action := range []domain.WebSecurityAuditAction{domain.WebSecurityLoginFailed, domain.WebSecurityCSRFRejected} {
		event := domain.WebSecurityAuditEvent{RequestID: "original-security-event", Action: action, Provider: domain.IdentityProviderYandex, TenantID: "tenant-test", UserID: "user-test", MembershipSecurityVersion: 1, ReasonCode: "original-denial", OccurredAt: identityRowsV2At()}
		prepared, err := prepareWebSecurityAuditEvent(event)
		raw, marshalErr := json.Marshal(event)
		bucket, bucketErr := webBucket(event.RequestID)
		if err != nil || marshalErr != nil || bucketErr != nil || prepared.payload != string(raw) || prepared.bucket != bucket || prepared.event != event {
			t.Fatal("factored writer changed original validation/encoding/key", err)
		}
	}
	// Both validation and encoding errors still happen BEFORE authTx/DB use.
	store := &Store{}
	for _, event := range []domain.WebSecurityAuditEvent{
		{Action: domain.WebSecurityLoginFailed},
		{RequestID: "invalid-encoded-time", Action: domain.WebSecurityLoginFailed, Provider: domain.IdentityProviderYandex, ReasonCode: "original-denial", OccurredAt: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		if err := store.RecordWebSecurityEvent(context.Background(), event); err == nil {
			t.Fatal("invalid original security event reached the database")
		}
	}
}
