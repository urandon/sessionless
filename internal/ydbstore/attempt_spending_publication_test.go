package ydbstore

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

// Explicit synthetic storage intent installation, never a production payer
// issuer. Authentication and exact test-only allowlisting are both required.
type fixtureSpendingVerifier struct {
	key     []byte
	allowed map[string]bool
	fail    bool
}

func (v *fixtureSpendingVerifier) install(t *testing.T, g ports.AttemptSpendingPublicationGrantV1) ports.AttemptSpendingPublicationGrantV1 {
	t.Helper()
	g = g.Clone()
	g.Authenticator = make([]byte, 32)
	raw, err := g.IntentBytes()
	if err != nil {
		t.Fatal(err)
	}
	v.allowed[string(raw)] = true
	h := hmac.New(sha256.New, v.key)
	h.Write(raw)
	g.Authenticator = h.Sum(nil)
	return g
}
func (v *fixtureSpendingVerifier) VerifyAttemptSpendingPublication(g ports.AttemptSpendingPublicationGrantV1, at time.Time) error {
	raw, err := g.IntentBytes()
	if err != nil || v.fail || !v.allowed[string(raw)] || at.Before(g.IssuedAt) || !at.Before(g.ExpiresAt) {
		return domain.ErrMembershipDenied
	}
	h := hmac.New(sha256.New, v.key)
	h.Write(raw)
	if !hmac.Equal(g.Authenticator, h.Sum(nil)) {
		return domain.ErrMembershipDenied
	}
	return nil
}

type spendingSQLFixture struct {
	*ownedCurrentSQLFixture
	audits        map[string][]driver.Value
	pendingRows   [4][]driver.Value
	pendingAudits map[string][]driver.Value
	failAudit     bool
}

func (f *spendingSQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *spendingSQLFixture) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	f.isolations = append(f.isolations, options.Isolation)
	if options.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, errors.New("requires serializable transaction")
	}
	for i := range f.spendingRows {
		f.pendingRows[i] = append([]driver.Value(nil), f.spendingRows[i]...)
	}
	f.pendingAudits = make(map[string][]driver.Value)
	return spendingFixtureTx{f}, nil
}

type spendingFixtureTx struct{ f *spendingSQLFixture }

func (tx spendingFixtureTx) Commit() error {
	f := tx.f
	f.commits++
	mode := ""
	if len(f.commitModes) > 0 {
		mode, f.commitModes = f.commitModes[0], f.commitModes[1:]
	}
	if mode != "rollback" {
		for i := range f.pendingRows {
			f.spendingRows[i] = append([]driver.Value(nil), f.pendingRows[i]...)
		}
		for k, v := range f.pendingAudits {
			f.audits[k] = append([]driver.Value(nil), v...)
		}
	}
	f.pendingRows, f.pendingAudits = [4][]driver.Value{}, nil
	if f.afterCommit != nil {
		f.afterCommit()
	}
	if mode == "rollback" || mode == "ambiguous" {
		return retry.RetryableError(errors.New("synthetic lost commit ACK"), retry.WithBackoff(retry.TypeNoBackoff))
	}
	if mode == "unknown" {
		return errors.New("synthetic unknown commit")
	}
	return nil
}
func (tx spendingFixtureTx) Rollback() error {
	tx.f.rollbacks++
	tx.f.pendingRows, tx.f.pendingAudits = [4][]driver.Value{}, nil
	return nil
}
func (f *spendingSQLFixture) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query != spendingAuditSQL {
		return f.ownedCurrentSQLFixture.QueryContext(ctx, query, args)
	}
	f.statements = append(f.statements, query)
	if len(args) != 5 {
		return nil, errors.New("invalid audit lookup")
	}
	values := make([]driver.Value, 4)
	for i := range values {
		values[i] = args[i].Value
	}
	known := false
	for _, scope := range f.spendingArgs {
		known = known || reflect.DeepEqual(values, scope)
	}
	if !known {
		return nil, errors.New("foreign spending audit lookup")
	}
	key := args[2].Value.(string) + "\x00" + args[4].Value.(string)
	row := append([]driver.Value(nil), f.audits[key]...)
	for i, value := range row {
		if raw, ok := value.(string); ok {
			cap := 65
			if i == 1 {
				cap = ownedAuthorityHeadBytes + 1
			}
			if i >= 5 {
				cap = 161
			}
			if len(raw) > cap {
				row[i] = raw[:cap]
			}
		}
	}
	return &explanationFixtureRows{values: row}, nil
}
func (f *spendingSQLFixture) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	f.execCalls++
	f.statements = append(f.statements, query)
	values := make([]driver.Value, len(args))
	for i := range values {
		values[i] = args[i].Value
	}
	if len(values) < 4 {
		return nil, errors.New("invalid spending write")
	}
	index := -1
	for i, scope := range f.spendingArgs {
		if reflect.DeepEqual(values[:4], scope) {
			index = i
		}
	}
	if index < 0 {
		return nil, errors.New("foreign spending write")
	}
	switch query {
	case spendingWriteSQL:
		if len(values) != 15 {
			return nil, errors.New("invalid head write")
		}
		f.pendingRows[index] = append([]driver.Value(nil), values[4:]...)
	case spendingAuditWriteSQL:
		if f.failAudit {
			return nil, errors.New("synthetic audit failure")
		}
		if len(values) != 12 {
			return nil, errors.New("invalid audit write")
		}
		key := values[2].(string) + "\x00" + values[4].(string)
		if f.audits[key] != nil || f.pendingAudits[key] != nil {
			return nil, errors.New("duplicate immutable receipt")
		}
		f.pendingAudits[key] = append([]driver.Value(nil), values[5:]...)
	default:
		return nil, errors.New("non-manifest write")
	}
	return driver.RowsAffected(1), nil
}

func newSpendingPublicationFixture(t *testing.T, paid bool) (*Store, *spendingSQLFixture, *fixtureSpendingVerifier, [4]ports.AttemptSpendingPublicationGrantV1) {
	t.Helper()
	r := newOwnedReaderFixture(t)
	if paid {
		r.profile.CostCeiling.ProviderPriceState = domain.ProviderPriceKnownV1
		r.profile.CostCeiling.MaxProviderAmountMicrounits = new(uint64)
		*r.profile.CostCeiling.MaxProviderAmountMicrounits = 7
		*r.profile.CostCeiling.MaxTotalAmountMicrounits += 7
		r.profile.Substrate.AdmissionCostCeilingDigest, _ = r.profile.CostCeiling.Digest()
		r.ref.Digest, _ = r.profile.Digest()
		for i := range r.spending {
			r.spending[i].SourceProfile = r.ref
		}
		r.spending[0].PriceState = domain.ExternalServicePriceKnownPaidV1
		r.spending[0].MaxAmountMicrounits = new(uint64)
		*r.spending[0].MaxAmountMicrounits = 5
		for i := range r.spending {
			bindReaderSpendingSource(t, &r.spending[i])
		}
		r.sync(t)
	}
	f := &spendingSQLFixture{ownedCurrentSQLFixture: r.driver, audits: make(map[string][]driver.Value)}
	f.spendingRows = [4][]driver.Value{}
	v := &fixtureSpendingVerifier{key: []byte("test-only-independent-synthetic-spending-source"), allowed: make(map[string]bool)}
	db := sql.OpenDB(f)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	s, err := New(db, Options{AttemptSpendingPublicationVerifier: v})
	if err != nil {
		t.Fatal(err)
	}
	var grants [4]ports.AttemptSpendingPublicationGrantV1
	for i, a := range r.spending {
		grants[i] = v.install(t, ports.AttemptSpendingPublicationGrantV1{Authorization: a.Clone(), IssuedAt: f.at, ExpiresAt: f.at.Add(10 * time.Second)})
	}
	return s, f, v, grants
}
func spendingRevoke(t *testing.T, v *fixtureSpendingVerifier, g ports.AttemptSpendingPublicationGrantV1, r ports.AttemptSpendingPublicationReceiptV1, at time.Time) ports.AttemptSpendingPublicationGrantV1 {
	t.Helper()
	g = g.Clone()
	g.Authorization = r.Authorization.Clone()
	g.Authorization.State = domain.OwnedAgentCurrentRevokedV1
	g.Authorization.RevocationGeneration = 2
	g.Authorization.MutationID = "explicit-cleanup-revoke"
	g.ExpectedGeneration = 1
	g.ExpectedDigest = r.Digest
	g.IssuedAt = at
	g.ExpiresAt = at.Add(10 * time.Second)
	return v.install(t, g)
}

func revokeSpendingFixtureProfile(t *testing.T, f *spendingSQLFixture) {
	t.Helper()
	p, err := domain.DecodeOwnedAgentProfileCurrentV1([]byte(f.profileRow[0].(string)))
	if err != nil {
		t.Fatal(err)
	}
	p.RevocationGeneration++
	p.State = domain.OwnedAgentCurrentRevokedV1
	raw, err := domain.EncodeOwnedAgentProfileCurrentV1(p)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	f.profileRow[0], f.profileRow[3], f.profileRow[4], f.profileRow[5] = string(raw), p.RevocationGeneration, string(p.State), digest
}

func TestAttemptSpendingPublicationAllComponentsCreateRevokeReplay(t *testing.T) {
	for _, paid := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit_free", true: "finite_synthetic_paid"}[paid], func(t *testing.T) {
			s, f, v, grants := newSpendingPublicationFixture(t, paid)
			for i, g := range grants {
				f.at = f.at.Add(time.Microsecond)
				first, err := s.PublishAttemptSpending(t.Context(), g)
				if err != nil || first.Replayed || first.Authorization.RevocationGeneration != 1 || !first.Authorization.UpdatedAt.Equal(f.at) {
					t.Fatalf("create component=%s err=%v gen=%d", g.Authorization.Locator.ComponentKind, err, first.Authorization.RevocationGeneration)
				}
				r := spendingRevoke(t, v, g, first, f.at)
				if _, err := s.PublishAttemptSpending(t.Context(), r); err != nil {
					t.Fatal(err)
				}
				writes := f.execCalls
				replayed, err := s.PublishAttemptSpending(t.Context(), g)
				if err != nil || !replayed.Replayed || !reflect.DeepEqual(first.Authorization, replayed.Authorization) || writes != f.execCalls || f.spendingRows[i][3] != string(domain.OwnedAgentCurrentRevokedV1) {
					t.Fatalf("replay component=%s err=%v writes=%d", g.Authorization.Locator.ComponentKind, err, f.execCalls)
				}
				*replayed.Authorization.MaxAmountMicrounits = 999
				again, err := s.PublishAttemptSpending(t.Context(), g)
				if err != nil || *again.Authorization.MaxAmountMicrounits != *first.Authorization.MaxAmountMicrounits {
					t.Fatal("receipt aliases persisted amount")
				}
			}
			if len(f.audits) != 8 {
				t.Fatalf("immutable receipts=%d want8", len(f.audits))
			}
		})
	}
}

func TestAttemptSpendingPublicationDenialsPreserveHeads(t *testing.T) {
	for _, name := range []string{"missing_verifier", "foreign_verifier", "forged_mac", "changed_intent", "future", "expired", "missing_profile", "revoked_profile", "changed_profile_generation", "wrong_source_digest", "payer", "component", "resource", "grant", "price", "terms", "currency", "amount", "existing_create", "conflicting_audit", "wrong_predecessor", "absent_predecessor", "increase_on_revoke", "extend_on_revoke"} {
		t.Run(name, func(t *testing.T) {
			s, f, v, grants := newSpendingPublicationFixture(t, false)
			g := grants[0].Clone()
			switch name {
			case "missing_verifier":
				s.attemptSpendingPublicationVerifier = nil
			case "foreign_verifier":
				s.attemptSpendingPublicationVerifier = &fixtureSpendingVerifier{key: []byte("foreign"), allowed: v.allowed}
			case "forged_mac":
				g.Authenticator[0] ^= 1
			case "changed_intent":
				g.Authorization.MutationID = "unapproved"
			case "future":
				f.at = g.IssuedAt.Add(-time.Microsecond)
			case "expired":
				f.at = g.ExpiresAt
			case "missing_profile":
				f.profileRow = nil
			case "revoked_profile", "changed_profile_generation":
				p, err := domain.DecodeOwnedAgentProfileCurrentV1([]byte(f.profileRow[0].(string)))
				if err != nil {
					t.Fatal(err)
				}
				p.RevocationGeneration++
				if name == "revoked_profile" {
					p.State = domain.OwnedAgentCurrentRevokedV1
				}
				raw, err := domain.EncodeOwnedAgentProfileCurrentV1(p)
				if err != nil {
					t.Fatal(err)
				}
				digest, _ := p.Digest()
				f.profileRow[0], f.profileRow[3], f.profileRow[4], f.profileRow[5] = string(raw), uint64(p.RevocationGeneration), string(p.State), digest
			case "wrong_source_digest":
				g.Authorization.SourceProfile.Digest = strings.Repeat("f", 64)
			case "payer":
				g.Authorization.Locator.BillingOwnerUserID = "foreign-payer"
			case "component":
				g.Authorization.Locator.ComponentKind = domain.AttemptSpendingWebSearchV1
				g.Authorization.ResourceGeneration = 1 // structurally legal, wrong original component
			case "resource":
				g.Authorization.ResourceID = "foreign-resource"
			case "grant":
				g.Authorization.ScopedGrantDigest = strings.Repeat("f", 64)
			case "price":
				g.Authorization.PriceRevision = "foreign-price"
			case "terms":
				g.Authorization.TermsEvidenceDigest = strings.Repeat("f", 64)
			case "currency":
				g.Authorization.Currency = "EUR"
			case "amount":
				g.Authorization.PriceState = domain.ExternalServicePriceKnownPaidV1
				*g.Authorization.MaxAmountMicrounits = 1
			case "existing_create", "conflicting_audit", "wrong_predecessor", "increase_on_revoke", "extend_on_revoke":
				first, err := s.PublishAttemptSpending(t.Context(), g)
				if err != nil {
					t.Fatal(err)
				}
				if name == "existing_create" {
					g.Authorization.MutationID = "other-create"
				} else if name == "conflicting_audit" {
					g.Authorization.SourceAuthorization.Ref = "other-source-ref"
				} else {
					g = spendingRevoke(t, v, g, first, f.at)
					if name == "wrong_predecessor" {
						g.ExpectedDigest = strings.Repeat("f", 64)
					}
					if name == "increase_on_revoke" {
						g.Authorization.PriceState = domain.ExternalServicePriceKnownPaidV1
						*g.Authorization.MaxAmountMicrounits = 1
					}
					if name == "extend_on_revoke" {
						g.Authorization.Window.ExpiresAt = g.Authorization.Window.ExpiresAt.Add(time.Second)
					}
				}
			case "absent_predecessor":
				g.ExpectedGeneration = 1
				g.ExpectedDigest = strings.Repeat("f", 64)
				g.Authorization.State = domain.OwnedAgentCurrentRevokedV1
				g.Authorization.RevocationGeneration = 2
			}
			if name != "missing_verifier" && name != "foreign_verifier" && name != "forged_mac" && name != "changed_intent" {
				bindReaderSpendingSource(t, &g.Authorization)
				if name == "conflicting_audit" {
					g.Authorization.SourceAuthorization.Ref = "conflicting-source-reference"
				}
				g = v.install(t, g)
			}
			before := f.spendingRows
			count := len(f.audits)
			f.execCalls, f.commits = 0, 0
			got, err := s.PublishAttemptSpending(t.Context(), g)
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.AttemptSpendingPublicationReceiptV1{}) || f.execCalls != 0 || f.commits != 0 || !reflect.DeepEqual(before, f.spendingRows) || count != len(f.audits) {
				t.Fatalf("case=%s err=%v gen=%d writes=%d commits=%d", name, err, got.Authorization.RevocationGeneration, f.execCalls, f.commits)
			}
		})
	}
}

func TestAttemptSpendingPublicationCommitRecoveryAndRetryRevocation(t *testing.T) {
	for _, name := range []string{"audit_failure", "rollback", "ambiguous", "unknown", "retry_expired", "retry_verifier_revoked", "retry_profile_revoked"} {
		t.Run(name, func(t *testing.T) {
			s, f, v, grants := newSpendingPublicationFixture(t, false)
			g := grants[0]
			switch name {
			case "audit_failure":
				f.failAudit = true
			case "rollback", "ambiguous", "unknown":
				f.commitModes = []string{name}
			case "retry_expired":
				f.commitModes = []string{"rollback"}
				f.afterCommit = func() { f.at = g.ExpiresAt }
			case "retry_verifier_revoked":
				f.commitModes = []string{"rollback"}
				f.afterCommit = func() { v.fail = true }
			case "retry_profile_revoked":
				f.commitModes = []string{"rollback"}
				f.afterCommit = func() { revokeSpendingFixtureProfile(t, f) }
			}
			got, err := s.PublishAttemptSpending(t.Context(), g)
			if name == "audit_failure" || name == "unknown" || strings.HasPrefix(name, "retry_") {
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.AttemptSpendingPublicationReceiptV1{}) {
					t.Fatalf("case=%s err=%v gen=%d", name, err, got.Authorization.RevocationGeneration)
				}
				if name != "unknown" && (len(f.audits) != 0 || f.spendingRows[0] != nil) {
					t.Fatal("rollback leaked head/audit")
				}
				return
			}
			wantWrites := 4
			if name == "ambiguous" {
				wantWrites = 2
			}
			if err != nil || got.Replayed != (name == "ambiguous") || f.execCalls != wantWrites || len(f.audits) != 1 || f.spendingRows[0] == nil {
				t.Fatalf("case=%s err=%v replay=%v writes=%d audits=%d", name, err, got.Replayed, f.execCalls, len(f.audits))
			}
		})
	}
}

func TestAttemptSpendingPublicationCleanupAfterSourceExpiry(t *testing.T) {
	s, f, v, grants := newSpendingPublicationFixture(t, false)
	g := grants[0]
	first, err := s.PublishAttemptSpending(t.Context(), g)
	if err != nil {
		t.Fatal(err)
	}
	f.at = g.Authorization.Window.ExpiresAt.Add(time.Microsecond)
	f.profileRow = nil
	r := spendingRevoke(t, v, g, first, f.at)
	got, err := s.PublishAttemptSpending(t.Context(), r)
	if err != nil || got.Authorization.State != domain.OwnedAgentCurrentRevokedV1 || !got.Authorization.Window.ExpiresAt.Equal(first.Authorization.Window.ExpiresAt) {
		t.Fatalf("cleanup err=%v state=%s", err, got.Authorization.State)
	}
	if _, err := json.Marshal(g); err == nil {
		t.Fatal("grant serialized")
	}
	var decoded ports.AttemptSpendingPublicationGrantV1
	if err := json.Unmarshal([]byte(`{}`), &decoded); err == nil {
		t.Fatal("public grant decoded")
	}
}

func TestAttemptSpendingPublicationAuditScalarsAndCoherentTime(t *testing.T) {
	for _, name := range []string{"intent", "body", "digest", "generation", "time", "run", "session", "oversize", "float", "null_then_long_body", "coherent_pre_grant"} {
		t.Run(name, func(t *testing.T) {
			s, f, v, grants := newSpendingPublicationFixture(t, false)
			g := grants[0]
			first, err := s.PublishAttemptSpending(t.Context(), g)
			if err != nil {
				t.Fatal(err)
			}
			if name == "coherent_pre_grant" {
				f.at = f.at.Add(time.Second)
				g = spendingRevoke(t, v, g, first, f.at)
				if _, err := s.PublishAttemptSpending(t.Context(), g); err != nil {
					t.Fatal(err)
				}
			}
			key := string(g.Authorization.Locator.ComponentKind) + "\x00" + g.Authorization.MutationID
			row := f.audits[key]
			switch name {
			case "intent":
				row[0] = strings.Repeat("f", 64)
			case "body":
				row[1] = strings.Replace(row[1].(string), `"version":1`, `"version":2`, 1)
			case "digest":
				row[2] = strings.Repeat("f", 64)
			case "generation":
				row[3] = uint64(99)
			case "time":
				row[4] = f.at.Add(time.Microsecond)
			case "run":
				row[5] = "other-run"
			case "session":
				row[6] = "other-session"
			case "oversize":
				row[1] = strings.Repeat("x", ownedAuthorityHeadBytes+50)
			case "float":
				row[3] = float64(1)
			case "null_then_long_body":
				row[0] = nil
				row[1] = strings.Repeat("x", ownedAuthorityHeadBytes+50)
			case "coherent_pre_grant":
				a, err := domain.DecodeAttemptSpendingAuthorizationCurrentV1([]byte(row[1].(string)))
				if err != nil {
					t.Fatal(err)
				}
				a.UpdatedAt = g.IssuedAt.Add(-time.Microsecond)
				raw, err := domain.EncodeAttemptSpendingAuthorizationCurrentV1(a)
				if err != nil {
					t.Fatal(err)
				}
				digest, _ := a.Digest()
				row[1], row[2], row[4] = string(raw), digest, a.UpdatedAt
			}
			before := f.spendingRows
			f.execCalls, f.commits = 0, 0
			got, err := s.PublishAttemptSpending(t.Context(), g)
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.AttemptSpendingPublicationReceiptV1{}) || f.execCalls != 0 || f.commits != 0 || !reflect.DeepEqual(before, f.spendingRows) {
				t.Fatalf("scalar=%s err=%v writes=%d commits=%d", name, err, f.execCalls, f.commits)
			}
		})
	}
}
