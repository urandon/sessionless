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

// Scripted fixture approval only: a preinstalled exact intent allowlist AND its
// own process-local HMAC. No production issuer or reviewed installed runtime is
// proved here; the test helper is not compiled into serving binaries.
type fixtureProfileVerifier struct {
	key     []byte
	allowed map[string]bool
	fail    bool
}

func (v *fixtureProfileVerifier) install(t *testing.T, g ports.OwnedProfilePublicationGrantV1) ports.OwnedProfilePublicationGrantV1 {
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

func (v *fixtureProfileVerifier) VerifyOwnedProfilePublication(g ports.OwnedProfilePublicationGrantV1, at time.Time) error {
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

type publicationSQLFixture struct {
	*ownedCurrentSQLFixture
	audits         map[string][]driver.Value
	pendingProfile []driver.Value
	pendingAudits  map[string][]driver.Value
	failAudit      bool
}

func (f *publicationSQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *publicationSQLFixture) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	f.isolations = append(f.isolations, options.Isolation)
	if options.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, errors.New("requires serializable transaction")
	}
	f.pendingProfile = append([]driver.Value(nil), f.profileRow...)
	f.pendingAudits = make(map[string][]driver.Value)
	return publicationFixtureTx{f}, nil
}

type publicationFixtureTx struct{ f *publicationSQLFixture }

func (tx publicationFixtureTx) Commit() error {
	f := tx.f
	f.commits++
	mode := ""
	if len(f.commitModes) > 0 {
		mode, f.commitModes = f.commitModes[0], f.commitModes[1:]
	}
	if mode != "rollback" {
		f.profileRow = append([]driver.Value(nil), f.pendingProfile...)
		for k, v := range f.pendingAudits {
			f.audits[k] = append([]driver.Value(nil), v...)
		}
	}
	f.pendingProfile, f.pendingAudits = nil, nil
	if f.afterCommit != nil {
		f.afterCommit()
	}
	if mode == "rollback" || mode == "ambiguous" {
		return retry.RetryableError(errors.New("fixture lost commit ACK"), retry.WithBackoff(retry.TypeNoBackoff))
	}
	if mode == "unknown" {
		return errors.New("fixture unknown commit")
	}
	return nil
}
func (tx publicationFixtureTx) Rollback() error {
	tx.f.rollbacks++
	tx.f.pendingProfile, tx.f.pendingAudits = nil, nil
	return nil
}
func (f *publicationSQLFixture) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query != ownedProfileAuditSQL {
		return f.ownedCurrentSQLFixture.QueryContext(ctx, query, args)
	}
	f.statements = append(f.statements, query)
	if len(args) != 3 || args[0].Value != f.profileArgs[0] {
		return nil, errors.New("foreign audit scope")
	}
	row := append([]driver.Value(nil), f.audits[args[2].Value.(string)]...)
	for i, value := range row {
		if raw, ok := value.(string); ok {
			cap := 65
			if i == 1 {
				cap = ownedAuthorityHeadBytes + 1
			}
			if len(raw) > cap {
				row[i] = raw[:cap]
			}
		}
	}
	return &explanationFixtureRows{values: row}, nil
}
func (f *publicationSQLFixture) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	f.execCalls++
	f.statements = append(f.statements, query)
	values := make([]driver.Value, len(args))
	for i := range args {
		values[i] = args[i].Value
	}
	switch query {
	case ownedProfileWriteSQL:
		if len(values) != 20 || !reflect.DeepEqual(values[:9], f.profileArgs) {
			return nil, errors.New("foreign write scope")
		}
		f.pendingProfile = append([]driver.Value(nil), values[9:]...)
	case ownedProfileAuditWriteSQL:
		if f.failAudit {
			return nil, errors.New("fixture audit write failure")
		}
		if len(values) != 8 || values[0] != f.profileArgs[0] {
			return nil, errors.New("foreign audit write")
		}
		id := values[2].(string)
		if f.audits[id] != nil || f.pendingAudits[id] != nil {
			return nil, errors.New("duplicate immutable audit")
		}
		f.pendingAudits[id] = []driver.Value{values[3], values[4], values[5], values[6], values[7]}
	default:
		return nil, errors.New("non-manifest write")
	}
	return driver.RowsAffected(1), nil
}

func newPublicationFixture(t *testing.T) (*Store, *publicationSQLFixture, *fixtureProfileVerifier, ports.OwnedProfilePublicationGrantV1) {
	t.Helper()
	r := newOwnedReaderFixture(t)
	f := &publicationSQLFixture{ownedCurrentSQLFixture: r.driver, audits: make(map[string][]driver.Value)}
	f.profileRow = nil
	verifier := &fixtureProfileVerifier{key: []byte(strings.Repeat("fixture-key", 4)), allowed: make(map[string]bool)}
	db := sql.OpenDB(f)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := New(db, Options{OwnedProfilePublicationVerifier: verifier})
	if err != nil {
		t.Fatal(err)
	}
	g := ports.OwnedProfilePublicationGrantV1{Profile: r.profile.Clone(), IssuedAt: f.at, ExpiresAt: f.at.Add(10 * time.Second)}
	g = verifier.install(t, g)
	return store, f, verifier, g
}

func TestOwnedProfilePublicationCreateRevokeHistoricalReplay(t *testing.T) {
	s, f, v, g := newPublicationFixture(t)
	// Publisher's candidate timestamps are not a fake DB clock. Only the two
	// documented publication/update fields are replaced with transaction time.
	f.at = f.at.Add(time.Microsecond)
	first, err := s.PublishOwnedProfile(t.Context(), g)
	if err != nil || first.Replayed || first.Profile.RevocationGeneration != 1 || !first.Profile.UpdatedAt.Equal(f.at) || !first.Profile.Window.PublishedAt.Equal(f.at) || f.execCalls != 2 || len(f.audits) != 1 {
		t.Fatalf("create err=%v generation=%d replay=%v writes=%d audits=%d", err, first.Profile.RevocationGeneration, first.Replayed, f.execCalls, len(f.audits))
	}
	revoke := g.Clone()
	revoke.Profile = first.Profile.Clone()
	revoke.Profile.State = domain.OwnedAgentCurrentRevokedV1
	revoke.Profile.RevocationGeneration++
	revoke.Profile.MutationID = "revoke-fixture"
	revoke.ExpectedGeneration = 1
	revoke.ExpectedDigest = first.Digest
	revoke = v.install(t, revoke)
	second, err := s.PublishOwnedProfile(t.Context(), revoke)
	if err != nil || second.Replayed || second.Profile.State != domain.OwnedAgentCurrentRevokedV1 || f.execCalls != 4 || len(f.audits) != 2 {
		t.Fatalf("revoke err=%v state=%s replay=%v writes=%d audits=%d", err, second.Profile.State, second.Replayed, f.execCalls, len(f.audits))
	}
	tombstone := append([]driver.Value(nil), f.profileRow...)
	replayed, err := s.PublishOwnedProfile(t.Context(), g)
	if err != nil || !replayed.Replayed || !reflect.DeepEqual(replayed.Profile, first.Profile) || replayed.Digest != first.Digest || f.execCalls != 4 || !reflect.DeepEqual(f.profileRow, tombstone) {
		t.Fatalf("historical replay must not resurrect err=%v writes=%d generation=%d replay=%v", err, f.execCalls, replayed.Profile.RevocationGeneration, replayed.Replayed)
	}
	// Receipt result clones cannot mutate the persisted audit or current head.
	replayed.Profile.InputModalities[0] = domain.AgentInputImageV1
	again, err := s.PublishOwnedProfile(t.Context(), g)
	if err != nil || !reflect.DeepEqual(again.Profile, first.Profile) {
		t.Fatalf("receipt alias err=%v", err)
	}
}

func TestOwnedProfilePublicationDeniesBeforeWrites(t *testing.T) {
	for _, name := range []string{"missing_verifier", "foreign_issuer", "forged_mac", "changed_intent", "conflicting_id", "expired", "future", "verifier_error", "existing_create", "wrong_predecessor", "absent_predecessor", "unknown_price"} {
		t.Run(name, func(t *testing.T) {
			s, f, v, g := newPublicationFixture(t)
			switch name {
			case "missing_verifier":
				s.ownedProfilePublicationVerifier = nil
			case "foreign_issuer":
				s.ownedProfilePublicationVerifier = &fixtureProfileVerifier{key: []byte("foreign"), allowed: v.allowed}
			case "forged_mac":
				g.Authenticator[0] ^= 1
			case "changed_intent":
				g.Profile.InstructionVersion = "other-instructions"
			case "conflicting_id":
				if _, err := s.PublishOwnedProfile(t.Context(), g); err != nil {
					t.Fatal(err)
				}
				g.Profile.InstructionVersion = "conflicting"
				g = v.install(t, g)
				f.execCalls = 0
				f.commits = 0
			case "expired":
				f.at = g.ExpiresAt
			case "future":
				f.at = g.IssuedAt.Add(-time.Microsecond)
			case "verifier_error":
				v.fail = true
			case "existing_create":
				r := newOwnedReaderFixture(t)
				f.profileRow = append([]driver.Value(nil), r.driver.profileRow...)
			case "wrong_predecessor", "absent_predecessor":
				g.ExpectedGeneration = 1
				g.ExpectedDigest = strings.Repeat("f", 64)
				g.Profile.RevocationGeneration = 2
				g.Profile.State = domain.OwnedAgentCurrentRevokedV1
				g.Profile.MutationID = "revoke"
				g = v.install(t, g)
				if name == "wrong_predecessor" {
					r := newOwnedReaderFixture(t)
					f.profileRow = append([]driver.Value(nil), r.driver.profileRow...)
				}
			case "unknown_price":
				g.Profile.CostCeiling.MCPToolCost.PriceState = domain.ExternalServicePriceUnknownV1
				g.Profile.CostCeiling.MCPToolCost.MaxAmountMicrounits = nil
				g.Profile.CostCeiling.MaxTotalAmountMicrounits = nil
				g.Profile.Substrate.AdmissionCostCeilingDigest, _ = g.Profile.CostCeiling.Digest()
				g = v.install(t, g)
			}
			before := append([]driver.Value(nil), f.profileRow...)
			auditCount := len(f.audits)
			got, err := s.PublishOwnedProfile(t.Context(), g)
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.OwnedProfilePublicationReceiptV1{}) || f.execCalls != 0 || f.commits != 0 || !reflect.DeepEqual(before, f.profileRow) || len(f.audits) != auditCount {
				t.Fatalf("case=%s err=%v generation=%d receipt_digest=%s writes=%d commits=%d", name, err, got.Profile.RevocationGeneration, got.Digest, f.execCalls, f.commits)
			}
		})
	}
}

func TestOwnedProfilePublicationAuditFailureAndCommitRecovery(t *testing.T) {
	for _, name := range []string{"audit_failure", "rolled_back_retry", "committed_lost_ack", "unknown_commit", "retry_expired", "retry_source_revoked"} {
		t.Run(name, func(t *testing.T) {
			s, f, v, g := newPublicationFixture(t)
			switch name {
			case "audit_failure":
				f.failAudit = true
			case "rolled_back_retry":
				f.commitModes = []string{"rollback"}
			case "committed_lost_ack":
				f.commitModes = []string{"ambiguous"}
			case "unknown_commit":
				f.commitModes = []string{"unknown"}
			case "retry_expired":
				f.commitModes = []string{"rollback"}
				f.afterCommit = func() { f.at = g.ExpiresAt }
			case "retry_source_revoked":
				f.commitModes = []string{"rollback"}
				f.afterCommit = func() { v.fail = true }
			}
			got, err := s.PublishOwnedProfile(t.Context(), g)
			if name == "audit_failure" || name == "retry_expired" || name == "unknown_commit" || name == "retry_source_revoked" {
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.OwnedProfilePublicationReceiptV1{}) {
					t.Fatalf("ambiguous/denied result must be zero case=%s err=%v generation=%d receipt_digest=%s", name, err, got.Profile.RevocationGeneration, got.Digest)
				}
				if name != "unknown_commit" && (len(f.audits) != 0 || f.profileRow != nil) {
					t.Fatalf("rollback leaked head/audit case=%s", name)
				}
				return
			}
			if err != nil || got.Digest == "" || len(f.audits) != 1 || f.profileRow == nil || got.Replayed != (name == "committed_lost_ack") {
				t.Fatalf("recovery case=%s err=%v generation=%d replay=%v audits=%d", name, err, got.Profile.RevocationGeneration, got.Replayed, len(f.audits))
			}
			wantWrites := 4
			if name == "committed_lost_ack" {
				wantWrites = 2
			}
			if f.execCalls != wantWrites {
				t.Fatalf("case=%s writes=%d want=%d", name, f.execCalls, wantWrites)
			}
		})
	}
}

func TestOwnedProfilePublicationGrantCannotCrossJSONBoundary(t *testing.T) {
	_, _, _, g := newPublicationFixture(t)
	if _, err := json.Marshal(g); err == nil {
		t.Fatal("grant serialized")
	}
	var decoded ports.OwnedProfilePublicationGrantV1
	if err := json.Unmarshal([]byte(`{}`), &decoded); err == nil {
		t.Fatal("public grant decoded")
	}
}

func TestOwnedProfilePublicationEveryAuditScalarCorrelates(t *testing.T) {
	for _, name := range []string{"intent", "body", "digest", "generation", "time", "oversize", "float_generation", "null_then_long_body"} {
		t.Run(name, func(t *testing.T) {
			s, f, _, g := newPublicationFixture(t)
			if _, err := s.PublishOwnedProfile(t.Context(), g); err != nil {
				t.Fatal(err)
			}
			row := f.audits[g.Profile.MutationID]
			switch name {
			case "intent":
				row[0] = strings.Repeat("f", 64)
			case "body":
				row[1] = strings.Replace(row[1].(string), `"version":1`, `"version":2`, 1)
			case "digest":
				row[2] = strings.Repeat("f", 64)
			case "generation":
				row[3] = uint64(2)
			case "time":
				row[4] = f.at.Add(time.Microsecond)
			case "oversize":
				row[1] = strings.Repeat("x", ownedAuthorityHeadBytes+50)
			case "float_generation":
				row[3] = float64(1)
			case "null_then_long_body":
				row[0] = nil
				row[1] = strings.Repeat("x", ownedAuthorityHeadBytes+50)
			}
			before := append([]driver.Value(nil), f.profileRow...)
			f.execCalls = 0
			f.commits = 0
			got, err := s.PublishOwnedProfile(t.Context(), g)
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.OwnedProfilePublicationReceiptV1{}) || f.execCalls != 0 || f.commits != 0 || !reflect.DeepEqual(before, f.profileRow) {
				t.Fatalf("audit scalar=%s err=%v writes=%d commits=%d", name, err, f.execCalls, f.commits)
			}
		})
	}
}

func TestOwnedProfilePublicationReplacementAndExpiredRevoke(t *testing.T) {
	s, f, v, g := newPublicationFixture(t)
	first, err := s.PublishOwnedProfile(t.Context(), g)
	if err != nil {
		t.Fatal(err)
	}
	replacement := g.Clone()
	replacement.ExpectedGeneration = 1
	replacement.ExpectedDigest = first.Digest
	replacement.Profile = first.Profile.Clone()
	replacement.Profile.RevocationGeneration = 2
	replacement.Profile.ProfileRevision = 2
	replacement.Profile.InstallationRevision = 2
	replacement.Profile.ReviewReceipt.Digest = strings.Repeat("c", 64)
	replacement.Profile.MutationID = "reviewed-replacement"
	replacement = v.install(t, replacement)
	second, err := s.PublishOwnedProfile(t.Context(), replacement)
	if err != nil || second.Profile.RevocationGeneration != 2 {
		t.Fatalf("replacement err=%v", err)
	}
	// Cleanup/source-owner revocation can still retain an expired tombstone.
	// It requires a new independently installed bounded revoke intent, not a
	// refreshed old mutation or execution-window permission.
	f.at = second.Profile.Window.ExpiresAt.Add(time.Microsecond)
	revoke := replacement.Clone()
	revoke.Profile = second.Profile.Clone()
	revoke.Profile.State = domain.OwnedAgentCurrentRevokedV1
	revoke.Profile.RevocationGeneration = 3
	revoke.Profile.MutationID = "expired-source-revoke"
	revoke.ExpectedGeneration = 2
	revoke.ExpectedDigest = second.Digest
	revoke.IssuedAt = f.at
	revoke.ExpiresAt = f.at.Add(10 * time.Second)
	revoke = v.install(t, revoke)
	third, err := s.PublishOwnedProfile(t.Context(), revoke)
	if err != nil || third.Profile.State != domain.OwnedAgentCurrentRevokedV1 || !third.Profile.Window.ExpiresAt.Equal(second.Profile.Window.ExpiresAt) {
		t.Fatalf("expired revoke err=%v state=%s generation=%d", err, third.Profile.State, third.Profile.RevocationGeneration)
	}
}

func TestOwnedProfilePublicationCoherentReceiptCannotPredateGrant(t *testing.T) {
	s, f, v, g := newPublicationFixture(t)
	first, err := s.PublishOwnedProfile(t.Context(), g)
	if err != nil {
		t.Fatal(err)
	}
	f.at = f.at.Add(5 * time.Second)
	r := g.Clone()
	r.Profile = first.Profile.Clone()
	r.Profile.State = domain.OwnedAgentCurrentRevokedV1
	r.Profile.RevocationGeneration = 2
	r.Profile.MutationID = "later-revoke"
	r.ExpectedGeneration = 1
	r.ExpectedDigest = first.Digest
	r.IssuedAt = f.at
	r.ExpiresAt = f.at.Add(10 * time.Second)
	r = v.install(t, r)
	second, err := s.PublishOwnedProfile(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	// Change the canonical body AND physical projection coherently. This is
	// not merely corrupt JSON or divergent scalar time: without the grant lower
	// bound it reconstructs and hashes as a valid (but impossible) prior result.
	impossible := second.Profile.Clone()
	impossible.UpdatedAt = r.IssuedAt.Add(-time.Microsecond)
	raw, err := domain.EncodeOwnedAgentProfileCurrentV1(impossible)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := impossible.Digest()
	if err != nil {
		t.Fatal(err)
	}
	row := f.audits[r.Profile.MutationID]
	row[1], row[2], row[4] = string(raw), digest, impossible.UpdatedAt
	f.execCalls = 0
	f.commits = 0
	before := append([]driver.Value(nil), f.profileRow...)
	got, err := s.PublishOwnedProfile(t.Context(), r)
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.OwnedProfilePublicationReceiptV1{}) || f.execCalls != 0 || f.commits != 0 || !reflect.DeepEqual(before, f.profileRow) {
		t.Fatalf("coherent pre-grant receipt err=%v generation=%d writes=%d commits=%d", err, got.Profile.RevocationGeneration, f.execCalls, f.commits)
	}
}
