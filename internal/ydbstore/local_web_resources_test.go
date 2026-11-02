package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Canonical principal DATA plus the actual original installation publication
// peer. No Attempt, spending head, subscription, admission or execution exists.
type localWebObservationSQL struct {
	*localInstallationSQLFixture
	scope          domain.AttemptExecutionSourceScopeV1
	session        domain.Session
	member         domain.TenantMembership
	participant    domain.SessionParticipant
	principalReads int
}

func (s *localWebObservationSQL) Connect(context.Context) (driver.Conn, error) { return s, nil }
func (s *localWebObservationSQL) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if statement != ownedAdmissionPrincipalSQL {
		return s.localInstallationSQLFixture.QueryContext(ctx, statement, args)
	}
	bucket, err := webBucket(string(s.scope.OwnerUserID))
	if err != nil {
		return nil, err
	}
	want := []driver.Value{string(s.scope.TenantID), string(s.scope.SessionID), string(s.scope.OwnerUserID), int64(bucket)}
	got := make([]driver.Value, len(args))
	for i, a := range args {
		got[i] = a.Value
	}
	if !reflect.DeepEqual(want, got) {
		return nil, errors.New("local status foreign principal key")
	}
	s.principalReads++
	encoded := func(v any) (string, error) { raw, e := json.Marshal(v); return string(raw), e }
	sr, e := encoded(s.session)
	if e != nil {
		return nil, e
	}
	mr, e := encoded(s.member)
	if e != nil {
		return nil, e
	}
	pr, e := encoded(s.participant)
	if e != nil {
		return nil, e
	}
	return &ownedAdmissionJoinRows{columns: make([]string, 13), values: []driver.Value{sr, string(s.session.CreatedBy), string(s.session.Status), int64(s.session.LastEventSequence), int64(0), mr, string(s.member.Role), string(s.member.Status), int64(s.member.SecurityVersion), pr, string(s.participant.Role), string(s.participant.Status), int64(0)}}, nil
}
func (*localWebObservationSQL) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("compute observation attempted mutation")
}

func localWebObservationFixture(t *testing.T) (*originalLocalRecoveryFixture, *localWebObservationSQL, ports.WebResourceStore, ports.ComputeConnectionResolveRequest) {
	t.Helper()
	f := newOriginalLocalRecoveryFixture(t, 0)
	at := f.sql.at
	s := &localWebObservationSQL{localInstallationSQLFixture: f.sql, scope: f.scope,
		session:     domain.Session{ID: f.scope.SessionID, TenantID: f.scope.TenantID, CreatedBy: f.scope.OwnerUserID, Status: domain.SessionActive, LastEventSequence: 1, CreatedAt: at.Add(-time.Hour), UpdatedAt: at},
		member:      domain.TenantMembership{TenantID: f.scope.TenantID, UserID: f.scope.OwnerUserID, Role: domain.TenantMembershipMember, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: at.Add(-time.Hour), UpdatedAt: at},
		participant: domain.SessionParticipant{TenantID: f.scope.TenantID, SessionID: f.scope.SessionID, UserID: f.scope.OwnerUserID, Role: domain.SessionParticipantMember, Status: domain.SessionParticipantActive, CreatedAt: at.Add(-time.Hour), UpdatedAt: at}}
	db := sql.OpenDB(s)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if e := db.Close(); e != nil {
			t.Error(e)
		}
	})
	f.store.db = db
	r, err := NewLocalWebResourceStore(f.store, f.source, f.receipt)
	if err != nil {
		t.Fatal(err)
	}
	return f, s, r, ports.ComputeConnectionResolveRequest{TenantID: f.scope.TenantID, UserID: f.scope.OwnerUserID, SessionID: f.scope.SessionID}
}

func TestLocalWebComputeObservationReadOnlyWithoutAttemptFunding(t *testing.T) {
	f, s, resources, request := localWebObservationFixture(t)
	heads, audits, writes := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits), f.sql.execCalls
	for range 3 {
		connections, err := resources.ResolveComputeConnectionsForUser(t.Context(), request)
		if err != nil || len(connections) != 1 {
			t.Fatal("original configured local observation", len(connections), err)
		}
		c := connections[0]
		if c.ID != "" || c.Entitlement != "" || c.Quota != "" || c.Provider != "local_model" || !c.ObservedAt.Equal(f.sql.at) || c.LocalCapacityAssociation == nil || c.LocalCapacityAssociation.ValidateForScope(request.TenantID, request.UserID, f.receipt.Profile.Locator.ResourceID) != nil {
			t.Fatal("local observation borrowed subscription or lost original identity")
		}
	}
	if s.principalReads != 3 || len(f.sql.fundingHeads) != 0 || len(f.sql.fundingAudits) != 0 {
		t.Fatal("observation published Attempt funding or skipped principal")
	}
	assertOriginalRecoveryNoWrites(t, f, writes, heads, audits)
}

func TestLocalWebComputeObservationDeniesLostCurrentAuthority(t *testing.T) {
	for _, fault := range []string{"foreign owner", "foreign tenant", "suspended membership", "viewer participant", "source closed", "profile revoked", "profile expired"} {
		t.Run(fault, func(t *testing.T) {
			f, s, resources, request := localWebObservationFixture(t)
			switch fault {
			case "foreign owner":
				request.UserID = "foreign-owner"
			case "foreign tenant":
				request.TenantID = "foreign-tenant"
			case "suspended membership":
				s.member.Status = domain.TenantMembershipSuspended
			case "viewer participant":
				s.participant.Role = domain.SessionParticipantViewer
			case "source closed":
				_ = f.source.Close()
			case "profile expired":
				f.sql.at = f.receipt.Profile.Window.ExpiresAt
			case "profile revoked":
				v := f.receipt.Profile.Clone()
				v.State, v.RevocationGeneration, v.MutationID = domain.OwnedAgentCurrentRevokedV1, 2, "local-status-revocation"
				raw, e := domain.EncodeOwnedAgentProfileCurrentV1(v)
				if e != nil {
					t.Fatal(e)
				}
				digest, e := v.Digest()
				if e != nil {
					t.Fatal(e)
				}
				row := f.sql.heads[localInstallationHeadKey(f.receipt.Profile)]
				row[0], row[3], row[4], row[5] = string(raw), int64(v.RevocationGeneration), string(v.State), digest
			}
			heads, audits, writes := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits), f.sql.execCalls
			c, e := resources.ResolveComputeConnectionsForUser(t.Context(), request)
			if len(c) != 0 {
				t.Fatal("lost authority remained selectable", fault, e)
			}
			if (fault == "foreign owner" || fault == "foreign tenant" || fault == "suspended membership" || fault == "viewer participant") && !errors.Is(e, domain.ErrMembershipDenied) {
				t.Fatal("unauthorized reader was not denied", fault, e)
			}
			assertOriginalRecoveryNoWrites(t, f, writes, heads, audits)
		})
	}
}
