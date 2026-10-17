package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This test-only overlay models the existing first-proxy row over the actual
// Source/allocation fixture. It does not model native YDB serializability,
// socket/producer authenticity, HTTP effects or serving composition.
type localProxySQL struct {
	allocationConn
	row, pending []driver.Value
	writes       int
}
type localProxyConnector struct{ f *localProxySQL }
type localProxyDriver struct{ f *localProxySQL }
type localProxyTx struct {
	driver.Tx
	f *localProxySQL
}

func (c localProxyConnector) Connect(context.Context) (driver.Conn, error) { return c.f, nil }
func (c localProxyConnector) Driver() driver.Driver                        { return localProxyDriver{c.f} }
func (d localProxyDriver) Open(string) (driver.Conn, error)                { return d.f, nil }
func (f *localProxySQL) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := f.allocationConn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	f.pending = nil
	return localProxyTx{Tx: tx, f: f}, nil
}
func (f *localProxySQL) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if statement != ownedProxyRegistrationSQL {
		return f.allocationConn.QueryContext(ctx, statement, args)
	}
	f.f.parent.base.recordQuery(statement, args)
	row := f.row
	if f.pending != nil {
		row = f.pending
	}
	if row == nil {
		row = []driver.Value{uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), "", "", "", "", "", ""}
	}
	return &explanationFixtureRows{values: append([]driver.Value(nil), row...)}, nil
}
func (f *localProxySQL) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if statement != ownedProxyRegistrationCreateSQL {
		return f.allocationConn.ExecContext(ctx, statement, args)
	}
	if len(args) != 9 || f.row != nil || f.f.row == nil || args[7].Value != f.f.parent.parentRow[14] || args[8].Value != f.f.row[9] {
		return nil, errors.New("local proxy fixture requires exact absent child and original parent/allocation")
	}
	r, err := f.f.parent.base.ExecContext(ctx, statement, args)
	if err != nil {
		return nil, err
	}
	f.writes++
	f.pending = []driver.Value{uint64(1), uint64(1), uint64(1), uint64(1), uint64(1), uint64(1), args[0].Value, args[1].Value, args[0].Value, args[1].Value, args[2].Value, args[3].Value}
	return r, nil
}
func (tx localProxyTx) Commit() error {
	err := tx.Tx.Commit()
	if err == nil && tx.f.pending != nil {
		tx.f.row = append([]driver.Value(nil), tx.f.pending...)
	}
	tx.f.pending = nil
	return err
}
func (tx localProxyTx) Rollback() error { tx.f.pending = nil; return tx.Tx.Rollback() }

// Deliberately scripted identity/current verification to isolate Store DATA
// behavior. It is not an implementation of the actual retained connector.
type localProxyDataVerifier struct {
	expected                     ports.OwnedProxyPublicationV2
	historyDenied, currentDenied bool
	historyCalls, currentCalls   int
}

func (v *localProxyDataVerifier) VerifyOwnedProxyPublication(p ports.OwnedProxyPublicationV2, at time.Time) error {
	v.historyCalls++
	if v.historyDenied || at.Before(p.ObservedAt) || !reflect.DeepEqual(p, v.expected) {
		return domain.ErrMembershipDenied
	}
	return nil
}
func (v *localProxyDataVerifier) VerifyOwnedProxyPublicationCurrent(p ports.OwnedProxyPublicationV2, at time.Time) error {
	v.currentCalls++
	if v.currentDenied || !at.Before(p.OperationDeadline) {
		return domain.ErrMembershipDenied
	}
	return v.VerifyOwnedProxyPublication(p, at)
}

type localProxyDataFixture struct {
	localRuntimeConnectedStore
	allocation domain.RegisteredAllocationV2
	proof      ports.OwnedProxyPublicationV2
	proxySQL   *localProxySQL
	verifier   *localProxyDataVerifier
}

func newLocalProxyDataFixture(t *testing.T) localProxyDataFixture {
	t.Helper()
	f := newLocalRuntimeConnectedStore(t)
	a, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := ports.BuildLocalLoopbackEgressPolicyV1(f.actual.config)
	if err != nil {
		t.Fatal(err)
	}
	pd, _ := policy.Digest()
	if pd != a.Original.Substrate.EgressPolicyDigest {
		t.Fatal("static policy was not frozen before installation")
	}
	at := f.parent.graph.at
	deadline := at.Add(min(5*time.Second, policy.Limits.OperationTimeout))
	r := a.Original.LocalRuntime
	peer, _ := policy.LiteralPeerDigest()
	att := ports.LocalLoopbackEgressAttestationV1{Version: 1, PolicyDigest: pd, ObservedAt: at, ExpiresAt: deadline,
		Observation: domain.RegisteredLocalLoopbackProxyV2{ConfiguredProfileDigest: r.ConfiguredProfileDigest, EndpointPolicy: policy.EndpointPolicy, ConnectorArtifactDigest: r.WorkerExecutableDigest, InstallationID: r.InstallationID, InstallationRevision: r.InstallationRevision, HostIdentityDigest: r.HostIdentityDigest, NetworkNamespaceIdentityDigest: r.NetworkNamespaceIdentityDigest, LiteralPeerDigest: peer}}
	pb, _ := json.Marshal(policy)
	ab, _ := json.Marshal(att)
	ad, _ := a.Original.Digest()
	sd, _ := f.parent.source.Digest()
	res := f.parent.parent.Reservation
	proof := ports.OwnedProxyPublicationV2{TransportKind: domain.RegisteredProxyTransportLocalLoopbackHTTPV2, OperationSequence: 1, OperationDigest: domain.AttemptOperationDigestV1(strings.Repeat("a", 64)), OperationDeadline: deadline, ServiceKind: domain.RegisteredProxyProviderV1,
		Scope: f.parent.source.Scope, LeaseID: res.LeaseID, FenceToken: res.FenceToken, PhysicalInvocationClaimID: res.PhysicalInvocationClaimID, RegisteredAllocationDigest: ad, SourceDigest: sd, ObservedAt: at, PolicyBody: pb, AttestationBody: ab, Authenticator: bytes.Repeat([]byte{1}, 32)}
	if err := proof.Validate(); err != nil {
		t.Fatalf("local DATA publication: %v", err)
	}
	fixture := &localProxySQL{allocationConn: allocationConn{f.sql}}
	db := sql.OpenDB(localProxyConnector{fixture})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	f.parent.store.db = db
	v := &localProxyDataVerifier{expected: proof.Clone()}
	f.parent.store.ownedProxyPublicationVerifier = v
	return localProxyDataFixture{f, a.Original, proof, fixture, v}
}
func (f localProxyDataFixture) read(t *testing.T) (*ownedProxySnapshot, error) {
	t.Helper()
	var value *ownedProxySnapshot
	err := f.parent.store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
		var err error
		value, err = readOwnedProxySnapshot(ctx, q, f.parent.parent, f.parent.source, f.allocation)
		return err
	})
	return value, err
}

func TestLocalProxyStoreDataPublicationReplayAndHistoricalRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalProxyDataFixture(t)
		first, replay, err := f.parent.store.PublishOwnedProxy(t.Context(), f.parent.request, f.proof)
		if err != nil || replay || first.TransportKind != domain.RegisteredProxyTransportLocalLoopbackHTTPV2 || first.LocalLoopback == nil || first.PinnedHTTPS != nil || f.proxySQL.writes != 1 {
			stage := "transaction"
			diagnostic := f.parent.store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
				stage = "parent"
				parent, source, burnAt, e := f.parent.store.readOwnedAllocationParent(ctx, q, f.parent.request)
				if e != nil {
					return e
				}
				stage = "allocation"
				allocation, e := readOwnedAllocationSnapshots(ctx, q, parent, source, burnAt)
				if e != nil || allocation == nil {
					return domain.ErrMembershipDenied
				}
				stage = "current"
				if e = f.parent.store.validateCurrentOwnedAttemptParent(ctx, q, f.parent.request, parent); e != nil {
					return e
				}
				stage = "accounting"
				accounting, e := readOwnedAttemptAccounting(ctx, q, parent, source)
				if e != nil || accounting == nil {
					return domain.ErrMembershipDenied
				}
				stage = "builder"
				bodies, e := f.proof.Bodies()
				if e != nil {
					return e
				}
				_, e = buildOwnedProxyRecord(bodies, allocation.original, source, f.proof.ServiceKind, f.proof.PolicyBody, f.proof.AttestationBody, domain.OwnedAgentAuthorityWindowV1{ObservedAt: f.proof.ObservedAt, PublishedAt: q.at, ExpiresAt: f.proof.OperationDeadline}, 1, f.proof.OperationDigest)
				return e
			})
			t.Fatalf("local Store DATA publication: replay=%t writes=%d err=%v history=%d current=%d diagnostic-stage=%s diagnostic=%v", replay, f.proxySQL.writes, err, f.verifier.historyCalls, f.verifier.currentCalls, stage, diagnostic)
		}
		row := append([]driver.Value(nil), f.proxySQL.row...)
		if f.verifier.currentCalls != 1 {
			t.Fatal("fresh publication omitted installed current verifier")
		}
		f.verifier.currentDenied = true // models closed producer, NOT an actual socket close
		second, replay, err := f.parent.store.PublishOwnedProxy(t.Context(), f.parent.request, f.proof.Clone())
		if err != nil || !replay || !reflect.DeepEqual(second, first) || f.proxySQL.writes != 1 || !reflect.DeepEqual(f.proxySQL.row, row) || f.verifier.currentCalls != 1 {
			t.Fatalf("historical replay renewed current permission/write: replay=%t err=%v", replay, err)
		}
		f.advance(t, f.proof.OperationDeadline)
		retained, err := f.read(t)
		if err != nil || retained == nil || !reflect.DeepEqual(retained.original, first) {
			t.Fatalf("expired historical body read: %v", err)
		}
		f.verifier.historyDenied = true
		if _, _, err := f.parent.store.PublishOwnedProxy(t.Context(), f.parent.request, f.proof); err == nil || f.proxySQL.writes != 1 {
			t.Fatal("unauthenticated historical carrier replayed")
		}
	})
}

func TestLocalProxyStoreDataFreshDenials(t *testing.T) {
	for _, name := range []string{"missing-producer", "foreign-producer", "closed-current", "expired", "source", "allocation", "session", "fence", "claim", "sequence", "mixed-body", "unknown-body", "noncanonical", "configured-profile", "installation", "literal-peer", "model-service"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newLocalProxyDataFixture(t)
				proof := f.proof.Clone()
				switch name {
				case "missing-producer":
					f.parent.store.ownedProxyPublicationVerifier = nil
				case "foreign-producer":
					f.verifier.historyDenied = true
				case "closed-current":
					f.verifier.currentDenied = true
				case "expired":
					f.advance(t, proof.OperationDeadline)
				case "source":
					proof.SourceDigest = strings.Repeat("b", 64)
				case "allocation":
					proof.RegisteredAllocationDigest = domain.RegisteredAllocationDigestV2(strings.Repeat("b", 64))
				case "session":
					proof.Scope.SessionID = "foreign-session"
				case "fence":
					proof.FenceToken++
				case "claim":
					proof.PhysicalInvocationClaimID = "foreign-claim"
				case "sequence":
					proof.OperationSequence = 2
				case "mixed-body":
					proof.TransportKind = domain.RegisteredProxyTransportPinnedHTTPSV2
				case "unknown-body":
					proof.AttestationBody = bytes.Replace(proof.AttestationBody, []byte(`{`), []byte(`{"unknown":true,`), 1)
				case "noncanonical":
					proof.PolicyBody = append([]byte(" "), proof.PolicyBody...)
				case "model-service":
					proof.ServiceKind = domain.RegisteredProxyWebSearchV1
				default:
					var att ports.LocalLoopbackEgressAttestationV1
					if err := json.Unmarshal(proof.AttestationBody, &att); err != nil {
						t.Fatal(err)
					}
					switch name {
					case "configured-profile":
						att.Observation.ConfiguredProfileDigest = strings.Repeat("b", 64)
					case "installation":
						att.Observation.InstallationRevision++
					case "literal-peer":
						att.Observation.LiteralPeerDigest = strings.Repeat("b", 64)
					}
					proof.AttestationBody, _ = json.Marshal(att)
				}
				// Give DATA substitutions the scripted verifier's assent so actual
				// canonical Store/body joins, not only fixture equality, must deny.
				if name != "foreign-producer" {
					f.verifier.expected = proof.Clone()
				}
				if got, _, err := f.parent.store.PublishOwnedProxy(t.Context(), f.parent.request, proof); err == nil || !reflect.DeepEqual(got, domain.RegisteredProxyAttestationV2{}) || f.proxySQL.writes != 0 {
					t.Fatalf("%s escaped Store denial: writes=%d err=%v", name, f.proxySQL.writes, err)
				}
			})
		})
	}
}

func TestLocalProxyStoreRetainedBodyCorruptionDenied(t *testing.T) {
	for _, name := range []string{"partial-row", "noncanonical-body", "unknown-body", "body-ref", "transport-digest", "tag", "source", "operation", "sequence", "expires", "divergent-current"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newLocalProxyDataFixture(t)
				first, _, err := f.parent.store.PublishOwnedProxy(t.Context(), f.parent.request, f.proof)
				if err != nil {
					t.Fatal(err)
				}
				changed := first.Clone()
				switch name {
				case "partial-row":
					f.proxySQL.row[5] = uint64(0)
				case "noncanonical-body":
					f.proxySQL.row[10] = " " + f.proxySQL.row[10].(string)
				case "unknown-body":
					f.proxySQL.row[11] = string(bytes.Replace([]byte(f.proxySQL.row[11].(string)), []byte(`{`), []byte(`{"unknown":true,`), 1))
				case "body-ref":
					changed.PolicyBody.Digest = strings.Repeat("b", 64)
				case "transport-digest":
					changed.TransportDigest = strings.Repeat("b", 64)
				case "tag":
					changed.TransportKind = domain.RegisteredProxyTransportPinnedHTTPSV2
				case "source":
					changed.SourceDigest = strings.Repeat("b", 64)
				case "operation":
					changed.OperationDigest = domain.AttemptOperationDigestV1(strings.Repeat("b", 64))
				case "sequence":
					changed.OperationSequence++
				case "expires":
					changed.Window.ExpiresAt = changed.Window.ExpiresAt.Add(time.Microsecond)
				case "divergent-current":
					f.proxySQL.row[9] = strings.Repeat("b", 64)
				}
				if !reflect.DeepEqual(first, changed) {
					raw, _ := json.Marshal(changed) // corrupt DATA must reach the reader, even if invalid
					digest := ports.OwnedFixtureBytesDigestV1(raw)
					f.proxySQL.row[6], f.proxySQL.row[8] = string(raw), string(raw)
					f.proxySQL.row[7], f.proxySQL.row[9] = digest, digest
					// A structurally valid changed operation is just another retained
					// observation; operation reservation/before-send owns its actual
					// original digest comparison. The body reader must preserve it.
					if name == "operation" {
						d, e := changed.Digest()
						if e != nil {
							t.Fatal(e)
						}
						f.proxySQL.row[7], f.proxySQL.row[9] = string(d), string(d)
						got, err := f.read(t)
						if err != nil || got.original.OperationDigest != changed.OperationDigest {
							t.Fatalf("reader rewrote operation digest: %v", err)
						}
						return
					}
					// Use the actual canonical domain digest where the mutated record
					// is otherwise valid; a bare hash mismatch must not hide body gaps.
					if d, e := changed.Digest(); e == nil {
						f.proxySQL.row[7], f.proxySQL.row[9] = string(d), string(d)
					}
				}
				if got, err := f.read(t); err == nil || got != nil {
					t.Fatalf("%s retained corruption accepted: %v", name, err)
				}
			})
		})
	}
}
