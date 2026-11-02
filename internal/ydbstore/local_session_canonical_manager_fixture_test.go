//go:build sessionless_mcp_offline

package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

// Only public worker-load/checkpoint and real Manager lease-write DATA are
// added. Lease IDs come from actual Store writes, never a preseeded claim.
type canonicalManagerSQL struct {
	*canonicalTerminalSQL
	leaseWrites, runningRunWrites, runningAttemptWrites, preleasePrincipals int
}

func (f *canonicalManagerSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *canonicalManagerSQL) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Rows, error) {
	seed := f.seed.LocalCanonicalLoopSeed
	r := seed.peer.base.request
	flat := completionSQL(s)
	if s == quotaExpiryStorageSQLV1 && len(seed.peer.base.rows["quota_expiry_v2"]) == 0 {
		bucket, e := ydbpartition.BucketV1(string(seed.Loaded.Reservation.ID))
		if e != nil || !f.exact(args, int64(bucket), seed.Loaded.Reservation.ExpiresAt, string(r.TenantID), string(seed.Loaded.Reservation.ID)) {
			return nil, errors.New("canonical Manager absent original terminal expiry key")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		return &compactionSourceRows{}, nil
	}
	if flat == "SELECT payload FROM checkpoints WHERE tenant_id = $1 AND attempt_id = $2 ORDER BY sequence DESC LIMIT 1" {
		if !f.exact(args, string(r.TenantID), string(r.AttemptID)) {
			return nil, errors.New("canonical Manager exact checkpoint scope")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		return &compactionSourceRows{}, nil
	}
	if flat == "SELECT payload FROM leases WHERE tenant_id = $1 AND lease_id = $2" && len(seed.peer.base.rows["leases"]) == 0 {
		if len(args) != 2 || args[0].Value != string(r.TenantID) {
			return nil, errors.New("canonical Manager absent lease exact tenant")
		}
		id, ok := args[1].Value.(string)
		if !ok || domain.LeaseID(id).Validate() != nil {
			return nil, errors.New("canonical Manager absent lease key")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		return &compactionSourceRows{}, nil
	}
	if s == ownedAdmissionPrincipalSQL && len(seed.peer.base.rows["lease_heads"]) == 0 {
		f.preleasePrincipals++
	}
	return f.canonicalTerminalSQL.QueryContext(ctx, s, args)
}
func (f *canonicalManagerSQL) ExecContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Result, error) {
	seed := f.seed.LocalCanonicalLoopSeed
	flat := completionSQL(s)
	if flat == localLifecycleLeaseWrite {
		var lease domain.Lease
		if len(args) != 10 || json.Unmarshal([]byte(args[9].Value.(string)), &lease) != nil || lease.TenantID != seed.Loaded.Run.TenantID || lease.RunID != seed.Loaded.Run.ID || lease.AttemptID != seed.Loaded.Attempt.ID || lease.WorkerID != seed.peer.worker || lease.FenceToken != 1 || lease.ValidateForAttempt(seed.Loaded.Run, seed.Loaded.Attempt) != nil {
			return nil, errors.New("canonical Manager actual lease projection")
		}
		if seed.peer.leaseID != "" && seed.peer.leaseID != lease.ID {
			return nil, errors.New("canonical Manager replaced original lease identity")
		}
		// Fixture routing learns only the ID already supplied to an actual
		// physical UPSERT. No domain Lease/authority is created by this assignment.
		seed.peer.leaseID = lease.ID
	}
	result, err := f.canonicalTerminalSQL.ExecContext(ctx, s, args)
	if err == nil {
		switch flat {
		case localLifecycleLeaseWrite:
			f.leaseWrites++
		}
		if !f.rawCanonical {
			if entry, ok := localCanonicalJoinWrites[flat]; ok {
				if entry.table == "runs" {
					var r domain.Run
					if json.Unmarshal([]byte(args[entry.payload].Value.(string)), &r) == nil && r.Status == domain.RunRunning {
						f.runningRunWrites++
					}
				}
				if entry.table == "attempts" {
					var a domain.Attempt
					if json.Unmarshal([]byte(args[entry.payload].Value.(string)), &a) == nil && a.Status == domain.AttemptRunning {
						f.runningAttemptWrites++
					}
				}
			}
		}
	}
	return result, err
}

type LocalCanonicalManagerSeed struct {
	*LocalCanonicalTerminalSeed
	manager *canonicalManagerSQL
}

func NewLocalCanonicalManagerSeed(t *testing.T, endpoint string) *LocalCanonicalManagerSeed {
	t.Helper()
	seed := NewLocalCanonicalTerminalSeed(t, endpoint)
	base := seed.LocalCanonicalLoopSeed.peer
	if seed.Loaded.Run.Status != domain.RunQueued || seed.Loaded.Attempt.Status != domain.AttemptCreated || len(base.base.rows["leases"]) != 0 || len(base.base.rows["lease_heads"]) != 0 || len(base.base.rows["lease_expiry_v2"]) != 0 || base.parent.parentRow != nil || base.alloc.row != nil {
		t.Fatal("Manager must start genuinely queued with no invocation facts")
	}
	base.leaseID = "" // no claimed ID until the actual Manager/Store writes it
	peer := &canonicalManagerSQL{canonicalTerminalSQL: seed.terminal}
	db := sql.OpenDB(peer)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if e := db.Close(); e != nil {
			t.Error(e)
		}
	})
	seed.DB = db
	return &LocalCanonicalManagerSeed{LocalCanonicalTerminalSeed: seed, manager: peer}
}
func (s *LocalCanonicalManagerSeed) ManagerLifecycle() (int, int, int, int) {
	return s.manager.preleasePrincipals, s.manager.leaseWrites, s.manager.runningRunWrites, s.manager.runningAttemptWrites
}
func (s *LocalCanonicalManagerSeed) ManagerParent(t *testing.T) domain.OwnedAttemptEffectParentV1 {
	t.Helper()
	row := s.LocalCanonicalLoopSeed.peer.parent.parentRow
	if len(row) != 15 {
		t.Fatal("actual Manager parent absent")
	}
	var parent domain.OwnedAttemptEffectParentV1
	if json.Unmarshal([]byte(row[13].(string)), &parent) != nil || parent.Validate() != nil {
		t.Fatal("actual Manager parent invalid")
	}
	return parent
}

// The exact terminal-absence route above cannot authorize missing queued
// capacity. Public Load is Manager's FIRST operation, before preflight/claim.
func TestLocalSessionCredentialedMCPOfflineCanonicalQueuedManagerMissingExpiry(t *testing.T) {
	seed := NewLocalCanonicalManagerSeed(t, "http://127.0.0.1:35421/v1")
	seed.LocalCanonicalLoopSeed.peer.base.rows["quota_expiry_v2"] = nil
	before := seed.Stats()
	store, e := New(seed.DB, seed.Options)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	_, _, e = store.LoadWorkerJob(ctx, seed.Loaded.Run.TenantID, seed.Loaded.Run.ID)
	if !errors.Is(e, domain.ErrMembershipDenied) || seed.Stats() != before || seed.Stats().LeasePresent || seed.Stats().Parents != 0 || seed.Stats().Burns != 0 || seed.Stats().Allocations != 0 {
		t.Fatal("missing queued hold authorized original Manager load/effects", e)
	}
}
