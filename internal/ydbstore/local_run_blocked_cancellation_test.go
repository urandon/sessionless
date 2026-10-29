package ydbstore

import (
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestLocalRunCancellationAfterActualPressureDenial(t *testing.T) {
	for _, pressure := range []string{"slot busy", "tenant queue limit"} {
		t.Run(pressure, func(t *testing.T) {
			store, f, original, admission, request := localCancellationFixture(t)
			if pressure == "slot busy" {
				association := &domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: request.TenantID, OwnerUserID: request.UserID, ResourceID: original.receipt.Profile.Locator.ResourceID}
				physical, e := encodeCapacityStorageV1(request.TenantID, "", association)
				if e != nil {
					t.Fatal(e)
				}
				// Other queued work owns this same resource. This storage DATA
				// cannot authorize cancellation to touch that work or its hold.
				f.slot = []driver.Value{string(domain.SchedulerPressured), "other-run", "other-reservation", time.Unix(0, 0).UTC(), f.at, physical.association.String, physical.digest.String}
				f.counters = []driver.Value{int64(1), int64(0)}
			} else {
				f.counters = []driver.Value{int64(admission.Limits.MaxTenantQueueDepth), int64(0)}
			}
			blocked, e := store.AdmitDispatch(t.Context(), admission)
			if e != nil || blocked.Admitted {
				t.Fatalf("actual pressure did not deny admission: admitted=%t code=%s err=%v", blocked.Admitted, blocked.Code, e)
			}
			var run domain.Run
			if json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run) != nil || run.Status != domain.RunQuotaBlocked || len(f.rows["worker_jobs"]) != 0 || len(f.rows["quota_reservations"]) != 0 {
				t.Fatal("denial did not retain actual unadmitted blocked Run")
			}
			slot, counters, expiry := append([]driver.Value(nil), f.slot...), append([]driver.Value(nil), f.counters...), append([]driver.Value(nil), f.expiry...)
			cancelled, e := store.RequestManagedRunCancellationV1(t.Context(), request)
			if e != nil || cancelled.Status != domain.RunCancelled || !cancelled.NoHold || cancelled.Replayed {
				t.Fatalf("pressure-blocked cancellation: got=%+v err=%v", cancelled, e)
			}
			if !reflect.DeepEqual(slot, f.slot) || !reflect.DeepEqual(counters, f.counters) || !reflect.DeepEqual(expiry, f.expiry) || len(f.rows["worker_jobs"]) != 0 || len(f.rows["quota_reservations"]) != 0 {
				t.Fatal("no-hold cancellation changed capacity or another queued owner")
			}
			// Even with pressure cleared and the original Source still alive,
			// the cancelled intent cannot become a fresh admitted invocation.
			if len(f.slot) != 0 {
				f.slot[1], f.slot[2] = "", ""
			}
			f.counters = []driver.Value{int64(0), int64(0)}
			later, _ := store.AdmitDispatch(t.Context(), admission)
			if later.Admitted || len(f.rows["worker_jobs"]) != 0 || len(f.rows["quota_reservations"]) != 0 {
				t.Fatal("cancelled blocked work was later admitted")
			}
			writes := f.writes
			fresh, e := New(store.db, Options{OperationalRetention: fixtureRetention})
			if e != nil {
				t.Fatal(e)
			}
			replay, e := fresh.RequestManagedRunCancellationV1(t.Context(), request)
			if e != nil || !replay.Replayed || replay.Status != domain.RunCancelled || f.writes != writes {
				t.Fatal("fresh blocked-cancel replay repeated effects", replay, e)
			}
		})
	}
}

func TestLocalRunCancellationExpiredHistoryRejectsInconsistentCustody(t *testing.T) {
	for _, fault := range []string{"old run still owns slot", "old reservation still owns slot", "expiry projection remains", "queued with expired hold", "blocked with held quota"} {
		t.Run(fault, func(t *testing.T) {
			store, f, _, admission, request := localCancellationFixture(t)
			if admitted, e := store.AdmitDispatch(t.Context(), admission); e != nil || !admitted.Admitted {
				t.Fatal("actual admission", admitted, e)
			}
			var quota domain.QuotaReservation
			if json.Unmarshal([]byte(f.rows["quota_reservations"][0].(string)), &quota) != nil {
				t.Fatal("actual quota missing")
			}
			expiry := append([]driver.Value(nil), f.expiry...)
			f.at = quota.ExpiresAt
			if expired, e := store.ExpireQuotaReservation(t.Context(), ports.ExpiredQuotaReservation{TenantID: request.TenantID, RunID: request.RunID, ReservationID: quota.ID, LocalCapacityAssociation: quota.LocalCapacityAssociation, ExpiresAt: quota.ExpiresAt}, f.at); e != nil || !expired {
				t.Fatal("actual expiry", expired, e)
			}
			switch fault {
			case "old run still owns slot":
				f.slot[1], f.slot[2] = string(request.RunID), "inconsistent-reservation"
			case "old reservation still owns slot":
				f.slot[1], f.slot[2] = "inconsistent-run", string(quota.ID)
			case "expiry projection remains":
				f.expiry = expiry
			case "queued with expired hold":
				var r domain.Run
				if json.Unmarshal([]byte(f.rows["runs"][0].(string)), &r) != nil {
					t.Fatal("actual Run missing")
				}
				r.Status = domain.RunQueued
				body, e := json.Marshal(r)
				if e != nil {
					t.Fatal(e)
				}
				f.rows["runs"] = []driver.Value{string(body)}
			case "blocked with held quota":
				body, e := json.Marshal(quota)
				if e != nil {
					t.Fatal(e)
				}
				f.rows["quota_reservations"] = []driver.Value{string(body)}
				f.expiry = expiry
			}
			rows := cloneLocalInstallationRows(f.rows)
			slot, counters, expiryBefore := append([]driver.Value(nil), f.slot...), append([]driver.Value(nil), f.counters...), append([]driver.Value(nil), f.expiry...)
			writes := f.writes
			got, e := store.RequestManagedRunCancellationV1(t.Context(), request)
			if e == nil || got != (ports.ManagedRunCancellationResultV1{}) || f.writes != writes || !reflect.DeepEqual(rows, f.rows) || !reflect.DeepEqual(slot, f.slot) || !reflect.DeepEqual(counters, f.counters) || !reflect.DeepEqual(expiryBefore, f.expiry) {
				t.Fatal("inconsistent expired custody was accepted or repaired", fault, got, e)
			}
		})
	}
}

func TestLocalRunCancellationAfterActualQueuedTTL(t *testing.T) {
	store, f, original, admission, request := localCancellationFixture(t)
	if admitted, e := store.AdmitDispatch(t.Context(), admission); e != nil || !admitted.Admitted {
		t.Fatalf("actual admission: %+v %v", admitted, e)
	}
	var quota domain.QuotaReservation
	if json.Unmarshal([]byte(f.rows["quota_reservations"][0].(string)), &quota) != nil {
		t.Fatal("actual quota missing")
	}
	f.at = quota.ExpiresAt
	expired, e := store.ExpireQuotaReservation(t.Context(), ports.ExpiredQuotaReservation{TenantID: request.TenantID, RunID: request.RunID, ReservationID: quota.ID, LocalCapacityAssociation: quota.LocalCapacityAssociation, ExpiresAt: quota.ExpiresAt}, f.at)
	if e != nil || !expired {
		t.Fatalf("actual preclaim TTL: expired=%t err=%v", expired, e)
	}
	var run domain.Run
	if json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run) != nil || run.Status != domain.RunQuotaBlocked || json.Unmarshal([]byte(f.rows["quota_reservations"][0].(string)), &quota) != nil || quota.Status != domain.ReservationExpired || len(f.rows["worker_jobs"]) == 0 || len(f.expiry) != 0 {
		t.Fatal("TTL did not retain the original expired Job/hold")
	}
	_ = original.source.Close() // Retained cleanup must not need new funding.
	// The already-cleared resource may belong to another Run now. Cancellation
	// must not release it again or decrement that Run's counters.
	f.slot[1], f.slot[2] = "new-slot-owner", "new-slot-reservation"
	f.counters = []driver.Value{int64(1), int64(0)}
	slot, counters := append([]driver.Value(nil), f.slot...), append([]driver.Value(nil), f.counters...)
	quotaBefore := f.rows["quota_reservations"][0]
	got, e := store.RequestManagedRunCancellationV1(t.Context(), request)
	if e != nil || got.Status != domain.RunCancelled || !got.NoHold || got.Replayed {
		t.Fatalf("expired queued cancellation: got=%+v err=%v", got, e)
	}
	if f.rows["quota_reservations"][0] != quotaBefore || len(f.expiry) != 0 || !reflect.DeepEqual(slot, f.slot) || !reflect.DeepEqual(counters, f.counters) {
		t.Fatal("expired cancellation released capacity twice or touched its new owner")
	}
	writes := f.writes
	if got, e = store.RequestManagedRunCancellationV1(t.Context(), request); e != nil || !got.Replayed || f.writes != writes {
		t.Fatal("expired cancellation replay repeated effects", got, e)
	}
}
