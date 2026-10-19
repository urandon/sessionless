package ydbstore

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func privateConsentDiscoveryInput(f *privateConsentViewFixture) ports.DiscoverPrivateWebSearchConsentRequestV1 {
	return ports.DiscoverPrivateWebSearchConsentRequestV1{SessionID: f.input.SessionID, RunID: f.input.RunID, WebSessionDigest: f.input.WebSessionDigest}
}

func privateConsentDiscoveryStop(f *privateConsentViewFixture) domain.WebSearchConsentStateV1 {
	state := f.state.Clone()
	state.Status, state.Revision = domain.WebSearchConsentCancelledV1, 2
	return state
}

func privateConsentDiscoveryRun(t *testing.T, f *privateConsentViewFixture, change func(*domain.Run)) {
	t.Helper()
	var run domain.Run
	if err := json.Unmarshal([]byte(f.allocation.parent.graph.rows["runs"][0].(string)), &run); err != nil {
		t.Fatal("discovery fixture canonical Run", err)
	}
	change(&run)
	if err := run.Validate(); err != nil {
		t.Fatal("discovery fixture changed Run must remain valid", err)
	}
	ratedFixturePut(t, f.allocation.parent.graph.ratedSQLFixture, "runs", run)
}

func privateConsentDiscoveryJob(t *testing.T, f *privateConsentViewFixture, change func(*domain.WorkerJob)) {
	t.Helper()
	var job domain.WorkerJob
	if err := json.Unmarshal([]byte(f.allocation.parent.graph.rows["worker_jobs"][0].(string)), &job); err != nil {
		t.Fatal("discovery fixture canonical WorkerJob", err)
	}
	change(&job)
	ratedFixturePut(t, f.allocation.parent.graph.ratedSQLFixture, "worker_jobs", job)
}

func TestPrivateWebSearchConsentDiscoveryOpaqueCurrentPending(t *testing.T) {
	for _, twoStates := range []bool{false, true} {
		name := "one-pending"
		if twoStates {
			name = "one-pending-among-two-canonical-states"
		}
		t.Run(name, func(t *testing.T) {
			f := newPrivateConsentViewFixture(t)
			if twoStates {
				other := privateConsentDiscoveryStop(f)
				other.Request.ID, other.Request.CallID = "previous-request", "previous-call"
				other.Request.ArgumentsDigest, other.Request.QueryDigest = strings.Repeat("2", 64), strings.Repeat("3", 64)
				other.Request.Arguments.SHA256 = other.Request.ArgumentsDigest
				other.Request.Arguments.Key += ".previous"
				f.consentStates = []domain.WebSearchConsentStateV1{other, f.state.Clone()}
			}
			// Discovery may not read even a grant body. The exact-query route
			// owns that separate materialization and fresh authority rejoin.
			f.bodies = nil
			ctx, cancel := ownedAuthorityCallContext(t.Context())
			defer cancel()
			writes, web := f.allocation.parent.graph.writes, f.web
			start := len(f.allocation.parent.base.statements)
			got, err := f.allocation.parent.store.DiscoverPrivateWebSearchConsent(ctx, privateConsentDiscoveryInput(f))
			want := &ports.WebSearchConsentPendingV1{Locator: consentLocator(f.state.Request), ExpiresAt: f.state.Request.ExpiresAt}
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("current pending opaque locator: err=%v got=%+v want=%+v", err, got, want)
			}
			b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
			if b != f.budget || b.callbacks != 1 || b.statements > ownedAuthorityMaxStatements || b.bytes > ownedAuthorityMaxBytes || f.commits != 1 || len(f.reads) != 0 || f.allocation.parent.graph.writes != writes || f.web != web {
				t.Fatalf("discovery widened authority/effects/budget: callbacks=%d statements=%d bytes=%d commits=%d bodyReads=%v writes=%d/%d", b.callbacks, b.statements, b.bytes, f.commits, f.reads, f.allocation.parent.graph.writes, writes)
			}
			jobSQL, _, _ := ownedCanonicalJSONSelect("SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2")
			jobReads := 0
			for _, statement := range f.allocation.parent.base.statements[start:] {
				if statement == jobSQL {
					jobReads++
				}
				if strings.Contains(statement, "SELECT attempt_id FROM worker_jobs") || strings.Contains(statement, "ORDER BY") {
					t.Fatalf("discovery used an unvalidated scalar/scan instead of the canonical point payload: %s", statement)
				}
			}
			if jobReads != 1 {
				t.Fatalf("expected one cached canonical WorkerJob payload read, got=%d", jobReads)
			}
		})
	}
}

func TestPrivateWebSearchConsentDiscoveryAuthenticatedAbsence(t *testing.T) {
	for _, name := range []string{"queued-no-job", "running-no-job", "no-parent-yet", "no-consent-projection", "stopped-not-pending", "expired-pending"} {
		t.Run(name, func(t *testing.T) {
			f := newPrivateConsentViewFixture(t)
			switch name {
			case "queued-no-job":
				f.jobAbsent = true
				privateConsentDiscoveryRun(t, f, func(run *domain.Run) { run.Status, run.StartedAt = domain.RunQueued, nil })
			case "running-no-job":
				f.jobAbsent = true
			case "no-parent-yet":
				f.parentAbsent = true
			case "no-consent-projection":
				f.consentAbsent = true
			case "stopped-not-pending":
				f.state = privateConsentDiscoveryStop(f)
			case "expired-pending":
				f.allocation.parent.graph.at = f.state.Request.ExpiresAt
			}
			writes, web := f.allocation.parent.graph.writes, f.web
			got, err := f.allocation.parent.store.DiscoverPrivateWebSearchConsent(t.Context(), privateConsentDiscoveryInput(f))
			if err != nil || got != nil || f.commits != 1 || len(f.reads) != 0 || f.allocation.parent.graph.writes != writes || f.web != web {
				t.Fatalf("authenticated absence must be an effect-free confirmed read: err=%v got=%+v commits=%d bodyReads=%v writes=%d/%d", err, got, f.commits, f.reads, f.allocation.parent.graph.writes, writes)
			}
		})
	}
}

func TestPrivateWebSearchConsentDiscoveryDoesNotMaskDeniedAsNone(t *testing.T) {
	for _, name := range []string{"invalid-run", "foreign-cookie", "foreign-tenant", "foreign-session", "foreign-user", "stale-web-security", "revoked-web", "removed-participant", "foreign-job-session", "foreign-job-attempt", "malformed-job", "malformed-projection", "two-pending", "revoked-source", "cancelled-context", "unknown-commit", "cancel-after-commit"} {
		t.Run(name, func(t *testing.T) {
			f := newPrivateConsentViewFixture(t)
			input := privateConsentDiscoveryInput(f)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch name {
			case "invalid-run":
				input.RunID = ""
			case "foreign-cookie":
				input.WebSessionDigest = domain.DigestSecret("foreign-discovery-cookie")
			case "foreign-tenant":
				f.web.ActiveTenantID = "another-tenant"
			case "foreign-session":
				input.SessionID = "another-session"
			case "foreign-user":
				f.web.UserID = "another-user"
			case "stale-web-security":
				f.web.MembershipSecurityVersion++
			case "revoked-web":
				at := f.state.Request.CreatedAt
				f.web.RevokedAt = &at
			case "removed-participant":
				var participant domain.SessionParticipant
				if err := json.Unmarshal([]byte(f.allocation.parent.base.participant[0].(string)), &participant); err != nil {
					t.Fatal(err)
				}
				participant.Status = domain.SessionParticipantRemoved
				raw, err := json.Marshal(participant)
				if err != nil {
					t.Fatal(err)
				}
				f.allocation.parent.base.participant[0], f.allocation.parent.base.participant[2] = string(raw), string(participant.Status)
			case "foreign-job-session":
				privateConsentDiscoveryJob(t, f, func(job *domain.WorkerJob) { job.SessionID = "foreign-job-session" })
			case "foreign-job-attempt":
				privateConsentDiscoveryJob(t, f, func(job *domain.WorkerJob) { job.AttemptID = "another-attempt" })
			case "malformed-job":
				f.allocation.parent.graph.rows["worker_jobs"] = []driver.Value{"{"}
			case "malformed-projection":
				f.consentRowOverride = []driver.Value{uint64(1), uint64(1), "{", strings.Repeat("9", 64)}
			case "two-pending":
				second := f.state.Clone()
				second.Request.ID, second.Request.CallID = "second-request", "second-call"
				second.Request.ArgumentsDigest, second.Request.QueryDigest = strings.Repeat("2", 64), strings.Repeat("3", 64)
				second.Request.Arguments.SHA256 = second.Request.ArgumentsDigest
				f.consentStates = []domain.WebSearchConsentStateV1{f.state.Clone(), second}
			case "revoked-source":
				f.allocation.parent.verifier.reject = true
			case "cancelled-context":
				cancel()
			case "unknown-commit":
				f.commitErrorAt = 1
			case "cancel-after-commit":
				f.afterCommit = cancel
			}
			writes := f.allocation.parent.graph.writes
			got, err := f.allocation.parent.store.DiscoverPrivateWebSearchConsent(ctx, input)
			if !errors.Is(err, domain.ErrMembershipDenied) || got != nil || len(f.reads) != 0 || f.allocation.parent.graph.writes != writes {
				t.Fatalf("denied discovery became absence/locator or effects: err=%v got=%+v bodyReads=%v writes=%d/%d", err, got, f.reads, f.allocation.parent.graph.writes, writes)
			}
		})
	}
}

func TestPrivateWebSearchConsentDiscoveryNoJobStillAuthenticates(t *testing.T) {
	for _, name := range []string{"revoked-cookie", "foreign-session", "stale-security"} {
		t.Run(name, func(t *testing.T) {
			f := newPrivateConsentViewFixture(t)
			f.jobAbsent = true
			input := privateConsentDiscoveryInput(f)
			switch name {
			case "revoked-cookie":
				at := f.state.Request.CreatedAt
				f.web.RevokedAt = &at
			case "foreign-session":
				input.SessionID = "foreign-session"
			case "stale-security":
				f.web.MembershipSecurityVersion++
			}
			got, err := f.allocation.parent.store.DiscoverPrivateWebSearchConsent(t.Context(), input)
			if !errors.Is(err, domain.ErrMembershipDenied) || got != nil || len(f.reads) != 0 {
				t.Fatalf("missing Job bypassed session authorization: err=%v got=%+v reads=%v", err, got, f.reads)
			}
		})
	}
}

func TestPrivateWebSearchConsentDiscoveryOriginalBudget(t *testing.T) {
	for _, name := range []string{"callbacks", "metadata"} {
		t.Run(name, func(t *testing.T) {
			f := newPrivateConsentViewFixture(t)
			ctx, cancel := ownedAuthorityCallContext(t.Context())
			defer cancel()
			b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
			if name == "callbacks" {
				b.callbacks = ownedAuthorityMaxCallbacks
			} else {
				b.bytes = ownedAuthorityMaxBytes
			}
			got, err := f.allocation.parent.store.DiscoverPrivateWebSearchConsent(ctx, privateConsentDiscoveryInput(f))
			if !errors.Is(err, domain.ErrMembershipDenied) || got != nil || len(f.reads) != 0 || f.commits != 0 {
				t.Fatalf("discovery reset original %s budget: err=%v got=%+v reads=%v commits=%d", name, err, got, f.reads, f.commits)
			}
		})
	}
}
