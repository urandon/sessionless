package ydbstore

import (
	"encoding/json"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// This validates the existing scripted running fixture's DATA projection, not
// normal canonical admission, a claim, funding or a new execution grant.
func TestLocalSessionFixturePreservesExplicitLocalCapacity(t *testing.T) {
	for _, auth := range []domain.ConfiguredInferenceAuthV1{domain.ConfiguredInferenceAuthNoneV1, domain.ConfiguredInferenceAuthBearerV1} {
		t.Run(string(auth), func(t *testing.T) {
			seed := newLocalSessionConnectedSeedWithAuth(t, "http://127.0.0.1:35421/v1", false, LocalCompactionConnectedLimits{}, auth)
			var run domain.Run
			var job domain.WorkerJob
			var quota domain.QuotaReservation
			var outbox domain.DispatchOutbox
			var attempt domain.Attempt
			for table, target := range map[string]any{"runs": &run, "worker_jobs": &job, "quota_reservations": &quota, "dispatch_outbox": &outbox, "attempts": &attempt} {
				rows := seed.parent.graph.rows[table]
				if len(rows) != 1 || json.Unmarshal([]byte(rows[0].(string)), target) != nil {
					t.Fatalf("scripted local fixture missing %s", table)
				}
			}
			if run.SubscriptionConnectionID != "" || quota.SubscriptionConnectionID != "" || run.LocalCapacityAssociation == nil || job.LocalCapacityAssociation == nil || quota.LocalCapacityAssociation == nil || outbox.LocalCapacityAssociation == nil ||
				*run.LocalCapacityAssociation != *job.LocalCapacityAssociation || *run.LocalCapacityAssociation != *quota.LocalCapacityAssociation || *run.LocalCapacityAssociation != *outbox.LocalCapacityAssociation ||
				run.LocalCapacityAssociation.ValidateForScope(seed.Scope.TenantID, seed.Scope.OwnerUserID, seed.parent.source.Profile.Locator.ResourceID) != nil || job.ValidateForRun(run) != nil || quota.ValidateForRun(run) != nil || outbox.ValidateForAttempt(run, attempt) != nil {
				t.Fatal("scripted local fixture mixed or lost original local identity")
			}
			missing := job.Clone()
			missing.LocalCapacityAssociation = nil
			if missing.ValidateForRun(run) == nil {
				t.Fatal("fixture compatibility weakened missing projection denial")
			}
			mixed := run.Clone()
			mixed.SubscriptionConnectionID = "legacy-subscription"
			if mixed.Validate() == nil {
				t.Fatal("fixture compatibility weakened mixed-arm denial")
			}
		})
	}
}
