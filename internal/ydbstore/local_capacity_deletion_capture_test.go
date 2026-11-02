package ydbstore

import (
	"encoding/json"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestLocalCapacityRetainedDeletionRunScalars(t *testing.T) {
	legacy := currentWorkerAuthorityFixture(t).Run
	local := legacy.Clone()
	local.SubscriptionConnectionID = ""
	local.LocalCapacityAssociation = &domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1,
		TenantID: local.TenantID, OwnerUserID: "owner-capture", ResourceID: "local-capture-resource"}
	for _, run := range []domain.Run{legacy, local} {
		name := "subscription"
		if run.LocalCapacityAssociation != nil {
			name = "local"
		}
		t.Run(name, func(t *testing.T) {
			if err := run.Validate(); err != nil {
				t.Fatal("valid original Run required", err)
			}
			body, err := json.Marshal(run)
			if err != nil {
				t.Fatal(err)
			}
			cells := map[string]deletionCaptureCell{
				"tenant_id": deletionCaptureTestCell(t, "s", run.TenantID), "run_id": deletionCaptureTestCell(t, "s", run.ID),
				"session_id": deletionCaptureTestCell(t, "s", run.SessionID), "trigger_event_id": deletionCaptureTestCell(t, "s", run.TriggerEventID),
				"subscription_connection_id": deletionCaptureTestCell(t, "s", run.SubscriptionConnectionID), "status": deletionCaptureTestCell(t, "s", run.Status),
				"created_at": deletionCaptureTestCell(t, "t", run.CreatedAt), "updated_at": deletionCaptureTestCell(t, "t", run.UpdatedAt),
			}
			if err := validateDeletionCaptureScalars("runs", cells, body); err != nil {
				t.Fatal("original selected-arm Run/scalar correspondence rejected", err)
			}
			for _, fault := range []string{"missing_sql", "null_sql", "foreign_sql", "missing_json", "null_json", "empty_json", "foreign_json", "mixed_arm", "null_local", "malformed_local", "foreign_local_tenant", "wrong_run_scalar"} {
				t.Run(fault, func(t *testing.T) {
					var record map[string]json.RawMessage
					if err := json.Unmarshal(body, &record); err != nil {
						t.Fatal(err)
					}
					candidate := make(map[string]deletionCaptureCell, len(cells))
					for key, value := range cells {
						candidate[key] = value
					}
					switch fault {
					case "missing_sql":
						delete(candidate, "subscription_connection_id")
					case "null_sql":
						candidate["subscription_connection_id"] = deletionCaptureCell{Kind: "s", Value: json.RawMessage("null")}
					case "foreign_sql":
						candidate["subscription_connection_id"] = deletionCaptureTestCell(t, "s", "other-subscription")
					case "missing_json":
						if run.LocalCapacityAssociation != nil {
							delete(record, "local_capacity_association")
						} else {
							delete(record, "subscription_connection_id")
						}
					case "null_json":
						record["subscription_connection_id"] = json.RawMessage("null")
					case "empty_json":
						record["subscription_connection_id"] = json.RawMessage(`""`)
					case "foreign_json":
						record["subscription_connection_id"] = json.RawMessage(`"other-subscription"`)
					case "mixed_arm":
						raw, _ := json.Marshal(local.LocalCapacityAssociation)
						record["local_capacity_association"] = raw
						record["subscription_connection_id"] = json.RawMessage(`"subscription-mixed"`)
					case "null_local":
						record["local_capacity_association"] = json.RawMessage("null")
					case "malformed_local":
						record["local_capacity_association"] = json.RawMessage(`{"version":1,"kind":"unknown"}`)
					case "foreign_local_tenant":
						foreign := *local.LocalCapacityAssociation
						foreign.TenantID = "other-tenant"
						raw, _ := json.Marshal(foreign)
						record["local_capacity_association"] = raw
					case "wrong_run_scalar":
						candidate["run_id"] = deletionCaptureTestCell(t, "s", "other-run")
					}
					raw, err := json.Marshal(record)
					if err != nil {
						t.Fatal(err)
					}
					if err := validateDeletionCaptureScalars("runs", candidate, raw); err == nil {
						t.Fatal("invalid selected-arm Run/scalar correspondence accepted")
					}
				})
			}
		})
	}
}
