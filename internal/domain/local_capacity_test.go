package domain_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func localCapacityData() domain.LocalCapacityAssociationV1 {
	return domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: "tenant", OwnerUserID: "owner", ResourceID: "resource"}
}

func TestLocalCapacityCanonicalAssociation(t *testing.T) {
	t.Parallel()
	const golden = `{"version":1,"kind":"local_endpoint","tenant_id":"tenant","owner_id":"owner","resource_id":"resource"}`
	value := localCapacityData()
	raw, err := domain.EncodeLocalCapacityAssociationV1(value)
	if err != nil || string(raw) != golden {
		t.Fatalf("canonical association got=%s err=%v", raw, err)
	}
	decoded, err := domain.DecodeLocalCapacityAssociationV1(raw)
	if err != nil || decoded != value {
		t.Fatal("canonical roundtrip", err)
	}
	sum := sha256.Sum256([]byte(golden))
	want := hex.EncodeToString(sum[:])
	digest, err := value.Digest()
	key, keyErr := value.CapacityKey()
	if err != nil || keyErr != nil || digest != want || key != "local-v1/"+want || len(key) != 73 {
		t.Fatalf("digest/address mismatch digest=%s key=%s err=%v/%v", digest, key, err, keyErr)
	}
	if domain.SubscriptionConnectionID(key).Validate() == nil {
		t.Fatal("physical local address became a domain subscription identity")
	}
	for _, wrong := range []domain.LocalCapacityAssociationV1{
		{Version: 2, Kind: value.Kind, TenantID: value.TenantID, OwnerUserID: value.OwnerUserID, ResourceID: value.ResourceID},
		{Version: 1, Kind: domain.ProviderResourceSubscriptionV1, TenantID: value.TenantID, OwnerUserID: value.OwnerUserID, ResourceID: value.ResourceID},
		{Version: 1, Kind: value.Kind, TenantID: "tenant", OwnerUserID: "owner", ResourceID: ""},
		{Version: 1, Kind: value.Kind, TenantID: "tenant", OwnerUserID: "owner", ResourceID: strings.Repeat("r", 161)},
		{Version: 1, Kind: value.Kind, TenantID: "tenant", OwnerUserID: "owner", ResourceID: "resource/slash"},
	} {
		if raw, err := domain.EncodeLocalCapacityAssociationV1(wrong); err == nil || raw != nil {
			t.Fatal("invalid association encoded")
		}
	}
	for _, scope := range []struct {
		tenant   domain.TenantID
		owner    domain.UserID
		resource string
	}{
		{tenant: "foreign", owner: "owner", resource: "resource"},
		{tenant: "tenant", owner: "foreign", resource: "resource"},
		{tenant: "tenant", owner: "owner", resource: "foreign"},
	} {
		if value.ValidateForScope(scope.tenant, scope.owner, scope.resource) == nil {
			t.Fatal("foreign capacity scope accepted")
		}
	}
}

func TestLocalCapacityRejectsNoncanonicalBytes(t *testing.T) {
	t.Parallel()
	raw, err := domain.EncodeLocalCapacityAssociationV1(localCapacityData())
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"empty": nil, "null": []byte("null"), "array": []byte("[]"),
		"leading whitespace":  append([]byte(" "), raw...),
		"trailing whitespace": append(bytes.Clone(raw), '\n'),
		"trailing value":      append(bytes.Clone(raw), []byte("{}")...),
		"unknown":             append(bytes.Clone(raw[:len(raw)-1]), []byte(`,"extra":true}`)...),
		"duplicate":           bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"case alias":          bytes.Replace(raw, []byte(`"owner_id"`), []byte(`"OWNER_ID"`), 1),
		"numeric alias":       bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1.0`), 1),
		"string version":      bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":"1"`), 1),
		"number owner":        bytes.Replace(raw, []byte(`"owner_id":"owner"`), []byte(`"owner_id":1`), 1),
		"null owner":          bytes.Replace(raw, []byte(`"owner_id":"owner"`), []byte(`"owner_id":null`), 1),
		"missing field":       bytes.Replace(raw, []byte(`,"resource_id":"resource"`), nil, 1),
		"order alias":         []byte(`{"kind":"local_endpoint","version":1,"tenant_id":"tenant","owner_id":"owner","resource_id":"resource"}`),
		"format whitespace":   []byte(`{ "version" : 1, "kind" : "local_endpoint", "tenant_id" : "tenant", "owner_id" : "owner", "resource_id" : "resource" }`),
		"over cap":            bytes.Repeat([]byte(" "), domain.OwnedAgentCurrentMaxRecordBytesV1+1),
		"invalid UTF8":        append(bytes.Clone(raw), 0xff),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			value, err := domain.DecodeLocalCapacityAssociationV1(body)
			if err == nil || value != (domain.LocalCapacityAssociationV1{}) {
				t.Fatal("noncanonical record produced usable association")
			}
			var nested struct {
				Association domain.LocalCapacityAssociationV1 `json:"association"`
			}
			// JSONDocument may reorder members or change whitespace, but its
			// structural contract does not permit key/type/value aliases. Only
			// the separately persisted String requires exact producer bytes.
			err = json.Unmarshal(append(append([]byte(`{"association":`), body...), '}'), &nested)
			formatOnly := name == "leading whitespace" || name == "trailing whitespace" || name == "order alias" || name == "format whitespace"
			if formatOnly {
				if err != nil || nested.Association != localCapacityData() {
					t.Fatalf("JSONDocument formatting changed valid association: format=%s err=%v", name, err)
				}
			} else if err == nil {
				t.Fatal("outer JSON decoder introduced a key/type/value alias")
			}
		})
	}
}

func TestLocalCapacityNestedRequiredMembersRemainStrict(t *testing.T) {
	t.Parallel()
	canonical, err := domain.EncodeLocalCapacityAssociationV1(localCapacityData())
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"version", "kind", "tenant_id", "owner_id", "resource_id"} {
		for _, fault := range []string{"missing", "null", "case"} {
			t.Run(field+"/"+fault, func(t *testing.T) {
				var members map[string]json.RawMessage
				if err := json.Unmarshal(canonical, &members); err != nil {
					t.Fatal(err)
				}
				switch fault {
				case "missing":
					delete(members, field)
				case "null":
					members[field] = json.RawMessage("null")
				case "case":
					members[strings.ToUpper(field)] = members[field]
					delete(members, field)
				}
				body, err := json.Marshal(members)
				if err != nil {
					t.Fatal(err)
				}
				var value domain.LocalCapacityAssociationV1
				if err := json.Unmarshal(body, &value); err == nil {
					t.Fatalf("structural decoder accepted field=%s fault=%s", field, fault)
				}
			})
		}
	}
}

func TestLocalCapacityJSONDocumentNormalizationPreservesCanonicalIdentity(t *testing.T) {
	t.Parallel()
	association := localCapacityData()
	canonical, err := domain.EncodeLocalCapacityAssociationV1(association)
	if err != nil {
		t.Fatal("encode original canonical association", err)
	}
	digest, err := association.Digest()
	if err != nil {
		t.Fatal("digest original canonical association", err)
	}
	at := time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	run := domain.Run{ID: "run", TenantID: association.TenantID, SessionID: "session", TriggerEventID: "event", LocalCapacityAssociation: &association,
		Status: domain.RunQueued, IdempotencyKey: "intent", CreatedAt: at, UpdatedAt: at}
	reservation := domain.QuotaReservation{ID: "reservation", TenantID: run.TenantID, RunID: run.ID, LocalCapacityAssociation: &association,
		Status: domain.ReservationHeld, CapacityUnits: 1, HeldAt: at, ExpiresAt: at.Add(time.Hour), UpdatedAt: at}
	normalize := func(value any) []byte {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal("marshal original JSONDocument carrier", err)
		}
		// RawMessage preserves exact integer/string VALUES. Maps deliberately
		// reorder members; formatting is independent of semantic association.
		var document map[string]json.RawMessage
		if err := json.Unmarshal(raw, &document); err != nil {
			t.Fatal("read carrier members without float conversion", err)
		}
		var members map[string]json.RawMessage
		if err := json.Unmarshal(document["local_capacity_association"], &members); err != nil {
			t.Fatal("read association members without float conversion", err)
		}
		reordered, err := json.Marshal(members)
		if err != nil || bytes.Equal(reordered, canonical) {
			t.Fatal("causal setup did not reorder canonical association", err)
		}
		if _, err := domain.DecodeLocalCapacityAssociationV1(reordered); err == nil {
			t.Fatal("strict persisted String accepted reordered members")
		}
		document["local_capacity_association"] = reordered
		raw, err = json.Marshal(document)
		if err != nil {
			t.Fatal("marshal reordered carrier", err)
		}
		var formatted bytes.Buffer
		if err := json.Indent(&formatted, raw, "", "  "); err != nil {
			t.Fatal("format normalized carrier", err)
		}
		return formatted.Bytes()
	}
	var decodedRun domain.Run
	if err := json.Unmarshal(normalize(run), &decodedRun); err != nil || decodedRun.Validate() != nil {
		t.Fatalf("normalized Run rejected its original structural association: err=%v", err)
	}
	var decodedReservation domain.QuotaReservation
	if err := json.Unmarshal(normalize(reservation), &decodedReservation); err != nil || decodedReservation.ValidateForRun(decodedRun) != nil {
		t.Fatalf("normalized quota rejected original Run/capacity correlation: err=%v", err)
	}
	for _, value := range []*domain.LocalCapacityAssociationV1{decodedRun.LocalCapacityAssociation, decodedReservation.LocalCapacityAssociation} {
		if value == nil || *value != association {
			t.Fatal("JSONDocument normalization changed association values")
		}
		got, err := domain.EncodeLocalCapacityAssociationV1(*value)
		gotDigest, digestErr := value.Digest()
		if err != nil || digestErr != nil || !bytes.Equal(got, canonical) || gotDigest != digest {
			t.Fatal("JSONDocument formatting changed exact canonical String identity/digest")
		}
	}
	// Optional pointer decoding must not turn a NULL local value into a usable
	// no-subscription Run/reservation or infer a legacy association.
	for _, carrier := range []any{run, reservation} {
		raw, err := json.Marshal(carrier)
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]json.RawMessage
		if err := json.Unmarshal(raw, &document); err != nil {
			t.Fatal(err)
		}
		document["local_capacity_association"] = json.RawMessage("null")
		raw, err = json.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		switch carrier.(type) {
		case domain.Run:
			var value domain.Run
			if json.Unmarshal(raw, &value) == nil && value.Validate() == nil {
				t.Fatal("NULL association became usable local Run")
			}
		case domain.QuotaReservation:
			var value domain.QuotaReservation
			if json.Unmarshal(raw, &value) == nil && value.ValidateForRun(run) == nil {
				t.Fatal("NULL association became usable local reservation")
			}
		}
	}
}

func TestLocalCapacityRunReservationAndSlotUseOneArm(t *testing.T) {
	association := localCapacityData()
	at := time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	run := domain.Run{ID: "run", TenantID: association.TenantID, SessionID: "session", TriggerEventID: "event", LocalCapacityAssociation: &association, Status: domain.RunQueued, IdempotencyKey: "intent", CreatedAt: at, UpdatedAt: at}
	reservation := domain.QuotaReservation{ID: "reservation", TenantID: run.TenantID, RunID: run.ID, LocalCapacityAssociation: &association, Status: domain.ReservationHeld, CapacityUnits: 1, HeldAt: at, ExpiresAt: at.Add(time.Hour), UpdatedAt: at}
	slot := domain.SubscriptionSchedulerSlot{TenantID: run.TenantID, LocalCapacityAssociation: &association, State: domain.SchedulerReady, UpdatedAt: at}
	if run.Validate() != nil || reservation.ValidateForRun(run) != nil || slot.Validate() != nil {
		t.Fatal("explicit local arm rejected")
	}
	clonedRun, clonedReservation := run.Clone(), reservation.Clone()
	clonedRun.LocalCapacityAssociation.ResourceID = "foreign"
	clonedReservation.LocalCapacityAssociation.OwnerUserID = "foreign"
	if *run.LocalCapacityAssociation != association || *reservation.LocalCapacityAssociation != association {
		t.Fatal("cloned capacity mutation reached original")
	}
	if clonedReservation.ValidateForRun(run) == nil {
		t.Fatal("foreign reservation accepted")
	}
	for _, fault := range []string{"mixed", "missing", "foreign tenant", "missing reservation", "wrong resource"} {
		t.Run(fault, func(t *testing.T) {
			r, q := run.Clone(), reservation.Clone()
			s := slot
			a := association
			s.LocalCapacityAssociation = &a
			switch fault {
			case "mixed":
				r.SubscriptionConnectionID, q.SubscriptionConnectionID, s.SubscriptionConnectionID = "subscription", "subscription", "subscription"
			case "missing":
				r.LocalCapacityAssociation, q.LocalCapacityAssociation, s.LocalCapacityAssociation = nil, nil, nil
			case "foreign tenant":
				r.LocalCapacityAssociation.TenantID, q.LocalCapacityAssociation.TenantID, s.LocalCapacityAssociation.TenantID = "foreign", "foreign", "foreign"
			case "missing reservation":
				q.LocalCapacityAssociation = nil
			case "wrong resource":
				q.LocalCapacityAssociation.ResourceID = "foreign"
			}
			if q.ValidateForRun(r) == nil {
				t.Fatal("mixed/missing/foreign reservation accepted")
			}
			if fault == "mixed" || fault == "missing" || fault == "foreign tenant" {
				if r.Validate() == nil || s.Validate() == nil {
					t.Fatal("mixed/missing/foreign Run or slot accepted")
				}
			}
		})
	}
	legacy := run.Clone()
	legacy.LocalCapacityAssociation, legacy.SubscriptionConnectionID = nil, "subscription"
	encoded, err := json.Marshal(legacy)
	const golden = `{"id":"run","tenant_id":"tenant","session_id":"session","trigger_event_id":"event","subscription_connection_id":"subscription","status":"queued","idempotency_key":"intent","created_at":"2026-10-20T01:00:00Z","updated_at":"2026-10-20T01:00:00Z"}`
	if err != nil || string(encoded) != golden || legacy.Validate() != nil {
		t.Fatalf("legacy bytes changed got=%s err=%v", encoded, err)
	}
	var roundtrip domain.Run
	encoded, _ = json.Marshal(run)
	if json.Unmarshal(encoded, &roundtrip) != nil || !reflect.DeepEqual(roundtrip, run) || bytes.Contains(encoded, []byte("subscription_connection_id")) {
		t.Fatal("local Run lost explicit arm or serialized fabricated subscription")
	}
}
