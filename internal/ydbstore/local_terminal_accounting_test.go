package ydbstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func captureTerminalAccountingFixture(t *testing.T, f *compactionSourceFixture, h *LocalTerminalV1) (*localTerminalAccountingV2, error) {
	t.Helper()
	call, cancel := ownedAuthorityCallContext(t.Context())
	defer cancel()
	f.entry.preparationMu.Lock()
	defer f.entry.preparationMu.Unlock()
	h.source.state.mu.Lock()
	defer h.source.state.mu.Unlock()
	return prepareLocalTerminalAccounting(call, h, f.parent.graph.at, domain.OwnedAgentProfileCurrentV1{})
}

// This is residual DATA validation, not a retirement/erasure/consent producer.
// The MODEL frontier comes from the existing original capture. Synthetic stopped
// consent and retired-allocation postimages deliberately cannot mint its opaque
// erasure receipt, complete a terminal turn or authorize Session deletion.
func TestOwnedOperationResidualV2ConsentBodyCorrespondence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _, h := terminalOutputFixture(t)
		c, err := captureTerminalAccountingFixture(t, f, h)
		if err != nil {
			t.Fatal("original MODEL accounting prerequisite", err)
		}
		scope := c.disposition.Scope
		raw, err := json.Marshal(scope)
		if err != nil {
			t.Fatal(err)
		}
		ns := sha256.Sum256(append([]byte("sessionless.operation-body-scope.v1\x00"), raw...))
		prefix := domain.SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) + "attempt-operations-v1/" + hex.EncodeToString(ns[:]) + "/"
		original := f.entry.allocation.Clone()
		retired := original.Clone()
		retired.State, retired.UpdatedAt = domain.RegisteredPreparationRetiredV1, original.UpdatedAt.Add(time.Microsecond)
		retired.RevocationGeneration++
		if err := retired.ValidateTransitionFrom(original); err != nil {
			t.Fatal("structural retired postimage prerequisite", err)
		}
		v := ports.OwnedOperationResidualInventoryV2{Version: 2, Parent: f.entry.parent.Clone(), Source: f.entry.source.Clone(), OriginalAllocation: original, RetiredAllocation: retired, AccountingDisposition: c.disposition.Clone(), Operations: c.operations, Proxies: c.proxies, PhysicalInventoryDigest: strings.Repeat("d", 64)}
		for _, row := range c.operations {
			op, observation := row.Reservation.Operation, row.Observation
			v.Bodies = append(v.Bodies,
				domain.BlobRef{TenantID: scope.TenantID, Key: prefix + op.RequestRef, Size: int64(op.RequestBytes), SHA256: op.RequestDigest},
				domain.BlobRef{TenantID: scope.TenantID, Key: prefix + observation.ResponseRef, Size: int64(observation.ResponseBytes), SHA256: observation.ResponseDigest})
		}
		if err := v.Validate(); err != nil {
			t.Fatal("unchanged ordinary residual DATA prerequisite", err)
		}
		args := []byte(`{"query":"public"}`)
		argument, err := domain.AttemptOperationConsentArgumentsRefV1(scope, uint64(len(args)), ownedConsentTestHash(args))
		if err != nil {
			t.Fatal(err)
		}
		at := c.operations[0].Observation.ObservedAt
		request := domain.WebSearchConsentRequestV1{Version: 1, ID: "residual-custody-consent", Scope: ownedConsentScope(v.Parent), InitiatingUserID: scope.OwnerUserID, ModelResponseDigest: strings.Repeat("6", 64), CallID: "structural-call", ArgumentsDigest: argument.SHA256, QueryDigest: ownedConsentTestHash([]byte("public")), GrantDigest: strings.Repeat("7", 64), ConsentRevision: 1, RevocationGeneration: 1, Arguments: argument, CreatedAt: at, ExpiresAt: at.Add(time.Second)}
		if err := request.ValidateArguments(args); err != nil {
			t.Fatal("exact argument DATA prerequisite", err)
		}
		v.Consents = []domain.WebSearchConsentStateV1{{Request: request, Revision: 2, Status: domain.WebSearchConsentRevokedV1, UpdatedAt: at.Add(time.Microsecond)}}
		v.Bodies = append(v.Bodies, argument)
		if err := v.Validate(); err != nil {
			t.Fatal("exact mixed-arm residual DATA rejected", err)
		}
		encoded, err := v.IntentBytes()
		if err != nil {
			t.Fatal("mixed-arm residual canonical DATA", err)
		}
		for _, name := range []string{"missing-body", "foreign-body-scope", "noncanonical-key", "changed-body-digest", "changed-consent-size", "pending-consent", "ordinary-response-as-consent-key"} {
			var bad ports.OwnedOperationResidualInventoryV2
			if err := json.Unmarshal(encoded, &bad); err != nil {
				t.Fatal(err)
			}
			i := len(bad.Bodies) - 1
			switch name {
			case "missing-body":
				bad.Bodies = bad.Bodies[:i]
			case "foreign-body-scope":
				foreign := scope
				foreign.PhysicalInvocationClaimID = "other-claim"
				bad.Bodies[i], err = domain.AttemptOperationConsentArgumentsRefV1(foreign, uint64(argument.Size), argument.SHA256)
				if err != nil {
					t.Fatal(err)
				}
			case "noncanonical-key":
				bad.Bodies[i].Key += "/alias"
			case "changed-body-digest":
				bad.Bodies[i].SHA256 = strings.Repeat("8", 64)
			case "changed-consent-size":
				bad.Consents[0].Request.Arguments.Size++
			case "pending-consent":
				bad.Consents[0].Status, bad.Consents[0].Revision, bad.Consents[0].UpdatedAt = domain.WebSearchConsentPendingV1, 1, at
			case "ordinary-response-as-consent-key":
				// Keep the whole operation and every committed identity unchanged.
				// Matching bytes in a consent key cannot replace its ordinary body.
				// Use the small normalized response: the full MODEL request now
				// includes the installed tool schema and exceeds the consent cap.
				bad.Bodies[1], err = domain.AttemptOperationConsentArgumentsRefV1(scope, uint64(bad.Bodies[1].Size), bad.Bodies[1].SHA256)
				if err != nil {
					t.Fatal("same-size/digest alternate-key prerequisite", err)
				}
			}
			if err := bad.Validate(); err == nil {
				t.Error("invalid residual correspondence accepted", name)
			}
		}
		if _, err := c.Residual(retired, nil); err == nil {
			t.Fatal("structurally valid consent DATA replaced original erasure custody")
		}
	})
}

// Actual original Source/entry/issuer/Store capture, with scripted SQL funding
// and normalized MODEL DATA. No live provider, erasure or terminal commit.
func TestLocalTerminalAccountingOriginalCaptureHistoricalLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _, h := terminalOutputFixture(t)
		c, err := captureTerminalAccountingFixture(t, f, h)
		if err != nil || c == nil {
			t.Fatal("original accounting capture", err)
		}
		digest, err := c.Digest()
		if err != nil || domain.ValidateSHA256Digest("accounting", digest) != nil {
			t.Fatal("bounded accounting digest", err)
		}
		if c.disposition.ValidateFor(f.entry.parent, f.entry.source) != nil || len(c.proxies) != 1 || len(c.operations) != 1 {
			t.Fatal("incomplete original frontier")
		}
		row := c.disposition.Operations[0]
		if row.Disposition != ports.OwnedOperationUnknownReservedV2 || row.Reserved != c.operations[0].Reservation.Operation.Debit || row.Usage.InputTokens != nil || row.Usage.OutputTokens != nil || row.Usage.AmountMicrounits != nil {
			t.Fatal("unknown usage synthesized measurement or refunded reserve")
		}
		if !reflect.DeepEqual(c.disposition.AggregateDebit, h.source.state.snapshot.Accounting.Aggregate) || c.disposition.ComponentAmounts != h.source.state.snapshot.Accounting.Amounts {
			t.Fatal("original aggregate/components changed")
		}
		if _, err := json.Marshal(c); err == nil {
			t.Fatal("private original accounting serialized")
		}
		copyCapture := *c
		if _, err := copyCapture.Digest(); err == nil {
			t.Fatal("copied capture acquired ownership")
		}
		if err := f.actual.source.Close(); err != nil {
			t.Fatal(err)
		}
		m := f.entry.preparedModel.Load()
		if f.store.validateOwnedLocalInvoiceEvidence(m.price.local, m.price.profile, f.entry.source, m.operation, f.parent.graph.at) == nil {
			t.Fatal("active invoice check weakened after Source close")
		}
		after, err := c.Digest()
		if err != nil || after != digest {
			t.Fatal("captured historical content requires live Source", err)
		}
		if _, err := captureTerminalAccountingFixture(t, f, h); err == nil {
			t.Fatal("new capture accepted closed Source")
		}
		if _, err := c.Residual(domain.RegisteredAllocationV2{}, nil); err == nil {
			t.Fatal("DATA capture substituted retirement/physical erasure")
		}
	})
}

func TestLocalTerminalAccountingRejectsCorruptedCapturedData(t *testing.T) {
	for _, name := range []string{"invoice", "reservation", "unknown_enum", "scope", "row_count", "component"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _, h := terminalOutputFixture(t)
				c, err := captureTerminalAccountingFixture(t, f, h)
				if err != nil {
					t.Fatal(err)
				}
				switch name {
				case "invoice":
					c.invoice.terms.ProviderInvoiceAmount = 1
				case "reservation":
					c.disposition.Operations[0].Reserved.OutputTokens = 1
				case "unknown_enum":
					c.disposition.Operations[0].Disposition = "free"
				case "scope":
					c.disposition.Scope.RunID = "foreign"
				case "row_count":
					c.disposition.Operations = nil
				case "component":
					c.disposition.ComponentAmounts[0] = 1
				}
				if _, err := c.Digest(); err == nil {
					t.Fatal("corrupted historical accounting accepted")
				}
			})
		})
	}
}
