package ydbstore

import (
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/ports"
)

// Mutations affect only scripted CURRENT SQL DATA, not an original Source,
// private witness, policy or receipt. Genuine original entry/Store methods must
// rejoin those rows both before deriving and before sending the successor.
func TestLocalMCPConservativeCurrentFundingBeforeSuccessorAndSend(t *testing.T) {
	for _, stage := range []string{"prepare", "before_send"} {
		for _, fault := range []string{"payer", "currency", "price_revision", "resource_generation", "revocation_generation", "price_evidence", "terms_evidence", "expiry"} {
			t.Run(stage+"/"+fault, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f, mcp := completedOriginalMCPFixture(t)
					var capability ports.AttemptOperationCapabilityV1
					if stage == "before_send" {
						next, err := f.entry.PrepareModel(t.Context())
						if err != nil {
							t.Fatal("original pre-mutation successor", err)
						}
						installPreparedFutureProxy(t, f, next.operation)
						r, err := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: next.operation})
						if err != nil || r.Capability == nil {
							t.Fatal("original pre-mutation successor reservation", err)
						}
						capability = r.Capability.Clone()
					}
					reader := f.parent.base.reader
					spending := &reader.spending[2]
					switch fault {
					case "payer":
						spending.Locator.BillingOwnerUserID = "different-current-payer"
						spending.OwnerUserID = spending.Locator.BillingOwnerUserID
						spending.SourceProfile.Locator.OwnerUserID = spending.OwnerUserID
					case "currency":
						spending.Currency = "EUR"
					case "price_revision":
						spending.PriceRevision = "different-current-price"
					case "resource_generation":
						spending.ResourceGeneration++
					case "revocation_generation":
						spending.RevocationGeneration++
					case "price_evidence":
						spending.PriceEvidenceDigest = strings.Repeat("e", 64)
					case "terms_evidence":
						spending.TermsEvidenceDigest = strings.Repeat("e", 64)
					case "expiry":
						spending.Window.ExpiresAt = f.parent.graph.at.Add(time.Microsecond)
					}
					// Rehash only the public DATA envelope, not a Source MAC. This
					// makes the changed SQL record syntactically coherent while the
					// real original Source and its exact published row remain sealed.
					bindReaderSpendingSource(t, spending)
					reader.sync(t)
					if f.actual.source.VerifyOwnedAttemptEffectSource(f.entry.source) != nil {
						t.Fatal("negative SQL case accidentally invalidated original Source history")
					}
					rows := cloneGenerationRows(f.queries.operations)
					accounting := append([]driver.Value(nil), f.parent.sql.accountingRow...)
					objects := make(map[string][]byte, len(f.blobs.objects))
					for key, body := range f.blobs.objects {
						objects[key] = append([]byte(nil), body...)
					}
					writes, calls, observations := f.sql.operationWrites, f.sql.callWrites, f.sql.observationWrites
					if stage == "prepare" {
						if next, err := f.entry.PrepareModel(t.Context()); err == nil || next != nil {
							t.Fatal("changed current funding admitted successor", fault)
						}
					} else if err := f.store.AuthorizeAttemptOperationBeforeSend(t.Context(), capability); err == nil {
						t.Fatal("changed current funding admitted successor send", fault)
					}
					if !reflect.DeepEqual(rows, f.queries.operations) || !reflect.DeepEqual(accounting, f.parent.sql.accountingRow) || !reflect.DeepEqual(objects, f.blobs.objects) || f.sql.operationWrites != writes || f.sql.callWrites != calls || f.sql.observationWrites != observations || mcp.observation.Observation.Usage.AmountMicrounits != nil {
						t.Fatal("funding denial changed durable debit/observation or created another effect", fault)
					}
				})
			})
		}
	}
}
