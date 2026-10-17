package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Actual private-file intake and real publisher code over an owned SQL model;
// synthetic declarations and executable BYTES only, never native/model proof.
func localOperatorFiles(t *testing.T) (root, executable string, manifest localoperator.ManifestV1) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	executable = filepath.Join(root, "worker-fixture")
	if err := os.WriteFile(executable, []byte("synthetic bytes; never executed"), 0o700); err != nil {
		t.Fatal(err)
	}
	identity, err := localoperator.InspectRuntime(t.Context(), executable)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Minute)
	r := newOwnedReaderFixtureAt(t, at, "-local-source")
	g := syntheticLocalInstallationGrant(t, r.profile)
	c, p := g.LocalInstallation.Profile.Clone(), g.Profile.Clone()
	c.WorkerExecutableDigest, c.LocalHostIdentityDigest, c.LocalNetworkNamespaceIdentityDigest = identity.WorkerExecutableDigest, identity.LocalHostIdentityDigest, identity.LocalNetworkNamespaceIdentityDigest
	p.Substrate.OuterHarnessArtifactDigest, p.Substrate.LocalHostIdentityDigest, p.Substrate.LocalNetworkNamespaceIdentityDigest = identity.WorkerExecutableDigest, identity.LocalHostIdentityDigest, identity.LocalNetworkNamespaceIdentityDigest
	terms := localoperator.InvoiceTermsV1{Version: 1, TenantID: c.TenantID, OwnerUserID: c.OwnerUserID, BaseURL: c.BaseURL, ModelVendorID: c.ModelVendorID, ModelID: c.ModelID, Currency: p.CostCeiling.Currency, Revision: p.CostCeiling.PriceRevision, OwnerSuppliesModel: true, OwnerSuppliesHost: true, IssuedAt: c.IssuedAt, ExpiresAt: c.ExpiresAt}
	termBytes, err := json.Marshal(terms)
	if err != nil {
		t.Fatal(err)
	}
	body := func(id string, raw []byte) localoperator.EvidenceBodyV1 {
		sum := sha256.Sum256(raw)
		return localoperator.EvidenceBodyV1{Reference: domain.OwnedAgentAuthorityBodyRefV1{Ref: id, Digest: hex.EncodeToString(sum[:]), Bytes: uint64(len(raw))}, Body: raw}
	}
	invoice := body("synthetic-owner-local-invoice", termBytes)
	capability := body("synthetic-capability-declarations", []byte("test declarations only, not a model qualification"))
	review := body("synthetic-review-body", []byte("test metadata only; not independent runtime review"))
	c.OperatorTerms, c.CapabilityEvidence = invoice.Reference, capability.Reference
	p.ProviderPriceDigest, p.ProviderTermsDigest, p.SubstratePriceDigest, p.SubstrateTermsDigest = invoice.Reference.Digest, invoice.Reference.Digest, invoice.Reference.Digest, invoice.Reference.Digest
	p.CapabilityEvidenceDigest, p.ReviewReceipt = capability.Reference.Digest, review.Reference
	c.Resource.ResourceID, c.Resource.Revision, c.ProfileRevision, c.LocalInstallationID, c.LocalInstallationRevision = "", 0, 0, "", 0
	c.OriginalSource = domain.OwnedAgentAuthorityBodyRefV1{}
	p.Resource = c.Resource
	p.ProfileRevision, p.RevocationGeneration, p.InstallationRevision = 0, 0, 0
	p.State, p.InstallationID, p.MutationID = "", "", ""
	p.Locator.ResourceID, p.Locator.BackendProfileDigest, p.Backend.BackendProfileDigest = "", "", ""
	p.Substrate.LocalInstallationID, p.Substrate.LocalInstallationRevision = "", 0
	p.InstalledManifest = domain.OwnedAgentAuthorityBodyRefV1{}
	key := bytes.Repeat([]byte{7}, 32) // synthetic source signing seed, not a provider key
	keyHash := sha256.Sum256(key)
	manifest = localoperator.ManifestV1{Version: 1, KeyFingerprint: hex.EncodeToString(keyHash[:]), Configuration: c, Profile: p, InvoiceTerms: terms, Evidence: []localoperator.EvidenceBodyV1{invoice, capability, review}}
	writeLocalOperatorManifest(t, root, manifest)
	if err := os.WriteFile(filepath.Join(root, "source.key"), key, 0o600); err != nil {
		t.Fatal(err)
	}
	return root, executable, manifest
}

func writeLocalOperatorManifest(t *testing.T, root string, m localoperator.ManifestV1) {
	t.Helper()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLocalOperatorActualPrivateSourcePublishesAndResumes(t *testing.T) {
	root, exe, _ := localOperatorFiles(t)
	source, err := localoperator.OpenFresh(t.Context(), root, exe)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close() })
	g, err := source.Publication()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(g.Profile.Resource.ResourceID, "pvr_") || !strings.HasPrefix(g.Profile.InstallationID, "oin_") || source.VerifyOwnedProfilePublication(g, g.IssuedAt) != nil {
		t.Fatal("fresh source did not privately mint/authenticate exact identity")
	}
	store, f, _, _ := newLocalInstallationFixture(t)
	f.at = g.IssuedAt
	store.ownedProfilePublicationVerifier = source
	receipt, err := store.PublishOwnedProfile(t.Context(), g)
	if err != nil {
		t.Fatalf("actual source-backed publication: %v", err)
	}
	if err := source.AcceptReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	restarted, err := localoperator.OpenExisting(t.Context(), root, exe, receipt)
	if err != nil {
		t.Fatalf("same source/receipt resume: %v", err)
	}
	t.Cleanup(func() { restarted.Close() })
	if _, err := restarted.Publication(); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("resume minted a fresh head")
	}
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: g.Profile.Locator.TenantID, OwnerUserID: g.Profile.Locator.OwnerUserID, SessionID: "local-source-session", RunID: "local-source-run", AttemptID: "local-source-attempt"}
	for _, kind := range []domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1} {
		spending, err := restarted.SpendingPublication(t.Context(), scope, kind)
		if err != nil {
			t.Fatalf("genuine local invoice %s: %v", kind, err)
		}
		if restarted.VerifyAttemptSpendingPublication(spending, spending.IssuedAt) != nil || source.VerifyAttemptSpendingPublication(spending, spending.IssuedAt) != nil {
			t.Fatalf("same original source across processes cannot authenticate %s", kind)
		}
		bad := spending.Clone()
		bad.Authorization.SessionID = "foreign-session"
		if restarted.VerifyAttemptSpendingPublication(bad, bad.IssuedAt) == nil {
			t.Fatal("changed session admitted by source signature")
		}
	}
	for _, kind := range []domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1} {
		if _, err := source.SpendingPublication(t.Context(), scope, kind); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatal("local no-invoice source funded an external service")
		}
	}
	if strings.Contains(fmt.Sprintf("%#v", source), "KeyFingerprint") || strings.Contains(fmt.Sprintf("%v", source), g.Profile.Resource.ResourceID) {
		t.Fatal("source diagnostic exposed installation internals")
	}
	clone := g.Clone()
	clone.LocalInstallation.Profile.BaseURL = "http://127.0.0.1:11435/v1"
	if source.VerifyOwnedProfilePublication(clone, g.IssuedAt) == nil {
		t.Fatal("copied caller configuration became approval")
	}
	if _, err := json.Marshal(g); err == nil {
		t.Fatal("source grant became a serializable approval DTO")
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if source.VerifyOwnedProfilePublication(g, g.IssuedAt) == nil {
		t.Fatal("closed keeper still verifies")
	}
}

func TestLocalOperatorRejectsUnsafeOrSpoofedInstallation(t *testing.T) {
	for _, name := range []string{"chosen_resource", "chosen_installation", "chosen_mutation", "missing_evidence", "copied_digest", "invoice_not_explicit", "paid_provider", "wrong_terms", "wrong_executable", "wrong_host", "key_rotation", "unsafe_root", "unsafe_manifest", "manifest_symlink", "key_symlink", "key_size", "trailing_json", "oversized_manifest"} {
		t.Run(name, func(t *testing.T) {
			root, exe, m := localOperatorFiles(t)
			fileChange := false
			switch name {
			case "chosen_resource":
				m.Configuration.Resource.ResourceID = "caller-picked"
			case "chosen_installation":
				m.Profile.InstallationID = "caller-picked"
			case "chosen_mutation":
				m.Profile.MutationID = "caller-picked"
			case "missing_evidence":
				m.Evidence = nil
			case "copied_digest":
				m.Evidence[1].Body = []byte("different actual bytes")
			case "invoice_not_explicit":
				m.InvoiceTerms.OwnerSuppliesHost = false
			case "paid_provider":
				m.InvoiceTerms.ProviderInvoiceAmount = 1
			case "wrong_terms":
				m.Configuration.OperatorTerms.Digest = strings.Repeat("f", 64)
			case "wrong_executable":
				m.Configuration.WorkerExecutableDigest = strings.Repeat("f", 64)
			case "wrong_host":
				m.Configuration.LocalHostIdentityDigest = strings.Repeat("f", 64)
			case "key_rotation":
				if err := os.WriteFile(filepath.Join(root, "source.key"), bytes.Repeat([]byte{8}, 32), 0o600); err != nil {
					t.Fatal(err)
				}
				fileChange = true
			case "unsafe_root":
				if err := os.Chmod(root, 0o755); err != nil {
					t.Fatal(err)
				}
				fileChange = true
			case "unsafe_manifest":
				if err := os.Chmod(filepath.Join(root, "manifest.json"), 0o644); err != nil {
					t.Fatal(err)
				}
				fileChange = true
			case "manifest_symlink", "key_symlink":
				filename := "manifest.json"
				if name == "key_symlink" {
					filename = "source.key"
				}
				path := filepath.Join(root, filename)
				backup := filepath.Join(root, "original-"+filename)
				if err := os.Rename(path, backup); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(backup, path); err != nil {
					t.Fatal(err)
				}
				fileChange = true
			case "key_size":
				if err := os.WriteFile(filepath.Join(root, "source.key"), []byte("short"), 0o600); err != nil {
					t.Fatal(err)
				}
				fileChange = true
			case "trailing_json":
				raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, "manifest.json"), append(raw, []byte("{}")...), 0o600); err != nil {
					t.Fatal(err)
				}
				fileChange = true
			case "oversized_manifest":
				if err := os.WriteFile(filepath.Join(root, "manifest.json"), bytes.Repeat([]byte{'x'}, (256<<10)+1), 0o600); err != nil {
					t.Fatal(err)
				}
				fileChange = true
			}
			if !fileChange {
				writeLocalOperatorManifest(t, root, m)
			}
			s, err := localoperator.OpenFresh(t.Context(), root, exe)
			if !errors.Is(err, domain.ErrMembershipDenied) || s != nil {
				if s != nil {
					s.Close()
				}
				t.Fatalf("spoof/unsafe %s accepted err=%v", name, err)
			}
		})
	}
}

func TestLocalOperatorCancellationAndCloseRace(t *testing.T) {
	root, exe, _ := localOperatorFiles(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s, err := localoperator.OpenFresh(ctx, root, exe)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	g, err := s.Publication()
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(3)
	go func() { defer workers.Done(); <-start; s.Close() }()
	go func() { defer workers.Done(); <-start; s.VerifyOwnedProfilePublication(g, g.IssuedAt) }()
	go func() { defer workers.Done(); <-start; cancel() }()
	close(start)
	workers.Wait()
	if _, err := s.Publication(); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("closed/cancelled keeper issued a new grant")
	}
	var forged localoperator.Source
	if forged.VerifyOwnedProfilePublication(g, g.IssuedAt) == nil {
		t.Fatal("zero/copy-constructed source became authority")
	}
}

func actualLocalOperatorStore(t *testing.T) (*localoperator.Source, *Store, *localInstallationSQLFixture, ports.OwnedProfilePublicationGrantV1, ports.OwnedProfilePublicationReceiptV1, string, string, localoperator.ManifestV1) {
	t.Helper()
	root, exe, manifest := localOperatorFiles(t)
	source, err := localoperator.OpenFresh(t.Context(), root, exe)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close() })
	grant, err := source.Publication()
	if err != nil {
		t.Fatal(err)
	}
	store, fixture, _, _ := newLocalInstallationFixture(t)
	fixture.at = grant.IssuedAt
	store.ownedProfilePublicationVerifier = source
	receipt, err := store.PublishOwnedProfile(t.Context(), grant)
	if err != nil {
		t.Fatalf("actual initial source publication: %v", err)
	}
	if err := source.AcceptReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	return source, store, fixture, grant, receipt, root, exe, manifest
}

func actualLocalReplacement(t *testing.T, source *localoperator.Source, original ports.OwnedProfilePublicationReceiptV1, manifest localoperator.ManifestV1) (*localoperator.Source, ports.OwnedProfilePublicationGrantV1, string, string) {
	t.Helper()
	root, exe, _ := localOperatorFiles(t)
	manifest.Configuration.BaseURL = "http://127.0.0.1:11435/v1"
	manifest.InvoiceTerms.BaseURL = manifest.Configuration.BaseURL
	terms, err := json.Marshal(manifest.InvoiceTerms)
	if err != nil {
		t.Fatal(err)
	}
	for index := range manifest.Evidence {
		body := &manifest.Evidence[index]
		switch body.Reference {
		case manifest.Configuration.OperatorTerms:
			body.Body = terms
			sum := sha256.Sum256(terms)
			body.Reference.Digest, body.Reference.Bytes = hex.EncodeToString(sum[:]), uint64(len(terms))
			manifest.Configuration.OperatorTerms = body.Reference
			manifest.Profile.ProviderPriceDigest, manifest.Profile.ProviderTermsDigest, manifest.Profile.SubstratePriceDigest, manifest.Profile.SubstrateTermsDigest = body.Reference.Digest, body.Reference.Digest, body.Reference.Digest, body.Reference.Digest
		case manifest.Profile.ReviewReceipt:
			body.Body = []byte("synthetic updated operator review declaration; not model qualification")
			sum := sha256.Sum256(body.Body)
			body.Reference.Digest, body.Reference.Bytes = hex.EncodeToString(sum[:]), uint64(len(body.Body))
			manifest.Profile.ReviewReceipt = body.Reference
		}
	}
	writeLocalOperatorManifest(t, root, manifest)
	child, err := source.Replacement(t.Context(), root, exe, original)
	if err != nil {
		t.Fatalf("actual private replacement source: %v", err)
	}
	t.Cleanup(func() { child.Close() })
	grant, err := child.Publication()
	if err != nil {
		t.Fatal(err)
	}
	return child, grant, root, exe
}

func TestLocalOperatorAuthenticatedReplacementAndOldSourceDenial(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source, store, fixture, _, first, _, _, manifest := actualLocalOperatorStore(t)
		scope := domain.AttemptExecutionSourceScopeV1{TenantID: first.Profile.Locator.TenantID, OwnerUserID: first.Profile.Locator.OwnerUserID, SessionID: "old-local-session", RunID: "old-local-run", AttemptID: "old-local-attempt"}
		funding, err := source.SpendingPublication(t.Context(), scope, domain.AttemptSpendingProviderV1)
		if err != nil {
			t.Fatal(err)
		}
		if err := source.VerifyAttemptSpendingPublication(funding, funding.IssuedAt); err != nil {
			t.Fatalf("actual original funding signature: %v", err)
		}
		// Seed only storage representation of an actual source-issued envelope.
		// This is not a native database or a funding-publisher transaction proof.
		v := funding.Authorization
		raw, err := domain.EncodeAttemptSpendingAuthorizationCurrentV1(v)
		if err != nil {
			t.Fatal(err)
		}
		digest, err := v.Digest()
		if err != nil {
			t.Fatal(err)
		}
		fixture.spendingArgs[0] = []driver.Value{string(v.Locator.TenantID), string(v.Locator.AttemptID), string(v.Locator.ComponentKind), string(v.Locator.BillingOwnerUserID)}
		fixture.spendingRows[0] = []driver.Value{string(raw), uint64(v.Version), v.RevocationGeneration, string(v.State), digest, string(v.RunID), string(v.SessionID), string(v.OwnerUserID), v.Window.PublishedAt, v.Window.ExpiresAt, v.UpdatedAt}
		fixture.at = funding.IssuedAt
		ref := ownedSpendingReference{locator: v.Locator, generation: v.RevocationGeneration, digest: digest}
		compare := func() error {
			return store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
				p, exists, err := loadOwnedProfileProjection(ctx, q, first.Profile.Locator)
				if err != nil || !exists {
					return domain.ErrMembershipDenied
				}
				s, err := readOwnedSpendingCurrent(ctx, q, ref)
				if err != nil {
					return err
				}
				return s.ValidateAgainstProfile(p, q.at)
			})
		}
		if err := compare(); err != nil {
			t.Fatalf("original source-backed current profile/spending positive: %v", err)
		}
		child, grant, newRoot, newExe := actualLocalReplacement(t, source, first, manifest)
		store.ownedProfilePublicationVerifier = child
		fixture.at = grant.IssuedAt
		second, err := store.PublishOwnedProfile(t.Context(), grant)
		if err != nil || second.Replayed {
			t.Fatalf("source-authenticated two-head replacement: err=%v replay=%t", err, second.Replayed)
		}
		if err := child.AcceptReceipt(second); err != nil {
			t.Fatal(err)
		}
		if len(fixture.heads) != 2 || len(fixture.audits) != 3 {
			t.Fatalf("paired replacement heads=%d audits=%d", len(fixture.heads), len(fixture.audits))
		}
		if err := compare(); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatalf("original current spending survived predecessor retirement: %v", err)
		}
		oldRef := domain.OwnedAgentProfileReferenceV1{Locator: first.Profile.Locator, RevocationGeneration: first.Profile.RevocationGeneration, Digest: first.Digest}
		err = store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
			_, err := readOwnedProfileCurrent(ctx, q, oldRef)
			return err
		})
		if !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatalf("old current profile reference survived retirement: %v", err)
		}
		resumed, err := localoperator.OpenExisting(t.Context(), newRoot, newExe, second)
		if err != nil {
			t.Fatalf("actual successor exact source/receipt resume: %v", err)
		}
		t.Cleanup(func() { resumed.Close() })
		for _, component := range []domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1} {
			g, err := resumed.SpendingPublication(t.Context(), scope, component)
			if err != nil || g.Authorization.SourceProfile.Locator != second.Profile.Locator || resumed.VerifyAttemptSpendingPublication(g, g.IssuedAt) != nil {
				t.Fatalf("successor %s funding correlation: %v", component, err)
			}
		}
	})
}

func TestLocalOperatorReplacementLostCommitRecovery(t *testing.T) {
	for _, mode := range []string{"ambiguous", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				source, store, fixture, _, first, _, _, manifest := actualLocalOperatorStore(t)
				child, grant, _, _ := actualLocalReplacement(t, source, first, manifest)
				store.ownedProfilePublicationVerifier, fixture.at = child, grant.IssuedAt
				fixture.commitModes = []string{mode}
				receipt, err := store.PublishOwnedProfile(t.Context(), grant)
				if mode == "unknown" {
					if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(receipt, ports.OwnedProfilePublicationReceiptV1{}) {
						t.Fatalf("unknown commit leaked success: err=%v", err)
					}
				} else if err != nil || !receipt.Replayed {
					t.Fatalf("lost ACK exact audit recovery: err=%v replay=%t", err, receipt.Replayed)
				}
				if len(fixture.heads) != 2 || len(fixture.audits) != 3 {
					t.Fatalf("ambiguous commit lost atomic pair: heads=%d audits=%d", len(fixture.heads), len(fixture.audits))
				}
				heads, audits, writes := cloneLocalInstallationRows(fixture.heads), cloneLocalInstallationRows(fixture.audits), fixture.execCalls
				recovered, err := store.PublishOwnedProfile(t.Context(), grant)
				if err != nil || !recovered.Replayed {
					t.Fatalf("explicit source-backed replay/recovery: err=%v replay=%t", err, recovered.Replayed)
				}
				if !reflect.DeepEqual(heads, fixture.heads) || !reflect.DeepEqual(audits, fixture.audits) || fixture.execCalls != writes {
					t.Fatal("lost-commit replay rewrote original heads/audits")
				}
				if err := child.AcceptReceipt(recovered); err != nil {
					t.Fatalf("accept recovered exact receipt: %v", err)
				}
			})
		})
	}
}

func TestLocalOperatorResumeRequiresExactReceiptKeyAndSource(t *testing.T) {
	for _, name := range []string{"missing_receipt", "wrong_digest", "wrong_owner", "changed_source", "rotated_key_and_manifest"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				_, _, _, _, receipt, root, exe, manifest := actualLocalOperatorStore(t)
				switch name {
				case "missing_receipt":
					receipt = ports.OwnedProfilePublicationReceiptV1{}
				case "wrong_digest":
					receipt.Digest = strings.Repeat("f", 64)
				case "wrong_owner":
					receipt.Profile.Locator.OwnerUserID = "foreign-owner"
				case "changed_source":
					manifest.Configuration.ServerVersion = "different-source-version"
					writeLocalOperatorManifest(t, root, manifest)
				case "rotated_key_and_manifest":
					key := bytes.Repeat([]byte{9}, 32)
					if err := os.WriteFile(filepath.Join(root, "source.key"), key, 0o600); err != nil {
						t.Fatal(err)
					}
					sum := sha256.Sum256(key)
					manifest.KeyFingerprint = hex.EncodeToString(sum[:])
					writeLocalOperatorManifest(t, root, manifest)
				}
				resumed, err := localoperator.OpenExisting(t.Context(), root, exe, receipt)
				if resumed != nil {
					resumed.Close()
				}
				if !errors.Is(err, domain.ErrMembershipDenied) || resumed != nil {
					t.Fatalf("mismatched original %s resumed: %v", name, err)
				}
			})
		})
	}
}

func TestLocalOperatorPendingReplacementRequiresOriginalKeeper(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source, store, fixture, _, first, _, _, manifest := actualLocalOperatorStore(t)
		child, grant, _, _ := actualLocalReplacement(t, source, first, manifest)
		if err := child.VerifyOwnedProfilePublication(grant, grant.IssuedAt); err != nil {
			t.Fatalf("pending replacement positive: %v", err)
		}
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}
		if err := child.VerifyOwnedProfilePublication(grant, grant.IssuedAt); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatal("closed original keeper still authenticated pending replacement")
		}
		store.ownedProfilePublicationVerifier = child
		fixture.at = grant.IssuedAt
		heads, audits, writes := cloneLocalInstallationRows(fixture.heads), cloneLocalInstallationRows(fixture.audits), fixture.execCalls
		receipt, err := store.PublishOwnedProfile(t.Context(), grant)
		if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(receipt, ports.OwnedProfilePublicationReceiptV1{}) || !reflect.DeepEqual(heads, fixture.heads) || !reflect.DeepEqual(audits, fixture.audits) || writes != fixture.execCalls {
			t.Fatal("pending replacement survived parent close or wrote partial storage state")
		}
	})
}

func TestLocalOperatorFiniteGrantExpiryAndCloneCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source, _, _, grant, receipt, root, exe, _ := actualLocalOperatorStore(t)
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		resumed, err := localoperator.OpenExisting(ctx, root, exe, receipt)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resumed.Close() })
		scope := domain.AttemptExecutionSourceScopeV1{TenantID: receipt.Profile.Locator.TenantID, OwnerUserID: receipt.Profile.Locator.OwnerUserID, SessionID: "expiry-session", RunID: "expiry-run", AttemptID: "expiry-attempt"}
		funding, err := resumed.SpendingPublication(t.Context(), scope, domain.AttemptSpendingProviderV1)
		if err != nil {
			t.Fatal(err)
		}
		clone := funding.Clone()
		clone.Authenticator[0]++
		clone.Authorization.SourceAuthorization.Ref = "spoofed-actual-source"
		if resumed.VerifyAttemptSpendingPublication(clone, funding.IssuedAt) == nil || resumed.VerifyAttemptSpendingPublication(funding, funding.IssuedAt) != nil {
			t.Fatal("clone mutation altered original or forged source approval")
		}
		cancel()
		if resumed.VerifyAttemptSpendingPublication(funding, funding.IssuedAt) == nil {
			t.Fatal("cancelled actual keeper retained funding authority")
		}
		if source.VerifyOwnedProfilePublication(grant, grant.ExpiresAt) == nil {
			t.Fatal("exclusive publication expiry admitted")
		}
		// Advancing a synctest timer advances logical time only, not wall time.
		timer := time.NewTimer(11 * time.Second)
		t.Cleanup(func() { timer.Stop() })
		<-timer.C
		if _, err := source.Publication(); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatal("expired publication renewed its original grant")
		}
		if source.VerifyAttemptSpendingPublication(funding, funding.ExpiresAt) == nil {
			t.Fatal("exclusive funding grant expiry admitted")
		}
	})
}

func registerActualFundingKey(f *localInstallationSQLFixture, g ports.AttemptSpendingPublicationGrantV1) {
	if f.fundingKeys == nil {
		f.fundingKeys = make(map[string]bool)
	}
	l := g.Authorization.Locator
	f.fundingKeys[localInstallationSQLKey([]driver.Value{string(l.TenantID), string(l.AttemptID), string(l.ComponentKind), string(l.BillingOwnerUserID)})] = true
}

// Connect the actual private-file source to the actual Store funding writer;
// only the serializable storage/retry mechanics are a deterministic SQL model.
func TestLocalOperatorConnectedFundingPublicationAndStaleSource(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source, store, f, _, first, _, _, manifest := actualLocalOperatorStore(t)
		store.attemptSpendingPublicationVerifier = source
		scope := domain.AttemptExecutionSourceScopeV1{TenantID: first.Profile.Locator.TenantID, OwnerUserID: first.Profile.Locator.OwnerUserID, SessionID: "connected-session", RunID: "connected-run", AttemptID: "connected-attempt"}
		for _, kind := range []domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1} {
			grant, err := source.SpendingPublication(t.Context(), scope, kind)
			if err != nil {
				t.Fatal(err)
			}
			registerActualFundingKey(f, grant)
			f.at = grant.IssuedAt
			bad := grant.Clone()
			bad.Authorization.SessionID = "changed-session"
			heads, audits, writes := cloneLocalInstallationRows(f.fundingHeads), cloneLocalInstallationRows(f.fundingAudits), f.execCalls
			denied, err := store.PublishAttemptSpending(t.Context(), bad)
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(denied, ports.AttemptSpendingPublicationReceiptV1{}) || !reflect.DeepEqual(heads, f.fundingHeads) || !reflect.DeepEqual(audits, f.fundingAudits) || writes != f.execCalls {
				t.Fatalf("changed source carrier mutated %s funding: %v", kind, err)
			}
			beforeQueries := len(f.statements)
			receipt, err := store.PublishAttemptSpending(t.Context(), grant)
			if err != nil || receipt.Replayed || receipt.Authorization.SourceKind != domain.AttemptSpendingOwnerLocalSourceV1 {
				t.Fatalf("actual %s source -> Store funding publication: err=%v replay=%t", kind, err, receipt.Replayed)
			}
			joinedProfile := false
			for _, statement := range f.statements[beforeQueries:] {
				joinedProfile = joinedProfile || statement == ownedProfileCurrentSQL
			}
			if !joinedProfile {
				t.Fatal("connected funding bypassed exact current profile SQL")
			}
			if err := receipt.Authorization.ValidateAgainstProfile(first.Profile, f.at); err != nil {
				t.Fatalf("published %s source/profile facts: %v", kind, err)
			}
			replayWrites := f.execCalls
			replayed, err := store.PublishAttemptSpending(t.Context(), grant)
			if err != nil || !replayed.Replayed || replayed.Digest != receipt.Digest || f.execCalls != replayWrites {
				t.Fatalf("actual %s funding replay changed original row: %v", kind, err)
			}
			other, err := source.SpendingPublication(t.Context(), scope, kind)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.PublishAttemptSpending(t.Context(), other); !errors.Is(err, domain.ErrMembershipDenied) || f.execCalls != replayWrites {
				t.Fatalf("second initial %s allocation became a top-up: %v", kind, err)
			}
		}
		if len(f.fundingHeads) != 2 || len(f.fundingAudits) != 2 {
			t.Fatalf("provider/substrate funding heads=%d audits=%d", len(f.fundingHeads), len(f.fundingAudits))
		}
		for _, kind := range []domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1} {
			if _, err := source.SpendingPublication(t.Context(), scope, kind); !errors.Is(err, domain.ErrMembershipDenied) {
				t.Fatal("local operator invoice source funded an external service")
			}
		}
		// A genuine but not-yet-published original grant remains authenticated;
		// replacing its source profile must deny at the actual current SQL join.
		scope.AttemptID = "unpublished-original-attempt"
		stale, err := source.SpendingPublication(t.Context(), scope, domain.AttemptSpendingProviderV1)
		if err != nil {
			t.Fatal(err)
		}
		registerActualFundingKey(f, stale)
		child, replacement, _, _ := actualLocalReplacement(t, source, first, manifest)
		store.ownedProfilePublicationVerifier = child
		f.at = replacement.IssuedAt
		if _, err := store.PublishOwnedProfile(t.Context(), replacement); err != nil {
			t.Fatalf("replace actual source before stale funding publication: %v", err)
		}
		if source.VerifyAttemptSpendingPublication(stale, f.at) != nil {
			t.Fatal("stale-source test lost authentic unexpired original provenance")
		}
		heads, audits, writes, queries := cloneLocalInstallationRows(f.fundingHeads), cloneLocalInstallationRows(f.fundingAudits), f.execCalls, len(f.statements)
		got, err := store.PublishAttemptSpending(t.Context(), stale)
		if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.AttemptSpendingPublicationReceiptV1{}) || !reflect.DeepEqual(heads, f.fundingHeads) || !reflect.DeepEqual(audits, f.fundingAudits) || writes != f.execCalls {
			t.Fatalf("retired original profile admitted source-backed funding: %v", err)
		}
		joinedProfile := false
		for _, statement := range f.statements[queries:] {
			joinedProfile = joinedProfile || statement == ownedProfileCurrentSQL
		}
		if !joinedProfile {
			t.Fatal("stale funding denied before exercising required current profile join")
		}
	})
}

func TestLocalOperatorConnectedFundingAtomicRecovery(t *testing.T) {
	for _, mode := range []string{"ambiguous", "unknown", "rollback", "audit_failure"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				source, store, f, _, first, _, _, _ := actualLocalOperatorStore(t)
				store.attemptSpendingPublicationVerifier = source
				scope := domain.AttemptExecutionSourceScopeV1{TenantID: first.Profile.Locator.TenantID, OwnerUserID: first.Profile.Locator.OwnerUserID, SessionID: "recovery-session", RunID: "recovery-run", AttemptID: "recovery-attempt"}
				grant, err := source.SpendingPublication(t.Context(), scope, domain.AttemptSpendingProviderV1)
				if err != nil {
					t.Fatal(err)
				}
				registerActualFundingKey(f, grant)
				f.at = grant.IssuedAt
				if mode == "audit_failure" {
					f.failFundingAudit = true
				} else {
					f.commitModes = []string{mode}
				}
				receipt, err := store.PublishAttemptSpending(t.Context(), grant)
				if mode == "audit_failure" {
					if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(receipt, ports.AttemptSpendingPublicationReceiptV1{}) || len(f.fundingHeads) != 0 || len(f.fundingAudits) != 0 {
						t.Fatalf("funding head escaped audit rollback: %v", err)
					}
					return
				}
				if mode == "unknown" {
					if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(receipt, ports.AttemptSpendingPublicationReceiptV1{}) {
						t.Fatal("unknown funding commit returned speculative success")
					}
				} else if err != nil {
					t.Fatalf("source-backed funding commit recovery %s: %v", mode, err)
				}
				if len(f.fundingHeads) != 1 || len(f.fundingAudits) != 1 {
					t.Fatalf("funding pair not atomic after %s: heads=%d audits=%d", mode, len(f.fundingHeads), len(f.fundingAudits))
				}
				if mode == "ambiguous" && !receipt.Replayed {
					t.Fatal("lost ACK did not recover original funding audit")
				}
				heads, audits, writes := cloneLocalInstallationRows(f.fundingHeads), cloneLocalInstallationRows(f.fundingAudits), f.execCalls
				recovered, err := store.PublishAttemptSpending(t.Context(), grant)
				if err != nil || !recovered.Replayed || !reflect.DeepEqual(heads, f.fundingHeads) || !reflect.DeepEqual(audits, f.fundingAudits) || f.execCalls != writes {
					t.Fatalf("exact original funding recovery rewrote source %s: %v", mode, err)
				}
			})
		})
	}
}
