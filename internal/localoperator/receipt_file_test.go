//go:build darwin || linux

package localoperator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
	"golang.org/x/sys/unix"
)

func receiptTestPath(t *testing.T) (string, string) {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "operator")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	return root, filepath.Join(root, "receipt.json")
}

func receiptRealFileOps() receiptFileOps {
	return receiptFileOps{sync: (*os.File).Sync, write: (*os.File).Write}
}

func requireReceiptSlotDenied(t *testing.T, path string) {
	t.Helper()
	if slot, err := ReserveProfileReceipt(t.Context(), path); !errors.Is(err, ErrDenied) || slot != nil {
		if slot != nil {
			_ = slot.Close()
		}
		t.Fatalf("existing pathname was reused: slot=%v err=%v", slot != nil, err)
	}
}

func TestProfileReceiptExclusiveReservation(t *testing.T) {
	root, path := receiptTestPath(t)
	slot, err := ReserveProfileReceipt(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = slot.Close() })
	var dir, file unix.Stat_t
	if unix.Stat(root, &dir) != nil || !privateOperatorRoot(dir) || unix.Lstat(path, &file) != nil || file.Mode&unix.S_IFMT != unix.S_IFREG || file.Mode&07777 != 0600 || file.Uid != uint32(os.Geteuid()) || file.Nlink != 1 || file.Size != 0 {
		t.Fatal("reservation did not create one private owner-only empty regular slot")
	}
	requireReceiptSlotDenied(t, path)
	if value, err := ReadProfileReceipt(t.Context(), path); !errors.Is(err, ErrDenied) || !reflect.DeepEqual(value, ports.OwnedProfilePublicationReceiptV1{}) {
		t.Fatal("empty reserved slot became a usable receipt")
	}
	if slot.Close() != nil || slot.Close() != nil {
		t.Fatal("Close is not idempotent")
	}
	if info, err := os.Stat(path); err != nil || info.Size() != 0 {
		t.Fatal("Close removed or rewrote the original reservation")
	}
	requireReceiptSlotDenied(t, path)
}

func TestProfileReceiptConcurrentReservationHasOneOwner(t *testing.T) {
	_, path := receiptTestPath(t)
	type result struct {
		slot *ProfileReceiptFile
		err  error
	}
	start, results := make(chan struct{}), make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			slot, err := ReserveProfileReceipt(t.Context(), path)
			results <- result{slot, err}
		}()
	}
	close(start)
	winners := 0
	for range 2 {
		r := <-results
		if r.slot != nil {
			t.Cleanup(func() { _ = r.slot.Close() })
		}
		if r.err == nil && r.slot != nil {
			winners++
		} else if !errors.Is(r.err, ErrDenied) || r.slot != nil {
			t.Errorf("unexpected reservation result: slot=%t err=%v", r.slot != nil, r.err)
		}
	}
	if winners != 1 {
		t.Fatalf("exclusive path owners=%d, want one", winners)
	}
}

func TestProfileReceiptDecoderRejectsMalformedHistoricalData(t *testing.T) {
	raw, err := json.Marshal(receiptFileDataFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"empty": nil, "null": []byte("null"), "truncated": raw[:len(raw)-1],
		"oversize":         bytes.Repeat([]byte(" "), domain.OwnedAgentCurrentMaxRecordBytesV1+1),
		"duplicate digest": bytes.Replace(raw, []byte(`"Digest":`), []byte(`"Digest":"foreign","Digest":`), 1),
		"unknown field":    append(bytes.Clone(raw[:len(raw)-1]), []byte(`,"unknown":true}`)...),
	}
	for _, fault := range []string{"missing profile", "null profile", "wrong digest", "string replayed", "null replayed"} {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		switch fault {
		case "missing profile":
			delete(fields, "Profile")
		case "null profile":
			fields["Profile"] = json.RawMessage(`null`)
		case "wrong digest":
			fields["Digest"] = json.RawMessage(`"foreign"`)
		case "string replayed":
			fields["Replayed"] = json.RawMessage(`"false"`)
		case "null replayed":
			fields["Replayed"] = json.RawMessage(`null`)
		}
		encoded, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		cases[fault] = encoded
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if got, err := DecodeProfileReceipt(body); !errors.Is(err, ErrDenied) || !reflect.DeepEqual(got, ports.OwnedProfilePublicationReceiptV1{}) {
				t.Fatal("malformed historical receipt accepted", err)
			}
			_, path := receiptTestPath(t)
			if err := os.WriteFile(path, body, 0o600); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadProfileReceipt(t.Context(), path); !errors.Is(err, ErrDenied) || !reflect.DeepEqual(got, ports.OwnedProfilePublicationReceiptV1{}) {
				t.Fatal("private reader accepted malformed receipt", err)
			}
		})
	}
}

func TestProfileReceiptReaderRejectsWrongPhysicalObject(t *testing.T) {
	for _, fault := range []string{"symlink", "hardlink", "directory", "fifo", "public file", "public parent", "missing", "cancelled"} {
		t.Run(fault, func(t *testing.T) {
			root, path := receiptTestPath(t)
			raw, err := json.Marshal(receiptFileDataFixture(t))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			switch fault {
			case "symlink", "directory", "fifo", "missing":
				if err := os.Rename(path, path+".original"); err != nil {
					t.Fatal(err)
				}
				switch fault {
				case "symlink":
					err = os.Symlink(path+".original", path)
				case "directory":
					err = os.Mkdir(path, 0o700)
				case "fifo":
					err = unix.Mkfifo(path, 0o600)
				}
			case "hardlink":
				err = os.Link(path, path+".link")
			case "public file":
				err = os.Chmod(path, 0o644)
			case "public parent":
				err = os.Chmod(root, 0o755)
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if err != nil {
				t.Fatal(err)
			}
			if got, err := ReadProfileReceipt(ctx, path); !errors.Is(err, ErrDenied) || !reflect.DeepEqual(got, ports.OwnedProfilePublicationReceiptV1{}) {
				t.Fatalf("%s became historical receipt: %v", fault, err)
			}
		})
	}
}

func TestProfileReceiptExistingObjectsNeverOverwritten(t *testing.T) {
	for _, kind := range []string{"empty", "full", "directory", "symlink", "dangling symlink"} {
		t.Run(kind, func(t *testing.T) {
			root, path := receiptTestPath(t)
			var err error
			switch kind {
			case "empty":
				err = os.WriteFile(path, nil, 0600)
			case "full":
				err = os.WriteFile(path, []byte("original unrelated private bytes"), 0600)
			case "directory":
				err = os.Mkdir(path, 0700)
			case "symlink", "dangling symlink":
				target := filepath.Join(root, "other")
				if kind == "symlink" {
					if err := os.WriteFile(target, []byte("original target"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				err = os.Symlink(target, path)
			}
			if err != nil {
				t.Fatal(err)
			}
			var before, after unix.Stat_t
			if unix.Lstat(path, &before) != nil {
				t.Fatal("existing object stat failed")
			}
			requireReceiptSlotDenied(t, path)
			if unix.Lstat(path, &after) != nil || !stableOperatorStat(before, after) {
				t.Fatal("reservation modified an existing object")
			}
			if kind == "full" {
				body, err := os.ReadFile(path)
				if err != nil || string(body) != "original unrelated private bytes" {
					t.Fatal("existing private content overwritten")
				}
			}
		})
	}
}

func TestProfileReceiptReservationPathAndContextDenials(t *testing.T) {
	for _, fault := range []string{"relative", "unclean", "public root", "symlink parent", "nil context", "cancelled context"} {
		t.Run(fault, func(t *testing.T) {
			root, path := receiptTestPath(t)
			ctx := t.Context()
			switch fault {
			case "relative":
				path = "relative-receipt.json"
			case "unclean":
				path = root + "/../operator/receipt.json"
			case "public root":
				if err := os.Chmod(root, 0755); err != nil {
					t.Fatal(err)
				}
			case "symlink parent":
				link := root + "-link"
				if err := os.Symlink(root, link); err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(link, "receipt.json")
			case "nil context":
				ctx = nil
			case "cancelled context":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if slot, err := ReserveProfileReceipt(ctx, path); !errors.Is(err, ErrDenied) || slot != nil {
				if slot != nil {
					_ = slot.Close()
				}
				t.Fatalf("%s reserved an invalid pathname: err=%v", fault, err)
			}
			if _, err := os.Lstat(filepath.Join(root, "receipt.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid intake created an output slot")
			}
		})
	}
}

func TestProfileReceiptReservationSyncFailuresRetainEmptySlot(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run([]string{"file", "directory"}[failAt-1], func(t *testing.T) {
			_, path := receiptTestPath(t)
			calls, writes := 0, 0
			ops := receiptRealFileOps()
			ops.sync = func(file *os.File) error {
				calls++
				if calls == failAt {
					return io.ErrUnexpectedEOF
				}
				return file.Sync()
			}
			ops.write = func(*os.File, []byte) (int, error) { writes++; return 0, io.ErrUnexpectedEOF }
			if slot, err := reserveProfileReceipt(t.Context(), path, ops); !errors.Is(err, ErrDenied) || slot != nil || calls != failAt || writes != 0 {
				t.Fatalf("sync failure did not precede publication: err=%v syncs=%d writes=%d", err, calls, writes)
			}
			body, err := os.ReadFile(path)
			if err != nil || len(body) != 0 {
				t.Fatal("failed reservation did not retain original empty slot")
			}
			requireReceiptSlotDenied(t, path)
			if _, err := ReadProfileReceipt(t.Context(), path); !errors.Is(err, ErrDenied) {
				t.Fatal("prepublication sync failure produced a usable receipt")
			}
		})
	}
}

// Canonical profile/receipt DATA only, not a published profile or Source. Real
// original Source→Store publication→receipt→OpenExisting is tested separately.
func receiptFileDataFixture(t *testing.T) ports.OwnedProfilePublicationReceiptV1 {
	t.Helper()
	at := time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	a, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2("receipt-tenant", "receipt-owner", "receipt-run", "receipt-attempt", "receipt-subscription", at)
	if err != nil {
		t.Fatal(err)
	}
	h := strings.Repeat("a", 64)
	cost := a.AdmissionCostCeiling.Clone()
	service := func(kind domain.ExternalServiceKindV1) *domain.ExternalServiceCostCeilingV1 {
		zero := uint64(0)
		return &domain.ExternalServiceCostCeilingV1{Version: 1, Kind: kind, TenantID: "receipt-tenant", OwnerUserID: "receipt-owner", BillingOwnerUserID: "receipt-owner", ResourceID: string(kind) + "-resource", ResourceRevision: 1, RevocationGeneration: 1, ProfileID: string(kind) + "-profile", ProfileRevision: 1, ScopedGrantDigest: h, Currency: cost.Currency, PriceRevision: cost.PriceRevision, PriceObservedAt: cost.PriceObservedAt, PriceExpiresAt: cost.PriceExpiresAt, PriceEvidenceDigest: h, TermsEvidenceDigest: h, PriceState: domain.ExternalServicePriceKnownFreeV1, MaxAmountMicrounits: &zero}
	}
	cost.MCPToolCost, cost.WebSearchCost = service(domain.ExternalServiceMCPToolV1), service(domain.ExternalServiceWebSearchV1)
	substrate := a.SubstrateBinding
	substrate.AdmissionCostCeilingDigest, err = cost.Digest()
	if err != nil {
		t.Fatal(err)
	}
	p := domain.OwnedAgentProfileCurrentV1{Version: 1,
		Locator:         domain.OwnedAgentProfileLocatorV1{TenantID: "receipt-tenant", OwnerUserID: "receipt-owner", Profile: domain.AgentLoopOwnedPublicV1, BackendProfileDigest: a.HarnessBinding.Backend.BackendProfileDigest, ResourceKind: a.HarnessBinding.Resource.Kind, ResourceID: a.HarnessBinding.Resource.ResourceID, ModelVendorID: a.HarnessBinding.ModelVendorID, ModelID: a.HarnessBinding.ModelID, InputDataClass: domain.ProviderDataPublicV1},
		ProfileRevision: 1, RevocationGeneration: 1, State: domain.OwnedAgentCurrentActiveV1, Backend: a.HarnessBinding.Backend, Resource: a.HarnessBinding.Resource, Substrate: substrate,
		InstallationID: "receipt-installation", InstallationRevision: 1, InstalledManifest: domain.OwnedAgentAuthorityBodyRefV1{Ref: "passive-manifest", Digest: h, Bytes: 10}, ReviewReceipt: domain.OwnedAgentAuthorityBodyRefV1{Ref: "passive-review", Digest: h, Bytes: 10},
		ProviderCatalogDigest: h, ProviderRouteDigest: h, CapabilityEvidenceDigest: h, PrivacyPolicyDigest: h, EffectivePolicyDigest: h, ProviderPriceDigest: h, ProviderTermsDigest: h, SubstratePriceDigest: h, SubstrateTermsDigest: h, SubstratePriceState: domain.ExternalServicePriceKnownFreeV1,
		BillingOwnerUserID: "receipt-owner", InputModalities: []domain.AgentInputModalityV1{domain.AgentInputTextV1}, ModelWindowTokens: 2, TokenEstimatorVersion: "passive-estimator.v1", CalibrationDigest: h, InstructionVersion: "passive-instructions.v1", CompactionPolicyDigest: h, MCPToolGrantDigest: h, WebSearchGrantDigest: h,
		Limits:      domain.AgentLoopLimitsV1{MaxConcurrentOperations: 1, MaxModelCalls: 1, MaxCompactionCalls: 1, MaxToolDispatches: 1, MaxWebSearches: 1, MaxInputTokensPerCall: 1, MaxAggregateInputTokens: 1, MaxOutputTokensPerCall: 1, MaxAggregateOutputTokens: 1, MaxActiveDuration: time.Second, MaxNetworkOperationDuration: time.Second, MaxCleanupDuration: time.Second, MaxContextViewBytes: 1, MaxSummaryBytes: 1, MaxToolArgumentBytes: 1, MaxToolResultBytes: 1, MaxMCPOutboundMessages: 1, MaxMCPCatalogPages: 1, MaxMCPInboundFrames: 1, MaxMCPInboundBytes: 1, MaxOperations: 2, MaxAggregateBodyBytes: 1},
		CostCeiling: cost, Window: domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at, ExpiresAt: at.Add(time.Hour)}, UpdatedAt: at, MutationID: "passive-original-publication"}
	if err := p.ValidateInitial(); err != nil {
		t.Fatalf("passive receipt DATA fixture: %v", err)
	}
	digest, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return ports.OwnedProfilePublicationReceiptV1{Profile: p, Digest: digest}
}

func TestProfileReceiptWriteAndSharedRead(t *testing.T) {
	_, path := receiptTestPath(t)
	want := receiptFileDataFixture(t)
	slot, err := ReserveProfileReceipt(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = slot.Close() })
	if err := slot.WriteReceipt(t.Context(), want); err != nil {
		t.Fatal(err)
	}
	if !slot.finished || slot.file != nil || slot.directory != nil || slot.Close() != nil {
		t.Fatal("successful handoff retained open slot handles")
	}
	got, err := ReadProfileReceipt(t.Context(), path)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("shared original reader roundtrip: equal=%t err=%v", reflect.DeepEqual(got, want), err)
	}
	raw, err := os.ReadFile(path)
	expected, encodeErr := json.Marshal(want)
	if err != nil || encodeErr != nil || !bytes.Equal(raw, expected) {
		t.Fatal("receipt writer changed exact original producer bytes")
	}
	if !errors.Is(slot.WriteReceipt(t.Context(), want), ErrDenied) {
		t.Fatal("successful receipt was written twice")
	}
	requireReceiptSlotDenied(t, path)
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if value, err := ReadProfileReceipt(t.Context(), path); !errors.Is(err, ErrDenied) || !reflect.DeepEqual(value, ports.OwnedProfilePublicationReceiptV1{}) {
		t.Fatal("shared reader accepted a nonprivate receipt")
	}
}

func TestProfileReceiptWriteFailuresRetainSlotWithoutRetry(t *testing.T) {
	for _, fault := range []string{"short write", "partial write error", "file sync", "directory sync", "path replacement", "cancel after write"} {
		t.Run(fault, func(t *testing.T) {
			_, path := receiptTestPath(t)
			want := receiptFileDataFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			syncs, writes := 0, 0
			ops := receiptRealFileOps()
			ops.sync = func(file *os.File) error {
				syncs++
				if fault == "file sync" && syncs == 3 || fault == "directory sync" && syncs == 4 {
					return io.ErrUnexpectedEOF
				}
				return file.Sync()
			}
			ops.write = func(file *os.File, raw []byte) (int, error) {
				writes++
				if fault == "short write" || fault == "partial write error" {
					n, err := file.Write(raw[:len(raw)/2])
					if err == nil && fault == "partial write error" {
						err = io.ErrUnexpectedEOF
					}
					return n, err
				}
				n, err := file.Write(raw)
				if err != nil {
					return n, err
				}
				if fault == "path replacement" {
					if err := os.Rename(path, path+".original"); err != nil {
						return n, err
					}
					if err := os.WriteFile(path, []byte("replacement must survive"), 0600); err != nil {
						return n, err
					}
				}
				if fault == "cancel after write" {
					cancel()
				}
				return n, nil
			}
			slot, err := reserveProfileReceipt(ctx, path, ops)
			if err != nil || syncs != 2 || writes != 0 {
				t.Fatalf("causal reservation setup failed: err=%v syncs=%d writes=%d", err, syncs, writes)
			}
			t.Cleanup(func() { _ = slot.Close() })
			if err := slot.WriteReceipt(ctx, want); !errors.Is(err, ErrDenied) || writes != 1 || !slot.finished {
				t.Fatalf("%s returned completed handoff: err=%v writes=%d finished=%t", fault, err, writes, slot.finished)
			}
			if err := slot.WriteReceipt(t.Context(), want); !errors.Is(err, ErrDenied) || writes != 1 {
				t.Fatal("failed/uncertain write was automatically retried")
			}
			if slot.Close() != nil {
				t.Fatal("failed write cleanup did not close original handles")
			}
			requireReceiptSlotDenied(t, path)
			body, err := os.ReadFile(path)
			if err != nil || len(body) == 0 {
				t.Fatal("failed write erased retained partial/uncertain output")
			}
			if fault == "path replacement" && string(body) != "replacement must survive" {
				t.Fatal("failure cleanup overwrote or erased replacement object")
			}
			if fault == "short write" || fault == "partial write error" {
				if _, err := ReadProfileReceipt(t.Context(), path); !errors.Is(err, ErrDenied) {
					t.Fatal("partial bytes became a usable receipt")
				}
			}
			// A complete body may decode after a sync failure, but this operation
			// still MUST deny: readable bytes are not a successful durable handoff.
		})
	}
}

func TestProfileReceiptOriginalSlotPhysicalChangesDenyBeforeWrite(t *testing.T) {
	for _, fault := range []string{"hardlink", "file rename", "directory rename", "cancelled context"} {
		t.Run(fault, func(t *testing.T) {
			root, path := receiptTestPath(t)
			writes := 0
			ops := receiptRealFileOps()
			ops.write = func(file *os.File, body []byte) (int, error) { writes++; return file.Write(body) }
			slot, err := reserveProfileReceipt(t.Context(), path, ops)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = slot.Close() })
			ctx := t.Context()
			switch fault {
			case "hardlink":
				err = os.Link(path, path+".link")
			case "file rename":
				err = os.Rename(path, path+".original")
				if err == nil {
					err = os.WriteFile(path, []byte("replacement"), 0600)
				}
			case "directory rename":
				err = os.Rename(root, root+".original")
				if err == nil {
					err = os.Mkdir(root, 0700)
				}
			case "cancelled context":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := slot.WriteReceipt(ctx, receiptFileDataFixture(t)); !errors.Is(err, ErrDenied) || writes != 0 {
				t.Fatalf("%s used substituted ownership: err=%v writes=%d", fault, err, writes)
			}
		})
	}
}
