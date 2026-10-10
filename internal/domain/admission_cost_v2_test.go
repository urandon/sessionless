package domain_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func admissionCostV2Fixture(t testing.TB) (domain.AdmissionCostCeilingV2, time.Time) {
	t.Helper()
	search, at := externalServiceCostFixture(t)
	search.PriceObservedAt = at.Add(-time.Hour)
	*search.MaxAmountMicrounits = 40
	mcp := search.Clone()
	mcp.Kind, mcp.ResourceID, mcp.ProfileID = domain.ExternalServiceMCPToolV1, "mcp-resource", "docs-profile"
	mcp.ScopedGrantDigest = strings.Repeat("d", 64)
	*mcp.MaxAmountMicrounits = 30
	substrate, model, total := uint64(10), uint64(20), uint64(100)
	value := domain.AdmissionCostCeilingV2{
		Version: domain.AdmissionCostCeilingVersionV2, Currency: "USD", PriceRevision: "model-substrate-price.v1",
		PriceObservedAt: at, PriceExpiresAt: at.Add(time.Hour), MaxDeliveries: 5,
		MaxPreEffectDurationPerDelivery: time.Minute, MaxActiveDuration: 40 * time.Minute, MaxCleanupAndReconcileDuration: 5 * time.Minute,
		ConfiguredMemoryBytes: 256 << 20, ConfiguredVCPUMillis: 1000, MaxIngressBytes: 1 << 20, MaxEgressBytes: 1 << 20,
		MaxLogBytes: 2 << 20, MaxEvidenceBytes: 1 << 20, SubstratePriceState: domain.CostEvidenceKnownV1, ProviderPriceState: domain.ProviderPriceKnownV1,
		MaxSubstrateAmountMicrounits: &substrate, MaxProviderAmountMicrounits: &model, MaxTotalAmountMicrounits: &total,
		MCPToolCost: &mcp, WebSearchCost: &search,
	}
	if err := value.ValidateAt(at); err != nil {
		t.Fatalf("valid V2 cost fixture at %s: %v", at, err)
	}
	return value, at
}

func TestAdmissionCostV2AllPartitionPriceStatesAndMissingAuthority(t *testing.T) {
	t.Parallel()
	base, at := admissionCostV2Fixture(t)
	for _, substrate := range []string{"known", "unknown"} {
		for _, model := range []string{"known", "known_zero", "free", "unknown"} {
			for _, mcp := range []string{"absent", "paid", "free", "unknown"} {
				for _, search := range []string{"absent", "paid", "free", "unknown"} {
					name := strings.Join([]string{substrate, model, mcp, search}, "/")
					t.Run(name, func(t *testing.T) {
						value := base.Clone()
						unknown := substrate == "unknown" || model == "unknown" || mcp == "unknown" || search == "unknown"
						if substrate == "unknown" {
							value.SubstratePriceState, value.MaxSubstrateAmountMicrounits = domain.CostEvidenceUnknownV1, nil
						}
						switch model {
						case "unknown":
							value.ProviderPriceState, value.MaxProviderAmountMicrounits = domain.ProviderPriceUnknownV1, nil
						case "free":
							value.ProviderPriceState = domain.ProviderPriceKnownFreeV1
							*value.MaxProviderAmountMicrounits = 0
						case "known_zero":
							*value.MaxProviderAmountMicrounits = 0
						}
						for _, component := range []struct {
							slot  **domain.ExternalServiceCostCeilingV1
							state string
						}{{&value.MCPToolCost, mcp}, {&value.WebSearchCost, search}} {
							switch component.state {
							case "absent":
								*component.slot = nil
							case "free":
								(*component.slot).PriceState = domain.ExternalServicePriceKnownFreeV1
								*(*component.slot).MaxAmountMicrounits = 0
							case "unknown":
								(*component.slot).PriceState, (*component.slot).MaxAmountMicrounits = domain.ExternalServicePriceUnknownV1, nil
							}
						}
						if unknown {
							value.MaxTotalAmountMicrounits = nil
						} else {
							setAdmissionCostV2Total(&value)
						}
						if err := value.Validate(); err != nil {
							t.Fatalf("representable partition states %s: %v", name, err)
						}
						if err := value.ValidateAt(at); (err == nil) == unknown {
							t.Errorf("partition states %s execution error=%v, want executable=%t", name, err, !unknown)
						}
						encoded, err := json.Marshal(value)
						if err != nil {
							t.Fatalf("marshal %s: %v", name, err)
						}
						var restored domain.AdmissionCostCeilingV2
						if err := json.Unmarshal(encoded, &restored); err != nil {
							t.Fatalf("unmarshal %s: %v", name, err)
						}
						if !reflect.DeepEqual(value, restored) {
							t.Errorf("partition states %s changed after JSON round trip", name)
						}
						before, beforeErr := value.Digest()
						after, afterErr := restored.Digest()
						if beforeErr != nil || afterErr != nil || before != after {
							t.Errorf("round-trip %s digests=%q/%q errors=%v/%v", name, before, after, beforeErr, afterErr)
						}
					})
				}
			}
		}
	}
}

func setAdmissionCostV2Total(value *domain.AdmissionCostCeilingV2) {
	var total uint64
	for _, amount := range []*uint64{value.MaxSubstrateAmountMicrounits, value.MaxProviderAmountMicrounits} {
		if amount != nil {
			total += *amount
		}
	}
	for _, component := range []*domain.ExternalServiceCostCeilingV1{value.MCPToolCost, value.WebSearchCost} {
		if component != nil && component.MaxAmountMicrounits != nil {
			total += *component.MaxAmountMicrounits
		}
	}
	value.MaxTotalAmountMicrounits = &total
}

func TestAdmissionCostV2RejectsEveryMissingOrUnknownNumericPartition(t *testing.T) {
	t.Parallel()
	base, _ := admissionCostV2Fixture(t)
	for name, mutate := range map[string]func(*domain.AdmissionCostCeilingV2){
		"substrate_known_missing": func(v *domain.AdmissionCostCeilingV2) { v.MaxSubstrateAmountMicrounits = nil },
		"model_known_missing":     func(v *domain.AdmissionCostCeilingV2) { v.MaxProviderAmountMicrounits = nil },
		"all_known_total_missing": func(v *domain.AdmissionCostCeilingV2) { v.MaxTotalAmountMicrounits = nil },
		"wrong_sum":               func(v *domain.AdmissionCostCeilingV2) { *v.MaxTotalAmountMicrounits++ },
		"substrate_unknown_numeric": func(v *domain.AdmissionCostCeilingV2) {
			v.SubstratePriceState = domain.CostEvidenceUnknownV1
			v.MaxTotalAmountMicrounits = nil
		},
		"model_unknown_numeric": func(v *domain.AdmissionCostCeilingV2) {
			v.ProviderPriceState = domain.ProviderPriceUnknownV1
			v.MaxTotalAmountMicrounits = nil
		},
		"model_free_missing": func(v *domain.AdmissionCostCeilingV2) {
			v.ProviderPriceState = domain.ProviderPriceKnownFreeV1
			v.MaxProviderAmountMicrounits = nil
		},
		"model_free_positive": func(v *domain.AdmissionCostCeilingV2) { v.ProviderPriceState = domain.ProviderPriceKnownFreeV1 },
		"substrate_unknown_total": func(v *domain.AdmissionCostCeilingV2) {
			v.SubstratePriceState, v.MaxSubstrateAmountMicrounits = domain.CostEvidenceUnknownV1, nil
		},
		"model_unknown_total": func(v *domain.AdmissionCostCeilingV2) {
			v.ProviderPriceState, v.MaxProviderAmountMicrounits = domain.ProviderPriceUnknownV1, nil
		},
		"mcp_unknown_total": func(v *domain.AdmissionCostCeilingV2) {
			v.MCPToolCost.PriceState, v.MCPToolCost.MaxAmountMicrounits = domain.ExternalServicePriceUnknownV1, nil
		},
		"search_unknown_total": func(v *domain.AdmissionCostCeilingV2) {
			v.WebSearchCost.PriceState, v.WebSearchCost.MaxAmountMicrounits = domain.ExternalServicePriceUnknownV1, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base.Clone()
			mutate(&candidate)
			assertAdmissionCostV2Invalid(t, candidate)
		})
	}
	for _, slot := range []string{"MCPToolCost", "WebSearchCost"} {
		for _, state := range []string{"paid_missing", "paid_zero", "free_missing", "free_positive", "unknown_zero", "unknown_positive"} {
			t.Run(slot+"/"+state, func(t *testing.T) {
				candidate := base.Clone()
				component := reflect.ValueOf(&candidate).Elem().FieldByName(slot).Interface().(*domain.ExternalServiceCostCeilingV1)
				switch state {
				case "paid_missing":
					component.MaxAmountMicrounits = nil
				case "paid_zero":
					*component.MaxAmountMicrounits = 0
				case "free_missing":
					component.PriceState, component.MaxAmountMicrounits = domain.ExternalServicePriceKnownFreeV1, nil
				case "free_positive":
					component.PriceState = domain.ExternalServicePriceKnownFreeV1
				case "unknown_zero":
					component.PriceState = domain.ExternalServicePriceUnknownV1
					*component.MaxAmountMicrounits = 0
					candidate.MaxTotalAmountMicrounits = nil
				case "unknown_positive":
					component.PriceState = domain.ExternalServicePriceUnknownV1
					candidate.MaxTotalAmountMicrounits = nil
				}
				assertAdmissionCostV2Invalid(t, candidate)
			})
		}
	}
}

func assertAdmissionCostV2Invalid(t testing.TB, value domain.AdmissionCostCeilingV2) {
	t.Helper()
	if err := value.Validate(); err == nil {
		t.Error("invalid V2 admission cost passed structural validation")
	}
	if digest, err := value.Digest(); err == nil || digest != "" {
		t.Errorf("invalid V2 cost digest=%q err=%v, want empty/error", digest, err)
	}
	if err := value.ValidateAt(time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)); err == nil {
		t.Error("invalid V2 admission cost authorized execution")
	}
}

func TestAdmissionCostV2CheckedSumRejectsEveryPartitionOverflowIncludingUnknown(t *testing.T) {
	t.Parallel()
	for first := 0; first < 4; first++ {
		for second := first + 1; second < 4; second++ {
			for unknownIndex := -1; unknownIndex < 4; unknownIndex++ {
				if unknownIndex == first || unknownIndex == second {
					continue
				}
				t.Run(fmt.Sprintf("partitions_%d_%d_unknown_%d", first, second, unknownIndex), func(t *testing.T) {
					value, _ := admissionCostV2Fixture(t)
					amounts := []*uint64{value.MaxSubstrateAmountMicrounits, value.MaxProviderAmountMicrounits, value.MCPToolCost.MaxAmountMicrounits, value.WebSearchCost.MaxAmountMicrounits}
					for _, amount := range amounts {
						*amount = 0
					}
					*amounts[first], *amounts[second] = ^uint64(0), 1
					for _, component := range []*domain.ExternalServiceCostCeilingV1{value.MCPToolCost, value.WebSearchCost} {
						if *component.MaxAmountMicrounits == 0 {
							component.PriceState = domain.ExternalServicePriceKnownFreeV1
						}
					}
					if unknownIndex >= 0 {
						switch unknownIndex {
						case 0:
							value.SubstratePriceState, value.MaxSubstrateAmountMicrounits = domain.CostEvidenceUnknownV1, nil
						case 1:
							value.ProviderPriceState, value.MaxProviderAmountMicrounits = domain.ProviderPriceUnknownV1, nil
						case 2:
							value.MCPToolCost.PriceState, value.MCPToolCost.MaxAmountMicrounits = domain.ExternalServicePriceUnknownV1, nil
						case 3:
							value.WebSearchCost.PriceState, value.WebSearchCost.MaxAmountMicrounits = domain.ExternalServicePriceUnknownV1, nil
						}
						value.MaxTotalAmountMicrounits = nil
					} else {
						*value.MaxTotalAmountMicrounits = 0
					}
					if err := value.Validate(); err == nil || !strings.Contains(err.Error(), "overflow") {
						t.Errorf("partition subtotal overflow error=%v, want explicit overflow rejection", err)
					}
					assertAdmissionCostV2Invalid(t, value)
				})
			}
		}
	}
	for index := 0; index < 4; index++ {
		t.Run(fmt.Sprintf("maximum_fits_partition_%d", index), func(t *testing.T) {
			value, at := admissionCostV2Fixture(t)
			amounts := []*uint64{value.MaxSubstrateAmountMicrounits, value.MaxProviderAmountMicrounits, value.MCPToolCost.MaxAmountMicrounits, value.WebSearchCost.MaxAmountMicrounits}
			for _, amount := range amounts {
				*amount = 0
			}
			*amounts[index] = ^uint64(0)
			for _, component := range []*domain.ExternalServiceCostCeilingV1{value.MCPToolCost, value.WebSearchCost} {
				if *component.MaxAmountMicrounits == 0 {
					component.PriceState = domain.ExternalServicePriceKnownFreeV1
				}
			}
			*value.MaxTotalAmountMicrounits = ^uint64(0)
			if err := value.ValidateAt(at); err != nil {
				t.Errorf("bounded exact maximum partition %d rejected: %v", index, err)
			}
		})
	}
}

func TestAdmissionCostV2FixedKindsScopeCurrencyAndContainedWindows(t *testing.T) {
	t.Parallel()
	base, _ := admissionCostV2Fixture(t)
	for _, slot := range []string{"MCPToolCost", "WebSearchCost"} {
		for name, mutate := range map[string]func(*domain.ExternalServiceCostCeilingV1){
			"wrong_kind": func(v *domain.ExternalServiceCostCeilingV1) {
				if v.Kind == domain.ExternalServiceMCPToolV1 {
					v.Kind = domain.ExternalServiceWebSearchV1
				} else {
					v.Kind = domain.ExternalServiceMCPToolV1
				}
			},
			"currency":      func(v *domain.ExternalServiceCostCeilingV1) { v.Currency = "EUR" },
			"tenant":        func(v *domain.ExternalServiceCostCeilingV1) { v.TenantID += "-other" },
			"owner":         func(v *domain.ExternalServiceCostCeilingV1) { v.OwnerUserID += "-other" },
			"billing_owner": func(v *domain.ExternalServiceCostCeilingV1) { v.BillingOwnerUserID += "-other" },
			"later_observation": func(v *domain.ExternalServiceCostCeilingV1) {
				v.PriceObservedAt = base.PriceObservedAt.Add(time.Nanosecond)
			},
			"earlier_expiry": func(v *domain.ExternalServiceCostCeilingV1) {
				v.PriceExpiresAt = base.PriceExpiresAt.Add(-time.Nanosecond)
			},
		} {
			t.Run(slot+"/"+name, func(t *testing.T) {
				candidate := base.Clone()
				component := reflect.ValueOf(&candidate).Elem().FieldByName(slot).Interface().(*domain.ExternalServiceCostCeilingV1)
				mutate(component)
				if err := component.Validate(); err != nil {
					t.Fatalf("independently valid component %s/%s: %v", slot, name, err)
				}
				assertAdmissionCostV2Invalid(t, candidate)
			})
		}
	}
	for _, slot := range []string{"MCPToolCost", "WebSearchCost"} {
		t.Run(slot+"/invalid_independent_evidence", func(t *testing.T) {
			candidate := base.Clone()
			component := reflect.ValueOf(&candidate).Elem().FieldByName(slot).Interface().(*domain.ExternalServiceCostCeilingV1)
			component.TermsEvidenceDigest = ""
			assertAdmissionCostV2Invalid(t, candidate)
		})
	}
	// Structural composition does not claim a lone component's owner is trusted.
	lone := base.Clone()
	lone.WebSearchCost = nil
	lone.MCPToolCost.OwnerUserID = "unverified-owner"
	setAdmissionCostV2Total(&lone)
	if err := lone.Validate(); err != nil {
		t.Errorf("lone independent component structure: %v", err)
	}
}

func TestAdmissionCostV2FiniteFieldsAndCanonicalStructure(t *testing.T) {
	t.Parallel()
	base, _ := admissionCostV2Fixture(t)
	for _, field := range []string{"MaxDeliveries", "MaxPreEffectDurationPerDelivery", "MaxActiveDuration", "MaxCleanupAndReconcileDuration", "ConfiguredMemoryBytes", "ConfiguredVCPUMillis", "MaxIngressBytes", "MaxEgressBytes", "MaxLogBytes", "MaxEvidenceBytes"} {
		t.Run(field+"/zero", func(t *testing.T) {
			candidate := base.Clone()
			value := reflect.ValueOf(&candidate).Elem().FieldByName(field)
			value.Set(reflect.Zero(value.Type()))
			assertAdmissionCostV2Invalid(t, candidate)
		})
	}
	for _, field := range []string{"MaxPreEffectDurationPerDelivery", "MaxActiveDuration", "MaxCleanupAndReconcileDuration"} {
		for _, invalid := range []time.Duration{-1, 24*time.Hour + time.Nanosecond, time.Duration(1<<63 - 1)} {
			t.Run(fmt.Sprintf("%s/%d", field, invalid), func(t *testing.T) {
				candidate := base.Clone()
				reflect.ValueOf(&candidate).Elem().FieldByName(field).SetInt(int64(invalid))
				assertAdmissionCostV2Invalid(t, candidate)
			})
		}
	}
	for name, mutate := range map[string]func(*domain.AdmissionCostCeilingV2){
		"legacy_version": func(v *domain.AdmissionCostCeilingV2) { v.Version = 1 }, "missing_version": func(v *domain.AdmissionCostCeilingV2) { v.Version = 0 },
		"delivery_cap": func(v *domain.AdmissionCostCeilingV2) { v.MaxDeliveries = 101 }, "delivery_overflow": func(v *domain.AdmissionCostCeilingV2) { v.MaxDeliveries = ^uint32(0) },
		"currency_missing": func(v *domain.AdmissionCostCeilingV2) { v.Currency = "" }, "currency_noncanonical": func(v *domain.AdmissionCostCeilingV2) { v.Currency = "usd" },
		"currency_nonascii": func(v *domain.AdmissionCostCeilingV2) { v.Currency = "USД" },
		"revision_missing":  func(v *domain.AdmissionCostCeilingV2) { v.PriceRevision = "" }, "revision_long": func(v *domain.AdmissionCostCeilingV2) { v.PriceRevision = strings.Repeat("p", 129) },
		"revision_whitespace":     func(v *domain.AdmissionCostCeilingV2) { v.PriceRevision = " p" },
		"missing_substrate_state": func(v *domain.AdmissionCostCeilingV2) { v.SubstratePriceState = "" }, "substrate_free_alias": func(v *domain.AdmissionCostCeilingV2) { v.SubstratePriceState = "known_free" },
		"missing_model_state": func(v *domain.AdmissionCostCeilingV2) { v.ProviderPriceState = "" }, "model_paid_alias": func(v *domain.AdmissionCostCeilingV2) { v.ProviderPriceState = "known_paid" },
		"observation_zero": func(v *domain.AdmissionCostCeilingV2) { v.PriceObservedAt = time.Time{} }, "expiry_zero": func(v *domain.AdmissionCostCeilingV2) { v.PriceExpiresAt = time.Time{} },
		"expiry_equal": func(v *domain.AdmissionCostCeilingV2) { v.PriceExpiresAt = v.PriceObservedAt }, "expiry_before": func(v *domain.AdmissionCostCeilingV2) { v.PriceExpiresAt = v.PriceObservedAt.Add(-time.Nanosecond) },
		"observation_year_zero": func(v *domain.AdmissionCostCeilingV2) { v.PriceObservedAt = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC) },
		"expiry_year_10000": func(v *domain.AdmissionCostCeilingV2) {
			v.PriceExpiresAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base.Clone()
			mutate(&candidate)
			assertAdmissionCostV2Invalid(t, candidate)
		})
	}
	maximum := base.Clone()
	maximum.MaxDeliveries = 100
	maximum.MaxPreEffectDurationPerDelivery, maximum.MaxActiveDuration, maximum.MaxCleanupAndReconcileDuration = 24*time.Hour, 24*time.Hour, 24*time.Hour
	if err := maximum.Validate(); err != nil {
		t.Errorf("individually maximum finite durations/deliveries: %v", err)
	}
	assertAdmissionCostV2Invalid(t, domain.AdmissionCostCeilingV2{})
}

func TestAdmissionCostV2AllPriceWindowBoundaries(t *testing.T) {
	t.Parallel()
	base, at := admissionCostV2Fixture(t)
	for _, source := range []string{"outer", "mcp", "search"} {
		t.Run(source, func(t *testing.T) {
			candidate := base.Clone()
			// Equal service/outer endpoints show every price authority shares the
			// exclusive expiry fence; containing wider service windows never extend it.
			if source == "mcp" {
				candidate.MCPToolCost.PriceObservedAt = at
				candidate.MCPToolCost.PriceExpiresAt = candidate.PriceExpiresAt
			}
			if source == "search" {
				candidate.WebSearchCost.PriceObservedAt = at
				candidate.WebSearchCost.PriceExpiresAt = candidate.PriceExpiresAt
			}
			for _, tc := range []struct {
				name  string
				now   time.Time
				valid bool
			}{
				{"zero", time.Time{}, false}, {"before_observation", at.Add(-time.Nanosecond), false}, {"at_observation", at, true},
				{"before_expiry", candidate.PriceExpiresAt.Add(-time.Nanosecond), true}, {"at_expiry", candidate.PriceExpiresAt, false}, {"after_expiry", candidate.PriceExpiresAt.Add(time.Nanosecond), false},
				{"same_instant_other_zone", at.In(time.FixedZone("fixture", 3*3600)), true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if err := candidate.ValidateAt(tc.now); (err == nil) != tc.valid {
						t.Errorf("%s window at %s error=%v, want valid=%t", source, tc.now, err, tc.valid)
					}
				})
			}
		})
	}
}

func TestAdmissionCostV2DigestPresenceUnknownZeroAndEveryField(t *testing.T) {
	t.Parallel()
	base, _ := admissionCostV2Fixture(t)
	want, err := base.Digest()
	if err != nil {
		t.Fatal(err)
	}
	const golden = "368ae3c39a023322341f360bed97f8811166c50344182e2e89fceca3479890d2"
	if string(want) != golden {
		t.Fatalf("V2 fixed fixture digest=%q want=%q", want, golden)
	}
	if got := admissionCostV2IndependentDigest(t, base); string(want) != got {
		t.Fatalf("V2 framed digest=%q independently encoded=%q", want, got)
	}
	mutations := map[string]func(*domain.AdmissionCostCeilingV2){
		"Version": func(v *domain.AdmissionCostCeilingV2) { v.Version++ },
		"Currency": func(v *domain.AdmissionCostCeilingV2) {
			v.Currency = "EUR"
			v.MCPToolCost.Currency, v.WebSearchCost.Currency = "EUR", "EUR"
		},
		"PriceRevision":                   func(v *domain.AdmissionCostCeilingV2) { v.PriceRevision += "-other" },
		"PriceObservedAt":                 func(v *domain.AdmissionCostCeilingV2) { v.PriceObservedAt = v.PriceObservedAt.Add(time.Nanosecond) },
		"PriceExpiresAt":                  func(v *domain.AdmissionCostCeilingV2) { v.PriceExpiresAt = v.PriceExpiresAt.Add(-time.Nanosecond) },
		"MaxDeliveries":                   func(v *domain.AdmissionCostCeilingV2) { v.MaxDeliveries++ },
		"MaxPreEffectDurationPerDelivery": func(v *domain.AdmissionCostCeilingV2) { v.MaxPreEffectDurationPerDelivery++ },
		"MaxActiveDuration":               func(v *domain.AdmissionCostCeilingV2) { v.MaxActiveDuration++ },
		"MaxCleanupAndReconcileDuration":  func(v *domain.AdmissionCostCeilingV2) { v.MaxCleanupAndReconcileDuration++ },
		"ConfiguredMemoryBytes":           func(v *domain.AdmissionCostCeilingV2) { v.ConfiguredMemoryBytes++ }, "ConfiguredVCPUMillis": func(v *domain.AdmissionCostCeilingV2) { v.ConfiguredVCPUMillis++ },
		"MaxIngressBytes": func(v *domain.AdmissionCostCeilingV2) { v.MaxIngressBytes++ }, "MaxEgressBytes": func(v *domain.AdmissionCostCeilingV2) { v.MaxEgressBytes++ },
		"MaxLogBytes": func(v *domain.AdmissionCostCeilingV2) { v.MaxLogBytes++ }, "MaxEvidenceBytes": func(v *domain.AdmissionCostCeilingV2) { v.MaxEvidenceBytes++ },
		"SubstratePriceState": func(v *domain.AdmissionCostCeilingV2) {
			v.SubstratePriceState, v.MaxSubstrateAmountMicrounits, v.MaxTotalAmountMicrounits = domain.CostEvidenceUnknownV1, nil, nil
		},
		"ProviderPriceState": func(v *domain.AdmissionCostCeilingV2) {
			v.ProviderPriceState, v.MaxProviderAmountMicrounits, v.MaxTotalAmountMicrounits = domain.ProviderPriceUnknownV1, nil, nil
		},
		"MaxSubstrateAmountMicrounits": func(v *domain.AdmissionCostCeilingV2) {
			*v.MaxSubstrateAmountMicrounits++
			*v.MaxTotalAmountMicrounits++
		},
		"MaxProviderAmountMicrounits": func(v *domain.AdmissionCostCeilingV2) {
			*v.MaxProviderAmountMicrounits++
			*v.MaxTotalAmountMicrounits++
		},
		"MaxTotalAmountMicrounits": func(v *domain.AdmissionCostCeilingV2) { *v.MaxTotalAmountMicrounits++ },
		"MCPToolCost":              func(v *domain.AdmissionCostCeilingV2) { v.MCPToolCost.ScopedGrantDigest = strings.Repeat("e", 64) },
		"WebSearchCost":            func(v *domain.AdmissionCostCeilingV2) { v.WebSearchCost.ScopedGrantDigest = strings.Repeat("f", 64) },
	}
	typ := reflect.TypeOf(base)
	if len(mutations) != typ.NumField() {
		t.Fatalf("digest field coverage=%d want=%d", len(mutations), typ.NumField())
	}
	for index := range typ.NumField() {
		name := typ.Field(index).Name
		t.Run(name, func(t *testing.T) {
			mutate, ok := mutations[name]
			if !ok {
				t.Fatalf("no digest mutation for %s", name)
			}
			candidate := base.Clone()
			mutate(&candidate)
			got, err := candidate.Digest()
			if name == "Version" || name == "MaxTotalAmountMicrounits" {
				if err == nil || got != "" {
					t.Errorf("invalid %s got digest=%q err=%v", name, got, err)
				}
				return
			}
			if err != nil || got == want {
				t.Errorf("changed %s got digest=%q err=%v want distinct from %q", name, got, err, want)
			}
		})
	}
	seen := make(map[domain.AdmissionCostCeilingDigestV2]string)
	for _, presence := range []string{"absent", "free", "unknown", "paid"} {
		candidate := base.Clone()
		candidate.WebSearchCost = nil
		switch presence {
		case "absent":
			candidate.MCPToolCost = nil
		case "free":
			candidate.MCPToolCost.PriceState = domain.ExternalServicePriceKnownFreeV1
			*candidate.MCPToolCost.MaxAmountMicrounits = 0
		case "unknown":
			candidate.MCPToolCost.PriceState, candidate.MCPToolCost.MaxAmountMicrounits = domain.ExternalServicePriceUnknownV1, nil
		}
		setAdmissionCostV2Total(&candidate)
		if presence == "unknown" {
			candidate.MaxTotalAmountMicrounits = nil
		}
		digest, err := candidate.Digest()
		if err != nil {
			t.Fatalf("presence %s digest: %v", presence, err)
		}
		if prior, ok := seen[digest]; ok {
			t.Errorf("presence %s collapsed into %s", presence, prior)
		}
		seen[digest] = presence
	}
	for _, invalid := range []string{"", strings.Repeat("a", 63), strings.Repeat("a", 65), strings.Repeat("A", 64), strings.Repeat("g", 64)} {
		if err := domain.AdmissionCostCeilingDigestV2(invalid).Validate(); err == nil {
			t.Errorf("invalid V2 digest %q accepted", invalid)
		}
	}
}

func admissionCostV2IndependentDigest(t testing.TB, value domain.AdmissionCostCeilingV2) string {
	t.Helper()
	hash := sha256.New()
	hash.Write([]byte("sessionless.admission-cost-ceiling.v2\x00"))
	rawUint := func(value uint64) {
		var bytes [8]byte
		binary.BigEndian.PutUint64(bytes[:], value)
		hash.Write(bytes[:])
	}
	uintField := func(value uint64) { rawUint(8); rawUint(value) }
	stringField := func(value string) { rawUint(uint64(len(value))); hash.Write([]byte(value)) }
	instant := func(value time.Time) { stringField(value.UTC().Format(time.RFC3339Nano)) }
	optional := func(value *uint64) {
		if value == nil {
			uintField(0)
		} else {
			uintField(1)
			uintField(*value)
		}
	}
	uintField(uint64(value.Version))
	stringField(value.Currency)
	stringField(value.PriceRevision)
	instant(value.PriceObservedAt)
	instant(value.PriceExpiresAt)
	uintField(uint64(value.MaxDeliveries))
	uintField(uint64(value.MaxPreEffectDurationPerDelivery))
	uintField(uint64(value.MaxActiveDuration))
	uintField(uint64(value.MaxCleanupAndReconcileDuration))
	for _, field := range []uint64{value.ConfiguredMemoryBytes, value.ConfiguredVCPUMillis, value.MaxIngressBytes, value.MaxEgressBytes, value.MaxLogBytes, value.MaxEvidenceBytes} {
		uintField(field)
	}
	stringField(string(value.SubstratePriceState))
	stringField(string(value.ProviderPriceState))
	optional(value.MaxSubstrateAmountMicrounits)
	optional(value.MaxProviderAmountMicrounits)
	optional(value.MaxTotalAmountMicrounits)
	for _, component := range []*domain.ExternalServiceCostCeilingV1{value.MCPToolCost, value.WebSearchCost} {
		if component == nil {
			uintField(0)
			continue
		}
		uintField(1)
		digest, err := component.Digest()
		if err != nil {
			t.Fatalf("independent transcript component: %v", err)
		}
		stringField(string(digest))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func TestAdmissionCostV2ExplicitKnownZeroIsNotUnknownOrAbsentServiceAuthority(t *testing.T) {
	t.Parallel()
	known, at := admissionCostV2Fixture(t)
	*known.MaxSubstrateAmountMicrounits, *known.MaxProviderAmountMicrounits, *known.MaxTotalAmountMicrounits = 0, 0, 0
	known.ProviderPriceState = domain.ProviderPriceKnownFreeV1
	for _, component := range []*domain.ExternalServiceCostCeilingV1{known.MCPToolCost, known.WebSearchCost} {
		component.PriceState = domain.ExternalServicePriceKnownFreeV1
		*component.MaxAmountMicrounits = 0
	}
	if err := known.ValidateAt(at); err != nil {
		t.Fatalf("explicit independently evidenced known-zero components: %v", err)
	}
	missing := known.Clone()
	missing.MaxTotalAmountMicrounits = nil
	assertAdmissionCostV2Invalid(t, missing)
	unknown := known.Clone()
	unknown.SubstratePriceState, unknown.MaxSubstrateAmountMicrounits, unknown.MaxTotalAmountMicrounits = domain.CostEvidenceUnknownV1, nil, nil
	if err := unknown.Validate(); err != nil {
		t.Fatalf("unknown subtotal/total representation: %v", err)
	}
	if err := unknown.ValidateAt(at); err == nil {
		t.Error("unknown amount authorized execution as numeric zero")
	}
	absent := known.Clone()
	absent.MCPToolCost, absent.WebSearchCost = nil, nil
	pricedZero := known.Clone()
	pricedZero.ProviderPriceState = domain.ProviderPriceKnownV1
	seen := make(map[domain.AdmissionCostCeilingDigestV2]string)
	for name, value := range map[string]domain.AdmissionCostCeilingV2{"known_free": known, "known_priced_zero": pricedZero, "unknown": unknown, "service_absent": absent} {
		digest, err := value.Digest()
		if err != nil {
			t.Fatalf("%s digest: %v", name, err)
		}
		if prior, found := seen[digest]; found {
			t.Errorf("%s collapsed into %s", name, prior)
		}
		seen[digest] = name
	}
}

func TestAdmissionCostV2CloneOwnsEveryPointerAndNormalizesEquivalentZones(t *testing.T) {
	t.Parallel()
	base, _ := admissionCostV2Fixture(t)
	zone := time.FixedZone("fixture", 3*3600)
	base.PriceObservedAt, base.PriceExpiresAt = base.PriceObservedAt.In(zone), base.PriceExpiresAt.In(zone)
	base.MCPToolCost.PriceObservedAt = base.MCPToolCost.PriceObservedAt.In(zone)
	base.WebSearchCost.PriceExpiresAt = base.WebSearchCost.PriceExpiresAt.In(zone)
	want, err := base.Digest()
	if err != nil {
		t.Fatal(err)
	}
	clone := base.Clone()
	got, err := clone.Digest()
	if err != nil || got != want {
		t.Fatalf("equivalent mixed-zone clone digests=%q/%q err=%v", got, want, err)
	}
	if clone.MCPToolCost == base.MCPToolCost || clone.WebSearchCost == base.WebSearchCost {
		t.Error("clone retained mutable service component")
	}
	for index, pair := range [][2]*uint64{{base.MaxSubstrateAmountMicrounits, clone.MaxSubstrateAmountMicrounits}, {base.MaxProviderAmountMicrounits, clone.MaxProviderAmountMicrounits}, {base.MaxTotalAmountMicrounits, clone.MaxTotalAmountMicrounits}, {base.MCPToolCost.MaxAmountMicrounits, clone.MCPToolCost.MaxAmountMicrounits}, {base.WebSearchCost.MaxAmountMicrounits, clone.WebSearchCost.MaxAmountMicrounits}} {
		if pair[0] == pair[1] {
			t.Errorf("clone retained amount pointer %d", index)
		}
		*pair[1]++
	}
	clone.MCPToolCost.ResourceID = "changed-resource"
	clone.WebSearchCost.ProfileRevision++
	unchanged, err := base.Digest()
	if err != nil || unchanged != want {
		t.Errorf("clone mutation changed original digest=%q want=%q err=%v", unchanged, want, err)
	}
	for _, instant := range []time.Time{clone.PriceObservedAt, clone.PriceExpiresAt, clone.MCPToolCost.PriceObservedAt, clone.MCPToolCost.PriceExpiresAt, clone.WebSearchCost.PriceObservedAt, clone.WebSearchCost.PriceExpiresAt} {
		if instant.Location() != time.UTC {
			t.Errorf("clone timestamp not UTC: %s", instant)
		}
	}
	unknown := base.Clone()
	unknown.SubstratePriceState, unknown.MaxSubstrateAmountMicrounits, unknown.MaxTotalAmountMicrounits = domain.CostEvidenceUnknownV1, nil, nil
	unknown.MCPToolCost.PriceState, unknown.MCPToolCost.MaxAmountMicrounits = domain.ExternalServicePriceUnknownV1, nil
	unknown.WebSearchCost = nil
	copy := unknown.Clone()
	if copy.MaxSubstrateAmountMicrounits != nil || copy.MaxTotalAmountMicrounits != nil || copy.MCPToolCost.MaxAmountMicrounits != nil || copy.WebSearchCost != nil {
		t.Error("clone invented missing/unknown numeric or service authority")
	}
}

func admissionCostV2SubstrateFixture(t testing.TB, cost domain.AdmissionCostCeilingV2) domain.SubstrateBindingV1 {
	t.Helper()
	digest, err := cost.Digest()
	if err != nil {
		t.Fatalf("fixture cost digest: %v", err)
	}
	return domain.SubstrateBindingV1{
		Version: domain.SubstrateBindingVersionV1, Kind: domain.SubstrateDeterministicFixtureV1, ProfileID: "v2-cost-fixture", ProfileRevision: 1,
		ProfileDigest: strings.Repeat("a", 64), ProfileEvidenceExpiresAt: cost.PriceExpiresAt, Region: "local-fixture", ImageDigest: strings.Repeat("b", 64),
		OuterHarnessArtifactDigest: strings.Repeat("c", 64), WorkloadMode: domain.SubstrateWorkloadInProcessDirectV1, IsolationProfileDigest: strings.Repeat("d", 64),
		EgressPolicyDigest: strings.Repeat("e", 64), CleanupPolicyDigest: strings.Repeat("f", 64), EgressProxyArtifactDigest: strings.Repeat("1", 64), EgressProxyIdentityDigest: strings.Repeat("2", 64),
		// Transitional typed field only: this is not V1 runtime compatibility.
		AdmissionCostCeilingDigest: domain.AdmissionCostCeilingDigestV2(digest),
		Limits: domain.SubstrateLimitsV1{InvocationTimeout: time.Hour, ExecutionTimeout: cost.MaxActiveDuration, CleanupTimeout: cost.MaxCleanupAndReconcileDuration,
			CPUMillis: cost.ConfiguredVCPUMillis, MemoryBytes: cost.ConfiguredMemoryBytes, ScratchBytes: 1 << 20, StdoutBytes: 1 << 20, StderrBytes: 1 << 20, NativeEventCount: 100, ArtifactBytes: 1 << 20},
	}
}

func TestAdmissionCostV2RetainsExactSubstrateFiniteCoverage(t *testing.T) {
	t.Parallel()
	base, _ := admissionCostV2Fixture(t)
	substrate := admissionCostV2SubstrateFixture(t, base)
	if err := base.ValidateForSubstrate(substrate); err != nil {
		t.Fatalf("exact substrate finite coverage: %v", err)
	}
	for name, mutate := range map[string]func(*domain.AdmissionCostCeilingV2){
		"memory": func(v *domain.AdmissionCostCeilingV2) { v.ConfiguredMemoryBytes-- }, "cpu": func(v *domain.AdmissionCostCeilingV2) { v.ConfiguredVCPUMillis-- },
		"active_smaller": func(v *domain.AdmissionCostCeilingV2) { v.MaxActiveDuration-- }, "active_larger": func(v *domain.AdmissionCostCeilingV2) { v.MaxActiveDuration++ },
		"cleanup": func(v *domain.AdmissionCostCeilingV2) { v.MaxCleanupAndReconcileDuration-- }, "pre_effect": func(v *domain.AdmissionCostCeilingV2) { v.MaxPreEffectDurationPerDelivery = 16 * time.Minute },
		"cleanup_no_room": func(v *domain.AdmissionCostCeilingV2) { v.MaxCleanupAndReconcileDuration = 24 * time.Hour },
		"ingress":         func(v *domain.AdmissionCostCeilingV2) { v.MaxIngressBytes-- }, "egress": func(v *domain.AdmissionCostCeilingV2) { v.MaxEgressBytes-- },
		"log": func(v *domain.AdmissionCostCeilingV2) { v.MaxLogBytes-- }, "evidence": func(v *domain.AdmissionCostCeilingV2) { v.MaxEvidenceBytes-- },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base.Clone()
			mutate(&candidate)
			other := substrate
			digest, err := candidate.Digest()
			if err != nil {
				t.Fatalf("independently valid changed %s cost: %v", name, err)
			}
			other.AdmissionCostCeilingDigest = domain.AdmissionCostCeilingDigestV2(digest)
			if err := candidate.ValidateForSubstrate(other); err == nil {
				t.Errorf("digest-resealed insufficient %s coverage accepted", name)
			}
		})
	}
	for name, mutate := range map[string]func(*domain.SubstrateBindingV1){
		"digest": func(v *domain.SubstrateBindingV1) {
			v.AdmissionCostCeilingDigest = domain.AdmissionCostCeilingDigestV2(strings.Repeat("9", 64))
		},
		"log_overflow":      func(v *domain.SubstrateBindingV1) { v.Limits.StdoutBytes = ^uint64(0); v.Limits.StderrBytes = 1 },
		"invalid_substrate": func(v *domain.SubstrateBindingV1) { v.Version = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := substrate
			mutate(&candidate)
			if err := base.ValidateForSubstrate(candidate); err == nil {
				t.Errorf("invalid substrate %s coverage accepted", name)
			}
		})
	}
	invalid := base.Clone()
	invalid.Version = 1
	if err := invalid.ValidateForSubstrate(substrate); err == nil {
		t.Error("invalid cost excused by exact substrate")
	}
}
