package pisdkmedia

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestHTTPInitOriginalSeventeenArgumentsAndSharedRetirement(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "history", true: "snapshot-tail"}[snapshot], func(t *testing.T) {
			cfg := initFixtureConfig(t)
			request, ledger := initFixtureProjection(t, cfg, snapshot)
			credentials := initFixtureCredentials(t, request)
			before := ledger.Accounting()
			plan, err := PrepareHTTPInitV1(t.Context(), request, []byte(initFixtureModel), 1000, credentials.source)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := plan.Close(); err != nil {
					t.Error(err)
				}
			})
			args, d, a := plan.EntryArguments(), plan.DecodeLimits(), plan.Accounting()
			l := cfg.Profile.Limits
			want := [17]uint64{l.ProcessFrameBytes, l.ProcessFrameBytes - 113, min(l.NativeRequestBytes, l.ProcessFrameBytes-85-l.RequestHeaderBytes), min(l.RawFinalBytes, l.StructuredFinalBytes, l.ProcessFrameBytes-17),
				l.RequestHeaderBytes, l.ResponseHeaderBytes, l.ResponseBodyBytes, l.ResponseChunkBytes, 4 << 20, l.ProcessStdoutBytes, l.ProcessEvents,
				a.MaximumBodyBytes, a.ChildCopyBytes, a.ChildJSONNodes, l.JSONDepth, d.ProfileBytes, uint64(len(request.Body()))}
			var digits uint64
			for i, text := range args {
				value, e := strconv.ParseUint(text, 10, 64)
				if e != nil || value != want[i] || strconv.FormatUint(value, 10) != text {
					t.Fatalf("argv%d got%s want%d: %v", i, text, want[i], e)
				}
				digits += uint64(len(text))
			}
			if d.BodyBytes != a.MaximumBodyBytes || d.BodyBytes >= l.ProcessFrameBytes-113 || a.EntryArgumentBytes != digits || a.After != ledger.Accounting() {
				t.Fatal("decoder funded maximum replaced by full wire cap", d, a)
			}
			if plan.state.base.EntryArguments() != [10]string{} {
				t.Fatal("HTTP plan leaked SPU2 arguments")
			}
			if _, e := plan.state.base.MaterializedBody(t.Context(), ports.ProviderInvocationCredentialV1{}, ports.ProviderCredentialMaterializationV1{}); e == nil {
				t.Fatal("HTTP plan silently used SPU2 emitter")
			}
			args[0] = "caller mutation"
			if plan.EntryArguments()[0] != strconv.FormatUint(l.ProcessFrameBytes, 10) {
				t.Fatal("suffix alias")
			}
			if credentials.store.reads != 0 || credentials.store.loads != 0 {
				t.Fatal("preparation touched credential backend")
			}
			copyPlan := *plan
			work, nodes := ledger.Accounting().CopyBytes, ledger.Accounting().Nodes
			if err := copyPlan.Close(); err != nil {
				t.Fatal(err)
			}
			if err := plan.Close(); err != nil || ledger.Accounting().LiveBytes != before.LiveBytes || ledger.Accounting().CopyBytes != work || ledger.Accounting().Nodes != nodes {
				t.Fatal("copy retirement/refund", err, ledger.Accounting(), before)
			}
			if copyPlan.EntryArguments() != [17]string{} || plan.DecodeLimits() != (InitDecodeLimitsV1{}) {
				t.Fatal("closed copy revived")
			}
		})
	}
}

func TestHTTPInitOriginalMaterializationAndBurnedFailures(t *testing.T) {
	for _, mode := range []string{"exact", "foreign handle", "foreign path", "cancelled", "prefix mutation", "busy first call"} {
		t.Run(mode, func(t *testing.T) {
			cfg := initFixtureConfig(t)
			request, _ := initFixtureProjection(t, cfg, false)
			credentials := initFixtureCredentials(t, request)
			plan, err := PrepareHTTPInitV1(t.Context(), request, []byte(initFixtureModel), 1000, credentials.source)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if e := plan.Close(); e != nil {
					t.Error(e)
				}
			})
			copyPlan := *plan
			credentials.consume(t, func(h ports.ProviderInvocationCredentialV1, m ports.ProviderCredentialMaterializationV1) error {
				ctx := t.Context()
				switch mode {
				case "foreign handle":
					h.HandleID = "foreign"
				case "foreign path":
					m.FilePath += "x"
				case "cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				case "prefix mutation":
					plan.state.base.wire[0] ^= 1
				case "busy first call":
					plan.state.mu.Lock()
				}
				body, e := copyPlan.MaterializedBody(ctx, h, m)
				if mode == "busy first call" {
					plan.state.mu.Unlock()
				}
				if (e == nil) != (mode == "exact") {
					t.Fatal("materialization", mode, e)
				}
				if mode == "exact" {
					if bytes.Contains(body, []byte(initPublicLiteral)) {
						t.Fatal("key in Init")
					}
					var v struct {
						ExpectedBody   string `json:"expected_body"`
						CredentialFile string `json:"credential_file"`
					}
					if e := json.Unmarshal(body, &v); e != nil || v.ExpectedBody != string(request.Body()) || v.CredentialFile != m.FilePath {
						t.Fatal("complete original emission", e)
					}
					if uint64(len(body))+113 > cfg.Profile.Limits.ProcessFrameBytes || uint64(len(body)) > plan.DecodeLimits().BodyBytes {
						t.Fatal("HTTP envelope overflow")
					}
				}
				if _, e := plan.MaterializedBody(t.Context(), h, m); e == nil {
					t.Fatal("copy/failure emitted twice")
				}
				return nil
			})
		})
	}
	var zero HTTPInitPlanV1
	if zero.Close() == nil || zero.EntryArguments() != [17]string{} {
		t.Fatal("zero valid")
	}
}

func TestHTTPInitIndependentTransportPartition(t *testing.T) {
	for _, requestBytes := range []uint64{1, 1 << 20} {
		t.Run(strconv.FormatUint(requestBytes, 10), func(t *testing.T) {
			l := initFixtureConfig(t).Profile.Limits
			l.NativeRequestBytes = requestBytes
			l.ResponseBodyBytes = 2045
			l.ResponseChunkBytes = 1
			l.ProcessEvents = 4096
			w, err := httpInitWireLimits(l)
			if err != nil {
				t.Fatal(err)
			}
			// Independent canonical maximum-N oracle, not observed successful debits.
			a := struct{ chunks, frames uint64 }{2045, 4096}
			q := min(l.NativeRequestBytes, l.ProcessFrameBytes-85-l.RequestHeaderBytes)
			final := min(l.RawFinalBytes, l.StructuredFinalBytes, l.ProcessFrameBytes-17)
			if requestBytes == 1 && q >= final {
				t.Fatal("fixture must exercise final larger than request")
			}
			// Obtain canonical accounting only for the production helper input; expected
			// values below derive directly from independent constants and frozen P.
			accounting, err := pisdkhttpwire.Measure(w)
			if err != nil {
				t.Fatal(err)
			}
			work, live, nodes := httpInitTransportPartition(l, accounting)
			if work != 8*(q+2*l.RequestHeaderBytes)+8*(2045+l.ResponseHeaderBytes+final)+4*(l.ProcessFrameBytes-113)+1024*a.frames+81920 ||
				live != q+l.ProcessFrameBytes-113+2045+2*l.RequestHeaderBytes+l.ResponseHeaderBytes+final+358+4096 || nodes != 4*(a.chunks+2)+32 {
				t.Fatal("independent partition", work, live, nodes)
			}
			l.ResponseBodyBytes++
			if _, err := httpInitWireLimits(l); err == nil {
				t.Fatal("first-over canonical frame count accepted")
			}
		})
	}
}

func TestHTTPInitRetirementFailureIsTerminalAcrossCopies(t *testing.T) {
	for _, failure := range []string{"base", "metadata"} {
		t.Run(failure, func(t *testing.T) {
			cfg := initFixtureConfig(t)
			request, ledger := initFixtureProjection(t, cfg, false)
			credentials := initFixtureCredentials(t, request)
			plan, err := PrepareHTTPInitV1(t.Context(), request, []byte(initFixtureModel), 1000, credentials.source)
			if err != nil {
				t.Fatal(err)
			}
			copyPlan, arena := *plan, plan.state.base.wire
			// Deliberately corrupt only this test-owned original ledger's live
			// accounting. This is an invariant-failure regression, not valid use.
			if err := ledger.EndLive(ledger.Accounting().LiveBytes); err != nil {
				t.Fatal(err)
			}
			remaining := plan.state.base.live
			if failure == "base" {
				remaining--
			}
			if err := ledger.Reserve(0, remaining, 0); err != nil {
				t.Fatal(err)
			}
			first := copyPlan.Close()
			if first == nil || !plan.state.closed || plan.state.base != nil || !bytes.Equal(arena, make([]byte, len(arena))) {
				t.Fatal("failed retirement did not clear and become terminal", first)
			}
			if failure == "base" && ledger.Accounting().LiveBytes != remaining {
				t.Fatal("base retirement failure still attempted metadata refund")
			}
			const otherOwner = 193
			if err := ledger.Reserve(0, otherOwner, 0); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := ledger.EndLive(otherOwner); err != nil {
					t.Error("retire later test owner", err)
				}
			})
			beforeRetry := ledger.Accounting()
			if err := plan.Close(); err != first || ledger.Accounting() != beforeRetry {
				t.Fatal("repeated Close lost terminal error or debited later owner", err, first, ledger.Accounting(), beforeRetry)
			}
			if plan.EntryArguments() != [17]string{} || plan.DecodeLimits() != (InitDecodeLimitsV1{}) {
				t.Fatal("failed Close left usable preparation")
			}
		})
	}
}

func TestHTTPInitExactEnvelopeAndFirstOverEscapedLocator(t *testing.T) {
	for _, extra := range []int{0, 1} {
		t.Run(strconv.Itoa(extra), func(t *testing.T) {
			cfg := initFixtureConfig(t)
			// Smaller, valid structural frame isolates the changed envelope from
			// the independent cumulative-work ceiling. Other original-cohort
			// tests retain F=1MiB; this is NOT full MIN/MAX/native-fit evidence.
			cfg.Profile.Limits.ProcessFrameBytes = 64 << 10
			request, ledger := initFixtureProjection(t, cfg, false)
			credentials := initFixtureCredentials(t, request)
			baseline, err := PrepareHTTPInitV1(t.Context(), request, []byte(initFixtureModel), 1000, credentials.source)
			if err != nil {
				t.Fatal(err)
			}
			a := baseline.Accounting()
			if a.MaximumBodyBytes != a.PrefixBytes+6*a.FilePathBytes+3 {
				t.Fatal("maximum does not include original locator's worst JSON escape", a)
			}
			if err := baseline.Close(); err != nil {
				t.Fatal(err)
			}
			// Pad only valid selected-model display DATA, leaving original history,
			// images, source/profile/ledger and actual pinned locator unchanged.
			// This tests the constructor's own 113-byte envelope, not global fit.
			padding := int(cfg.Profile.Limits.ProcessFrameBytes-113-a.MaximumBodyBytes) + extra
			model := strings.Replace(initFixtureModel, "structural init fixture", "structural init fixture"+strings.Repeat("x", padding), 1)
			before := ledger.Accounting()
			plan, err := PrepareHTTPInitV1(t.Context(), request, []byte(model), 1000, credentials.source)
			if extra == 1 {
				if plan != nil {
					_ = plan.Close()
				}
				if err == nil || plan != nil || ledger.Accounting().LiveBytes != before.LiveBytes {
					t.Fatal("first byte over 113-byte envelope accepted or leaked", err, before, ledger.Accounting())
				}
			} else {
				if err != nil {
					t.Fatal("exact 113-byte envelope rejected", err, "model", len(model), "padding", padding, "before", before, "after", ledger.Accounting(), "remaining", ledger.Remaining())
				}
				t.Cleanup(func() {
					if e := plan.Close(); e != nil {
						t.Error(e)
					}
				})
				a := plan.Accounting()
				if a.MaximumBodyBytes+113 != cfg.Profile.Limits.ProcessFrameBytes || a.MaximumBodyBytes != a.PrefixBytes+6*a.FilePathBytes+3 || plan.DecodeLimits().BodyBytes != a.MaximumBodyBytes {
					t.Fatal("exact complete escaped maximum not admitted", a)
				}
			}
			if credentials.store.reads != 0 || credentials.store.loads != 0 {
				t.Fatal("boundary preparation touched credential backend")
			}
		})
	}
}

func TestHTTPInitFirstPartitionExactAndOneShortOnOriginalLedger(t *testing.T) {
	metadata := uint64(reflect.TypeOf(HTTPInitPlanV1{}).Size() + reflect.TypeOf(httpInitState{}).Size() + reflect.TypeOf(InitPlanV1{}).Size())
	for _, dimension := range []string{"work", "live", "nodes"} {
		for _, short := range []bool{false, true} {
			t.Run(dimension+strconv.FormatBool(short), func(t *testing.T) {
				cfg := initFixtureConfig(t)
				request, ledger := initFixtureProjection(t, cfg, false)
				credentials := initFixtureCredentials(t, request)
				remaining := ledger.Remaining()
				var work, live, nodes uint64
				switch dimension {
				case "work":
					work = remaining.CopyBytes - (8192 + metadata)
					if short {
						work++
					}
				case "live":
					live = remaining.LiveBytes - metadata
					if short {
						live++
					}
				case "nodes":
					nodes = remaining.Nodes - 3
					if short {
						nodes++
					}
				}
				// Spend the existing original ledger; never substitute a fresh pool.
				if err := ledger.Reserve(work, live, nodes); err != nil {
					t.Fatal(err)
				}
				before := ledger.Accounting()
				p, err := PrepareHTTPInitV1(t.Context(), request, []byte(initFixtureModel), 1000, credentials.source)
				if err == nil || p != nil {
					t.Fatal("later preparation somehow fit a first-partition-only budget")
				}
				after := ledger.Accounting()
				if after.LiveBytes != before.LiveBytes {
					t.Fatal("denied preparation leaked or stole original live debit", before, after)
				}
				if short && after.CopyBytes != before.CopyBytes {
					t.Fatal("first one-short performed funded work", before, after)
				}
				if !short && after.CopyBytes < before.CopyBytes+8192+metadata {
					t.Fatal("exact first partition was not admitted", before, after)
				}
				if credentials.store.loads != 0 || credentials.store.reads != 0 {
					t.Fatal("funding failure touched credential backend")
				}
				if err := ledger.EndLive(live); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
