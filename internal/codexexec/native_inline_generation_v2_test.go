//go:build darwin || linux

package codexexec

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This runs the original joined owner and sole outer lifecycle. The callback
// models its successful CAS before Run returns, never patching a final slot or
// supplying a caller-authored completed result. Only an owned shell fixture is
// executed; no native binary, provider, real auth or engine is involved.
func TestNativeInlineGenerationV2OwnWriteBackKeepsOriginalReceipt(t *testing.T) {
	for _, changed := range []bool{false, true} {
		name := "unchanged"
		if changed {
			name = "own-refresh"
		}
		t.Run(name, func(t *testing.T) {
			r, invocation, life, launcher, metadata := nativeJoinedFixtureMediaV2(t, nativeInlineFinalFixtureV2, false, nativeJoinedOptionsV2{inlineOutput: true})
			original := invocation.VerifiedInput.Job.HarnessBinding.Clone()
			originalDigest, err := original.Digest()
			if err != nil {
				t.Fatal(err)
			}
			writes := 0
			life.writeBack = func(result ports.CredentialWriteBackResult) (ports.CredentialWriteBackResult, error) {
				writes++
				if changed {
					result.Changed = true
					result.Generation++
					metadata.binding.Generation = result.Generation
				}
				return result, nil
			}
			result, runErr := r.Run(t.Context(), invocation)
			if !result.Succeeded(runErr) || writes != 1 || len(launcher.envelopes) != 1 ||
				!reflect.DeepEqual(life.order, []string{"issue", "materialize", "writeback", "release"}) {
				t.Fatalf("original Run success=%t err=%v launches=%d writes=%d lifecycle=%v", result.Succeeded(runErr), runErr, len(launcher.envelopes), writes, life.order)
			}
			v, err := r.InlineReceiptCandidate(t.Context(), invocation.Identity, result, runErr)
			if err != nil {
				t.Fatalf("own changed=%t generation=%d blocked original completed receipt: %v", changed, result.CredentialGeneration, err)
			}
			candidate, producer, _, err := v.Declaration()
			if err != nil || producer.HarnessBindingDigest != originalDigest {
				t.Fatalf("original N producer changed digest=%s want=%s err=%v", producer.HarnessBindingDigest, originalDigest, err)
			}
			native := r.slot.original.native
			if !reflect.DeepEqual(native.bindingV2, original) || native.authority.ProviderResource != original.Resource ||
				native.evidenceV2.ValidateForBinding(original) != nil || invocation.VerifiedInput.Job.HarnessBinding.Resource != original.Resource {
				t.Fatal("receipt changed sealed N authority, job or evidence to the successor")
			}
			strictErr := nativeCredentialCurrentV2(t.Context(), metadata, native.authority, life.now)
			if (strictErr != nil) != changed {
				t.Fatalf("strict execution currentness changed=%t err=%v; sealed N must not follow refresh", changed, strictErr)
			}
			if native.child.Current() == nil {
				t.Fatal("receipt revived the original execution child")
			}
			reader, size, err := v.OpenBody(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reader.Close() })
			raw, err := io.ReadAll(reader)
			if err != nil || int64(len(raw)) != size {
				t.Fatalf("original body bytes=%d declared=%d err=%v", len(raw), size, err)
			}
			verified, err := attachedworkeroutput.VerifyInlineV2(candidate, raw)
			if err != nil {
				t.Fatalf("original ordered artifact content: %v", err)
			}
			verified.Clear()
			if _, _, err := v.OpenBody(t.Context()); err == nil {
				t.Error("own refresh reopened one-shot body")
			}
			if _, err := r.Run(t.Context(), invocation); err == nil || writes != 1 || len(launcher.envelopes) != 1 {
				t.Error("receipt enabled a second invocation or credential writeback")
			}
			final := r.slot.original.nativeFinalV2
			retained := final.artifacts[0].content
			if err := reader.Close(); err != nil || len(retained) == 0 {
				t.Fatalf("HTTP close retired original semantic custody: %v", err)
			}
			// Existing adapter/stack tests establish the real ACK join. Exercise
			// only this producer's exact retirement, not a forged protocol ACK.
			if err := r.RetireReceipt(invocation.Identity); err != nil {
				t.Fatal(err)
			}
			if len(final.artifacts) != 0 || !bytes.Equal(retained, make([]byte, len(retained))) || v.CheckCurrent(context.Background()) == nil {
				t.Fatal("exact retirement failed to clear/fence original receipt")
			}
		})
	}
}

func TestNativeInlineGenerationV2RejectsUnrelatedOrFailedWriteBack(t *testing.T) {
	for _, kind := range []string{
		"unchanged-unrelated-advance", "own-refresh-then-another-advance", "own-refresh-revoked",
		"foreign-owner", "foreign-tenant", "foreign-resource", "wrong-provider", "wrong-auth-mode", "inactive", "future",
		"metadata-missing", "metadata-error", "cancel-after-read", "unchanged-result-advance", "changed-result-unchanged",
		"changed-result-jump", "zero-result", "writeback-error", "release-error",
	} {
		t.Run(kind, func(t *testing.T) {
			r, invocation, life, launcher, metadata := nativeJoinedFixtureMediaV2(t, nativeInlineFinalFixtureV2, false, nativeJoinedOptionsV2{inlineOutput: true})
			original := invocation.VerifiedInput.Job.HarnessBinding.Resource.CredentialGeneration
			writes := 0
			life.writeBack = func(result ports.CredentialWriteBackResult) (ports.CredentialWriteBackResult, error) {
				writes++
				// Default is this original invocation's own successful rotation.
				result.Changed, result.Generation = true, original+1
				metadata.binding.Generation = result.Generation
				switch kind {
				case "unchanged-unrelated-advance", "unchanged-result-advance":
					result.Changed = false
					if kind == "unchanged-unrelated-advance" {
						result.Generation = original
					}
				case "changed-result-unchanged":
					result.Generation, metadata.binding.Generation = original, original
				case "changed-result-jump":
					result.Generation, metadata.binding.Generation = original+2, original+2
				case "zero-result":
					result.Generation = 0
				case "writeback-error":
					return result, ErrContract
				}
				return result, nil
			}
			if kind == "release-error" {
				life.releaseErr = ErrContract
			}
			result, runErr := r.Run(t.Context(), invocation)
			failedLifecycle := kind == "writeback-error" || kind == "release-error"
			if result.Succeeded(runErr) == failedLifecycle || writes != 1 || len(launcher.envelopes) != 1 ||
				!reflect.DeepEqual(life.order, []string{"issue", "materialize", "writeback", "release"}) {
				t.Fatalf("Run success=%t want=%t err=%v writes=%d launches=%d lifecycle=%v", result.Succeeded(runErr), !failedLifecycle, runErr, writes, len(launcher.envelopes), life.order)
			}
			// These are later metadata changes, not modifications of the genuine
			// captured outer result. None can follow its permitted successor.
			switch kind {
			case "own-refresh-then-another-advance":
				metadata.binding.Generation++
			case "own-refresh-revoked":
				metadata.binding.Generation++
				metadata.binding.Revoked = true
				metadata.binding.Entitlement = domain.EntitlementDisconnected
				metadata.binding.SecretRef = domain.CredentialSecretRef{}
				metadata.binding.SecretFingerprint = ""
			case "foreign-owner":
				metadata.binding.OwnerUserID = "foreign-owner"
			case "foreign-tenant":
				metadata.binding.TenantID = "foreign-tenant"
			case "foreign-resource":
				metadata.binding.SubscriptionConnectionID = "foreign-subscription"
			case "wrong-provider":
				metadata.binding.Provider = "foreign-provider"
			case "wrong-auth-mode":
				metadata.binding.AuthMode = "api-key"
			case "inactive":
				metadata.binding.Entitlement = domain.EntitlementUnknown
			case "future":
				metadata.binding.UpdatedAt = life.now.Add(1)
			case "metadata-missing":
				metadata.found = false
			case "metadata-error":
				metadata.err = ErrContract
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if kind == "cancel-after-read" {
				metadata.beforeReturn = cancel
			}
			if _, err := r.InlineReceiptCandidate(ctx, invocation.Identity, result, runErr); !errors.Is(err, ErrAuthorityUnavailable) && !errors.Is(err, ErrContract) {
				t.Fatalf("invalid original lifecycle/currentness exported: %v", err)
			}
			if r.slot.inline != nil {
				t.Error("rejected generation retained a usable body carrier")
			}
			if _, err := r.Run(t.Context(), invocation); err == nil || writes != 1 || len(launcher.envelopes) != 1 {
				t.Error("denied receipt enabled replay")
			}
		})
	}
}

func TestNativeInlineGenerationV2DerivationBoundaries(t *testing.T) {
	// Arithmetic-only boundaries: the original joined fixture deliberately
	// seals normal N before admission. These synthetic results grant neither
	// initial admission nor an active MaxUint64 binding in a real store.
	for _, tc := range []struct {
		name      string
		original  uint64
		returned  uint64
		changed   bool
		required  bool
		released  bool
		wantError bool
	}{
		{name: "unchanged", original: 7, returned: 7, required: true, released: true},
		{name: "own-successor", original: 7, returned: 8, changed: true, required: true, released: true},
		{name: "zero-original", returned: 1, changed: true, required: true, released: true, wantError: true},
		{name: "overflow-zero", original: math.MaxUint64, changed: true, required: true, released: true, wantError: true},
		{name: "overflow-unchanged", original: math.MaxUint64, returned: math.MaxUint64, changed: true, required: true, released: true, wantError: true},
		{name: "unchanged-max-arithmetic-only", original: math.MaxUint64, returned: math.MaxUint64, required: true, released: true},
		{name: "not-credential-lifecycle", original: 7, returned: 7, released: true, wantError: true},
		{name: "not-released", original: 7, returned: 7, required: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := attachedworkerdaemon.InvocationResult{Process: attachedworkerdaemon.AttemptResult{
				DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true},
				CredentialChanged: tc.changed, CredentialGeneration: tc.returned,
				CredentialReleaseRequired: tc.required, CredentialReleased: tc.released}
			got, err := nativeReceiptGenerationV2(tc.original, result, nil)
			if (err != nil) != tc.wantError || err == nil && got != tc.returned {
				t.Fatalf("original=%d changed=%t returned=%d got=%d err=%v wantError=%t", tc.original, tc.changed, tc.returned, got, err, tc.wantError)
			}
		})
	}
}
