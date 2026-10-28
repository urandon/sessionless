//go:build darwin || linux

package attachedworkerstack_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
)

type generationIdleSourceV3 struct{ loads atomic.Int64 }

func (v *generationIdleSourceV3) Load(context.Context, attachedworkerdaemontransport.MaterializationRequestV1) (attachedworkerdaemontransport.SealedInputV1, error) {
	v.loads.Add(1)
	return attachedworkerdaemontransport.SealedInputV1{}, errors.New("idle fixture must not load a job")
}

type generationIdleReceiptV3 struct{ calls atomic.Int64 }

func (v *generationIdleReceiptV3) Publish(context.Context, attachedworkerdaemontransport.ReceiptSubmissionV1) (attachedworkerdaemontransport.ReceiptCommitmentV1, error) {
	v.calls.Add(1)
	return attachedworkerdaemontransport.ReceiptCommitmentV1{}, errors.New("idle fixture must not publish a result")
}

func generationIdleAdapterConfigV3(t *testing.T, f *attachedworkerstack.NativeGenerationTransportFixtureV3ForTest) (*attachedworkerdaemontransport.BoundMaterializer, attachedworkerdaemontransport.Config, *generationIdleSourceV3, *generationIdleReceiptV3) {
	t.Helper()
	source, receipt := &generationIdleSourceV3{}, &generationIdleReceiptV3{}
	materializer, err := attachedworkerdaemontransport.NewNativeSubscriptionBoundMaterializerV1(source, 8192, f.CurrentTime)
	if err != nil {
		t.Fatal(err)
	}
	capability := f.Capability
	return materializer, attachedworkerdaemontransport.Config{Profile: attachedworkerdaemontransport.LocalProfileV1{Name: "native-generation-transport", CapabilityDigest: f.Session.Snapshot().CapabilityDigest,
		Executable: f.Executable, ExecutableDigest: f.ExecutableDigest, Arguments: slices.Clone(f.Arguments)}, CapabilityManifest: &capability, ReceiptPublisher: receipt, MaterializationRoot: f.Root, MaxInputBytes: 8192, Now: f.CurrentTime}, source, receipt
}

func TestNativeGenerationTransportV3InitialAndReconnectACKBeforeEveryFreshPoll(t *testing.T) {
	for _, choice := range []attachedworkerstack.NativeGenerationTransportCaseV3ForTest{attachedworkerstack.NativeGenerationTransportInitialV3ForTest, attachedworkerstack.NativeGenerationTransportReconnectV3ForTest} {
		name := map[attachedworkerstack.NativeGenerationTransportCaseV3ForTest]string{attachedworkerstack.NativeGenerationTransportInitialV3ForTest: "initial", attachedworkerstack.NativeGenerationTransportReconnectV3ForTest: "reconnect"}[choice]
		t.Run(name, func(t *testing.T) {
			f := attachedworkerstack.NewNativeGenerationTransportFixtureV3ForTest(t, choice)
			materializer, config, source, receipt := generationIdleAdapterConfigV3(t, f)
			if adapter, err := attachedworkerdaemontransport.NewPreparedAdapter(f.Session, materializer, config, f.Stack); err == nil || adapter != nil {
				t.Fatal("legacy contextless assembly accepted generation stack")
			}
			adapter, err := attachedworkerdaemontransport.NewNativeGenerationPreparedAdapterV3(t.Context(), f.Session, materializer, config, f.Stack, f.Mode)
			if err != nil {
				t.Fatal("actual generation adapter", err)
			}
			for range 2 {
				invocation, found, err := adapter.Next(t.Context())
				if err != nil || found || invocation.Credential != nil || invocation.VerifiedInput != nil {
					t.Fatalf("idle fresh poll: found=%t err=%v credential=%t input=%t", found, err, invocation.Credential != nil, invocation.VerifiedInput != nil)
				}
			}
			observation := f.Observation()
			wantReads := 0
			if name == "reconnect" {
				wantReads = 1
				if f.Session.Snapshot().ConnectionGeneration != 2 {
					t.Fatal("not a genuine fresh reconnect")
				}
			}
			if observation.Publishes != 1 || observation.Reads != wantReads || observation.Available != 2 || observation.BeforeACK != 0 || source.loads.Load() != 0 || receipt.calls.Load() != 0 {
				t.Fatalf("fresh/unchanged ACK effects: %+v loads=%d receipts=%d", observation, source.loads.Load(), receipt.calls.Load())
			}
			ack, available := slices.Index(observation.Events, "metadata-ack"), slices.Index(observation.Events, "available")
			if ack < 0 || available <= ack {
				t.Fatalf("Available preceded exact metadata ACK: %v", observation.Events)
			}
		})
	}
}

func TestNativeGenerationTransportV3BlockedACKCannotReturnPollAdapter(t *testing.T) {
	f := attachedworkerstack.NewNativeGenerationTransportFixtureV3ForTest(t, attachedworkerstack.NativeGenerationTransportBlockedACKV3ForTest)
	materializer, config, source, receipt := generationIdleAdapterConfigV3(t, f)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	type result struct {
		adapter *attachedworkerdaemontransport.Adapter
		err     error
	}
	created := make(chan result, 1)
	go func() {
		adapter, err := attachedworkerdaemontransport.NewNativeGenerationPreparedAdapterV3(ctx, f.Session, materializer, config, f.Stack, f.Mode)
		created <- result{adapter: adapter, err: err}
	}()
	select {
	case <-f.ACKEntered():
	case <-ctx.Done():
		t.Fatal("metadata did not enter", ctx.Err())
	}
	select {
	case result := <-created:
		t.Fatalf("adapter returned before ACK: adapter=%t err=%v", result.adapter != nil, result.err)
	default:
	}
	if observation := f.Observation(); observation.Available != 0 || source.loads.Load() != 0 || receipt.calls.Load() != 0 {
		t.Fatalf("effects before ACK: %+v", observation)
	}
	f.ContinueACK()
	var adapter *attachedworkerdaemontransport.Adapter
	select {
	case result := <-created:
		if result.err != nil || result.adapter == nil {
			t.Fatal("ACKed adapter", result.err)
		}
		adapter = result.adapter
	case <-ctx.Done():
		t.Fatal("adapter did not finish", ctx.Err())
	}
	if _, found, err := adapter.Next(ctx); err != nil || found {
		t.Fatalf("first idle poll found=%t err=%v", found, err)
	}
	if observation := f.Observation(); observation.Available != 1 || observation.BeforeACK != 0 {
		t.Fatalf("incorrect first Available ordering: %+v", observation)
	}
}

func TestNativeGenerationTransportV3FailedACKHasZeroAvailableEffects(t *testing.T) {
	f := attachedworkerstack.NewNativeGenerationTransportFixtureV3ForTest(t, attachedworkerstack.NativeGenerationTransportMismatchedACKV3ForTest)
	materializer, config, source, receipt := generationIdleAdapterConfigV3(t, f)
	adapter, err := attachedworkerdaemontransport.NewNativeGenerationPreparedAdapterV3(t.Context(), f.Session, materializer, config, f.Stack, f.Mode)
	if err == nil || adapter != nil {
		t.Fatal("mismatched ACK returned a poll adapter")
	}
	before := f.Observation()
	if before.Available != 0 || before.Publishes != 1 || before.Reads > 1 || source.loads.Load() != 0 || receipt.calls.Load() != 0 {
		t.Fatalf("failed ACK effects: %+v loads=%d receipts=%d", before, source.loads.Load(), receipt.calls.Load())
	}
	if adapter, err := attachedworkerdaemontransport.NewNativeGenerationPreparedAdapterV3(t.Context(), f.Session, materializer, config, f.Stack, f.Mode); err == nil || adapter != nil {
		t.Fatal("failed ACK reset one-shot bind")
	}
	after := f.Observation()
	if after.Available != 0 || after.Publishes != before.Publishes || after.Reads != before.Reads {
		t.Fatalf("failed ACK automatically retried: before=%+v after=%+v", before, after)
	}
}

func TestNativeGenerationTransportV3FreshSyncFailureStopsBeforeAvailable(t *testing.T) {
	f := attachedworkerstack.NewNativeGenerationTransportFixtureV3ForTest(t, attachedworkerstack.NativeGenerationTransportInitialV3ForTest)
	materializer, config, source, receipt := generationIdleAdapterConfigV3(t, f)
	adapter, err := attachedworkerdaemontransport.NewNativeGenerationPreparedAdapterV3(t.Context(), f.Session, materializer, config, f.Stack, f.Mode)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.CloseOriginalLifecycle(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		invocation, found, err := adapter.Next(t.Context())
		if !errors.Is(err, attachedworkerdaemontransport.ErrReconciliationRequired) || found || invocation.Credential != nil || invocation.VerifiedInput != nil {
			t.Fatalf("denied fresh sync returned invocation: found=%t err=%v", found, err)
		}
	}
	if observation := f.Observation(); observation.Available != 0 || observation.Publishes != 1 || observation.Reads != 0 || source.loads.Load() != 0 || receipt.calls.Load() != 0 {
		t.Fatalf("failed fresh synchronization effects: %+v loads=%d receipts=%d", observation, source.loads.Load(), receipt.calls.Load())
	}
}
