//go:build darwin || linux

package attachedworkersealedinput

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/pisdkmedia"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Genuine acquired Go composition, SYNTHETIC source replies only. These tests
// call its PRIVATE metadata plan, not the ineligible parent's phase observer.
// They prove no native/model/currentness/physical-send or serving authority.
func piPreparedPhaseFixture(t *testing.T) (*piOwnerFixture, *piOwnedPreparation, *attachedworkerdaemon.PreparedAPIDataV1) {
	t.Helper()
	f := newPiOwnerFixture(t)
	d, err := f.owner.PrepareOriginalAPIInvocation(f.ctx, f.inv.Identity, f.inv.VerifiedInput, f.inv.Process, f.inv.InputAcquisition)
	r := f.owner.current
	if !errors.Is(err, pisdkmedia.ErrMissingProducer) || d == nil || r == nil || r.phases == nil || r.init == nil || r.execution != nil ||
		d.InitArguments != [10]string{} || d.HTTPInitArguments == [17]string{} {
		t.Fatalf("actual preparation did not retain phase plan: data=%t owned=%t err=%v", d != nil, r != nil, err)
	}
	return f, r, d
}

func piPreparedPhaseRequest(p *piPreparedPhases, phase pisdkphasewire.Phase) pisdkphasewire.Request {
	r := p.scope
	r.Phase = phase
	r.CallerDeadline.RemainingNanoseconds = uint64(time.Minute)
	return r
}

func piPreparedPhaseReply(t *testing.T, f *piOwnerFixture, r *piOwnedPreparation) []byte {
	t.Helper()
	h := f.inv.VerifiedInput.AcceptedAttempt
	now := f.backend.now.UTC().Truncate(time.Microsecond)
	return piReplyBytes(t, pisdkphasewire.Reply{Status: ports.AttachedWorkerExecutionApplied, AcceptedAttempt: h, AcceptedAttemptRevision: h.Revision,
		HarnessBindingDigest: r.phases.scope.ExpectedHarnessBindingDigest, AttachedPiAPIInvocationPolicyDigest: r.phases.scope.ExpectedAttachedPiAPIInvocationPolicyDigest,
		CheckedAt: now, UseExpiresAt: now.Add(time.Second)})
}

func TestPiPreparedPhasesThreePrepaidExchangesAndCompositeRetirement(t *testing.T) {
	f := newPiOwnerFixture(t)
	baseline := f.ledger.Accounting()
	d, err := f.owner.PrepareOriginalAPIInvocation(f.ctx, f.inv.Identity, f.inv.VerifiedInput, f.inv.Process, f.inv.InputAcquisition)
	r := f.owner.current
	if !errors.Is(err, pisdkmedia.ErrMissingProducer) || d == nil || r == nil || r.phases == nil {
		t.Fatalf("missing actual phase preparation: %v", err)
	}
	p := r.phases
	if p.ledger != f.ledger || p.input != f.inv.InputAcquisition || p.factory != f.owner.source || p.client != f.owner.source.phase {
		t.Fatal("phase plan detached from original acquisition/client/ledger")
	}
	body := piPreparedPhaseReply(t, f, r)
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		data, err := io.ReadAll(io.LimitReader(req.Body, pisdkphasewire.RequestMaxBytes+1))
		decoded, decodeErr := pisdkphasewire.DecodeRequest(data)
		if err != nil || decodeErr != nil || decoded.Authorization != p.scope.Authorization || req.ProtoMajor != 1 || !req.Close || req.ContentLength != int64(len(data)) {
			t.Error("noncanonical original phase request")
		}
		piReplyHeaders(w)
		_, _ = w.Write(body)
	})
	f.phaseHandler.Store(&handler)
	prepaid := f.ledger.Accounting()
	for _, phase := range []pisdkphasewire.Phase{pisdkphasewire.PreIssue, pisdkphasewire.PhysicalSend, pisdkphasewire.ImmutableFinalize} {
		request := piPreparedPhaseRequest(p, phase)
		reply, err := p.request(f.ctx, request)
		if err != nil || pisdkphasewire.CompareReply(request, reply) != nil {
			t.Fatalf("phase %s: err=%v", phase, err)
		}
		if got := f.ledger.Accounting(); got != prepaid {
			t.Fatalf("phase %s replenished/refunded prepaid ledger: before=%+v after=%+v", phase, prepaid, got)
		}
	}
	if _, err := p.request(f.ctx, piPreparedPhaseRequest(p, pisdkphasewire.PreIssue)); err == nil || f.phase.Load() != 3 {
		t.Fatalf("fourth phase/reentry sent: calls=%d err=%v", f.phase.Load(), err)
	}
	if err := f.ledger.Reserve(0, 123, 0); err != nil {
		t.Fatal(err)
	}
	retirement, _ := r.Context()
	if err := f.owner.RetireOriginalAPIStaging(retirement, f.inv.Identity, d); err != nil {
		t.Fatal(err)
	}
	if got := f.ledger.Accounting(); got.LiveBytes != baseline.LiveBytes+123 || got.CopyBytes != prepaid.CopyBytes || got.Nodes != prepaid.Nodes || !p.closed || p.live != 0 || p.scope != (pisdkphasewire.Request{}) {
		t.Fatalf("incorrect composite phase retirement: baseline=%+v prepaid=%+v after=%+v", baseline, prepaid, got)
	}
	if err := f.ledger.EndLive(123); err != nil {
		t.Fatal(err)
	}
	if f.owner.source.phase.closed || len(f.owner.source.phase.bearer) == 0 {
		t.Fatal("phase retirement closed shared Session client")
	}
	f.noCredentialEffects(t)
}

func TestPiPreparedPhasesDenialSpendsWithoutRetry(t *testing.T) {
	for _, mode := range []string{"foreign-auth", "foreign-binding", "foreign-policy", "foreign-client", "out-of-order", "bad-window", "window-over", "later-deadline", "denied", "lost", "malformed", "syntax-ceiling", "first-over", "source-close", "original-cancel"} {
		t.Run(mode, func(t *testing.T) {
			f, r, _ := piPreparedPhaseFixture(t)
			p := r.phases
			request := piPreparedPhaseRequest(p, pisdkphasewire.PreIssue)
			ctx := f.ctx
			wantCalls := int32(0)
			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				piReplyHeaders(w)
				switch mode {
				case "denied":
					_, _ = w.Write([]byte(`{"status":"denied"}`))
				case "lost":
					w.WriteHeader(http.StatusServiceUnavailable)
				case "syntax-ceiling":
					_, _ = w.Write(bytes.Repeat([]byte{'x'}, pisdkphasewire.ReplyMaxBytes))
				case "first-over":
					_, _ = w.Write(bytes.Repeat([]byte{'x'}, pisdkphasewire.ReplyMaxBytes+1))
				default:
					_, _ = w.Write([]byte(`{"status":"applied"}`))
				}
			})
			f.phaseHandler.Store(&handler)
			switch mode {
			case "foreign-auth":
				request.Authorization.ExpectedAttemptRevision++
			case "foreign-binding":
				request.ExpectedHarnessBindingDigest = strings.Repeat("f", 64)
			case "foreign-policy":
				request.ExpectedAttachedPiAPIInvocationPolicyDigest = strings.Repeat("f", 64)
			case "foreign-client":
				p.factory = f.newSource()
			case "out-of-order":
				request.Phase = pisdkphasewire.PhysicalSend
			case "bad-window":
				request.CallerDeadline.RemainingNanoseconds = 0
			case "window-over":
				request.CallerDeadline.RemainingNanoseconds = uint64(math.MaxInt64) + 1
			case "later-deadline":
				deadline, _ := f.ctx.Deadline()
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(t.Context(), deadline.Add(time.Second))
				t.Cleanup(cancel)
			case "source-close":
				_ = f.owner.source.Close()
			case "original-cancel":
				f.cancel()
			default:
				wantCalls = 1
			}
			prepaid := f.ledger.Accounting()
			if reply, err := p.request(ctx, request); err == nil || reply != (pisdkphasewire.Reply{}) {
				t.Fatal("invalid/failed exchange returned usable reply")
			}
			if _, err := p.request(f.ctx, piPreparedPhaseRequest(p, pisdkphasewire.PreIssue)); err == nil || f.phase.Load() != wantCalls || f.ledger.Accounting() != prepaid {
				t.Fatalf("retry or scope escape: calls=%d want=%d err=%v", f.phase.Load(), wantCalls, err)
			}
			f.noCredentialEffects(t)
		})
	}
}

func TestPiPreparedPhasesExactBudgetAndFirstOverBeforeEncoding(t *testing.T) {
	for _, mode := range []string{"exact-fit", "copy-first-over", "live-first-over", "nodes-first-over"} {
		t.Run(mode, func(t *testing.T) {
			f := newPiOwnerFixture(t)
			// Constructor budget control only. Full-pipeline/native eligibility
			// is exercised separately and remains MissingProducer in every case.
			h := f.inv.VerifiedInput.AcceptedAttempt
			b := f.inv.VerifiedInput.Job.HarnessBinding
			digest, err := b.Digest()
			if err != nil {
				t.Fatal(err)
			}
			scope := pisdkphasewire.Request{Version: 1, Authorization: ports.AttachedWorkerSealedInputAuthorization{
				TenantID: h.TenantID, OwnerUserID: h.OwnerUserID, WorkerID: h.WorkerID, ConnectionID: h.ConnectionID,
				EnrollmentGeneration: h.EnrollmentGeneration, ConnectionGeneration: h.ConnectionGeneration, RunID: h.RunID, AttemptID: h.AttemptID,
				AttemptSequence: 1, LeaseID: h.LeaseID, LeaseGeneration: h.LeaseGeneration, FenceToken: h.FenceToken,
				LeaseExpiresAtUnixMicro: h.LeaseExpiresAt.UnixMicro(), ContextDigest: h.ContextDigest, CapabilityDigest: h.CapabilityDigest,
				PolicyDigest: h.PolicyDigest, ExpectedAttemptRevision: h.Revision}, ExpectedHarnessBindingDigest: string(digest),
				ExpectedAttachedPiAPIInvocationPolicyDigest: string(b.AttachedPiAPIInvocationPolicyDigest),
				CallerDeadline: pisdkphasewire.CallerDeadline{Version: 1, RemainingNanoseconds: math.MaxInt64}, Phase: pisdkphasewire.PreIssue}
			client := f.owner.source.phase
			work, live, nodes := piPhaseReservation(uint64(len(client.bearer)), uint64(len(client.endpoint)))
			wantCopy, wantLive, wantNodes := work+8*pisdkphasewire.RequestMaxBytes, live+pisdkphasewire.RequestMaxBytes, nodes+pisdkphasewire.RequestNodes
			switch mode {
			case "copy-first-over":
				wantCopy = work - 1
			case "live-first-over":
				wantLive = live - 1
			case "nodes-first-over":
				wantNodes = nodes - 1
			}
			remaining := f.ledger.Remaining()
			if remaining.CopyBytes < wantCopy || remaining.LiveBytes < wantLive || remaining.Nodes < wantNodes {
				t.Fatal("synthetic fixture cannot fit phase schedule")
			}
			heldLive := remaining.LiveBytes - wantLive
			if err := f.ledger.Reserve(remaining.CopyBytes-wantCopy, heldLive, remaining.Nodes-wantNodes); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = f.ledger.EndLive(heldLive) })
			before := f.ledger.Accounting()
			p, err := f.owner.source.preparePiPhases(f.inv.InputAcquisition.(*attachedworkerdaemontransport.PiInputAcquisitionV1), scope)
			if p != nil {
				// This test owns the constructor's standalone metadata partition,
				// not a composite record. Join first, then retire that exact debit.
				t.Cleanup(func() { p.clear(); _ = f.ledger.EndLive(live) })
			}
			if mode == "exact-fit" {
				if err != nil || p == nil {
					t.Fatalf("exact prepaid schedule rejected: %v", err)
				}
				after := f.ledger.Accounting()
				if after.CopyBytes-before.CopyBytes != wantCopy || after.Nodes-before.Nodes != wantNodes || after.LiveBytes-before.LiveBytes != live {
					t.Fatalf("wrong exact schedule: before=%+v after=%+v", before, after)
				}
			} else if err == nil || p != nil || f.ledger.Accounting() != before {
				t.Fatalf("%s encoded/allocated without full prepayment: err=%v plan=%t before=%+v after=%+v", mode, err, p != nil, before, f.ledger.Accounting())
			}
			f.noEffects(t)
		})
	}
}

func TestPiPreparedPhasesClearCancelsAndJoinsWithoutEarlyRefund(t *testing.T) {
	f, r, _ := piPreparedPhaseFixture(t)
	p := r.phases
	entered, released := make(chan struct{}), make(chan struct{})
	handler := http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body)
		close(entered)
		select {
		case <-req.Context().Done():
		case <-released:
		}
	})
	f.phaseHandler.Store(&handler)
	ctx, cancel := context.WithCancel(f.ctx)
	request := piPreparedPhaseRequest(p, pisdkphasewire.PreIssue)
	done := make(chan struct{})
	piCleanupJoin(t, func() { cancel(); close(released) }, done)
	go func() {
		defer close(done)
		if _, err := p.request(ctx, request); err == nil {
			t.Error("cancelled phase succeeded")
		}
	}()
	piWait(t, ctx, entered, "actual phase headers")
	if _, err := p.request(ctx, request); err == nil {
		t.Fatal("concurrent phase queued or reentered")
	}
	before := f.ledger.Accounting()
	p.clear()
	piWait(t, ctx, done, "cancelled phase join")
	if !p.closed || p.ctx.Err() == nil || f.ledger.Accounting() != before || f.owner.source.phase.closed {
		t.Fatal("phase clear refunded before composite retirement or closed Session")
	}
	p.clear()
	if _, err := p.request(ctx, request); err == nil || f.phase.Load() != 1 {
		t.Fatal("cleared phase plan reused")
	}
	f.noCredentialEffects(t)
}
