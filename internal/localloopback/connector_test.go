package localloopback

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func testLimits() domain.ConfiguredInferenceLimitsV1 {
	return domain.ConfiguredInferenceLimitsV1{RequestBytes: 1024, ResponseBytes: 1024, ResponseHeaderBytes: 1024, JSONDepth: 4, JSONMembers: 16, Choices: 1, ToolCalls: 1, ToolNameBytes: 32, ToolCallIDBytes: 32, ToolArgumentsBytes: 512, FinalTextBytes: 512, Messages: 8, Tools: 2, MaxInputTokens: 1024, MaxOutputTokens: 32, OperationTimeout: time.Second, CleanupTimeout: time.Second}
}

// These are pure payload/shape checks, not an actual installed Source, Store,
// operation reservation, socket observation, model, or current authority proof.
func TestLocalLoopbackPayloadOwnedExactAndBounded(t *testing.T) {
	l := testLimits()
	if err := l.Validate(); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"model":"declared","messages":[]}`)
	h := sha256.Sum256(payload)
	op := domain.AttemptOperationV1{RequestBytes: uint64(len(payload)), RequestDigest: hex.EncodeToString(h[:]), ResultBytes: 512, CleanupDuration: time.Second}
	owned, err := payloadCopy(op, payload, l)
	if err != nil || !bytes.Equal(owned, payload) {
		t.Fatalf("exact owned payload denied: %v", err)
	}
	payload[0] = 'x'
	if owned[0] != '{' {
		t.Fatal("retained payload aliases caller")
	}
	owned[1] = 'x'
	if payload[1] == 'x' {
		t.Fatal("caller payload aliases retained copy")
	}
	clear(owned)
	base := []byte(`{"x":1}`)
	h = sha256.Sum256(base)
	op.RequestBytes = uint64(len(base))
	op.RequestDigest = hex.EncodeToString(h[:])
	for _, tc := range []struct {
		name    string
		change  func(*domain.AttemptOperationV1, *domain.ConfiguredInferenceLimitsV1)
		payload []byte
	}{
		{"empty", func(*domain.AttemptOperationV1, *domain.ConfiguredInferenceLimitsV1) {}, nil},
		{"changed_digest", func(o *domain.AttemptOperationV1, _ *domain.ConfiguredInferenceLimitsV1) {
			o.RequestDigest = strings.Repeat("0", 64)
		}, base},
		{"changed_bytes", func(o *domain.AttemptOperationV1, _ *domain.ConfiguredInferenceLimitsV1) { o.RequestBytes++ }, base},
		{"request_ceiling", func(_ *domain.AttemptOperationV1, l *domain.ConfiguredInferenceLimitsV1) { l.RequestBytes = 1 }, base},
		{"zero_response", func(o *domain.AttemptOperationV1, _ *domain.ConfiguredInferenceLimitsV1) { o.ResultBytes = 0 }, base},
		{"response_ceiling", func(o *domain.AttemptOperationV1, l *domain.ConfiguredInferenceLimitsV1) {
			o.ResultBytes = l.ResponseBytes + 1
		}, base},
		{"zero_cleanup", func(o *domain.AttemptOperationV1, _ *domain.ConfiguredInferenceLimitsV1) { o.CleanupDuration = 0 }, base},
		{"cleanup_ceiling", func(o *domain.AttemptOperationV1, l *domain.ConfiguredInferenceLimitsV1) {
			o.CleanupDuration = l.CleanupTimeout + time.Nanosecond
		}, base},
		{"invalid_limits", func(_ *domain.AttemptOperationV1, l *domain.ConfiguredInferenceLimitsV1) { l.RequestBytes = 0 }, base},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, limits := op, l
			tc.change(&o, &limits)
			got, err := payloadCopy(o, tc.payload, limits)
			if !errors.Is(err, domain.ErrMembershipDenied) || got != nil {
				t.Fatalf("denied payload escaped: bytes=%d err=%v", len(got), err)
			}
		})
	}
}

func TestLocalLoopbackLiteralAddressNoNormalization(t *testing.T) {
	for _, tc := range []struct {
		url, network, address, ip string
		port                      int
	}{
		{"http://127.0.0.1:11434/v1", "tcp4", "127.0.0.1:11434", "127.0.0.1", 11434},
		{"http://[::1]:65535/v1", "tcp6", "[::1]:65535", "::1", 65535},
	} {
		t.Run(tc.network, func(t *testing.T) {
			network, address, ip, port, err := literalAddress(tc.url)
			if err != nil || network != tc.network || address != tc.address || ip.String() != tc.ip || port != tc.port {
				t.Fatalf("literal tuple changed: %s %s %v %d %v", network, address, ip, port, err)
			}
		})
	}
	for _, endpoint := range []string{"http://localhost:11434/v1", "http://127.1:11434/v1", "http://127.0.0.2:11434/v1", "http://2130706433:11434/v1", "http://127.0.0.1:011434/v1", "http://127.0.0.1:0/v1", "http://127.0.0.1:65536/v1", "http://[::ffff:127.0.0.1]:11434/v1", "http://[::1%lo0]:11434/v1", "https://127.0.0.1:11434/v1", "http://user@127.0.0.1:11434/v1", "http://127.0.0.1:11434/v1/", "http://127.0.0.1:11434/v1?x=1", "http://127.0.0.1:11434/v1#fragment", "HTTP://127.0.0.1:11434/v1", "http://127.0.0.1:11434/%76%31"} {
		t.Run(endpoint, func(t *testing.T) {
			_, _, _, _, err := literalAddress(endpoint)
			if !errors.Is(err, domain.ErrMembershipDenied) {
				t.Fatal("nonliteral or normalized endpoint accepted")
			}
		})
	}
}

func TestLocalLoopbackCallerDeadlineNeverRewritesOperation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deadline := time.Now().Add(time.Minute)
		if !callerFits(context.Background(), deadline) || callerFits(nil, deadline) {
			t.Fatal("optional caller deadline policy changed")
		}
		for _, offset := range []time.Duration{-time.Nanosecond, 0, time.Second} {
			ctx, cancel := context.WithDeadline(t.Context(), deadline.Add(offset))
			if got := callerFits(ctx, deadline); got != (offset >= 0) {
				t.Fatalf("offset %s: %t", offset, got)
			}
			cancel()
			if callerFits(ctx, deadline) {
				t.Fatal("cancelled caller admitted")
			}
		}
	})
}

// This synthetic cleanup signal tests only bounded Close/taint bookkeeping;
// it does not stand in for an installed Source, real lease, socket, or Store.
func TestLocalLoopbackCloseTimeoutCannotLaterBecomeClean(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, parentCancel := context.WithCancel(t.Context())
		defer parentCancel()
		owned, cancel := context.WithCancel(parent)
		c := &ConnectionV1{ctx: owned, cancel: cancel, cleanupDone: make(chan struct{}), cleanupTimeout: time.Second}
		c.self = c
		start := time.Now()
		if err := c.Close(); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatal("unjoined cleanup reported success")
		}
		if !c.cleanupTainted || time.Since(start) != time.Second || owned.Err() == nil || parent.Err() != nil {
			t.Fatal("timeout did not retain bounded taint or cancelled caller")
		}
		close(c.cleanupDone)
		if err := c.Close(); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatal("late join silently reset tainted ownership")
		}
	})
}

// Exercise the exact timer-selected verdict deterministically, rather than
// depending on select choosing the timer among two ready cases in a loop.
func TestLocalLoopbackCompletedCleanupWinsTimeoutVerdict(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &ConnectionV1{cleanupDone: make(chan struct{}), cleanupCommitted: true}
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		time.Sleep(time.Second) // advance virtual time; not readiness synchronization
		close(c.cleanupDone)
		select {
		case <-timer.C:
		default:
			t.Fatal("virtual timeout not ready")
		}
		if err := c.closeAfterWait(true); err != nil || c.cleanupTainted {
			t.Fatalf("completed cleanup lost to ready timeout: err=%v tainted=%t", err, c.cleanupTainted)
		}
		c.cleanupTainted = true
		if err := c.closeAfterWait(true); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatal("ready completion erased previously recorded cleanup taint")
		}
	})
}

func TestLocalLoopbackNilZeroAndCopiedOwnersDeny(t *testing.T) {
	var nilKeeper *KeeperV1
	var nilConnection *ConnectionV1
	if nilKeeper.Close() != nil || nilConnection.Close() != nil {
		t.Fatal("nil close must be harmless")
	}
	if got, err := NewKeeperV1(nil, nil, nil); got != nil || !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("nil tuple constructed")
	}
	owner := &KeeperV1{}
	copyOwner := &KeeperV1{self: owner}
	for _, k := range []*KeeperV1{nilKeeper, {}, copyOwner} {
		if got, err := k.Preflight(t.Context(), domain.RegisteredAllocationV2{}, domain.AttemptExecutionSourceV1{}, domain.AttemptOperationV1{}, nil); got != nil || !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatal("unconstructed keeper admitted preflight")
		}
		if k.VerifyOwnedProxyPublication(ports.OwnedProxyPublicationV2{}, time.Now()) == nil || k.VerifyOwnedProxyPublicationCurrent(ports.OwnedProxyPublicationV2{}, time.Now()) == nil {
			t.Fatal("unconstructed keeper authenticated publication")
		}
	}
	connectionOwner := &ConnectionV1{}
	copyConnection := &ConnectionV1{self: connectionOwner}
	for _, c := range []*ConnectionV1{nilConnection, {}, copyConnection} {
		if _, err := c.Publication(); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatal("unconstructed connection published")
		}
		if result, err := c.Invoke(t.Context(), domain.RegisteredProxyAttestationV2{}, ports.AttemptOperationCapabilityV1{}, nil); !errors.Is(err, domain.ErrMembershipDenied) || result.SendAttempted || len(result.Body) != 0 {
			t.Fatal("unconstructed connection invoked")
		}
	}
	if copyOwner.Close() == nil || copyConnection.Close() == nil {
		t.Fatal("copied owner closed original resources")
	}
	for _, value := range []any{nilKeeper, owner, copyOwner, nilConnection, connectionOwner, copyConnection} {
		if text := fmt.Sprintf("%v %#v", value, value); !strings.Contains(text, "[redacted]") {
			t.Fatal("private debug form exposed")
		}
	}
}
