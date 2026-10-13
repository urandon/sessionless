package attachedworkerdaemon

import (
	"errors"
	"testing"
	"time"
)

func TestAttachedPiAPIUseDeadlineV1Translation(t *testing.T) {
	// Capture one monotonic reference, then inject only explicit offsets. No
	// second clock sample, wait, scheduling or machine-speed window decides a case.
	anchor := time.Now()
	server := time.Date(2026, time.October, 13, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name       string
		server     time.Time
		window     time.Duration
		roundTrip  time.Duration
		slack      time.Duration
		localLimit time.Duration
		want       time.Duration
	}{
		{name: "full_round_trip_and_slack", server: server, window: 10 * time.Second, roundTrip: 2 * time.Second, slack: time.Second, localLimit: 20 * time.Second, want: 7 * time.Second},
		{name: "zero_round_trip", server: server, window: 10 * time.Second, slack: time.Second, localLimit: 20 * time.Second, want: 9 * time.Second},
		{name: "one_nanosecond_remaining", server: server, window: 3 * time.Nanosecond, roundTrip: time.Nanosecond, slack: time.Nanosecond, localLimit: time.Second, want: time.Nanosecond},
		{name: "earlier_caller_bound", server: server, window: 10 * time.Second, roundTrip: 2 * time.Second, slack: time.Second, localLimit: 2 * time.Second, want: 2 * time.Second},
		{name: "earlier_run_bound", server: server, window: 10 * time.Second, roundTrip: 2 * time.Second, slack: time.Second, localLimit: 3 * time.Second, want: 3 * time.Second},
		{name: "earlier_process_bound", server: server, window: 10 * time.Second, roundTrip: 2 * time.Second, slack: time.Second, localLimit: 4 * time.Second, want: 4 * time.Second},
		{name: "equal_local_bound", server: server, window: 10 * time.Second, roundTrip: 2 * time.Second, slack: time.Second, localLimit: 7 * time.Second, want: 7 * time.Second},
		{name: "server_centuries_behind", server: time.Date(1700, time.January, 1, 0, 0, 0, 0, time.UTC), window: 10 * time.Second, roundTrip: 2 * time.Second, slack: time.Second, localLimit: 20 * time.Second, want: 7 * time.Second},
		{name: "server_centuries_ahead", server: time.Date(2400, time.January, 1, 0, 0, 0, 0, time.UTC), window: 10 * time.Second, roundTrip: 2 * time.Second, slack: time.Second, localLimit: 20 * time.Second, want: 7 * time.Second},
		{name: "server_zone_is_not_local_authority", server: server.In(time.FixedZone("server", 11*60*60)), window: 10 * time.Second, roundTrip: 2 * time.Second, slack: time.Second, localLimit: 20 * time.Second, want: 7 * time.Second},
		{name: "server_monotonic_component_is_not_parent_authority", server: anchor, window: 10 * time.Second, roundTrip: 2 * time.Second, slack: time.Second, localLimit: 20 * time.Second, want: 7 * time.Second},
		{name: "maximum_exact_duration_clamps_before_add", server: server, window: time.Duration(1<<63 - 1), roundTrip: 2 * time.Nanosecond, slack: time.Nanosecond, localLimit: 2 * time.Second, want: 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receivedAt := anchor.Add(tc.roundTrip)
			localDeadline := receivedAt.Add(tc.localLimit)
			got, err := attachedPiAPIUseDeadlineV1(tc.server, tc.server.Add(tc.window), anchor, receivedAt, localDeadline, tc.slack)
			want := receivedAt.Add(tc.want)
			if err != nil || !got.Equal(want) {
				t.Fatalf("window=%v RTT=%v slack=%v local=%v: got remaining=%v err=%v; want=%v", tc.window, tc.roundTrip, tc.slack, tc.localLimit, got.Sub(receivedAt), err, tc.want)
			}
			if got == got.Round(0) {
				t.Error("translated deadline lost the original parent's monotonic component")
			}
			if got.After(localDeadline) {
				t.Error("translated deadline extends the earlier original local bound")
			}
			// Delayed use consumes the same deadline; it is never reanchored.
			if receivedAt.Add(tc.want).Before(got) || receivedAt.Add(tc.want+time.Nanosecond).Before(got) {
				t.Error("authority remains future at or after its original deadline")
			}
		})
	}
}

func TestAttachedPiAPIUseDeadlineV1Denials(t *testing.T) {
	anchor := time.Now()
	server := time.Date(2026, time.October, 13, 0, 0, 0, 0, time.UTC)
	receivedAt := anchor.Add(2 * time.Second)
	localDeadline := receivedAt.Add(20 * time.Second)
	maxDuration := time.Duration(1<<63 - 1)
	for _, tc := range []struct {
		name       string
		checkedAt  time.Time
		expiresAt  time.Time
		sentAt     time.Time
		receivedAt time.Time
		localEnd   time.Time
		slack      time.Duration
	}{
		{name: "zero_checked_at", expiresAt: server.Add(10 * time.Second), sentAt: anchor, receivedAt: receivedAt, localEnd: localDeadline, slack: time.Second},
		{name: "zero_use_expiry", checkedAt: server, sentAt: anchor, receivedAt: receivedAt, localEnd: localDeadline, slack: time.Second},
		{name: "equal_server_times", checkedAt: server, expiresAt: server, sentAt: anchor, receivedAt: receivedAt, localEnd: localDeadline, slack: time.Second},
		{name: "reversed_server_times", checkedAt: server, expiresAt: server.Add(-time.Nanosecond), sentAt: anchor, receivedAt: receivedAt, localEnd: localDeadline, slack: time.Second},
		{name: "server_duration_saturation", checkedAt: server, expiresAt: server.Add(maxDuration).Add(time.Nanosecond), sentAt: anchor, receivedAt: receivedAt, localEnd: localDeadline, slack: time.Second},
		{name: "huge_server_duration", checkedAt: server, expiresAt: time.Date(1000000, time.January, 1, 0, 0, 0, 0, time.UTC), sentAt: anchor, receivedAt: receivedAt, localEnd: localDeadline, slack: time.Second},
		{name: "zero_send_sample", checkedAt: server, expiresAt: server.Add(10 * time.Second), receivedAt: receivedAt, localEnd: localDeadline, slack: time.Second},
		{name: "zero_receive_sample", checkedAt: server, expiresAt: server.Add(10 * time.Second), sentAt: anchor, localEnd: localDeadline, slack: time.Second},
		{name: "zero_required_local_bound", checkedAt: server, expiresAt: server.Add(10 * time.Second), sentAt: anchor, receivedAt: receivedAt, slack: time.Second},
		{name: "wall_only_send", checkedAt: server, expiresAt: server.Add(10 * time.Second), sentAt: anchor.Round(0), receivedAt: receivedAt, localEnd: localDeadline, slack: time.Second},
		{name: "wall_only_receive", checkedAt: server, expiresAt: server.Add(10 * time.Second), sentAt: anchor, receivedAt: receivedAt.Round(0), localEnd: localDeadline, slack: time.Second},
		{name: "wall_only_local_bound", checkedAt: server, expiresAt: server.Add(10 * time.Second), sentAt: anchor, receivedAt: receivedAt, localEnd: localDeadline.Round(0), slack: time.Second},
		{name: "local_add_loses_monotonic_component", checkedAt: server, expiresAt: server.Add(10 * time.Second), sentAt: anchor, receivedAt: receivedAt, localEnd: receivedAt.Add(maxDuration), slack: time.Second},
		{name: "regressed_round_trip", checkedAt: server, expiresAt: server.Add(10 * time.Second), sentAt: anchor, receivedAt: anchor.Add(-time.Nanosecond), localEnd: localDeadline, slack: time.Second},
		{name: "zero_slack", checkedAt: server, expiresAt: server.Add(10 * time.Second), sentAt: anchor, receivedAt: receivedAt, localEnd: localDeadline},
		{name: "negative_slack", checkedAt: server, expiresAt: server.Add(10 * time.Second), sentAt: anchor, receivedAt: receivedAt, localEnd: localDeadline, slack: -time.Nanosecond},
		{name: "round_trip_exhausts_window", checkedAt: server, expiresAt: server.Add(2 * time.Second), sentAt: anchor, receivedAt: receivedAt, localEnd: localDeadline, slack: time.Second},
		{name: "round_trip_exceeds_window", checkedAt: server, expiresAt: server.Add(time.Second), sentAt: anchor, receivedAt: receivedAt, localEnd: localDeadline, slack: time.Second},
		{name: "slack_exhausts_remaining", checkedAt: server, expiresAt: server.Add(3 * time.Second), sentAt: anchor, receivedAt: receivedAt, localEnd: localDeadline, slack: time.Second},
		{name: "slack_exceeds_remaining", checkedAt: server, expiresAt: server.Add(3 * time.Second), sentAt: anchor, receivedAt: receivedAt, localEnd: localDeadline, slack: 2 * time.Second},
		{name: "overflowing_combined_deductions", checkedAt: server, expiresAt: server.Add(maxDuration), sentAt: anchor, receivedAt: receivedAt, localEnd: localDeadline, slack: maxDuration},
		{name: "local_deadline_equal_receive", checkedAt: server, expiresAt: server.Add(10 * time.Second), sentAt: anchor, receivedAt: receivedAt, localEnd: receivedAt, slack: time.Second},
		{name: "local_deadline_before_receive", checkedAt: server, expiresAt: server.Add(10 * time.Second), sentAt: anchor, receivedAt: receivedAt, localEnd: receivedAt.Add(-time.Nanosecond), slack: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := attachedPiAPIUseDeadlineV1(tc.checkedAt, tc.expiresAt, tc.sentAt, tc.receivedAt, tc.localEnd, tc.slack)
			if !errors.Is(err, errAttachedPiAPIUseDeadlineInvalid) || !got.IsZero() {
				t.Errorf("server interval=%v local RTT=%v slack=%v: got deadline=%v err=%v; want zero deadline and invalid error", tc.expiresAt.Sub(tc.checkedAt), tc.receivedAt.Sub(tc.sentAt), tc.slack, got, err)
			}
		})
	}
}
