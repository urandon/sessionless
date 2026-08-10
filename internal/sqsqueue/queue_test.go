package sqsqueue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewValidatesConfigurationBeforeLoadingCredentials(t *testing.T) {
	_, err := New(context.Background(), Config{})
	if err == nil || err.Error() != "queue region is required" {
		t.Fatalf("error = %v", err)
	}
}

func TestRetryDelayBounds(t *testing.T) {
	queue := &Queue{}
	err := queue.Retry(context.Background(), "receipt", 13*time.Hour)
	if err == nil {
		t.Fatal("expected retry delay validation error")
	}
}

func TestRetryVisibilityRoundsPositiveSubsecondDelayUp(t *testing.T) {
	tests := []struct {
		delay time.Duration
		want  int32
	}{
		{delay: 0, want: 0},
		{delay: time.Nanosecond, want: 1},
		{delay: 250 * time.Millisecond, want: 1},
		{delay: time.Second, want: 1},
		{delay: time.Second + time.Nanosecond, want: 2},
	}
	for _, test := range tests {
		if got := retryVisibilitySeconds(test.delay); got != test.want {
			t.Fatalf("retryVisibilitySeconds(%s) = %d, want %d", test.delay, got, test.want)
		}
	}
}

func TestNoMessageSentinel(t *testing.T) {
	if !errors.Is(ErrNoMessage, ErrNoMessage) {
		t.Fatal("ErrNoMessage must be usable with errors.Is")
	}
}
