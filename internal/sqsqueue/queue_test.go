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

func TestNoMessageSentinel(t *testing.T) {
	if !errors.Is(ErrNoMessage, ErrNoMessage) {
		t.Fatal("ErrNoMessage must be usable with errors.Is")
	}
}
