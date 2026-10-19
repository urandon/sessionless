package main

import (
	"context"
	"io"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestRetainedDeletionConfigurationIsExplicit(t *testing.T) {
	for _, test := range []struct {
		raw     string
		enabled bool
		invalid bool
	}{
		{raw: ""},
		{raw: "false"},
		{raw: "0"},
		{raw: "true", enabled: true},
		{raw: "1", enabled: true},
		{raw: "TRUE", enabled: true},
		{raw: "yes", invalid: true},
		{raw: "auto", invalid: true},
		{raw: " ", invalid: true},
		{raw: " true ", invalid: true},
	} {
		t.Run("value_"+test.raw, func(t *testing.T) {
			got, err := retainedDeletionEnabled(test.raw)
			if got != test.enabled || (err != nil) != test.invalid {
				t.Fatalf("raw=%q enabled=%t err=%v, want enabled=%t invalid=%t", test.raw, got, err, test.enabled, test.invalid)
			}
		})
	}
}

type configuredDeletionReader struct{}

func (*configuredDeletionReader) Open(context.Context, domain.TenantID, domain.BlobRef) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("controlled immutable bytes")), nil
}

func TestRetainedDeletionPinsOnlyConfiguredReader(t *testing.T) {
	reader := &configuredDeletionReader{}
	for _, enabled := range []bool{false, true} {
		options, err := retainedDeletionOptions(enabled, reader)
		if err != nil {
			t.Fatalf("constructor options enabled=%t: %v", enabled, err)
		}
		if enabled {
			if options.SessionDeletionBlobReader != reader {
				t.Fatal("execution Store does not receive the exact configured reader")
			}
		} else if options.SessionDeletionBlobReader != nil {
			t.Fatal("disabled default silently installs retained verification")
		}
	}
	if _, err := retainedDeletionOptions(true, nil); err == nil {
		t.Fatal("enabled configuration accepted a missing reader")
	}
	if options, err := retainedDeletionOptions(false, nil); err != nil || options.SessionDeletionBlobReader != nil {
		t.Fatalf("disabled legacy path requires a retained reader: options=%+v err=%v", options, err)
	}
}
