package main

import (
	"strings"
	"testing"
	"time"
)

func TestMigrationTimeout(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  time.Duration
	}{
		{name: "default", want: 2 * time.Minute},
		{name: "bounded short operation", value: "30s", want: 30 * time.Second},
		{name: "explicit default", value: "2m", want: 2 * time.Minute},
		{name: "cloud baseline maximum", value: "20m", want: 20 * time.Minute},
		{name: "equivalent maximum", value: "1200s", want: 20 * time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := migrationTimeout(test.value)
			if err != nil || got != test.want {
				t.Fatalf("migrationTimeout(%q) = %v, %v; want %v, nil", test.value, got, err, test.want)
			}
		})
	}
}

func TestMigrationTimeoutRejectsInvalidBounds(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "zero", value: "0"},
		{name: "zero duration", value: "0s"},
		{name: "negative", value: "-1m"},
		{name: "malformed", value: "later"},
		{name: "missing unit", value: "20"},
		{name: "whitespace", value: " "},
		{name: "surrounding whitespace", value: " 20m "},
		{name: "above maximum", value: "21m"},
		{name: "just above maximum", value: "20m1ns"},
		{name: "duration overflow", value: "999999999999999999h"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := migrationTimeout(test.value)
			if err == nil || got != 0 || !strings.Contains(err.Error(), "SCHEMA_MIGRATION_TIMEOUT") {
				t.Fatalf("migrationTimeout(%q) = %v, %v; want zero and named configuration error", test.value, got, err)
			}
		})
	}
}

func TestRunValidatesMigrationTimeoutBeforeConnection(t *testing.T) {
	t.Setenv("SCHEMA_MIGRATION_TIMEOUT", "21m")
	t.Setenv("YDB_CONNECTION_STRING", "")
	if err := run(); err == nil || !strings.Contains(err.Error(), "SCHEMA_MIGRATION_TIMEOUT") {
		t.Fatalf("run() error=%v, want timeout configuration rejection before connection setup", err)
	}
}

func TestMigrationLockTTL(t *testing.T) {
	for _, test := range []struct {
		name    string
		timeout time.Duration
		want    time.Duration
	}{
		{name: "default timeout preserves lease", timeout: 2 * time.Minute, want: 5 * time.Minute},
		{name: "one minute margin at minimum lease", timeout: 4 * time.Minute, want: 5 * time.Minute},
		{name: "five minute request needs longer lease", timeout: 5 * time.Minute, want: 6 * time.Minute},
		{name: "maximum cloud request", timeout: 20 * time.Minute, want: 21 * time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := migrationLockTTL(test.timeout)
			if got != test.want || got <= test.timeout {
				t.Fatalf("migrationLockTTL(%v) = %v, want %v and strictly greater than request", test.timeout, got, test.want)
			}
		})
	}
}
