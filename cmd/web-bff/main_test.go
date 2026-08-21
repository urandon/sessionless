package main

import "testing"

func TestWebBFFPrefersPlatformPort(t *testing.T) {
	t.Setenv("PORT", "8080")
	t.Setenv("WEB_PORT", "8083")
	if got := webListenAddress(); got != ":8080" {
		t.Fatalf("web BFF address = %q, want :8080", got)
	}
}

func TestWebBFFPortDefaultsAwayFromControlAPI(t *testing.T) {
	t.Setenv("PORT", "")
	t.Setenv("WEB_PORT", "")
	if got := webListenAddress(); got != ":8083" {
		t.Fatalf("web BFF default address = %q, want :8083", got)
	}
}

func TestWebBFFUsesDedicatedLocalPortVariable(t *testing.T) {
	t.Setenv("PORT", "")
	t.Setenv("WEB_PORT", "8084")
	if got := webListenAddress(); got != ":8084" {
		t.Fatalf("web BFF address = %q, want :8084", got)
	}
}
