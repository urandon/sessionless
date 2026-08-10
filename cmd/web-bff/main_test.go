package main

import "testing"

func TestWebBFFUsesDedicatedPortVariable(t *testing.T) {
	t.Setenv("PORT", "8080")
	t.Setenv("WEB_PORT", "8083")
	if got := webListenAddress(); got != ":8083" {
		t.Fatalf("web BFF address = %q, want :8083", got)
	}
}

func TestWebBFFPortDefaultsAwayFromControlAPI(t *testing.T) {
	t.Setenv("WEB_PORT", "")
	if got := webListenAddress(); got != ":8083" {
		t.Fatalf("web BFF default address = %q, want :8083", got)
	}
}
