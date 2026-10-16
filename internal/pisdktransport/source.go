// Package pisdktransport retains private Pi physical-transport assets.
// It does not register a driver, authorize a send or qualify a native profile.
package pisdktransport

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
)

//go:embed framing.mjs response.mjs policy.mjs request.mjs stdio.mjs authentication.mjs http_stdio.mjs
var modules embed.FS

// AuthenticationSource is SPU3 byte encoding DATA only. It is deliberately not
// imported by the existing SPU2 request owner or selected as a runnable driver.
func AuthenticationSource() ([]byte, string) { return moduleSource("authentication.mjs") }

// FramingSource returns independent bytes and their digest for the trusted
// supervisor's asset binding. This byte framer is not an effect/frame policy,
// allocation certificate, physical fetch guard or complete SDK adapter.
func FramingSource() ([]byte, string) {
	return moduleSource("framing.mjs")
}

// ResponseSource returns independent bytes and digest of the guarded-response
// plumbing. Its trusted owner must supply the same-fetch abort capability,
// original deadline signal and closed semantic policy; this asset proves none
// of those authorities and does not initiate HTTP or register a driver.
func ResponseSource() ([]byte, string) {
	return moduleSource("response.mjs")
}

// PolicySource returns independent bytes and digest of the closed Responses
// frame policy. The trusted owner supplies admitted limits and request echoes;
// this asset does not prove their provenance, authorize a send or register a
// driver. Stage all bound modules together for their relative imports.
func PolicySource() ([]byte, string) {
	return moduleSource("policy.mjs")
}

// RequestSource returns independent bytes and digest of the private one-send
// request owner. Its injected parent capability is not current-owner authority;
// this asset neither registers a driver nor supplies the original admitted
// request, deadline, source authorization or complete native allocation ledger.
func RequestSource() ([]byte, string) {
	return moduleSource("request.mjs")
}

// StdioSource returns the private child SPU2 module, not the test process
// fixture, SDK bootstrap, source grant or full native/clock fit certificate.
func StdioSource() ([]byte, string) {
	return moduleSource("stdio.mjs")
}

// HTTPStdioSource is a separate SPU3 child asset, not a SPU2 upgrade or a
// source-selected native artifact. Its original owner remains default-off.
func HTTPStdioSource() ([]byte, string) { return moduleSource("http_stdio.mjs") }

func moduleSource(name string) ([]byte, string) {
	data, err := modules.ReadFile(name)
	if err != nil {
		panic("embedded Pi transport asset missing")
	}
	digest := sha256.Sum256(data)
	return data, hex.EncodeToString(digest[:])
}
