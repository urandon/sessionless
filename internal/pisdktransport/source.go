// Package pisdktransport retains private Pi physical-transport assets.
// It does not register a driver, authorize a send or qualify a native profile.
package pisdktransport

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
)

//go:embed framing.mjs response.mjs
var modules embed.FS

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

func moduleSource(name string) ([]byte, string) {
	data, err := modules.ReadFile(name)
	if err != nil {
		panic("embedded Pi transport asset missing")
	}
	digest := sha256.Sum256(data)
	return data, hex.EncodeToString(digest[:])
}
