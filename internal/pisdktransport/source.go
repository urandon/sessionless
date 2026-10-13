// Package pisdktransport retains private Pi physical-transport assets.
// It does not register a driver, authorize a send or qualify a native profile.
package pisdktransport

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
)

//go:embed framing.mjs
var framing embed.FS

// FramingSource returns independent bytes and their digest for the trusted
// supervisor's asset binding. This byte framer is not an effect/frame policy,
// allocation certificate, physical fetch guard or complete SDK adapter.
func FramingSource() ([]byte, string) {
	data, err := framing.ReadFile("framing.mjs")
	if err != nil {
		panic("embedded Pi framing asset missing")
	}
	digest := sha256.Sum256(data)
	return data, hex.EncodeToString(digest[:])
}
