// Package pisdkbootstrap retains the private Pi SDK construction helper asset.
// It is not a driver registration, executable profile, or execution authority.
package pisdkbootstrap

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
)

//go:embed resources.mjs
var resources embed.FS

// ResourcesSource returns a caller-owned copy of the helper module. The actual
// prepared supervisor must verify the full qualified SDK/profile and confinement
// before importing the SDK or using these construction helpers.
func ResourcesSource() ([]byte, string) {
	data, err := resources.ReadFile("resources.mjs")
	if err != nil {
		panic("embedded Pi bootstrap asset missing")
	}
	digest := sha256.Sum256(data)
	return data, hex.EncodeToString(digest[:])
}
