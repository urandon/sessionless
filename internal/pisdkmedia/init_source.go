package pisdkmedia

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
)

// No wildcard SDK/bootstrap/test imports, native registration or activation.
// The actual runner artifact must bind this module plus its existing relative
// request/policy/response dependencies before its independent native validation.
//
//go:embed native/init.mjs
var initModule embed.FS

func InitSource() ([]byte, string) {
	b, err := initModule.ReadFile("native/init.mjs")
	if err != nil {
		panic("selected Pi Init asset missing")
	}
	s := sha256.Sum256(b)
	return b, hex.EncodeToString(s[:])
}
