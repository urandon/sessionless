package pisdkmedia

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"

	"gitcode.com/urandon/sessionless/internal/pisdkbootstrap"
	"gitcode.com/urandon/sessionless/internal/pisdktransport"
)

// Exactly selected private runner/test assets, not a wildcard directory or
// driver registration. The genuine prepared owner must bind every digest and
// the original compiled SDK/platform before constructing/launching the child.
//
//go:embed native/bootstrap.mjs native/credential_file.mjs native/entry.mjs native/request_contract.mjs native/http_entry.mjs
var nativeAssets embed.FS

type nativeAsset struct {
	path   string
	body   []byte
	digest string
}

// Artifact layout preserves all accepted relative imports. These are code
// assets only: no profile, credentials, authority, timer or source grant.
func fixedNativeAssets() []nativeAsset {
	var out []nativeAsset
	for _, name := range []string{"bootstrap.mjs", "credential_file.mjs", "entry.mjs", "request_contract.mjs"} {
		body, err := nativeAssets.ReadFile("native/" + name)
		if err != nil {
			panic("selected embedded native asset missing")
		}
		digest := sha256.Sum256(body)
		out = append(out, nativeAsset{path: "internal/pisdkmedia/native/" + name, body: body, digest: hex.EncodeToString(digest[:])})
	}
	for _, source := range []struct {
		path string
		read func() ([]byte, string)
	}{
		{"internal/pisdkmedia/native/init.mjs", InitSource},
		{"internal/pisdkbootstrap/resources.mjs", pisdkbootstrap.ResourcesSource},
		{"internal/pisdktransport/stdio.mjs", pisdktransport.StdioSource},
		{"internal/pisdktransport/framing.mjs", pisdktransport.FramingSource},
		{"internal/pisdktransport/response.mjs", pisdktransport.ResponseSource},
		{"internal/pisdktransport/policy.mjs", pisdktransport.PolicySource},
		{"internal/pisdktransport/request.mjs", pisdktransport.RequestSource},
	} {
		body, digest := source.read()
		out = append(out, nativeAsset{path: source.path, body: body, digest: digest})
	}
	return out
}

// Explicit separate asset closure. No old SPU2 artifact/profile is silently
// promoted, and no source factory/registration selects this unqualified entry.
func fixedHTTPNativeAssets() []nativeAsset {
	out := fixedNativeAssets()
	body, err := nativeAssets.ReadFile("native/http_entry.mjs")
	if err != nil {
		panic("selected embedded HTTP entry missing")
	}
	digest := sha256.Sum256(body)
	out = append(out, nativeAsset{path: "internal/pisdkmedia/native/http_entry.mjs", body: body, digest: hex.EncodeToString(digest[:])})
	for _, source := range []struct {
		path string
		read func() ([]byte, string)
	}{
		{"internal/pisdktransport/http_stdio.mjs", pisdktransport.HTTPStdioSource},
		{"internal/pisdktransport/authentication.mjs", pisdktransport.AuthenticationSource},
	} {
		body, digest := source.read()
		out = append(out, nativeAsset{path: source.path, body: body, digest: digest})
	}
	return out
}
