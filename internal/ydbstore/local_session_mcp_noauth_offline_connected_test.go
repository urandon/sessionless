//go:build sessionless_mcp_offline

package ydbstore_test

import (
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Prefix intentionally belongs to the unchanged additive Make offline gate.
// Actual loopback MODEL HTTP + local TLS MCP, original noauth constructor and
// shared finite engine; SQL/funding/storage/DNS/IP are synthetic, not cloud or
// native YDB/live endpoint qualification. No credential provider is installed.
func TestLocalSessionCredentialedMCPOfflineNoauthSameEngine(t *testing.T) {
	peer := newOfflineModelChainPeer(t)
	blobs := &cleanupSessionBlobs{terminalSessionBlobs: &terminalSessionBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}}
	seed := ydbstore.NewLocalTerminalPublicationSessionConnectedSeed(t, peer.endpoint(), blobs)
	fixture := seed.InstallTerminalCompletionSQL(t)
	blobs.seed = seed
	if seed.Profile.Auth != domain.ConfiguredInferenceAuthNoneV1 || seed.ProviderCredentialReadCount() != 0 {
		t.Fatal("noauth source unexpectedly admitted credential material")
	}
	session, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal("original noauth finite-engine constructor", err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error("joined noauth runtime close", err)
		}
	})
	peer.configure(t, seed.Profile.ModelID)
	grant := seed.MCPOfflineInstalledGrant(t)
	mcp := newOfflineMCPPeer(t, grant.Descriptor, 4, nil, false)
	if err := session.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); err != nil {
		t.Fatal("original inert noauth test-only network", err)
	}
	if err := session.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); err == nil {
		t.Fatal("noauth network instrumentation reset")
	}
	before := seed.Stats()
	result, terminal, err := session.Run(t.Context(), seed.Request)
	if err != nil {
		t.Fatalf("original noauth MODEL1/MCP2/MODEL3 finite engine: %v stats=%+v", err, seed.Stats())
	}
	mcp.wait(t)
	wires := peer.wait(t, false)
	reservations, observed := assertOfflineMCPChain(t, seed.LocalSessionConnectedSeed, blobs.sessionCompactionBlobs, result, terminal, wires, grant, mcp)
	stats := seed.Stats()
	if stats.Parent != before.Parent+1 || stats.Burn != before.Burn+1 || stats.Allocation != before.Allocation+1 || stats.Operation != before.Operation+3 || stats.Call != before.Call+3 || seed.ProviderCredentialReadCount() != 0 {
		t.Fatalf("zero-credential same finite-engine counters: before=%+v after=%+v credentialReads=%d", before, stats, seed.ProviderCredentialReadCount())
	}
	if err := session.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); err == nil {
		t.Fatal("completed original noauth invocation rewired")
	}
	copySession := *session
	again, next, err := copySession.Run(t.Context(), seed.Request)
	if err == nil || next != nil || again.Model.SendAttempted || seed.Stats() != stats || seed.ProviderCredentialReadCount() != 0 {
		t.Fatal("noauth same-engine duplicate dispatched another effect or read credentials")
	}
	finishOfflineMCPChain(t, terminal, seed, fixture, blobs, reservations, observed)
	if seed.ProviderCredentialReadCount() != 0 {
		t.Fatal("noauth publication/completion introduced credential lookup")
	}
}
