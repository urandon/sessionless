package ydbstore_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Same finite-engine entry without any credential provider/materializer. Actual
// owned loopback HTTP, Source, Store and keeper; SQL/storage/funding fixtures,
// not native YDB/S3, a live model, or a Web/cloud acceptance result.
func TestLocalSessionNoauthWholeRunAndDuplicateCannotCloseTerminal(t *testing.T) {
	peer := &sessionCompactionPeer{modelText: "whole noauth original answer"}
	server := httptest.NewUnstartedServer(peer)
	server.Config.ReadHeaderTimeout = 10 * time.Second
	server.Config.ReadTimeout = 20 * time.Second
	server.Config.WriteTimeout = 20 * time.Second
	server.Config.MaxHeaderBytes = 16 << 10
	t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
	server.Start()
	blobs := &cleanupSessionBlobs{terminalSessionBlobs: &terminalSessionBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}}
	seed := ydbstore.NewLocalTerminalPublicationSessionConnectedSeed(t, server.URL+"/v1", blobs)
	fixture := seed.InstallTerminalCompletionSQL(t)
	blobs.seed = seed
	session, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error(err)
		}
	})
	result, terminal, err := session.Run(t.Context(), seed.Request)
	if err != nil || terminal == nil || !result.Model.SendAttempted || result.Tool != nil || result.Compaction != nil {
		t.Fatal("original noauth whole engine", err)
	}
	deadline, err := terminal.Deadline()
	if err != nil {
		t.Fatal(err)
	}
	before := seed.Stats()
	copySession := *session
	if _, next, err := copySession.Run(t.Context(), seed.Request); err == nil || next != nil {
		t.Fatal("duplicate whole entry dispatched again")
	}
	if seed.Stats() != before {
		t.Fatal("duplicate whole entry performed original physical effects")
	}
	if _, err := seed.Source.Configuration(); err != nil {
		t.Fatal("denied duplicate stopped the borrowed original Source", err)
	}
	if got, err := terminal.Deadline(); err != nil || got != deadline {
		t.Fatal("denied duplicate damaged original terminal owner", err)
	}
	if _, err := terminal.PrepareOutput(t.Context()); err != nil {
		t.Fatal("original terminal remained usable", err)
	}
	if err := terminal.PublishOutput(t.Context()); err != nil {
		t.Fatal(err)
	}
	cleanup, err := terminal.FinishCleanup(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	fixture.ObserveCompletion()
	if err := cleanup.Complete(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := fixture.Snapshot(t)
	if got.Run.Status != domain.RunSucceeded || len(got.Events) != 1 || got.Events[0].Kind != domain.SessionEventAssistantMessage {
		t.Fatal("noauth whole run did not commit canonical terminal")
	}
	if err := cleanup.Complete(t.Context()); err != nil {
		t.Fatal("exact noauth terminal replay", err)
	}
}
