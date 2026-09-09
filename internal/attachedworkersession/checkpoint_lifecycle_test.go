package attachedworkersession

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
)

func TestManifestUpdateAndLogoutRetireReconnectCheckpoint(t *testing.T) {
	t.Run("manifest update", func(t *testing.T) {
		fixture := newSessionFixture(t)
		session := mustReadySession(t, fixture)
		if err := session.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		manifest, err := fixture.store.Load(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		secret, err := fixture.store.LoadSecret(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer clearLocalSecret(&secret)
		next := manifest
		next.Revision++
		next.UpdatedAt = manifest.UpdatedAt.Add(time.Second)
		nextSecret := secret
		nextSecret.ManifestRevision = next.Revision
		nextSecret.IdentityPrivateKey = append([]byte(nil), secret.IdentityPrivateKey...)
		nextSecret.ConnectionSecret = append([]byte(nil), secret.ConnectionSecret...)
		if err := fixture.store.Update(context.Background(), manifest.Revision, next, nextSecret); err != nil {
			t.Fatal(err)
		}
		lease, err := fixture.store.AcquireRuntime(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer lease.Close()
		if _, err := lease.LoadReconnectCheckpoint(context.Background()); !errors.Is(err, attachedworkerlocal.ErrStateMissing) {
			t.Fatalf("manifest update retained checkpoint: %v", err)
		}
	})

	t.Run("logout", func(t *testing.T) {
		fixture := newSessionFixture(t)
		session := mustReadySession(t, fixture)
		if err := session.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		manifest, err := fixture.store.Load(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		result, err := fixture.store.Logout(context.Background(), attachedworkerlocal.LogoutInputV1{
			ExpectedRevision: manifest.Revision,
			RequestID:        "logout-checkpoint-001",
		})
		if err != nil || result.Code != attachedworkerlocal.CodeOK {
			t.Fatalf("logout=%+v err=%v", result, err)
		}
		loaded, err := fixture.store.Load(context.Background())
		if err != nil || loaded.Lifecycle != attachedworkerlocal.LifecycleLoggedOut {
			t.Fatalf("logged-out manifest=%+v err=%v", loaded, err)
		}
	})
}
