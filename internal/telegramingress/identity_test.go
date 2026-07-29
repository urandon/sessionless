package telegramingress

import (
	"strings"
	"testing"
)

func TestIdentityResolverIsStableOpaqueAndChatIsolated(t *testing.T) {
	t.Parallel()
	resolver, err := NewIdentityResolver([]byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	first, err := resolver.ResolvePrivate(1001, 2001, "codex")
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := resolver.ResolvePrivate(1001, 2001, "codex")
	if err != nil {
		t.Fatal(err)
	}
	other, err := resolver.ResolvePrivate(1002, 2001, "codex")
	if err != nil {
		t.Fatal(err)
	}
	if first != repeated {
		t.Fatalf("identity is not deterministic: %#v != %#v", first, repeated)
	}
	if first.Tenant == other.Tenant || first.Conversation.ID == other.Conversation.ID {
		t.Fatal("distinct private chats must not share tenant or conversation IDs")
	}
	if first.Actor.ID == other.Actor.ID ||
		first.SubscriptionConnection == other.SubscriptionConnection {
		t.Fatal("actor and subscription IDs must not be correlatable across tenant chats")
	}
	if strings.Contains(string(first.Tenant), "1001") ||
		strings.Contains(string(first.Actor.ID), "2001") {
		t.Fatal("internal identities must not expose Telegram numeric IDs")
	}
}

func TestIdentityResolverRequiresDeploymentSecret(t *testing.T) {
	t.Parallel()
	if _, err := NewIdentityResolver([]byte("short")); err == nil {
		t.Fatal("short identity key unexpectedly accepted")
	}
}
