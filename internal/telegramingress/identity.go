// Package telegramingress translates Telegram webhook updates into
// tenant-scoped, harness-neutral runs.
package telegramingress

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const identityDigestBytes = 20

var identityEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

type IdentityResolver struct {
	key []byte
}

type Identity struct {
	Tenant                 domain.TenantID
	Actor                  domain.ActorRef
	Conversation           domain.ConversationRef
	SubscriptionConnection domain.SubscriptionConnectionID
}

func NewIdentityResolver(key []byte) (*IdentityResolver, error) {
	if len(key) < 32 {
		return nil, errors.New("Telegram identity HMAC key must contain at least 32 bytes")
	}
	return &IdentityResolver{key: append([]byte(nil), key...)}, nil
}

// ResolvePrivate creates stable, opaque identifiers without a database-global
// external-ID lookup. Every control-plane replica derives the same mapping.
func (resolver *IdentityResolver) ResolvePrivate(chatID, userID int64, provider string) (Identity, error) {
	if resolver == nil || len(resolver.key) == 0 {
		return Identity{}, errors.New("Telegram identity resolver is not configured")
	}
	if chatID <= 0 {
		return Identity{}, fmt.Errorf("private Telegram chat ID must be positive")
	}
	if userID <= 0 {
		return Identity{}, fmt.Errorf("Telegram user ID must be positive")
	}
	if err := domain.ValidateOpaqueID("provider", provider); err != nil {
		return Identity{}, err
	}
	tenantID := domain.TenantID(resolver.id("ten_", "private-chat", strconv.FormatInt(chatID, 10)))
	actorID := domain.ActorID(resolver.id(
		"act_", "chat-user",
		strconv.FormatInt(chatID, 10)+":"+strconv.FormatInt(userID, 10),
	))
	conversationID := domain.ConversationID(resolver.id("con_", "chat", strconv.FormatInt(chatID, 10)))
	subscriptionID := domain.SubscriptionConnectionID(
		resolver.id(
			"sub_", "chat-subscription",
			strconv.FormatInt(chatID, 10)+":"+strconv.FormatInt(userID, 10)+":"+provider,
		),
	)
	chat := domain.TelegramChatRef{TenantID: tenantID, ChatID: chatID}
	user := domain.TelegramUserRef{TenantID: tenantID, UserID: userID}
	return Identity{
		Tenant:                 tenantID,
		Actor:                  user.Actor(actorID),
		Conversation:           chat.Conversation(conversationID),
		SubscriptionConnection: subscriptionID,
	}, nil
}

func (resolver *IdentityResolver) id(prefix, kind, external string) string {
	mac := hmac.New(sha256.New, resolver.key)
	_, _ = mac.Write([]byte("sessionless:telegram:v1:" + kind + ":" + external))
	digest := mac.Sum(nil)
	return prefix + strings.ToLower(identityEncoding.EncodeToString(digest[:identityDigestBytes]))
}
