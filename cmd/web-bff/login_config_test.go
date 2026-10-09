package main

import (
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"testing"
)

func TestYandexLoginConfigurationNeedsNoTelegram(t *testing.T) {
	env := map[string]string{"WEB_LOGIN_PROVIDER": "yandex", "YANDEX_LOGIN_CLIENT_ID": "client-test", "YANDEX_LOGIN_CLIENT_SECRET": "test-secret"}
	get := func(name string) string { return env[name] }
	c, err := webLoginFromEnvironment(get, "https://web.example.invalid", false)
	if err != nil || c.oauth == nil || c.oidc != nil || c.redirectURI != "https://web.example.invalid"+webcontract.RouteLoginCallback {
		t.Fatalf("yandex configuration incorrect: err=%v", err)
	}
	delete(env, "YANDEX_LOGIN_CLIENT_SECRET")
	env["TELEGRAM_OIDC_CLIENT_ID"] = "123456"
	env["TELEGRAM_OIDC_CLIENT_SECRET"] = "telegram-test-secret"
	if _, err := webLoginFromEnvironment(get, "https://web.example.invalid", false); err == nil {
		t.Fatal("missing Yandex secret silently fell back to Telegram")
	}
}

func TestYandexConfigurationRejectsNonLocalEndpointOverrides(t *testing.T) {
	env := map[string]string{"WEB_LOGIN_PROVIDER": "yandex", "YANDEX_LOGIN_CLIENT_ID": "client-test", "YANDEX_LOGIN_CLIENT_SECRET": "test-secret", "YANDEX_LOGIN_TOKEN_ENDPOINT": "http://127.0.0.1:8000/token"}
	get := func(name string) string { return env[name] }
	if _, err := webLoginFromEnvironment(get, "https://web.example.invalid", false); err == nil {
		t.Fatal("cloud accepted fixture token endpoint")
	}
	if _, err := webLoginFromEnvironment(get, "https://web.example.invalid", true); err != nil {
		t.Fatalf("explicit local fixture rejected: %v", err)
	}
	env["WEB_LOGIN_PROVIDER"] = "browser-selected"
	if _, err := webLoginFromEnvironment(get, "https://web.example.invalid", true); err == nil {
		t.Fatal("unknown provider accepted")
	}
}
