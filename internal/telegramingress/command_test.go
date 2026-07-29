package telegramingress

import (
	"testing"

	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestParseCommand(t *testing.T) {
	t.Parallel()
	tests := []struct {
		text    string
		want    ports.TelegramCommandKind
		command bool
	}{
		{text: "hello", command: false},
		{text: "/connect codex", want: ports.TelegramCommandConnectCodex, command: true},
		{text: "/CONNECT CODEX", want: ports.TelegramCommandConnectCodex, command: true},
		{text: "/compute@sessionless_bot status", want: ports.TelegramCommandComputeStatus, command: true},
		{text: "/compute disconnect codex", want: ports.TelegramCommandDisconnectCodex, command: true},
		{text: "/new", want: ports.TelegramCommandNewContext, command: true},
		{text: "/new please", want: ports.TelegramCommandHelp, command: true},
		{text: "/unknown", want: ports.TelegramCommandHelp, command: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.text, func(t *testing.T) {
			t.Parallel()
			got, command := parseCommand(test.text)
			if got != test.want || command != test.command {
				t.Fatalf("parseCommand(%q) = %q, %t; want %q, %t",
					test.text, got, command, test.want, test.command)
			}
		})
	}
}
