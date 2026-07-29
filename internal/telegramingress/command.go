package telegramingress

import (
	"strings"

	"gitcode.com/urandon/sessionless/internal/ports"
)

// parseCommand recognizes the public Telegram command surface. Any
// slash-prefixed text is treated as a command so an unsupported command cannot
// accidentally become an AI workload.
func parseCommand(text string) (ports.TelegramCommandKind, bool) {
	fields := strings.Fields(strings.TrimSpace(text))
	if len(fields) == 0 || !strings.HasPrefix(fields[0], "/") {
		return "", false
	}
	name := strings.ToLower(strings.SplitN(fields[0], "@", 2)[0])
	lower := make([]string, len(fields))
	for index, field := range fields {
		lower[index] = strings.ToLower(field)
	}
	switch {
	case name == "/connect" && len(lower) == 2 && lower[1] == "codex":
		return ports.TelegramCommandConnectCodex, true
	case name == "/compute" && len(lower) == 2 && lower[1] == "status":
		return ports.TelegramCommandComputeStatus, true
	case name == "/compute" && len(lower) == 3 &&
		lower[1] == "disconnect" && lower[2] == "codex":
		return ports.TelegramCommandDisconnectCodex, true
	case name == "/new" && len(lower) == 1:
		return ports.TelegramCommandNewContext, true
	default:
		return ports.TelegramCommandHelp, true
	}
}
