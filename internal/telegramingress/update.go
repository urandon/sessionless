package telegramingress

import (
	"fmt"
	"strings"
)

type Update struct {
	UpdateID int64    `json:"update_id"`
	Message  *Message `json:"message,omitempty"`
}

type Message struct {
	MessageID int64       `json:"message_id"`
	From      User        `json:"from"`
	Chat      Chat        `json:"chat"`
	Date      int64       `json:"date"`
	Text      string      `json:"text,omitempty"`
	Caption   string      `json:"caption,omitempty"`
	Photo     []PhotoSize `json:"photo,omitempty"`
	Document  *Document   `json:"document,omitempty"`
}

type User struct {
	ID int64 `json:"id"`
}

type Chat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
}

type PhotoSize struct {
	FileID   string `json:"file_id"`
	Width    int    `json:"width"`
	Height   int    `json:"height"`
	FileSize int64  `json:"file_size,omitempty"`
}

type Document struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name,omitempty"`
	MIMEType string `json:"mime_type,omitempty"`
	FileSize int64  `json:"file_size,omitempty"`
}

func (update Update) ValidatePrivate() error {
	if update.UpdateID <= 0 {
		return fmt.Errorf("update_id must be positive")
	}
	if update.Message == nil {
		return fmt.Errorf("only message updates are supported")
	}
	message := update.Message
	if message.MessageID <= 0 {
		return fmt.Errorf("message_id must be positive")
	}
	if message.From.ID <= 0 {
		return fmt.Errorf("message.from.id must be positive")
	}
	if message.Chat.ID <= 0 || message.Chat.Type != "private" {
		return fmt.Errorf("only private Telegram chats are supported")
	}
	if strings.TrimSpace(message.Text) == "" &&
		strings.TrimSpace(message.Caption) == "" &&
		len(message.Photo) == 0 &&
		message.Document == nil {
		return fmt.Errorf("message has no supported content")
	}
	return nil
}
