// Package telegramfake provides a deterministic, credential-free subset of
// the Telegram Bot API for local development and adapter contract tests.
package telegramfake

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxRequestBytes = 8 << 20

type Capture struct {
	MessageID  int64           `json:"message_id"`
	Method     string          `json:"method"`
	ChatID     int64           `json:"chat_id"`
	Request    json.RawMessage `json:"request"`
	CapturedAt time.Time       `json:"captured_at"`
}

type Server struct {
	token  string
	logger *slog.Logger
	now    func() time.Time

	mu            sync.Mutex
	updates       map[int64]json.RawMessage
	updateOrder   []int64
	captures      []Capture
	nextMessageID int64
}

func NewHandler(token string, logger *slog.Logger) http.Handler {
	server := &Server{
		token:         token,
		logger:        logger,
		now:           time.Now,
		updates:       make(map[int64]json.RawMessage),
		nextMessageID: 1000,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.health)
	mux.HandleFunc("POST /test/updates", server.addUpdate)
	mux.HandleFunc("GET /test/captures", server.listCaptures)
	mux.HandleFunc("POST /test/reset", server.reset)
	mux.HandleFunc("/", server.botAPI)
	return mux
}

func (server *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (server *Server) addUpdate(w http.ResponseWriter, request *http.Request) {
	raw, err := readBody(request)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	var header struct {
		UpdateID int64 `json:"update_id"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode update: %w", err))
		return
	}
	if header.UpdateID <= 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("update_id must be positive"))
		return
	}

	server.mu.Lock()
	if _, exists := server.updates[header.UpdateID]; !exists {
		server.updateOrder = append(server.updateOrder, header.UpdateID)
	}
	server.updates[header.UpdateID] = append(json.RawMessage(nil), raw...)
	server.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "update_id": header.UpdateID})
}

func (server *Server) listCaptures(w http.ResponseWriter, _ *http.Request) {
	server.mu.Lock()
	captures := append([]Capture(nil), server.captures...)
	server.mu.Unlock()
	writeTelegramResult(w, captures)
}

func (server *Server) reset(w http.ResponseWriter, _ *http.Request) {
	server.mu.Lock()
	server.updates = make(map[int64]json.RawMessage)
	server.updateOrder = nil
	server.captures = nil
	server.nextMessageID = 1000
	server.mu.Unlock()
	writeTelegramResult(w, true)
}

func (server *Server) botAPI(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost && request.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	prefix := "/bot" + server.token + "/"
	if server.token == "" || !strings.HasPrefix(request.URL.Path, prefix) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"ok":          false,
			"error_code":  401,
			"description": "Unauthorized",
		})
		return
	}

	method := strings.TrimPrefix(request.URL.Path, prefix)
	switch method {
	case "getMe":
		writeTelegramResult(w, map[string]any{
			"id": 900000001, "is_bot": true, "first_name": "Sessionless Local",
			"username": "sessionless_local_bot",
		})
	case "getUpdates":
		server.getUpdates(w, request)
	case "sendMessage", "sendDocument", "sendPhoto":
		server.captureSend(w, request, method)
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{
			"ok":          false,
			"error_code":  404,
			"description": "Method not implemented by telegram-fake",
		})
	}
}

func (server *Server) getUpdates(w http.ResponseWriter, request *http.Request) {
	offset, _ := strconv.ParseInt(request.URL.Query().Get("offset"), 10, 64)
	if request.Method == http.MethodPost && strings.HasPrefix(request.Header.Get("Content-Type"), "application/json") {
		raw, err := readBody(request)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		var payload struct {
			Offset int64 `json:"offset"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("decode getUpdates request: %w", err))
			return
		}
		offset = payload.Offset
	}
	server.mu.Lock()
	result := make([]json.RawMessage, 0, len(server.updateOrder))
	for _, updateID := range server.updateOrder {
		if updateID >= offset {
			result = append(result, append(json.RawMessage(nil), server.updates[updateID]...))
		}
	}
	server.mu.Unlock()
	writeTelegramResult(w, result)
}

func (server *Server) captureSend(w http.ResponseWriter, request *http.Request, method string) {
	raw, err := readRequestPayload(request)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
		return
	}
	chatID, err := numericID(payload["chat_id"])
	if err != nil || chatID == 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("chat_id must be a non-zero integer"))
		return
	}

	server.mu.Lock()
	server.nextMessageID++
	messageID := server.nextMessageID
	capture := Capture{
		MessageID:  messageID,
		Method:     method,
		ChatID:     chatID,
		Request:    append(json.RawMessage(nil), raw...),
		CapturedAt: server.now().UTC(),
	}
	server.captures = append(server.captures, capture)
	server.mu.Unlock()

	server.logger.Info("captured Telegram Bot API call", "method", method, "chat_id", chatID, "message_id", messageID)
	writeTelegramResult(w, map[string]any{
		"message_id": messageID,
		"date":       capture.CapturedAt.Unix(),
		"chat":       map[string]any{"id": chatID, "type": "private"},
	})
}

func readRequestPayload(request *http.Request) ([]byte, error) {
	contentType := request.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "application/json") {
		return readBody(request)
	}
	var err error
	if strings.HasPrefix(contentType, "multipart/form-data") {
		err = request.ParseMultipartForm(maxRequestBytes)
	} else {
		err = request.ParseForm()
	}
	if err != nil {
		return nil, fmt.Errorf("parse form: %w", err)
	}
	payload := make(map[string]any, len(request.Form))
	for key := range request.Form {
		payload[key] = request.Form.Get(key)
	}
	return json.Marshal(payload)
}

func readBody(request *http.Request) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(request.Body, maxRequestBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read request: %w", err)
	}
	if len(data) > maxRequestBytes {
		return nil, fmt.Errorf("request exceeds %d bytes", maxRequestBytes)
	}
	if !json.Valid(data) {
		return nil, fmt.Errorf("request body must be valid JSON")
	}
	return data, nil
}

func numericID(value any) (int64, error) {
	switch typed := value.(type) {
	case float64:
		return int64(typed), nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported numeric value")
	}
}

func writeTelegramResult(w http.ResponseWriter, result any) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": result})
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{
		"ok":          false,
		"error_code":  status,
		"description": err.Error(),
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
