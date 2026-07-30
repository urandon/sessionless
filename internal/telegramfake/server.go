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

type storedFile struct {
	name      string
	mediaType string
	data      []byte
}

type Server struct {
	token  string
	logger *slog.Logger
	now    func() time.Time

	mu            sync.Mutex
	updates       map[int64]json.RawMessage
	updateOrder   []int64
	files         map[string]storedFile
	captures      []Capture
	nextMessageID int64
	failures      map[string]failurePlan
}

type failurePlan struct {
	Remaining int
	Status    int
}

func NewHandler(token string, logger *slog.Logger) http.Handler {
	server := &Server{
		token:         token,
		logger:        logger,
		now:           time.Now,
		updates:       make(map[int64]json.RawMessage),
		files:         make(map[string]storedFile),
		failures:      make(map[string]failurePlan),
		nextMessageID: 1000,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.health)
	mux.HandleFunc("POST /test/updates", server.addUpdate)
	mux.HandleFunc("POST /test/files/{fileID}", server.addFile)
	mux.HandleFunc("GET /test/captures", server.listCaptures)
	mux.HandleFunc("POST /test/failures", server.setFailure)
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
	server.files = make(map[string]storedFile)
	server.captures = nil
	server.failures = make(map[string]failurePlan)
	server.nextMessageID = 1000
	server.mu.Unlock()
	writeTelegramResult(w, true)
}

func (server *Server) setFailure(w http.ResponseWriter, request *http.Request) {
	raw, err := readBody(request)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var plan struct {
		Method string `json:"method"`
		Count  int    `json:"count"`
		Status int    `json:"status"`
	}
	if err := json.Unmarshal(raw, &plan); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode failure plan: %w", err))
		return
	}
	switch plan.Method {
	case "sendMessage", "sendDocument", "sendPhoto":
	default:
		writeError(w, http.StatusBadRequest, fmt.Errorf("unsupported failure method"))
		return
	}
	if plan.Count < 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("failure count must not be negative"))
		return
	}
	if plan.Status == 0 {
		plan.Status = http.StatusTooManyRequests
	}
	if plan.Status < 400 || plan.Status > 599 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("failure status must be 4xx or 5xx"))
		return
	}
	server.mu.Lock()
	if plan.Count == 0 {
		delete(server.failures, plan.Method)
	} else {
		server.failures[plan.Method] = failurePlan{
			Remaining: plan.Count,
			Status:    plan.Status,
		}
	}
	server.mu.Unlock()
	writeTelegramResult(w, true)
}

func (server *Server) botAPI(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost && request.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	filePrefix := "/file/bot" + server.token + "/"
	if strings.HasPrefix(request.URL.Path, filePrefix) {
		server.downloadFile(w, strings.TrimPrefix(request.URL.Path, filePrefix))
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
	case "getFile":
		server.getFile(w, request)
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

func (server *Server) addFile(w http.ResponseWriter, request *http.Request) {
	fileID := request.PathValue("fileID")
	if fileID == "" || strings.Contains(fileID, "/") {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid file ID"))
		return
	}
	data, err := io.ReadAll(io.LimitReader(request.Body, maxRequestBytes+1))
	if err != nil || len(data) > maxRequestBytes {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid file body"))
		return
	}
	name := request.URL.Query().Get("name")
	if name == "" {
		name = "attachment.bin"
	}
	mediaType := request.Header.Get("Content-Type")
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}
	server.mu.Lock()
	server.files[fileID] = storedFile{
		name: name, mediaType: mediaType, data: append([]byte(nil), data...),
	}
	server.mu.Unlock()
	writeTelegramResult(w, true)
}

func (server *Server) getFile(w http.ResponseWriter, request *http.Request) {
	var payload struct {
		FileID string `json:"file_id"`
	}
	if request.Method == http.MethodPost &&
		strings.HasPrefix(request.Header.Get("Content-Type"), "application/json") {
		raw, err := readBody(request)
		if err != nil || json.Unmarshal(raw, &payload) != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid getFile request"))
			return
		}
	} else {
		payload.FileID = request.URL.Query().Get("file_id")
	}
	server.mu.Lock()
	file, ok := server.files[payload.FileID]
	server.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"ok": false, "error_code": 400, "description": "file not found",
		})
		return
	}
	writeTelegramResult(w, map[string]any{
		"file_id": payload.FileID, "file_size": len(file.data),
		"file_path": "files/" + payload.FileID + "/" + file.name,
	})
}

func (server *Server) downloadFile(w http.ResponseWriter, filePath string) {
	parts := strings.SplitN(filePath, "/", 3)
	if len(parts) != 3 || parts[0] != "files" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	server.mu.Lock()
	file, ok := server.files[parts[1]]
	server.mu.Unlock()
	if !ok || file.name != parts[2] {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", file.mediaType)
	w.Header().Set("Content-Length", strconv.Itoa(len(file.data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(file.data)
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
	server.mu.Lock()
	plan := server.failures[method]
	if plan.Remaining > 0 {
		plan.Remaining--
		if plan.Remaining == 0 {
			delete(server.failures, method)
		} else {
			server.failures[method] = plan
		}
		server.mu.Unlock()
		writeJSON(w, plan.Status, map[string]any{
			"ok":          false,
			"error_code":  plan.Status,
			"description": "Injected deterministic Telegram failure",
		})
		return
	}
	server.mu.Unlock()

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
