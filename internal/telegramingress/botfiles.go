package telegramingress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

const defaultMaxTelegramFileBytes int64 = 64 << 20

type BotFileClient struct {
	baseURL      string
	token        string
	httpClient   *http.Client
	maxFileBytes int64
}

func NewBotFileClient(
	baseURL, token string,
	httpClient *http.Client,
	maxFileBytes int64,
) (*BotFileClient, error) {
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, errors.New("Telegram API base URL must be an absolute HTTP(S) URL")
	}
	if token == "" {
		return nil, errors.New("Telegram bot token must not be empty")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	if maxFileBytes <= 0 {
		maxFileBytes = defaultMaxTelegramFileBytes
	}
	return &BotFileClient{
		baseURL: parsed.String(), token: token,
		httpClient: httpClient, maxFileBytes: maxFileBytes,
	}, nil
}

func (client *BotFileClient) Fetch(ctx context.Context, fileID string) (File, error) {
	if strings.TrimSpace(fileID) == "" {
		return File{}, errors.New("Telegram file_id must not be empty")
	}
	payload, err := json.Marshal(map[string]string{"file_id": fileID})
	if err != nil {
		return File{}, err
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, client.baseURL+"/bot"+client.token+"/getFile",
		strings.NewReader(string(payload)),
	)
	if err != nil {
		return File{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		return File{}, fmt.Errorf("Telegram getFile: %w", err)
	}
	defer response.Body.Close()
	var result struct {
		OK     bool `json:"ok"`
		Result struct {
			FilePath string `json:"file_path"`
			FileSize int64  `json:"file_size"`
		} `json:"result"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
		return File{}, fmt.Errorf("decode Telegram getFile response: %w", err)
	}
	if response.StatusCode != http.StatusOK || !result.OK || result.Result.FilePath == "" {
		return File{}, fmt.Errorf("Telegram getFile failed with status %d: %s",
			response.StatusCode, result.Description)
	}
	if result.Result.FileSize > client.maxFileBytes {
		return File{}, fmt.Errorf("Telegram file exceeds %d bytes", client.maxFileBytes)
	}

	downloadURL := client.baseURL + "/file/bot" + client.token + "/" +
		strings.TrimLeft(result.Result.FilePath, "/")
	download, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return File{}, err
	}
	fileResponse, err := client.httpClient.Do(download)
	if err != nil {
		return File{}, fmt.Errorf("download Telegram file: %w", err)
	}
	if fileResponse.StatusCode != http.StatusOK {
		fileResponse.Body.Close()
		return File{}, fmt.Errorf("Telegram file download failed with status %d", fileResponse.StatusCode)
	}
	body := &limitedReadCloser{
		reader: io.LimitReader(fileResponse.Body, client.maxFileBytes+1),
		closer: fileResponse.Body,
		limit:  client.maxFileBytes,
	}
	return File{
		Name:      path.Base(result.Result.FilePath),
		MediaType: fileResponse.Header.Get("Content-Type"),
		Body:      body,
	}, nil
}

type limitedReadCloser struct {
	reader io.Reader
	closer io.Closer
	limit  int64
	read   int64
}

func (reader *limitedReadCloser) Read(buffer []byte) (int, error) {
	count, err := reader.reader.Read(buffer)
	reader.read += int64(count)
	if reader.read > reader.limit {
		return count, fmt.Errorf("Telegram file exceeds %d bytes", reader.limit)
	}
	return count, err
}

func (reader *limitedReadCloser) Close() error {
	return reader.closer.Close()
}

var _ FileFetcher = (*BotFileClient)(nil)
