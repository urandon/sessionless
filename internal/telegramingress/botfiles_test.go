package telegramingress

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestBotFileClientUsesGetFileAndDownload(t *testing.T) {
	t.Parallel()
	var calls int
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		switch calls {
		case 1:
			if request.Method != http.MethodPost ||
				request.URL.Path != "/bottest-token/getFile" {
				t.Fatalf("getFile request = %s %s", request.Method, request.URL.Path)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`{"ok":true,"result":{"file_path":"files/file-42/report.txt","file_size":15}}`,
				)),
			}, nil
		case 2:
			if request.Method != http.MethodGet ||
				request.URL.Path != "/file/bottest-token/files/file-42/report.txt" {
				t.Fatalf("download request = %s %s", request.Method, request.URL.Path)
			}
			header := make(http.Header)
			header.Set("Content-Type", "text/plain")
			return &http.Response{
				StatusCode: http.StatusOK, Header: header,
				Body: io.NopCloser(strings.NewReader("report contents")),
			}, nil
		default:
			t.Fatalf("unexpected HTTP call %d", calls)
			return nil, nil
		}
	})}
	client, err := NewBotFileClient(
		"https://telegram.invalid", "test-token", httpClient, 1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	file, err := client.Fetch(context.Background(), "file-42")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Body.Close()
	data, err := io.ReadAll(file.Body)
	if err != nil {
		t.Fatal(err)
	}
	if file.Name != "report.txt" || file.MediaType != "text/plain" ||
		string(data) != "report contents" || calls != 2 {
		t.Fatalf("fetched file = %#v, %q, calls=%d", file, data, calls)
	}
}
