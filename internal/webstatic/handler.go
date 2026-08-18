// Package webstatic serves the embedded SvelteKit static build in front of the
// same-origin Web BFF. API and authentication namespaces always remain owned by
// the backend; only browser document navigations receive the SPA fallback.
package webstatic

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/html"
)

const (
	fallbackDocument = "200.html"
	immutablePrefix  = "_app/immutable/"
)

type Config struct {
	Backend                    http.Handler
	ObjectStorageOrigin        string
	AllowLoopbackObjectStorage bool
	Random                     io.Reader
}

type Handler struct {
	backend             http.Handler
	assets              fs.FS
	fallback            []byte
	connectSources      string
	random              io.Reader
	compressedAvailable map[string]map[string]bool
}

// New constructs a handler from the frontend build embedded at compile time.
// It fails closed when web/build was not staged before compiling web-bff.
func New(config Config) (*Handler, error) {
	assets, err := fs.Sub(embeddedAssets, "dist")
	if err != nil {
		return nil, fmt.Errorf("open embedded Web UI: %w", err)
	}
	return NewFromFS(config, assets)
}

// NewFromFS is exported for tests and for validating a staged frontend build.
func NewFromFS(config Config, assets fs.FS) (*Handler, error) {
	if config.Backend == nil {
		return nil, errors.New("Web UI backend handler is required")
	}
	if assets == nil {
		return nil, errors.New("Web UI asset filesystem is required")
	}
	origin, err := validateOrigin(config.ObjectStorageOrigin, config.AllowLoopbackObjectStorage)
	if err != nil {
		return nil, err
	}
	fallback, err := fs.ReadFile(assets, fallbackDocument)
	if err != nil {
		return nil, fmt.Errorf("read embedded Web UI fallback: %w", err)
	}
	if _, err := transformDocument(fallback, "startup-validation-nonce"); err != nil {
		return nil, fmt.Errorf("validate embedded Web UI fallback: %w", err)
	}
	random := config.Random
	if random == nil {
		random = rand.Reader
	}
	connectSources := "'self'"
	if origin != "" {
		connectSources += " " + origin
	}
	return &Handler{
		backend:             config.Backend,
		assets:              assets,
		fallback:            fallback,
		connectSources:      connectSources,
		random:              random,
		compressedAvailable: indexCompressedAssets(assets),
	}, nil
}

func (handler *Handler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if isBackendPath(request.URL.Path) || (request.Method != http.MethodGet && request.Method != http.MethodHead) {
		handler.backend.ServeHTTP(w, request)
		return
	}

	assetPath, ok := assetName(request.URL.Path)
	if ok && handler.serveAsset(w, request, assetPath) {
		return
	}
	if !acceptsHTMLNavigation(request) {
		handler.backend.ServeHTTP(w, request)
		return
	}
	handler.serveFallback(w, request)
}

func (handler *Handler) serveFallback(w http.ResponseWriter, request *http.Request) {
	setStaticSecurityHeaders(w.Header(), handler.connectSources, "")
	w.Header().Set("Cache-Control", "no-store")
	nonceBytes := make([]byte, 18)
	if _, err := io.ReadFull(handler.random, nonceBytes); err != nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	nonce := base64.RawStdEncoding.EncodeToString(nonceBytes)
	document, err := transformDocument(handler.fallback, nonce)
	if err != nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}

	setStaticSecurityHeaders(w.Header(), handler.connectSources, nonce)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(document)))
	w.WriteHeader(http.StatusOK)
	if request.Method != http.MethodHead {
		_, _ = w.Write(document)
	}
}

func (handler *Handler) serveAsset(w http.ResponseWriter, request *http.Request, name string) bool {
	encoding, servedName := handler.selectEncoding(request.Header.Get("Accept-Encoding"), name)
	contents, err := fs.ReadFile(handler.assets, servedName)
	if err != nil {
		return false
	}

	setStaticSecurityHeaders(w.Header(), handler.connectSources, "")
	if strings.HasPrefix(name, immutablePrefix) {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "no-cache")
	}
	if encoding != "" {
		w.Header().Set("Content-Encoding", encoding)
	}
	if len(handler.compressedAvailable[name]) != 0 {
		// Vary is required for the identity response too; otherwise a shared
		// cache may reuse it for a client which advertised Brotli support.
		w.Header().Set("Vary", "Accept-Encoding")
	}
	contentType := mime.TypeByExtension(path.Ext(name))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	digest := sha256.Sum256(contents)
	w.Header().Set("ETag", `"`+base64.RawURLEncoding.EncodeToString(digest[:])+`"`)
	http.ServeContent(w, request, path.Base(name), time.Time{}, bytes.NewReader(contents))
	return true
}

func (handler *Handler) selectEncoding(acceptEncoding, name string) (string, string) {
	available := handler.compressedAvailable[name]
	bestQuality := 0.0
	selectedEncoding := ""
	selectedExtension := ""
	for _, candidate := range []struct {
		name      string
		extension string
	}{
		{name: "br", extension: ".br"},
		{name: "gzip", extension: ".gz"},
	} {
		quality := encodingQuality(acceptEncoding, candidate.name)
		if available[candidate.name] && quality > bestQuality {
			bestQuality = quality
			selectedEncoding = candidate.name
			selectedExtension = candidate.extension
		}
	}
	if selectedEncoding != "" {
		return selectedEncoding, name + selectedExtension
	}
	return "", name
}

func indexCompressedAssets(assets fs.FS) map[string]map[string]bool {
	result := make(map[string]map[string]bool)
	_ = fs.WalkDir(assets, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		var encoding, original string
		switch {
		case strings.HasSuffix(name, ".br"):
			encoding, original = "br", strings.TrimSuffix(name, ".br")
		case strings.HasSuffix(name, ".gz"):
			encoding, original = "gzip", strings.TrimSuffix(name, ".gz")
		default:
			return nil
		}
		if result[original] == nil {
			result[original] = make(map[string]bool)
		}
		result[original][encoding] = true
		return nil
	})
	return result
}

func assetName(urlPath string) (string, bool) {
	if urlPath == "" || urlPath == "/" || strings.Contains(urlPath, "\\") {
		return "", false
	}
	name := strings.TrimPrefix(urlPath, "/")
	if !fs.ValidPath(name) || path.Clean(name) != name {
		return "", false
	}
	// HTML is always rendered through serveFallback so inline bootstrap code
	// receives a fresh CSP nonce. Never expose the raw fallback document.
	if strings.EqualFold(path.Ext(name), ".html") {
		return "", false
	}
	for _, segment := range strings.Split(name, "/") {
		if strings.HasPrefix(segment, ".") {
			return "", false
		}
	}
	return name, true
}

func isBackendPath(requestPath string) bool {
	for _, prefix := range []string{"/api", "/auth", "/healthz", "/readyz", "/version"} {
		if requestPath == prefix || strings.HasPrefix(requestPath, prefix+"/") {
			return true
		}
	}
	return false
}

func acceptsHTMLNavigation(request *http.Request) bool {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		return false
	}
	if destination := request.Header.Get("Sec-Fetch-Dest"); destination != "" && destination != "document" {
		return false
	}
	for _, mediaRange := range strings.Split(request.Header.Get("Accept"), ",") {
		parts := strings.Split(mediaRange, ";")
		mediaType := strings.TrimSpace(parts[0])
		quality := 1.0
		for _, parameter := range parts[1:] {
			parameter = strings.TrimSpace(parameter)
			if strings.HasPrefix(parameter, "q=") {
				parsed, err := strconv.ParseFloat(strings.TrimPrefix(parameter, "q="), 64)
				if err != nil {
					quality = 0
				} else {
					quality = parsed
				}
			}
		}
		if quality > 0 && (mediaType == "text/html" || mediaType == "application/xhtml+xml") {
			return true
		}
	}
	return false
}

func encodingQuality(header, wanted string) float64 {
	wildcardQuality := 0.0
	wildcardFound := false
	for _, item := range strings.Split(header, ",") {
		parts := strings.Split(item, ";")
		name := strings.TrimSpace(parts[0])
		quality := 1.0
		for _, parameter := range parts[1:] {
			parameter = strings.TrimSpace(parameter)
			if strings.HasPrefix(parameter, "q=") {
				parsed, err := strconv.ParseFloat(strings.TrimPrefix(parameter, "q="), 64)
				if err != nil {
					quality = 0
				} else {
					quality = parsed
				}
			}
		}
		if name == wanted {
			return quality
		}
		if name == "*" {
			wildcardQuality = quality
			wildcardFound = true
		}
	}
	if wildcardFound {
		return wildcardQuality
	}
	return 0
}

func validateOrigin(raw string, allowLoopbackHTTP bool) (string, error) {
	if raw == "" {
		return "", nil
	}
	if raw != strings.TrimSpace(raw) || strings.Contains(raw, "*") {
		return "", errors.New("Web UI object storage origin must be one exact origin")
	}
	origin, err := url.Parse(raw)
	if err != nil || origin.Host == "" || origin.User != nil || origin.Path != "" ||
		origin.RawPath != "" || origin.RawQuery != "" || origin.Fragment != "" {
		return "", errors.New("Web UI object storage origin must contain only scheme and authority")
	}
	if origin.Scheme != "https" {
		host := strings.ToLower(origin.Hostname())
		ip := net.ParseIP(host)
		loopback := host == "localhost" || (ip != nil && ip.IsLoopback())
		if origin.Scheme != "http" || !allowLoopbackHTTP || !loopback {
			return "", errors.New("Web UI object storage origin must use HTTPS outside loopback development")
		}
	}
	if origin.String() != raw {
		return "", errors.New("Web UI object storage origin must be canonical")
	}
	return raw, nil
}

func setStaticSecurityHeaders(headers http.Header, connectSources, nonce string) {
	headers.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
	headers.Set("X-Content-Type-Options", "nosniff")
	headers.Set("Referrer-Policy", "strict-origin-when-cross-origin")
	scriptSources := "'self'"
	styleSources := "'self'"
	if nonce != "" {
		nonceSource := " 'nonce-" + nonce + "'"
		scriptSources += nonceSource
		styleSources += nonceSource
	}
	headers.Set("Content-Security-Policy", strings.Join([]string{
		"default-src 'self'",
		"script-src " + scriptSources,
		"style-src " + styleSources,
		"connect-src " + connectSources,
		"img-src 'self' data: blob:",
		"font-src 'self'",
		"object-src 'none'",
		"base-uri 'none'",
		"form-action 'self'",
		"frame-ancestors 'none'",
	}, "; "))
}

func transformDocument(document []byte, nonce string) ([]byte, error) {
	if nonce == "" {
		return nil, errors.New("CSP nonce is empty")
	}
	tokenizer := html.NewTokenizer(bytes.NewReader(document))
	var output bytes.Buffer
	foundHead := false
	for {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			if errors.Is(tokenizer.Err(), io.EOF) {
				if !foundHead {
					return nil, errors.New("fallback document has no head element")
				}
				return output.Bytes(), nil
			}
			return nil, tokenizer.Err()
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()
			attributes := token.Attr[:0]
			for _, attribute := range token.Attr {
				if attribute.Key == "nonce" && (token.Data == "script" || token.Data == "style") {
					continue
				}
				if attribute.Key == "style" {
					if token.Data == "div" && strings.TrimSpace(attribute.Val) == "display: contents" {
						continue
					}
					return nil, fmt.Errorf("inline style attribute on <%s> is incompatible with the Web UI CSP", token.Data)
				}
				attributes = append(attributes, attribute)
			}
			token.Attr = attributes
			if token.Data == "script" || token.Data == "style" {
				token.Attr = append(token.Attr, html.Attribute{Key: "nonce", Val: nonce})
			}
			output.WriteString(token.String())
			if token.Data == "head" {
				foundHead = true
				output.WriteString(`<meta property="csp-nonce" nonce="`)
				output.WriteString(nonce)
				output.WriteString(`">`)
			}
		default:
			output.Write(tokenizer.Raw())
		}
	}
}
