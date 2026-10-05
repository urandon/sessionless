package attachedworkersession

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
)

// HTTPExchangeFactoryConfig supplies the exact outbound origin and explicit
// TLS trust. The factory never inherits proxy settings from the environment.
type HTTPExchangeFactoryConfig struct {
	BaseURL        string
	RootCAs        *x509.CertPool
	RequestTimeout time.Duration
}

// HTTPExchangeFactory makes a separate, closable exchange client for each
// accepted connection. It never caches a bearer across generations.
type HTTPExchangeFactory struct {
	baseURL        string
	httpClient     *http.Client
	requestTimeout time.Duration
}

func NewHTTPExchangeFactory(config HTTPExchangeFactoryConfig) (*HTTPExchangeFactory, error) {
	var roots *x509.CertPool
	if config.RootCAs != nil {
		roots = config.RootCAs.Clone()
	}
	transport := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{
		MinVersion: tls.VersionTLS12, RootCAs: roots,
	}}
	client := &http.Client{Transport: transport}
	probe, err := attachedworkerhttp.NewClientFromBearerBytes(attachedworkerhttp.ClientBytesConfig{
		BaseURL: config.BaseURL, Bearer: []byte("probe"), HTTPClient: client,
		RequestTimeout: config.RequestTimeout,
	})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, ErrInvalidConfiguration
	}
	_ = probe.Close()
	return &HTTPExchangeFactory{
		baseURL: config.BaseURL, httpClient: client,
		requestTimeout: config.RequestTimeout,
	}, nil
}

func (factory *HTTPExchangeFactory) Open(binding ConnectionBindingV1, bearer []byte) (ExchangePort, error) {
	if factory == nil || !validBinding(binding) {
		return nil, ErrInvalidAuthority
	}
	client, err := attachedworkerhttp.NewClientFromBearerBytes(attachedworkerhttp.ClientBytesConfig{
		BaseURL: factory.baseURL, Bearer: bearer, HTTPClient: factory.httpClient,
		RequestTimeout: factory.requestTimeout,
	})
	if err != nil {
		return nil, ErrInvalidConfiguration
	}
	return client, nil
}

var _ ExchangeFactory = (*HTTPExchangeFactory)(nil)
