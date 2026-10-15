package attachedworkertransport

import (
	"crypto/tls"
	"errors"
	"net/http"
	"time"
)

var ErrInvalidControlHTTPClient = errors.New("invalid attached-worker control HTTP client")

// NewPrivateControlHTTPClient is the shared metadata-transport policy used by
// Pi phase control and output-upload control. It keeps explicit caller failure,
// header and TLS bounds, never supplies an invocation allowance or authority.
// The owner must validate its fixed HTTPS endpoint and remove Request.GetBody.
// Fresh private H1-only nonpooled connections avoid reused-connection replay;
// redirect/compression/session discovery and ambient proxies are disabled.
// This is not a full native allocation or arbitrary dialer-quiescence proof.
func NewPrivateControlHTTPClient(client *http.Client, hostname string) (*http.Client, *http.Transport, error) {
	if client == nil || client.Jar != nil || client.Timeout <= 0 || client.Timeout > time.Minute {
		return nil, nil, ErrInvalidControlHTTPClient
	}
	t, ok := client.Transport.(*http.Transport)
	if !ok || t == nil || t.Proxy != nil || t.DialContext == nil || t.DialTLS != nil || t.DialTLSContext != nil ||
		t.TLSClientConfig == nil || t.TLSClientConfig.InsecureSkipVerify || t.TLSClientConfig.MinVersion < tls.VersionTLS12 ||
		t.TLSClientConfig.ServerName != "" && t.TLSClientConfig.ServerName != hostname ||
		t.TLSClientConfig.GetClientCertificate != nil || t.TLSClientConfig.VerifyPeerCertificate != nil || t.TLSClientConfig.VerifyConnection != nil ||
		t.TLSClientConfig.GetCertificate != nil || t.TLSClientConfig.GetConfigForClient != nil || len(t.TLSClientConfig.Certificates) != 0 ||
		t.TLSClientConfig.KeyLogWriter != nil || t.TLSClientConfig.Time != nil || t.TLSClientConfig.Rand != nil ||
		t.TLSClientConfig.EncryptedClientHelloRejectionVerify != nil || t.TLSClientConfig.GetEncryptedClientHelloKeys != nil ||
		len(t.TLSClientConfig.EncryptedClientHelloConfigList) != 0 || len(t.TLSClientConfig.EncryptedClientHelloKeys) != 0 ||
		t.TLSClientConfig.MaxVersion != 0 && t.TLSClientConfig.MaxVersion < t.TLSClientConfig.MinVersion ||
		t.TLSHandshakeTimeout <= 0 || t.TLSHandshakeTimeout > client.Timeout ||
		t.ResponseHeaderTimeout <= 0 || t.ResponseHeaderTimeout > client.Timeout || t.MaxResponseHeaderBytes <= 0 ||
		!t.DisableCompression || len(t.TLSNextProto) != 0 || t.GetProxyConnectHeader != nil || len(t.ProxyConnectHeader) != 0 {
		return nil, nil, ErrInvalidControlHTTPClient
	}
	private := t.Clone()
	if private.TLSClientConfig.RootCAs != nil {
		private.TLSClientConfig.RootCAs = private.TLSClientConfig.RootCAs.Clone()
	}
	private.TLSClientConfig.CipherSuites = append([]uint16(nil), private.TLSClientConfig.CipherSuites...)
	private.TLSClientConfig.CurvePreferences = append([]tls.CurveID(nil), private.TLSClientConfig.CurvePreferences...)
	private.TLSClientConfig.ClientSessionCache = nil
	private.TLSClientConfig.SessionTicketsDisabled = true
	private.DisableKeepAlives = true
	private.DisableCompression = true
	private.ForceAttemptHTTP2 = false
	private.Protocols = &http.Protocols{}
	private.Protocols.SetHTTP1(true)
	private.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	private.TLSClientConfig.NextProtos = []string{"http/1.1"}
	copyClient := *client
	copyClient.Transport = private
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copyClient, private, nil
}
