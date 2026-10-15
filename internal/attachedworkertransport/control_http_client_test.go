package attachedworkertransport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestPrivateControlHTTPClientOwnsExplicitConfiguration(t *testing.T) {
	roots := x509.NewCertPool()
	transport := &http.Transport{
		DialContext: (&net.Dialer{Timeout: time.Second}).DialContext,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots,
			CipherSuites: []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}, CurvePreferences: []tls.CurveID{tls.X25519},
			NextProtos: []string{"h2"}, ClientSessionCache: tls.NewLRUClientSessionCache(1)},
		DisableCompression: true, ForceAttemptHTTP2: true, TLSHandshakeTimeout: time.Second,
		ResponseHeaderTimeout: time.Second, MaxResponseHeaderBytes: 4096,
	}
	input := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	client, private, err := NewPrivateControlHTTPClient(input, "control.example")
	if err != nil {
		t.Fatal("explicit transport rejected:", err)
	}
	t.Cleanup(private.CloseIdleConnections)
	if client == input || private == transport || private.TLSClientConfig == transport.TLSClientConfig ||
		private.TLSClientConfig.RootCAs == roots || client.Transport != private {
		t.Fatal("configuration aliases original owner")
	}
	transport.TLSClientConfig.CipherSuites[0] = 0
	transport.TLSClientConfig.CurvePreferences[0] = 0
	transport.TLSClientConfig.MinVersion = tls.VersionTLS13
	input.Timeout = time.Nanosecond
	if private.TLSClientConfig.CipherSuites[0] != tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 ||
		private.TLSClientConfig.CurvePreferences[0] != tls.X25519 || private.TLSClientConfig.MinVersion != tls.VersionTLS12 || client.Timeout != 2*time.Second {
		t.Fatal("caller mutation changed admitted private bounds")
	}
	if private.Proxy != nil || !private.DisableKeepAlives || !private.DisableCompression || private.ForceAttemptHTTP2 ||
		private.Protocols == nil || !private.Protocols.HTTP1() || private.Protocols.HTTP2() || private.Protocols.UnencryptedHTTP2() ||
		len(private.TLSNextProto) != 0 || !private.TLSClientConfig.SessionTicketsDisabled || private.TLSClientConfig.ClientSessionCache != nil ||
		len(private.TLSClientConfig.NextProtos) != 1 || private.TLSClientConfig.NextProtos[0] != "http/1.1" {
		t.Fatal("private H1/nonpooled/no-discovery policy weakened")
	}
	if err := client.CheckRedirect(&http.Request{}, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatal("redirect not stopped:", err)
	}
}

func TestPrivateControlHTTPClientRejectsMissingExplicitConfiguration(t *testing.T) {
	var typedNil *http.Transport
	for _, tc := range []struct {
		name, hostname string
		client         *http.Client
	}{
		{name: "nil", hostname: "control.example"},
		{name: "ambient default", hostname: "control.example", client: &http.Client{Timeout: time.Second}},
		{name: "typed nil", hostname: "control.example", client: &http.Client{Timeout: time.Second, Transport: typedNil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, transport, err := NewPrivateControlHTTPClient(tc.client, tc.hostname)
			if !errors.Is(err, ErrInvalidControlHTTPClient) || client != nil || transport != nil {
				t.Fatalf("missing explicit configuration: client=%v transport=%v err=%v", client != nil, transport != nil, err)
			}
		})
	}
}
