package nativeconnect

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/netip"
	"strings"
	"testing"
)

func TestValidateHosts(t *testing.T) {
	validMax := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	for _, hosts := range [][]string{{"api.example.com"}, {"a.example", "b.example"}, {validMax}, {"1.api.example"}, {"a-b.example"}, {"a.1"}, {"a.a", "b.a", "c.a", "d.a", "e.a", "f.a", "g.a", "h.a"}} {
		if err := ValidateHosts(hosts); err != nil {
			t.Errorf("ValidateHosts(%q) = %v; want nil", hosts, err)
		}
	}
	cases := []struct {
		name  string
		hosts []string
	}{
		{"empty", nil}, {"too many", []string{"a.a", "b.a", "c.a", "d.a", "e.a", "f.a", "g.a", "h.a", "i.a"}},
		{"unsorted", []string{"b.example", "a.example"}}, {"duplicate", []string{"a.example", "a.example"}},
		{"uppercase", []string{"API.example"}}, {"trailing dot", []string{"api.example."}},
		{"IPv4", []string{"8.8.8.8"}}, {"IPv6", []string{"2001:4860:4860::8888"}},
		{"numeric alias", []string{"123.456"}}, {"single label", []string{"localhost"}},
		{"userinfo", []string{"a@api.example"}}, {"port", []string{"api.example:443"}},
		{"underscore", []string{"api_test.example"}}, {"leading hyphen", []string{"-api.example"}},
		{"trailing hyphen", []string{"api-.example"}}, {"empty label", []string{"api..example"}},
		{"unicode", []string{"аpi.example"}}, {"whitespace", []string{"api.example "}},
		{"label too long", []string{strings.Repeat("a", 64) + ".example"}},
		{"name too long", []string{validMax + "a"}}, {"escape", []string{"api%2eexample"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateHosts(tc.hosts); !errors.Is(err, ErrInvalid) {
				t.Errorf("ValidateHosts(%q) = %v; want ErrInvalid", tc.hosts, err)
			}
		})
	}
}

func TestReadCONNECTPreservesTunnel(t *testing.T) {
	const header = "CONNECT api.example:443 HTTP/1.1\r\nHost: api.example:443\r\nUser-Agent: native-client/1.0\r\nProxy-Connection: keep-alive\r\n\r\n"
	tunnel := []byte{0x16, 0x03, 0x01, 0, 6, '\r', '\n', 0, 255, 0x17, 1}
	for _, size := range []int{16, 64, 4096} {
		t.Run(stringName(size), func(t *testing.T) {
			wire := append([]byte(header), tunnel...)
			r := bufio.NewReaderSize(bytes.NewReader(wire), size)
			if _, err := r.Peek(1); err != nil {
				t.Fatalf("prefetch header: %v", err)
			}
			host, err := ReadCONNECT(r, []string{"api.example"})
			if err != nil || host != "api.example" {
				t.Fatalf("ReadCONNECT = (%q, %v); want (api.example, nil)", host, err)
			}
			got, err := io.ReadAll(r)
			if err != nil || !bytes.Equal(got, tunnel) {
				t.Fatalf("remaining tunnel = %x, %v; want %x exactly once", got, err, tunnel)
			}
			if _, err := r.ReadByte(); err != io.EOF {
				t.Errorf("after tunnel: %v; want EOF", err)
			}
		})
	}
}

func stringName(size int) string {
	switch size {
	case 16:
		return "small reader"
	case 64:
		return "split header"
	default:
		return "prefetched tunnel"
	}
}

func TestReadCONNECTRejectsMalformed(t *testing.T) {
	const start = "CONNECT api.example:443 HTTP/1.1\r\n"
	const host = "Host: api.example:443\r\n"
	cases := map[string]string{
		"empty": "", "nil terminator": start + host,
		"LF request":         "CONNECT api.example:443 HTTP/1.1\n" + host + "\r\n",
		"LF header":          start + "Host: api.example:443\n\n",
		"bare CR":            start + "Host: api.example:443\rX\r\n\r\n",
		"bare LF terminator": start + host + "\n",
		"method case":        "connect api.example:443 HTTP/1.1\r\n" + host + "\r\n",
		"other method":       "GET api.example:443 HTTP/1.1\r\n" + host + "\r\n",
		"HTTP version":       "CONNECT api.example:443 HTTP/1.0\r\n" + host + "\r\n",
		"extra space":        "CONNECT  api.example:443 HTTP/1.1\r\n" + host + "\r\n",
		"extra token":        "CONNECT api.example:443 HTTP/1.1 extra\r\n" + host + "\r\n",
		"absolute URI":       "CONNECT https://api.example:443 HTTP/1.1\r\n" + host + "\r\n",
		"other host":         "CONNECT other.example:443 HTTP/1.1\r\n" + host + "\r\n",
		"other port":         "CONNECT api.example:80 HTTP/1.1\r\n" + host + "\r\n",
		"leading zero port":  "CONNECT api.example:0443 HTTP/1.1\r\n" + host + "\r\n",
		"missing port":       "CONNECT api.example HTTP/1.1\r\n" + host + "\r\n",
		"trailing dot":       "CONNECT api.example.:443 HTTP/1.1\r\n" + host + "\r\n",
		"missing host":       start + "\r\n", "duplicate host": start + host + host + "\r\n",
		"casefold duplicate":  start + host + "hOsT: api.example:443\r\n\r\n",
		"host mismatch":       start + "Host: other.example:443\r\n\r\n",
		"host missing port":   start + "Host: api.example\r\n\r\n",
		"host OWS":            start + "Host:  api.example:443\r\n\r\n",
		"host trailing space": start + "Host: api.example:443 \r\n\r\n",
		"header whitespace":   start + "Host : api.example:443\r\n\r\n",
		"tab separator":       start + "Host:\tapi.example:443\r\n\r\n",
		"obs fold":            start + host + " User-Agent: hidden\r\n\r\n",
		"empty useragent":     start + host + "User-Agent: \r\n\r\n",
		"long useragent":      start + host + "User-Agent: " + strings.Repeat("a", 257) + "\r\n\r\n",
		"control useragent":   start + host + "User-Agent: abc\x7fdef\r\n\r\n",
		"duplicate useragent": start + host + "User-Agent: a\r\nuSeR-aGeNt: b\r\n\r\n",
		"proxy close":         start + host + "Proxy-Connection: close\r\n\r\n",
		"duplicate proxy":     start + host + "Proxy-Connection: keep-alive\r\nproxy-connection: keep-alive\r\n\r\n",
		"NUL":                 start + "Ho\x00st: api.example:443\r\n\r\n",
		"long request":        "CONNECT " + strings.Repeat("a", MaxHeaderBytes) + "\r\n\r\n",
		"long header":         start + "Host: " + strings.Repeat("a", MaxHeaderBytes) + "\r\n\r\n",
	}
	for _, name := range []string{"Content-Length", "Transfer-Encoding", "Authorization", "Proxy-Authorization", "Connection", "Upgrade", "Trailer", "Expect", "X-Unknown"} {
		cases["forbidden "+name] = start + host + name + ": 0\r\n\r\nbody"
	}
	for name, wire := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := ReadCONNECT(bufio.NewReader(strings.NewReader(wire)), []string{"api.example"})
			if got != "" || !errors.Is(err, ErrInvalid) {
				t.Errorf("ReadCONNECT malformed %q = (%q, %v); want empty, ErrInvalid", name, got, err)
			}
		})
	}
}

func TestReadCONNECTLimitsAndConfiguration(t *testing.T) {
	const start = "CONNECT api.example:443 HTTP/1.1\r\nHost: api.example:443\r\n"
	for _, agent := range []string{"x", strings.Repeat("~", 256)} {
		host, err := ReadCONNECT(bufio.NewReader(strings.NewReader(start+"uSeR-aGeNt: "+agent+"\r\n\r\n")), []string{"api.example"})
		if err != nil || host != "api.example" {
			t.Errorf("User-Agent length %d: host=%q, err=%v", len(agent), host, err)
		}
	}
	for _, allowed := range [][]string{nil, {"API.example"}, {"api.example", "api.example"}} {
		r := bufio.NewReader(strings.NewReader(start + "\r\n"))
		if _, err := ReadCONNECT(r, allowed); !errors.Is(err, ErrInvalid) {
			t.Errorf("invalid allowlist %q: %v; want ErrInvalid", allowed, err)
		}
		if first, _ := r.ReadByte(); first != 'C' {
			t.Errorf("invalid configuration consumed input; first byte=%q", first)
		}
	}
	if _, err := ReadCONNECT(nil, []string{"api.example"}); !errors.Is(err, ErrInvalid) {
		t.Errorf("nil reader: %v; want ErrInvalid", err)
	}
	// Prebuffering must not let a single oversized line consume even one byte
	// beyond the total parser budget, including when that byte is already in r.
	wire := strings.Repeat("A", MaxHeaderBytes) + "!"
	r := bufio.NewReaderSize(strings.NewReader(wire), len(wire))
	if _, err := ReadCONNECT(r, []string{"api.example"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversized input: %v; want ErrInvalid", err)
	}
	if next, err := r.ReadByte(); err != nil || next != '!' {
		t.Errorf("byte after header budget=(%q,%v); want ! untouched", next, err)
	}
}

func TestPinPublicIPv4(t *testing.T) {
	addresses := []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("8.8.8.8")}
	for _, input := range [][]netip.Addr{addresses, {addresses[1], addresses[0]}, {addresses[1]}} {
		got, err := PinPublicIPv4(input)
		if err != nil || got.String() != "1.1.1.1" {
			t.Errorf("PinPublicIPv4(%v)=(%v,%v); want 1.1.1.1,nil", input, got, err)
		}
	}
	// Independent fixtures protect the contract if an implementation prefix is
	// accidentally removed or narrowed. Exercise both ends of every deny range.
	for _, literal := range []string{
		"0.0.0.0", "0.255.255.255", "10.0.0.0", "10.255.255.255",
		"100.64.0.0", "100.127.255.255", "127.0.0.0", "127.255.255.255",
		"169.254.0.0", "169.254.255.255", "172.16.0.0", "172.31.255.255",
		"192.0.0.0", "192.0.0.255", "192.0.2.0", "192.0.2.255",
		"192.31.196.0", "192.31.196.255", "192.52.193.0", "192.52.193.255",
		"192.88.99.0", "192.88.99.255", "192.168.0.0", "192.168.255.255",
		"192.175.48.0", "192.175.48.255", "198.18.0.0", "198.19.255.255",
		"198.51.100.0", "198.51.100.255", "203.0.113.0", "203.0.113.255",
		"224.0.0.0", "239.255.255.255", "240.0.0.0", "255.255.255.255",
	} {
		t.Run(literal, func(t *testing.T) {
			blocked := netip.MustParseAddr(literal)
			for _, input := range [][]netip.Addr{{blocked}, {addresses[0], blocked}, {blocked, addresses[0]}} {
				got, err := PinPublicIPv4(input)
				if got.IsValid() || !errors.Is(err, ErrInvalid) {
					t.Errorf("PinPublicIPv4(%v)=(%v,%v); want invalid,ErrInvalid", input, got, err)
				}
			}
		})
	}
	for _, literal := range []string{"::1", "2001:4860:4860::8888", "::ffff:8.8.8.8", "fe80::1%en0", "255.255.255.255"} {
		input := []netip.Addr{addresses[0], netip.MustParseAddr(literal)}
		if got, err := PinPublicIPv4(input); got.IsValid() || !errors.Is(err, ErrInvalid) {
			t.Errorf("mixed %q=(%v,%v); want invalid,ErrInvalid", literal, got, err)
		}
	}
	for _, input := range [][]netip.Addr{nil, {netip.Addr{}}, make([]netip.Addr, 65)} {
		if got, err := PinPublicIPv4(input); got.IsValid() || !errors.Is(err, ErrInvalid) {
			t.Errorf("invalid DNS set length %d=(%v,%v); want invalid,ErrInvalid", len(input), got, err)
		}
	}
	max := make([]netip.Addr, 64)
	for i := range max {
		max[i] = addresses[0]
	}
	if got, err := PinPublicIPv4(max); err != nil || got != addresses[0] {
		t.Errorf("64 permitted addresses=(%v,%v); want %v,nil", got, err, addresses[0])
	}
}

func FuzzReadCONNECT(f *testing.F) {
	for _, seed := range []string{
		"CONNECT api.example:443 HTTP/1.1\r\nHost: api.example:443\r\n\r\n",
		"CONNECT api.example:443 HTTP/1.1\nHost: api.example:443\n\n",
		"CONNECT api.example:443 HTTP/1.1\r\nHost: api.example:443\r\nhost: api.example:443\r\n\r\n",
		"CONNECT api.example:443 HTTP/1.1\r\nHost: api.example:443\r\nContent-Length: 1\r\n\r\nx",
		strings.Repeat("x", MaxHeaderBytes+1),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, wire string) {
		if len(wire) > MaxHeaderBytes+512 {
			return
		}
		r := bufio.NewReader(strings.NewReader(wire))
		host, err := ReadCONNECT(r, []string{"api.example"})
		if err != nil {
			if host != "" || !errors.Is(err, ErrInvalid) {
				t.Fatalf("failure=(%q,%v); want empty,ErrInvalid", host, err)
			}
			return
		}
		end := strings.Index(wire, "\r\n\r\n") + 4
		if host != "api.example" || end < 4 || end > MaxHeaderBytes {
			t.Fatalf("accepted host=%q header end=%d", host, end)
		}
		remaining, readErr := io.ReadAll(r)
		if readErr != nil || string(remaining) != wire[end:] {
			t.Fatalf("tunnel changed: got %x err=%v; want %x", remaining, readErr, wire[end:])
		}
	})
}
