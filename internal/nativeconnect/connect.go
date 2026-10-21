// Package nativeconnect parses the deliberately narrow native harness CONNECT
// boundary. It neither resolves names nor opens network connections.
package nativeconnect

import (
	"bufio"
	"errors"
	"net/netip"
	"sort"
	"strings"
)

const MaxHeaderBytes = 8192

var ErrInvalid = errors.New("invalid native CONNECT boundary")

// ValidateHosts accepts an explicit sorted set of canonical DNS names. Single
// label aliases, numeric-only names and all alternate authority forms fail shut.
func ValidateHosts(hosts []string) error {
	if len(hosts) == 0 || len(hosts) > 8 {
		return ErrInvalid
	}
	for i, host := range hosts {
		if !validHost(host) || (i > 0 && hosts[i-1] >= host) {
			return ErrInvalid
		}
	}
	return nil
}

func validHost(host string) bool {
	if len(host) > 253 || !strings.Contains(host, ".") {
		return false
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return false
	}
	letter := false
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, ch := range []byte(label) {
			if ch >= 'a' && ch <= 'z' {
				letter = true
			} else if !(ch >= '0' && ch <= '9') && ch != '-' {
				return false
			}
		}
	}
	return letter
}

// ReadCONNECT consumes only the bounded header, leaving any prefetched tunnel
// bytes in r. The caller must continue reading this same reader, not its source.
// The caller also owns the read deadline: a byte bound is not a time bound.
func ReadCONNECT(r *bufio.Reader, allowed []string) (string, error) {
	if r == nil || ValidateHosts(allowed) != nil {
		return "", ErrInvalid
	}
	remaining := MaxHeaderBytes
	line, err := readLine(r, &remaining)
	if err != nil || !strings.HasPrefix(line, "CONNECT ") || !strings.HasSuffix(line, " HTTP/1.1") {
		return "", ErrInvalid
	}
	authority := strings.TrimSuffix(strings.TrimPrefix(line, "CONNECT "), " HTTP/1.1")
	if !strings.HasSuffix(authority, ":443") {
		return "", ErrInvalid
	}
	host := strings.TrimSuffix(authority, ":443")
	index := sort.SearchStrings(allowed, host)
	if index == len(allowed) || allowed[index] != host {
		return "", ErrInvalid
	}
	seen := make(map[string]bool, 3)
	for {
		line, err = readLine(r, &remaining)
		if err != nil {
			return "", ErrInvalid
		}
		if line == "" {
			if !seen["host"] {
				return "", ErrInvalid
			}
			return host, nil
		}
		name, value, ok := strings.Cut(line, ": ")
		name = strings.ToLower(name)
		if !ok || seen[name] {
			return "", ErrInvalid
		}
		seen[name] = true
		switch name {
		case "host":
			if value != authority {
				return "", ErrInvalid
			}
		case "user-agent":
			if len(value) == 0 || len(value) > 256 {
				return "", ErrInvalid
			}
			for _, ch := range []byte(value) {
				if ch < ' ' || ch > '~' {
					return "", ErrInvalid
				}
			}
		case "proxy-connection":
			if value != "keep-alive" {
				return "", ErrInvalid
			}
		default:
			return "", ErrInvalid
		}
	}
}

func readLine(r *bufio.Reader, remaining *int) (string, error) {
	line := make([]byte, 0, 128)
	for *remaining > 0 {
		ch, err := r.ReadByte()
		if err != nil {
			return "", ErrInvalid
		}
		*remaining--
		if ch == '\n' {
			return "", ErrInvalid
		}
		if ch == '\r' {
			// Only a CR immediately followed by LF is permitted.
			if *remaining == 0 {
				return "", ErrInvalid
			}
			next, err := r.ReadByte()
			*remaining--
			if err != nil || next != '\n' {
				return "", ErrInvalid
			}
			return string(line), nil
		}
		if ch < ' ' || ch > '~' {
			return "", ErrInvalid
		}
		line = append(line, ch)
	}
	return "", ErrInvalid
}

// PinPublicIPv4 validates the whole DNS result before selecting a deterministic
// literal. One forbidden address poisons the result; no fallback/re-resolution
// may turn a mixed or rebound set into an authorized destination.
func PinPublicIPv4(addresses []netip.Addr) (netip.Addr, error) {
	if len(addresses) == 0 || len(addresses) > 64 {
		return netip.Addr{}, ErrInvalid
	}
	var selected netip.Addr
	for _, address := range addresses {
		if !publicIPv4(address) {
			return netip.Addr{}, ErrInvalid
		}
		if !selected.IsValid() || address.Less(selected) {
			selected = address
		}
	}
	return selected, nil
}

// This is the IPv4 subset of serverlessegress.pinnedPublicAddress's
// conservative special-purpose deny list, not just netip.IsGlobalUnicast.
var deniedIPv4 = [...]netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"), netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.31.196.0/24"), netip.MustParsePrefix("192.52.193.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("192.175.48.0/24"), netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"), netip.MustParsePrefix("240.0.0.0/4"),
}

func publicIPv4(address netip.Addr) bool {
	if !address.IsValid() || !address.Is4() || address.Is4In6() || address.Zone() != "" ||
		!address.IsGlobalUnicast() || address.IsPrivate() {
		return false
	}
	for _, denied := range deniedIPv4 {
		if denied.Contains(address) {
			return false
		}
	}
	return true
}
