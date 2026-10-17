package localoperator

import (
	"errors"
	"testing"
)

func TestLocalHostIdentityDistinguishesBootsWithSameHostnameAndUID(t *testing.T) {
	a, err := localHostIdentity("same-name", 501, "darwin", "12345678-1234-1234-1234-123456789abc")
	if err != nil {
		t.Fatal(err)
	}
	same, err := localHostIdentity("same-name", 501, "darwin", "12345678-1234-1234-1234-123456789ABC")
	if err != nil || same != a {
		t.Fatal("same original boot cannot resume its exact host identity")
	}
	other, err := localHostIdentity("same-name", 501, "darwin", "12345678-1234-1234-1234-123456789abd")
	if err != nil || other == a {
		t.Fatal("same hostname/UID on a different boot reconstructed original host authority")
	}
	for _, invalid := range []string{"", "hostname", "00000000-0000-0000-0000-000000000000", "123456781234-1234-1234-123456789abc", "12345678-1234-1234-1234-123456789abz"} {
		if _, err := localHostIdentity("same-name", 501, "linux", invalid); !errors.Is(err, ErrDenied) {
			t.Fatal("missing or invalid boot source accepted")
		}
	}
}
