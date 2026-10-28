package main

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func installationTestEnvironment() map[string]string {
	return map[string]string{
		"SESSIONLESS_ENVIRONMENT": "local", "YDB_CONNECTION_STRING": "grpc://local-database.invalid:2136/local",
		"WEB_LOCAL_INSTALLATION_ROOT": "/private/operator/installation", "WEB_LOCAL_WORKER_EXECUTABLE": "/private/operator/worker",
		"WEB_LOCAL_PROFILE_RECEIPT_FILE": "/private/operator/receipt.json",
	}
}

func TestLocalProfileInstallExplicitConfiguration(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"valid", "args", "nonlocal", "missing environment", "missing root", "missing executable", "missing receipt", "missing database", "relative root", "unclean executable", "unclean receipt", "root directory", "padded database", "attached true", "attached malformed", "attached pins", "alias only"} {
		t.Run(name, func(t *testing.T) {
			values := installationTestEnvironment()
			var args []string
			switch name {
			case "args":
				args = []string{"--confirm=private-canary"}
			case "nonlocal":
				values["SESSIONLESS_ENVIRONMENT"] = "cloud-dev"
			case "missing environment":
				delete(values, "SESSIONLESS_ENVIRONMENT")
			case "missing root":
				delete(values, "WEB_LOCAL_INSTALLATION_ROOT")
			case "missing executable":
				delete(values, "WEB_LOCAL_WORKER_EXECUTABLE")
			case "missing receipt":
				delete(values, "WEB_LOCAL_PROFILE_RECEIPT_FILE")
			case "missing database":
				delete(values, "YDB_CONNECTION_STRING")
			case "relative root":
				values["WEB_LOCAL_INSTALLATION_ROOT"] = "private-canary"
			case "unclean executable":
				values["WEB_LOCAL_WORKER_EXECUTABLE"] = "/private/operator/../worker"
			case "unclean receipt":
				values["WEB_LOCAL_PROFILE_RECEIPT_FILE"] = "/private//operator/receipt.json"
			case "root directory":
				values["WEB_LOCAL_INSTALLATION_ROOT"] = "/"
			case "padded database":
				values["YDB_CONNECTION_STRING"] += " "
			case "attached true":
				values["WEB_ATTACHED_EXECUTION_ENABLED"] = "true"
			case "attached malformed":
				values["WEB_ATTACHED_EXECUTION_ENABLED"] = "FALSE"
			case "attached pins":
				values["WEB_ATTACHED_RESOURCE_PINS"] = "private-canary"
			case "alias only":
				delete(values, "WEB_LOCAL_PROFILE_RECEIPT_FILE")
				values["LOCAL_PROFILE_RECEIPT_FILE"] = "/private/operator/receipt.json"
			}
			got, err := installationFromEnvironment(args, func(k string) string { return values[k] })
			if name == "valid" {
				want := installationConfig{root: values["WEB_LOCAL_INSTALLATION_ROOT"], executable: values["WEB_LOCAL_WORKER_EXECUTABLE"], receipt: values["WEB_LOCAL_PROFILE_RECEIPT_FILE"], connection: values["YDB_CONNECTION_STRING"]}
				if err != nil || got != want {
					t.Fatalf("explicit configuration: got=%+v want=%+v err=%v", got, want, err)
				}
			} else if !errors.Is(err, errLocalProfileInstall) || !reflect.DeepEqual(got, installationConfig{}) || strings.Contains(err.Error(), "private-canary") {
				t.Fatalf("%s configuration escaped closed/redacted contract: err=%v", name, err)
			}
		})
	}
	values := installationTestEnvironment()
	values["WEB_ATTACHED_EXECUTION_ENABLED"] = "false"
	if _, err := installationFromEnvironment(nil, func(k string) string { return values[k] }); err != nil {
		t.Fatal("explicit disabled attached mode was rejected")
	}
	if _, err := installationFromEnvironment(nil, nil); !errors.Is(err, errLocalProfileInstall) {
		t.Fatal("nil environment source accepted")
	}
}

type confirmationCountingReader struct {
	reader io.Reader
	bytes  int
	calls  int
}

func (r *confirmationCountingReader) Read(b []byte) (int, error) {
	r.calls++
	n, err := r.reader.Read(b)
	r.bytes += n
	return n, err
}

func TestLocalProfileInstallBoundedExplicitConfirmation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, input string
		accepted    bool
	}{
		{"exact line", installConfirmation + "\n", true},
		{"exact EOF", installConfirmation, true},
		{"empty", "", false}, {"generic yes", "yes\n", false},
		{"case alias", "install local profile\n", false},
		{"prefix", installConfirmation + " private-canary\n", false},
		{"padded", " " + installConfirmation + "\n", false},
		{"overflow", strings.Repeat("x", maxConfirmationBytes+1024), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := &confirmationCountingReader{reader: strings.NewReader(tc.input)}
			err := confirmInstallation(t.Context(), input)
			if (err == nil) != tc.accepted || input.bytes > maxConfirmationBytes+1 || input.calls == 0 {
				t.Fatalf("confirmation %s: accepted=%t want=%t bytes=%d calls=%d err=%v", tc.name, err == nil, tc.accepted, input.bytes, input.calls, err)
			}
			if err != nil && (!errors.Is(err, errLocalProfileInstall) || strings.Contains(err.Error(), "private-canary")) {
				t.Fatal("confirmation error exposed input")
			}
		})
	}
}

type confirmationCancelReader struct{ cancel context.CancelFunc }

func (r confirmationCancelReader) Read(b []byte) (int, error) {
	r.cancel()
	return copy(b, installConfirmation+"\n"), io.EOF
}

func TestLocalProfileInstallConfirmationCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	input := &confirmationCountingReader{reader: strings.NewReader(installConfirmation + "\n")}
	if err := confirmInstallation(ctx, input); !errors.Is(err, errLocalProfileInstall) || input.calls != 0 {
		t.Fatal("already-cancelled confirmation consumed input")
	}
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	if err := confirmInstallation(ctx, confirmationCancelReader{cancel}); !errors.Is(err, errLocalProfileInstall) {
		t.Fatal("cancellation during confirmation accepted explicit input")
	}
	if confirmInstallation(nil, strings.NewReader(installConfirmation)) == nil || confirmInstallation(t.Context(), nil) == nil {
		t.Fatal("missing confirmation dependency accepted")
	}
}
