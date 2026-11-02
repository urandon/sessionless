package localcontrolplane

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func TestControlPlaneLocalSelectionIsExplicit(t *testing.T) {
	base := map[string]string{"SESSIONLESS_ENVIRONMENT": "local", "WEB_LOCAL_EXECUTION_ENABLED": "true", "WEB_LOCAL_INSTALLATION_ROOT": "/private/operator", "WEB_LOCAL_WORKER_EXECUTABLE": "/private/bin/worker", "WEB_LOCAL_PROFILE_RECEIPT_FILE": "/private/operator/receipt"}
	for _, tc := range []struct {
		name, key, value string
		valid            bool
	}{
		{name: "local", valid: true},
		{name: "production", key: "SESSIONLESS_ENVIRONMENT", value: "cloud-dev"},
		{name: "implicit", key: "WEB_LOCAL_EXECUTION_ENABLED", value: ""},
		{name: "disabled stray paths", key: "WEB_LOCAL_EXECUTION_ENABLED", value: "false"},
		{name: "unknown flag", key: "WEB_LOCAL_EXECUTION_ENABLED", value: "yes"},
		{name: "attached", key: "WEB_ATTACHED_EXECUTION_ENABLED", value: "true"},
		{name: "unknown attached", key: "WEB_ATTACHED_EXECUTION_ENABLED", value: "yes"},
		{name: "attached pins", key: "WEB_ATTACHED_RESOURCE_PINS", value: "private-unlogged-config"},
		{name: "missing root", key: "WEB_LOCAL_INSTALLATION_ROOT", value: ""},
		{name: "relative executable", key: "WEB_LOCAL_WORKER_EXECUTABLE", value: "bin/worker"},
		{name: "noncanonical receipt", key: "WEB_LOCAL_PROFILE_RECEIPT_FILE", value: "/private/operator/../receipt"},
		{name: "broad root", key: "WEB_LOCAL_INSTALLATION_ROOT", value: "/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			getenv := func(k string) string {
				if k == tc.key {
					return tc.value
				}
				return base[k]
			}
			c, err := ConfigFromEnv(getenv)
			if tc.valid {
				if err != nil || !c.Enabled {
					t.Fatal("explicit local selection denied", err)
				}
			} else if !errors.Is(err, ErrUnavailable) || !reflect.DeepEqual(c, Config{}) {
				t.Fatal("invalid selection admitted or exposed config", c.Enabled, err)
			}
		})
	}
	if _, err := ConfigFromEnv(nil); !errors.Is(err, ErrUnavailable) {
		t.Fatal("nil reader accepted")
	}
}

func TestControlPlaneDisabledConstructionDoesNotTouchFilesOrDB(t *testing.T) {
	c, err := ConfigFromEnv(func(string) string { return "" })
	if err != nil || c.Enabled {
		t.Fatal("default changed", err)
	}
	i, err := Open(context.Background(), c)
	if err != nil || i != nil || i.Close() != nil || !reflect.DeepEqual(i.StoreOptions(), ydbstore.Options{}) {
		t.Fatal("disabled construction changed", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if i, err := Open(ctx, Config{Enabled: true}); !errors.Is(err, ErrUnavailable) || i != nil {
		t.Fatal("cancelled startup accepted", err)
	}
}
