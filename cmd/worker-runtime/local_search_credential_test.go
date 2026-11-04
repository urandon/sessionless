package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func TestWorkerLocalSearchKeySelectionIsExplicitAndNonsecret(t *testing.T) {
	for _, variant := range []string{"off", "off-stray-key", "off-stray-epoch", "enabled-omitted", "enabled-pair", "key-only", "epoch-only", "relative", "unclean", "zero-byte", "bad-epoch"} {
		t.Run(variant, func(t *testing.T) {
			values := map[string]string{}
			valid := variant == "off" || variant == "enabled-omitted" || variant == "enabled-pair"
			if !strings.HasPrefix(variant, "off") {
				values = map[string]string{"SESSIONLESS_ENVIRONMENT": "local", "WORKER_LOCAL_EXECUTION_ENABLED": "true", "WEB_LOCAL_INSTALLATION_ROOT": "/operator", "WEB_LOCAL_WORKER_EXECUTABLE": "/operator/worker", "WEB_LOCAL_PROFILE_RECEIPT_FILE": "/operator/receipt"}
			}
			if variant != "off" && variant != "enabled-omitted" {
				values["WORKER_SEARCH_CREDENTIAL_KEY_FILE"], values["WORKER_SEARCH_CREDENTIAL_KEY_EPOCH"] = "/operator/private-key", "epoch-1"
			}
			switch variant {
			case "key-only", "off-stray-key":
				delete(values, "WORKER_SEARCH_CREDENTIAL_KEY_EPOCH")
			case "epoch-only", "off-stray-epoch":
				delete(values, "WORKER_SEARCH_CREDENTIAL_KEY_FILE")
			case "relative":
				values["WORKER_SEARCH_CREDENTIAL_KEY_FILE"] = "private-key"
			case "unclean":
				values["WORKER_SEARCH_CREDENTIAL_KEY_FILE"] = "/operator/../private-key"
			case "zero-byte":
				values["WORKER_SEARCH_CREDENTIAL_KEY_FILE"] = "/operator/private\x00key"
			case "bad-epoch":
				values["WORKER_SEARCH_CREDENTIAL_KEY_EPOCH"] = "ALIAS/private-canary"
			}
			configuration, err := workerLocalExecutionFromEnv(func(key string) string { return values[key] })
			if valid {
				if err != nil || configuration.searchKeyFile != values["WORKER_SEARCH_CREDENTIAL_KEY_FILE"] || configuration.searchKeyEpoch != values["WORKER_SEARCH_CREDENTIAL_KEY_EPOCH"] {
					t.Fatal("explicit nonsecret configuration rejected/substituted", err)
				}
			} else if err != errWorkerLocalExecution || configuration != (workerLocalExecutionConfig{}) {
				t.Fatal("invalid/stray credential configuration accepted", err)
			}
		})
	}
}

func TestWorkerLocalSearchDisabledConstructionDoesNotTouchDependencies(t *testing.T) {
	owner, err := newWorkerLocalSearchCredentials(t.Context(), workerLocalExecutionConfig{}, nil, nil, nil)
	if err != nil || owner != nil {
		t.Fatal("default-off construction needs a dependency", err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal("absent optional process owner is not inert", err)
	}
	for _, c := range []workerLocalExecutionConfig{{enabled: true}, {searchKeyFile: "/private-canary"}, {searchKeyEpoch: "epoch-1"}} {
		if owner, err := newWorkerLocalSearchCredentials(t.Context(), c, nil, nil, nil); err != errWorkerLocalExecution || owner != nil {
			t.Fatal("missing original Source became credential absence/readiness", err)
		}
	}
}

func TestWorkerLocalSearchAssemblyClearsInputOnEveryConstructorOutcome(t *testing.T) {
	// Actual Store/SDK constructors, deliberately inert: the zero DB cannot
	// execute SQL and explicit synthetic S3 credentials avoid ambient discovery.
	// No request, candidate, grant, credential handle or provider send is issued.
	root := t.TempDir()
	for _, variable := range []string{"AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE"} {
		path := filepath.Join(root, variable)
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv(variable, path)
	}
	for variable, value := range map[string]string{
		"AWS_ACCESS_KEY_ID": "synthetic-test", "AWS_SECRET_ACCESS_KEY": "synthetic-test",
		"AWS_SESSION_TOKEN": "", "AWS_PROFILE": "", "AWS_DEFAULT_PROFILE": "",
		"AWS_WEB_IDENTITY_TOKEN_FILE": "", "AWS_ROLE_ARN": "", "AWS_EC2_METADATA_DISABLED": "true",
	} {
		t.Setenv(variable, value)
	}
	bindings, err := ydbstore.New(new(sql.DB), ydbstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	objects, err := s3store.New(t.Context(), s3store.Config{Endpoint: "https://storage.invalid", Region: "synthetic", Bucket: "synthetic-private", AccessKeyID: "synthetic-test", SecretAccessKey: "synthetic-test"})
	if err != nil {
		t.Fatal("inert SDK constructor prerequisite", err)
	}
	for _, outcome := range []string{"success", "bad-object-constructor", "bad-epoch", "nil-context", "cancelled", "short-key", "zero-key"} {
		t.Run(outcome, func(t *testing.T) {
			key := bytes.Repeat([]byte{0x31}, 32)
			t.Cleanup(func() { clear(key) })
			selected, epoch, ctx := objects, "epoch-1", t.Context()
			switch outcome {
			case "bad-object-constructor":
				selected = new(s3store.Store)
			case "bad-epoch":
				epoch = "not/accepted"
			case "nil-context":
				ctx = nil
			case "cancelled":
				call, cancel := context.WithCancel(ctx)
				cancel()
				ctx = call
			case "short-key":
				clear(key)
				key = bytes.Repeat([]byte{0x31}, 31)
			case "zero-key":
				clear(key)
			}
			owner, err := assembleWorkerLocalSearchCredentials(ctx, epoch, bindings, selected, key)
			if owner != nil {
				t.Cleanup(func() {
					cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					if err := owner.close(cleanup); err != nil {
						t.Error("owned inert adapter cleanup", err)
					}
				})
			}
			if !bytes.Equal(key, make([]byte, len(key))) {
				t.Fatal("caller-owned synthetic root key survived constructor outcome")
			}
			if outcome != "success" {
				if owner != nil || err != errWorkerLocalExecution {
					t.Fatal("invalid backend construction accepted", err)
				}
				return
			}
			if err != nil || owner == nil || owner.secrets == nil || owner.service.ValidateConstructor() != nil {
				t.Fatal("actual encrypted adapter/service constructor failed", err)
			}
			closeCtx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := owner.close(closeCtx); err != nil {
				t.Fatal("idle original encrypted adapter did not join", err)
			}
		})
	}
	if template, err := (localSearchClosedModelPlanner{}).PlanProviderCredentialDelivery(t.Context(), domain.HarnessBindingV2{}); err != errWorkerLocalExecution || !reflect.DeepEqual(template, ports.ProviderCredentialDeliveryTemplateV1{}) {
		t.Fatal("SEARCH-only assembly fabricated a MODEL delivery template", err)
	}
}

type workerCloseOrder struct {
	order *[]string
	err   error
}

func (o workerCloseOrder) close(context.Context) error {
	*o.order = append(*o.order, "search")
	return o.err
}

type workerSourceCloseOrder struct{ order *[]string }

func (o workerSourceCloseOrder) close() error {
	*o.order = append(*o.order, "source")
	return nil
}

type workerDBCloseOrder struct{ order *[]string }

func (o workerDBCloseOrder) Close(context.Context) error {
	*o.order = append(*o.order, "database")
	return nil
}

func TestWorkerProcessDependenciesRetainOwnersOnSearchJoinFailure(t *testing.T) {
	for _, failed := range []bool{false, true} {
		order := []string{}
		var failure error
		if failed {
			failure = errors.New("synthetic join incomplete")
		}
		err := closeWorkerProcessDependencies(t.Context(), workerCloseOrder{&order, failure}, workerSourceCloseOrder{&order}, workerDBCloseOrder{&order})
		want := []string{"search", "source", "database"}
		if failed {
			want = []string{"search"}
		}
		if !errors.Is(err, failure) || !reflect.DeepEqual(order, want) {
			t.Fatal("close ordering or failed-join retention violated", order, err)
		}
	}
}
