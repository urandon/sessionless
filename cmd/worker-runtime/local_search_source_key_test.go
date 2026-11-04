package main

import "testing"

func TestWorkerLocalSearchRejectsSourceHMACKeySelection(t *testing.T) {
	values := map[string]string{
		"SESSIONLESS_ENVIRONMENT":            "local",
		"WORKER_LOCAL_EXECUTION_ENABLED":     "true",
		"WEB_LOCAL_INSTALLATION_ROOT":        "/operator/installation",
		"WEB_LOCAL_WORKER_EXECUTABLE":        "/operator/worker",
		"WEB_LOCAL_PROFILE_RECEIPT_FILE":     "/operator/installation/receipt",
		"WORKER_SEARCH_CREDENTIAL_KEY_FILE":  "/operator/dedicated-search-key",
		"WORKER_SEARCH_CREDENTIAL_KEY_EPOCH": "epoch-1",
	}
	// Configuration DATA only. This test opens no file and uses no Source or
	// encryption key. The distinct valid selection is the positive prerequisite.
	getenv := func(key string) string { return values[key] }
	if config, err := workerLocalExecutionFromEnv(getenv); err != nil || config.searchKeyFile != values["WORKER_SEARCH_CREDENTIAL_KEY_FILE"] {
		t.Fatal("distinct explicit key selection prerequisite", err)
	}
	values["WORKER_SEARCH_CREDENTIAL_KEY_FILE"] = values["WEB_LOCAL_INSTALLATION_ROOT"] + "/source.key"
	if config, err := workerLocalExecutionFromEnv(getenv); err != errWorkerLocalExecution || config != (workerLocalExecutionConfig{}) {
		t.Fatal("known Source HMAC key accepted as dedicated encryption key", err)
	}
}

func TestWorkerLocalSearchRejectsReservedSourceKeyCaseAliases(t *testing.T) {
	config := workerLocalExecutionConfig{installation: "/operator/installation", searchKeyEpoch: "epoch-1"}
	for _, path := range []string{"/operator/installation/SOURCE.KEY", "/OPERATOR/INSTALLATION/source.key"} {
		config.searchKeyFile = path
		if config.searchKeySelectionValid() {
			t.Fatal("reserved Source key name became usable on a case-insensitive host")
		}
	}
}
