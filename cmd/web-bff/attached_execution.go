package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

const maxWebAttachedPinConfigBytes = 64 << 10

// Pins are exact operator-reviewed resource coordinates, never credentials or
// browser-provided routing policy. Enabling this binder cannot authorize a
// provider invocation; normal scheduler/resource gates still decide admission.
func webHarnessBinderFromEnv(getenv func(string) string, store sessionlessharness.AttachedResourceAuthorityStore) (ports.HarnessBinder, error) {
	flag := getenv("WEB_ATTACHED_EXECUTION_ENABLED")
	encoded := getenv("WEB_ATTACHED_RESOURCE_PINS")
	switch flag {
	case "", "false":
		if encoded != "" {
			return nil, fmt.Errorf("WEB_ATTACHED_RESOURCE_PINS requires explicit WEB_ATTACHED_EXECUTION_ENABLED=true")
		}
		return sessionlessharness.NewDeterministicFixtureBinderV1(), nil
	case "true":
	default:
		return nil, fmt.Errorf("WEB_ATTACHED_EXECUTION_ENABLED must be true or false")
	}
	if len(encoded) == 0 || len(encoded) > maxWebAttachedPinConfigBytes {
		return nil, fmt.Errorf("WEB_ATTACHED_RESOURCE_PINS must contain a bounded nonempty JSON array")
	}
	var pins []sessionlessharness.AttachedResourcePin
	decoder := json.NewDecoder(strings.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&pins) != nil || decoder.Decode(new(any)) != io.EOF {
		// Never log the operator configuration or a raw JSON decoding error.
		return nil, fmt.Errorf("WEB_ATTACHED_RESOURCE_PINS is invalid")
	}
	binder, err := sessionlessharness.NewAttachedResourceBinder(sessionlessharness.AttachedResourceBinderConfig{
		Enabled: true, Resources: pins,
	}, store)
	if err != nil {
		return nil, fmt.Errorf("WEB_ATTACHED_RESOURCE_PINS contains invalid attached authority")
	}
	return binder, nil
}
