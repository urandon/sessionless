// Package webauthconfig defines the one deployment-owned identity version
// selection shared by Web serving and retired legacy writer commands.
package webauthconfig

import "errors"

type Runtime struct {
	Version                    string
	SensitiveOperationsEnabled bool
}

// RuntimeFromEnvironment is pure validation, before dependencies are opened.
// A version is a codec/config selection, not proof of an external cutover.
func RuntimeFromEnvironment(lookup func(string) (string, bool)) (Runtime, error) {
	if lookup == nil {
		return Runtime{}, errors.New("WEB_IDENTITY_VERSION must explicitly be 1 or 2")
	}
	version, present := lookup("WEB_IDENTITY_VERSION")
	if !present || (version != "1" && version != "2") {
		return Runtime{}, errors.New("WEB_IDENTITY_VERSION must explicitly be 1 or 2")
	}
	sensitive, present := lookup("WEB_IDENTITY_SENSITIVE_OPERATIONS")
	if (version == "2" && !present) || (present && sensitive != "true" && sensitive != "false") || (version == "1" && sensitive == "true") {
		return Runtime{}, errors.New("WEB_IDENTITY_SENSITIVE_OPERATIONS requires exact true or false; true requires version 2")
	}
	return Runtime{Version: version, SensitiveOperationsEnabled: sensitive == "true"}, nil
}
